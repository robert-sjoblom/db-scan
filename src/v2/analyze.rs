use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::instrument;

use crate::{
    pipeline::PipelineContext,
    v2::{
        analyze::{
            checks::{
                check_archive, check_chained_replication, check_disk_errors, check_failover,
                check_lag, check_quorum, check_streaming, check_sync_commit, check_unreachable,
                check_writes_blocked, check_writes_unprotected,
            },
            split_brain::resolve_split_brain,
        },
        cluster::Cluster,
        scan::{AnalyzedNode, Role},
    },
};

/// WAL generation rate in bytes per second (approximately 16MB/s under typical load).
const WAL_GENERATION_RATE_BYTES_PER_SEC: u64 = 16_000_000;
/// Maximum acceptable replication lag in seconds.
const LAG_THRESHOLD_SECONDS: u64 = 5;
/// Replication lag threshold in bytes.
const LAG_THRESHOLD_BYTES: u64 = WAL_GENERATION_RATE_BYTES_PER_SEC * LAG_THRESHOLD_SECONDS;

type NodeName = String;

pub type SplitBrainInfo = crate::v2::analyze::split_brain::SplitBrainInfo;
pub type SplitBrainResolution = crate::v2::analyze::split_brain::SplitBrainResolution;

mod checks;
mod classify;
mod split_brain;

#[derive(Debug, Eq, PartialEq)]
pub struct AnalyzedCluster {
    pub cluster: Cluster,
    pub verdict: Verdict,
}

impl AnalyzedCluster {
    /// Get the cluster name.
    pub fn name(&self) -> &str {
        &self.cluster.name
    }
}

#[derive(Debug, Eq, PartialEq)]
/// Represents the overall health of the `PostgreSQL` cluster.
pub enum ClusterHealth {
    /// ✅ The cluster is fully operational and redundant.
    ///
    /// - One primary and two replicas are online.
    /// - Replication lag is within the acceptable threshold (< 5s).
    /// - Quorum is satisfied.
    Healthy {
        failover: bool,
        cluster: AnalyzedCluster,
    },

    /// ⚠️ The cluster is operational but has lost some redundancy or performance.
    ///    Customer impact is low, but the risk of a full outage is elevated.
    ///
    /// - **Reduced Redundancy:** One of the two replicas is offline or unhealthy.
    /// - **High Lag:** The primary is up, and replicas are connected, but replication
    ///   lag exceeds the 5s threshold.
    Degraded {
        lag: u64,
        cluster: AnalyzedCluster,
        reason: Reason,
    },
    /// 🚨 The cluster is in a non-operational or dangerous state requiring immediate
    ///    human intervention. Data is at risk, writes are failing, or the cluster is
    ///    operating without any redundancy.
    ///
    /// - **Split Brain:** The monitor detects more than one active primary.
    ///   While we do have quorum synchronous commit enabled, this is still a
    ///   dangerous state that requires immediate attention.
    /// - **`WritesBlocked`:** Primary has `sync_commit=on` but no sync replicas to satisfy quorum.
    /// - **`WritesUnprotected`:** Primary has `sync_commit=off` with no replicas (DR mode).
    /// - **`NoPrimary`:** No primary found in the cluster.
    Critical {
        cluster: AnalyzedCluster,
        reason: Reason,
    },

    /// ❓ The state of the cluster cannot be determined.
    ///
    /// - The monitoring tool cannot connect to any nodes.
    /// - Unexpected cluster topology.
    Unknown {
        cluster: AnalyzedCluster,
        reachable_nodes: usize,
        reason: Reason,
    },
}

impl ClusterHealth {
    /// Returns a reference to the analyzed cluster. Test-only accessor;
    /// production code matches each variant directly to extract the cluster.
    #[cfg(test)]
    pub fn cluster(&self) -> &AnalyzedCluster {
        match self {
            ClusterHealth::Healthy { cluster, .. }
            | ClusterHealth::Degraded { cluster, .. }
            | ClusterHealth::Critical { cluster, .. }
            | ClusterHealth::Unknown { cluster, .. } => cluster,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
#[cfg_attr(test, derive(strum::EnumIter))]
pub enum Reason {
    // Declaration order = ascending severity; classify picks the worst via
    // `max()`. Unknown reasons fire only when no nodes are reachable or the
    // topology is unrecognized, so they never coexist with the tiers below.

    // Unknown reasons
    /// Cannot connect to any nodes in the cluster.
    NoNodesReachable,
    /// Cluster has unexpected topology (e.g., more than 3 nodes).
    UnexpectedTopology,

    // Degraded reasons (least → most severe within the tier).
    /// I/O or block-device errors found in dmesg.
    DiskIoErrors,
    /// One or more streaming replicas have a `sync_state` other than `quorum`.
    NotInQuorum,
    /// A replica is replicating from another replica instead of the primary (cascading replication).
    ChainedReplica,
    /// A replica has `wal_receiver` = None, indicating it's rebuilding or disconnected.
    RebuildingReplica,
    HighReplicationLag,
    /// Archive was working but the most recent push failed
    /// (`last_failed_time > last_archived_time`). WAL is retained until the
    /// next successful push, so durability isn't yet lost.
    ArchiveLagging,
    /// One or more nodes are unreachable; pg-level reduced-redundancy state
    /// outranks lower Degraded findings so it surfaces as the headline reason.
    ReducedRedundancy,

    // Critical reasons (least → most severe within the tier).
    /// Quorum sync is not activated.
    SyncCommitOff,
    /// Archiving is not enabled.
    ArchivingDisabled,
    /// Archive command has never succeeded since becoming primary.
    ArchiveFailure,
    /// Filesystem-level errors found in dmesg.
    FilesystemErrors,
    /// Primary has `sync_commit=off` with no replicas - DR mode, no redundancy.
    WritesUnprotected,
    /// Primary has `sync_commit=on` but no sync replicas - writes are blocked.
    WritesBlocked,
    /// No primary found in the cluster.
    NoPrimary,
    /// Multiple nodes return `pg_is_in_recovery()` = false.
    SplitBrain,
}

#[derive(Debug, Eq, PartialEq)]
enum Tier {
    Degraded,
    Critical,
    Unknown,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct Verdict {
    node_verdicts: Vec<(NodeName, NodeVerdict)>,
    cluster_verdict: Option<ClusterVerdict>,
}

impl Verdict {
    fn push_node_verdict(&mut self, node_name: NodeName, verdict: NodeVerdict) {
        self.node_verdicts.push((node_name, verdict));
    }

    pub(super) fn max_lag(&self) -> u64 {
        self.node_verdicts
            .iter()
            .filter_map(|(_, v)| {
                let NodeVerdict::HighLag { bytes } = v else {
                    return None;
                };

                Some(*bytes)
            })
            .max()
            .unwrap_or(0)
    }

    pub fn has_failover(&self) -> bool {
        self.node_verdicts
            .iter()
            .any(|(_, v)| matches!(v, NodeVerdict::IsFailoverNode))
    }

    pub(super) fn node_reasons(&self) -> impl Iterator<Item = Reason> {
        self.node_verdicts
            .iter()
            .filter_map(|(_, v)| Option::<Reason>::from(v))
    }

    pub fn node_verdicts(&self) -> &[(String, NodeVerdict)] {
        &self.node_verdicts
    }

    pub fn cluster_verdict(&self) -> Option<&ClusterVerdict> {
        self.cluster_verdict.as_ref()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum NodeVerdict {
    ArchiveFailure {
        failed_count: i64,
        last_wal: Option<String>,
        last_failed_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    ArchiveLagging {
        failed_count: i64,
        last_wal: Option<String>,
        last_failed_at: Option<chrono::DateTime<chrono::Utc>>,
        last_archived_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    ArchivingDisabled,
    IsFailoverNode,
    HighLag {
        bytes: u64,
    },
    DiskIoErrors {
        io: u32,
        block: u32,
    },
    FilesystemErrors {
        count: u32,
    },
    ChainedReplication {
        upstream: String,
    },
    NotStreaming,
    NotInQuorum,
    SyncCommitOff,
    /// Node is reachable in inventory but unreachable for health checks
    /// `(Role::Unknown)`. One or more of these → cluster has reduced redundancy.
    Unreachable,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ClusterVerdict {
    SplitBrain(SplitBrainInfo),
    WritesBlocked,
    WritesUnprotected,
    NoPrimary,
    NoNodesReachable,
    UnexpectedTopology { replica_count: usize },
}

/// Async task that analyzes clusters and sends results through a channel.
///
/// This function receives [`Cluster`] instances from `cluster_rx`, performs health analysis,
/// and sends the resulting [`ClusterHealth`] through `analyzed_tx`.
///
/// # Arguments
///
/// * `cluster_rx` - Receiver channel for clusters to analyze
/// * `analyzed_tx` - Sender channel for analyzed cluster health results
///
/// # Behavior
///
/// The task runs until the `cluster_rx` channel is closed by the sender. Each received
/// cluster is processed through [`analyze_with_enrichment`] which fetches backup progress
/// data asynchronously before performing synchronous health analysis.
#[instrument(skip_all, level = "info")]
pub async fn analyze_clusters(
    _ctx: Arc<PipelineContext>,
    mut cluster_rx: UnboundedReceiver<Cluster>,
    analyzed_tx: UnboundedSender<ClusterHealth>,
) {
    while let Some(cluster) = cluster_rx.recv().await {
        let analyzed_cluster = analyze(cluster);
        let analyzed = classify::classify(analyzed_cluster);

        match analyzed_tx.send(analyzed) {
            Ok(()) => tracing::trace!("sent analyzed cluster"),
            Err(e) => tracing::error!(error = %e, "failed to send analyzed cluster"),
        }
    }
}

fn analyze(cluster: Cluster) -> AnalyzedCluster {
    let mut verdict = Verdict::default();

    let primaries: Vec<_> = cluster.primaries().collect();
    let replicas: Vec<_> = cluster.replicas().collect();

    // No reachable nodes (all Role::Unknown or empty cluster) - Unknown state.
    if primaries.is_empty() && replicas.is_empty() {
        verdict.cluster_verdict = Some(ClusterVerdict::NoNodesReachable);
        return AnalyzedCluster { cluster, verdict };
    }

    // Replicas reachable but no primary - Critical.
    if primaries.is_empty() {
        verdict.cluster_verdict = Some(ClusterVerdict::NoPrimary);
        return AnalyzedCluster { cluster, verdict };
    }

    // Multiple primaries - Critical (split brain)
    if primaries.len() > 1 {
        let split_brain_info = resolve_split_brain(&primaries, &replicas);
        verdict.cluster_verdict = Some(ClusterVerdict::SplitBrain(split_brain_info));
        return AnalyzedCluster { cluster, verdict };
    }

    if replicas.len() > 2 {
        verdict.cluster_verdict = Some(ClusterVerdict::UnexpectedTopology {
            replica_count: replicas.len(),
        });
        return AnalyzedCluster { cluster, verdict };
    }

    // At this point we have exactly 1 primary
    let primary = primaries[0];

    check_archive(primary, &mut verdict);
    check_sync_commit(primary, &mut verdict);
    check_failover(primary, &mut verdict);
    check_lag(primary, &replicas, &mut verdict);
    check_quorum(primary, &replicas, &mut verdict);
    check_chained_replication(primary, &replicas, &mut verdict);
    check_writes_unprotected(primary, &replicas, &mut verdict);
    check_writes_blocked(primary, &mut verdict);

    for node in cluster.nodes() {
        check_disk_errors(node, &mut verdict);
        check_streaming(node, &mut verdict);
        check_unreachable(node, primary, &mut verdict);
    }

    AnalyzedCluster { cluster, verdict }
}

/// Extract timeline ID from a primary node.
///
/// Returns `None` if the node is not a primary.
fn get_timeline(node: &AnalyzedNode) -> Option<i32> {
    match &node.role {
        Role::Primary { health } => Some(health.timeline_id),
        Role::Unknown | Role::UnknownPrimary | Role::UnknownReplica | Role::Replica { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::tests_common::{healthy, unhealthy};

    use pretty_assertions::assert_eq;

    #[test]
    fn test_healthy_cluster() {
        let cluster = healthy::non_failover_cluster();

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Healthy {
                    failover: false,
                    ..
                }
            ),
            "expected Healthy non-failover, got {actual:?}"
        );
    }

    #[test]
    fn test_degraded_cluster_one_replica_down() {
        let cluster = unhealthy::db001_unreachable_failover_with_replica();
        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Degraded {
                    reason: Reason::ReducedRedundancy,
                    ..
                }
            ),
            "expected Degraded ReducedRedundancy, got {actual:?}"
        );
    }

    #[test]
    fn test_degraded_cluster_rebuilding_replica() {
        // Scenario: db002 is primary (failover occurred), db003 is streaming replica,
        // db001 is online but rebuilding (wal_receiver = None, old last_transaction_replay_at)
        let cluster = unhealthy::db001_rebuilding_after_failover();
        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Degraded {
                    reason: Reason::RebuildingReplica,
                    ..
                }
            ),
            "expected Degraded RebuildingReplica, got {actual:?}"
        );
    }

    #[test]
    fn test_degraded_cluster_chained_replica() {
        // Scenario: db001 is primary, db002 replicates from db001, db003 replicates from db002 (chained)
        let cluster = unhealthy::chained_replica();
        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Degraded {
                    reason: Reason::ChainedReplica,
                    ..
                }
            ),
            "expected Degraded ChainedReplica, got {actual:?}"
        );

        #[expect(clippy::wildcard_enum_match_arm, reason = "it's a test")]
        // The chained replica details (which replica, what upstream) live in the verdict.
        let chained = actual
            .cluster()
            .verdict
            .node_verdicts
            .iter()
            .find_map(|(name, v)| match v {
                NodeVerdict::ChainedReplication { upstream } => {
                    Some((name.as_str(), upstream.as_str()))
                }
                _ => None,
            });
        assert_eq!(
            chained,
            Some((
                "dev-pg-app001-db003.sto3.example.com",
                "dev-pg-app001-db002.sto2.example.com",
            )),
        );
    }
}

#[cfg(test)]
mod cluster_state_tests {
    use std::collections::HashMap;

    use super::*;
    use crate::v2::{
        cluster::Cluster,
        scan::{
            AnalyzedNode, Role,
            disk_check::{DiskCheckOutcome, DiskCheckResult},
            health_check_primary::{ArchiverStats, PgSyncSettings, PrimaryHealthCheckResult},
            health_check_replica::{LagInfo, ReplicaHealthCheckResult},
        },
        tests_common::{ClusterBuilder, NodeBuilder, PrimaryHealthBuilder, ReplicaHealthBuilder},
    };
    use chrono::Utc;

    use pretty_assertions::assert_eq;

    // Convenience functions using the shared builders
    pub fn make_primary_health(
        replication_count: usize,
        replay_lag: Option<&str>,
    ) -> PrimaryHealthCheckResult {
        let mut builder = PrimaryHealthBuilder::new().with_replication(replication_count);
        if let Some(lag) = replay_lag {
            builder = builder.with_lag(lag);
        }
        builder.build()
    }

    pub fn make_primary_health_with_config(
        replication_count: usize,
        replay_lag: Option<&str>,
        configuration: HashMap<String, String>,
    ) -> PrimaryHealthCheckResult {
        let mut builder = PrimaryHealthBuilder::new()
            .with_replication(replication_count)
            .with_config(configuration);
        if let Some(lag) = replay_lag {
            builder = builder.with_lag(lag);
        }
        builder.build()
    }

    pub fn make_replica_health() -> ReplicaHealthCheckResult {
        ReplicaHealthBuilder::new().build()
    }

    pub fn make_node(id: u32, name: &str, role: Role) -> AnalyzedNode {
        NodeBuilder::new(name).with_id(id).build_with_role(role)
    }

    pub fn make_cluster(nodes: Vec<AnalyzedNode>) -> Cluster {
        ClusterBuilder::new("dev-pg-app001")
            .with_nodes(nodes)
            .build()
    }

    #[test]
    fn test_unknown_when_all_nodes_unreachable() {
        let cluster = make_cluster(vec![
            make_node(1, "dev-pg-app001-db001.sto1.example.com", Role::Unknown),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Unknown {
                    reachable_nodes: 0,
                    reason: Reason::NoNodesReachable,
                    ..
                }
            ),
            "expected Unknown NoNodesReachable, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_when_only_primary_reachable_sync_on() {
        // Single primary reachable with sync_commit=on (default) - writes are blocked
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
                },
            ),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesBlocked,
                    ..
                }
            ),
            "expected Critical WritesBlocked, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_when_no_primary_found() {
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::NoPrimary,
                    ..
                }
            ),
            "expected Critical NoPrimary, got {actual:?}"
        );
    }

    #[test]
    fn test_degraded_high_replication_lag() {
        // Create a cluster with high lag (> 80MB threshold = > 5 seconds)
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(2, Some("00:00:10.000000"))), // 10 seconds lag
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        // LSN diff: 48F/6957B540 - 48F/6357B540 = 0x06000000 = 100,663,296 bytes (~96MB)
        let ClusterHealth::Degraded {
            reason: Reason::HighReplicationLag,
            lag,
            ..
        } = &actual
        else {
            panic!("expected Degraded HighReplicationLag, got {actual:?}");
        };
        assert_eq!(*lag, 100_663_296);
    }

    #[test]
    fn test_healthy_with_failover_db002_is_primary() {
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(2, Some("00:00:00.001"))),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(&actual, ClusterHealth::Healthy { failover: true, .. }),
            "expected Healthy with failover, got {actual:?}"
        );
    }

    #[test]
    fn test_healthy_with_failover_db003_is_primary() {
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(2, Some("00:00:00.001"))),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(&actual, ClusterHealth::Healthy { failover: true, .. }),
            "expected Healthy with failover, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_writes_blocked_sync_commit_on_no_replicas() {
        // Scenario: Primary with sync_commit=on (default) but no replicas
        // This means writes will block waiting for quorum
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "on".to_owned());

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_config(0, None, config)),
                },
            ),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesBlocked,
                    ..
                }
            ),
            "expected Critical WritesBlocked, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_writes_unprotected_sync_commit_off() {
        // Scenario: Primary with sync_commit=off and no replicas (DR mode)
        // Writes succeed but no redundancy
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "off".to_owned());

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_config(0, None, config)),
                },
            ),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesUnprotected,
                    ..
                }
            ),
            "expected Critical WritesUnprotected, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_writes_unprotected_sync_commit_local() {
        // Scenario: Primary with sync_commit=local (equivalent to off for replication)
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "local".to_owned());

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_config(0, None, config)),
                },
            ),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesUnprotected,
                    ..
                }
            ),
            "expected Critical WritesUnprotected, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_writes_unprotected_empty_synchronous_standby_names() {
        // Scenario: Primary with sync_commit=remote_apply BUT synchronous_standby_names=""
        // When synchronous_standby_names is empty, PostgreSQL doesn't wait for any standby,
        // so writes proceed without blocking even though sync_commit suggests otherwise.
        // This is effectively unprotected writes (DR mode / misconfiguration).
        // See: https://postgresqlco.nf/doc/en/param/synchronous_commit/
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "remote_apply".to_owned());
        config.insert("synchronous_standby_names".to_owned(), String::new());

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_config(0, None, config)),
                },
            ),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesUnprotected,
                    ..
                }
            ),
            "expected Critical WritesUnprotected, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_archive_failure_and_writes_blocked_no_streaming() {
        // Scenario captured from prod machine where:
        // - Primary db003 (failover node) has archive_mode=on but archiving has
        //   never succeeded (archived_count=0, failed_count=237).
        // - synchronous_commit=remote_apply, synchronous_standby_names is set
        //   (non-empty) — sync replication is configured.
        // - pg_stat_replication is empty (no streaming replicas connected).
        // - Both replicas are reachable but report wal_receiver=None.
        //
        // Two Critical conditions coexist: ArchiveFailure (durability broken)
        // and WritesBlocked (writers hang waiting for an ack that won't come).
        // Customer-visible write hang outranks archive failure → headline
        // Reason should be WritesBlocked. The ArchiveFailure node verdict
        // must still be present so the operator sees both findings.
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "remote_apply".to_owned());
        config.insert(
            "synchronous_standby_names".to_owned(),
            "ANY 1 ( prod_pg_db001, prod_pg_db002 )".to_owned(),
        );
        config.insert("archive_mode".to_owned(), "on".to_owned());
        config.insert(
            "archive_command".to_owned(),
            "/usr/bin/pgbackrest --stanza=prod-pg archive-push %p".to_owned(),
        );

        let archiver = ArchiverStats {
            archived_count: 0,
            failed_count: 237,
            last_archived_wal: None,
            last_archived_time: None,
            last_failed_wal: Some("0000000B000003C0000000FC".to_owned()),
            last_failed_time: None,
        };

        let primary_health = PrimaryHealthBuilder::new()
            .with_replication(0)
            .with_config(config)
            .with_archiver(archiver)
            .build();

        let disconnected_replica = ReplicaHealthBuilder::new().without_wal_receiver().build();

        let cluster = make_cluster(vec![
            make_node(
                1,
                "prod-pg-db001.sto2.example.com",
                Role::Replica {
                    health: Box::new(disconnected_replica.clone()),
                },
            ),
            make_node(
                2,
                "prod-pg-db002.sto3.example.com",
                Role::Replica {
                    health: Box::new(disconnected_replica),
                },
            ),
            make_node(
                3,
                "prod-pg-db003.sto1.example.com",
                Role::Primary {
                    health: Box::new(primary_health),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesBlocked,
                    ..
                }
            ),
            "expected Critical WritesBlocked, got {actual:?}"
        );

        let verdict = &actual.cluster().verdict;

        assert_eq!(
            verdict.cluster_verdict(),
            Some(&ClusterVerdict::WritesBlocked),
        );

        #[expect(clippy::wildcard_enum_match_arm, reason = "it's a test")]
        let archive = verdict.node_verdicts().iter().find_map(|(_, v)| match v {
            NodeVerdict::ArchiveFailure {
                failed_count,
                last_wal,
                ..
            } => Some((*failed_count, last_wal.as_deref())),
            _ => None,
        });
        assert_eq!(archive, Some((237, Some("0000000B000003C0000000FC"))));
    }

    #[test]
    fn test_critical_writes_unprotected_disconnected_replicas_empty_standby_names() {
        // Scenario: Primary with sync_commit=remote_apply, synchronous_standby_names=""
        // Two replicas exist but are disconnected (wal_receiver=None).
        // This happens during DR when standby names were cleared and replicas
        // can't reconnect (e.g., broken prev-link, WAL issues).
        // Even though replicas are visible, no replication is happening.
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "remote_apply".to_owned());
        config.insert("synchronous_standby_names".to_owned(), String::new());

        // Replica health with no wal_receiver (disconnected)
        let disconnected_replica = ReplicaHealthCheckResult {
            timeline_id: 18,
            wal_receiver: None,
            lag: LagInfo {
                apply_lag_bytes: Some(0x0002_0000),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_config(0, None, config)),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(disconnected_replica.clone()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(disconnected_replica),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::WritesUnprotected,
                    ..
                }
            ),
            "expected Critical WritesUnprotected, got {actual:?}"
        );
    }

    #[test]
    fn test_critical_archive_failure_never_succeeded() {
        // Scenario: Primary with archive_mode=on but archiving has never succeeded
        // archived_count=0 with failed_count > 0 means archive command never worked
        let mut config = HashMap::new();
        config.insert("archive_mode".to_owned(), "on".to_owned());
        config.insert(
            "archive_command".to_owned(),
            "/usr/bin/pgbackrest --stanza=dev-pg-app001 archive-push %p".to_owned(),
        );

        let archiver = ArchiverStats {
            archived_count: 0,
            failed_count: 16452,
            last_archived_wal: None,
            last_archived_time: None,
            last_failed_wal: Some("000000120000058300000073".to_owned()),
            last_failed_time: None,
        };

        let primary_health = PrimaryHealthBuilder::new()
            .with_replication(2)
            .with_config(config)
            .with_archiver(archiver)
            .build();

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(primary_health),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::ArchiveFailure,
                    ..
                }
            ),
            "expected Critical ArchiveFailure, got {actual:?}"
        );

        #[expect(clippy::wildcard_enum_match_arm, reason = "it's a test")]
        let archive = actual
            .cluster()
            .verdict
            .node_verdicts
            .iter()
            .find_map(|(_, v)| match v {
                NodeVerdict::ArchiveFailure {
                    failed_count,
                    last_wal,
                    ..
                } => Some((*failed_count, last_wal.as_deref())),
                _ => None,
            });
        assert_eq!(archive, Some((16452, Some("000000120000058300000073"))),);
    }

    #[test]
    fn test_critical_sync_commit_off_when_standby_names_empty_with_async_replicas() {
        // Mirrors the real dump: synchronous_commit=on, synchronous_standby_names="",
        // both replicas streaming with sync_state=async. Empty standby_names
        // means postgres can't actually sync — sync replication is effectively
        // disabled at the primary regardless of sync_commit value. This is a
        // misconfiguration that puts writes at risk → Critical SyncCommitOff.
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "on".to_owned());
        config.insert("synchronous_standby_names".to_owned(), String::new());

        let primary_health = PrimaryHealthBuilder::new()
            .with_replication(2)
            .with_config(config)
            .with_sync_state(PgSyncSettings::Async)
            .build();

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(primary_health),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::SyncCommitOff,
                    ..
                }
            ),
            "expected Critical SyncCommitOff, got {actual:?}"
        );
    }

    #[test]
    fn test_degraded_when_one_replica_is_potential() {
        // Only one of the two replicas is in Quorum; the other is Potential.
        // Strict policy: any non-quorum replica → Degraded.
        // The builder applies sync_state uniformly to all replicas, so we
        // override one entry post-build to get a heterogeneous shape.
        let mut primary_health = PrimaryHealthBuilder::new()
            .with_replication(2)
            .with_sync_state(PgSyncSettings::Quorum)
            .build();
        primary_health.replication[1].sync_state = PgSyncSettings::Potential;

        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(primary_health),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
        ]);

        let actual = classify::classify(analyze(cluster));
        assert!(
            matches!(
                actual,
                ClusterHealth::Degraded {
                    reason: Reason::NotInQuorum,
                    ..
                }
            ),
            "expected Degraded NotInQuorum, got {actual:?}"
        );
    }

    #[test]
    fn analyze_pipeline_wires_split_brain_into_cluster_verdict() {
        // Integration check: two primaries with equal timelines, one replica streaming
        // from db002. Verifies analyze() routes through resolve_split_brain and surfaces
        // a populated ClusterVerdict::SplitBrain — regression guard for the wiring,
        // not the resolution logic (covered by split_brain unit tests).
        use std::net::Ipv4Addr;

        use crate::v2::analyze::split_brain::{SplitBrainInfo, SplitBrainResolution};

        let ip_db1 = Ipv4Addr::new(127, 1, 12, 151);
        let ip_db2 = Ipv4Addr::new(127, 2, 12, 151);

        let db1 = NodeBuilder::new("dev-pg-app001-db001.sto1.example.com")
            .with_id(1)
            .with_ip(ip_db1)
            .with_primary(PrimaryHealthBuilder::new().with_timeline(13).build())
            .build();
        let db2 = NodeBuilder::new("dev-pg-app001-db002.sto2.example.com")
            .with_id(2)
            .with_ip(ip_db2)
            .with_primary(PrimaryHealthBuilder::new().with_timeline(13).build())
            .build();
        let db3 = NodeBuilder::new("dev-pg-app001-db003.sto3.example.com")
            .with_id(3)
            .with_replica(
                crate::v2::tests_common::ReplicaHealthBuilder::new()
                    .with_timeline(13)
                    .with_sender_host(&ip_db2.to_string())
                    .build(),
            )
            .build();
        let cluster = make_cluster(vec![db1, db2, db3]);

        let actual = classify::classify(analyze(cluster));

        assert!(
            matches!(
                &actual,
                ClusterHealth::Critical {
                    reason: Reason::SplitBrain,
                    ..
                }
            ),
            "expected Critical SplitBrain, got {actual:?}"
        );

        let verdict = &actual.cluster().verdict;
        assert_eq!(
            verdict.cluster_verdict(),
            Some(&ClusterVerdict::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_owned()],
                resolution: SplitBrainResolution::ReplicaFollowing {
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_owned(),
                    ],
                },
            })),
        );
    }

    fn make_node_with_disk(
        id: u32,
        name: &str,
        role: Role,
        disk: Option<DiskCheckOutcome>,
    ) -> AnalyzedNode {
        let mut n = NodeBuilder::new(name).with_id(id).build_with_role(role);
        n.disk_check = disk;
        n
    }

    pub fn checked(io: u32, fs: u32, blk: u32) -> DiskCheckOutcome {
        DiskCheckOutcome::Checked(DiskCheckResult {
            io_errors: io,
            filesystem_errors: fs,
            block_errors: blk,
            sample_messages: vec![],
        })
    }

    #[test]
    fn disk_healthy_cluster_with_io_errors_becomes_degraded() {
        let cluster = make_cluster(vec![
            make_node_with_disk(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(2, None)),
                },
                Some(checked(2, 0, 1)),
            ),
            make_node_with_disk(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
            make_node_with_disk(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
        ]);

        let health = classify::classify(analyze(cluster));
        assert!(
            matches!(
                &health,
                ClusterHealth::Degraded {
                    reason: Reason::DiskIoErrors,
                    ..
                }
            ),
            "expected Degraded DiskIoErrors, got {health:?}"
        );
        assert!(
            health
                .cluster()
                .verdict
                .node_verdicts
                .iter()
                .any(|(_, v)| { matches!(v, NodeVerdict::DiskIoErrors { io: 2, block: 1 }) })
        );
    }

    #[test]
    fn disk_healthy_cluster_with_filesystem_errors_becomes_critical() {
        let cluster = make_cluster(vec![
            make_node_with_disk(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(2, None)),
                },
                Some(checked(0, 3, 0)),
            ),
            make_node_with_disk(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
            make_node_with_disk(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
        ]);

        let health = classify::classify(analyze(cluster));
        assert!(
            matches!(
                &health,
                ClusterHealth::Critical {
                    reason: Reason::FilesystemErrors,
                    ..
                }
            ),
            "expected Critical FilesystemErrors, got {health:?}"
        );
        assert!(
            health
                .cluster()
                .verdict
                .node_verdicts
                .iter()
                .any(|(_, v)| { matches!(v, NodeVerdict::FilesystemErrors { count: 3 }) })
        );
    }

    #[test]
    fn disk_degraded_cluster_with_filesystem_errors_upgrades_to_critical() {
        // Cluster is already Degraded (one replica down) + filesystem errors → Critical
        let cluster = make_cluster(vec![
            make_node_with_disk(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(1, None)),
                },
                Some(checked(0, 2, 0)),
            ),
            make_node_with_disk(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
            make_node_with_disk(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Unknown,
                None,
            ),
        ]);

        let health = classify::classify(analyze(cluster));
        assert!(
            matches!(
                &health,
                ClusterHealth::Critical {
                    reason: Reason::FilesystemErrors,
                    ..
                }
            ),
            "expected Critical FilesystemErrors, got {health:?}"
        );
        assert!(
            health
                .cluster()
                .verdict
                .node_verdicts
                .iter()
                .any(|(_, v)| { matches!(v, NodeVerdict::FilesystemErrors { count: 2 }) })
        );
    }

    #[test]
    fn disk_degraded_cluster_with_only_io_errors_stays_degraded_with_pg_reason() {
        // Cluster is already Degraded (one replica down) + only io errors → still Degraded (pg reason)
        let cluster = make_cluster(vec![
            make_node_with_disk(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(1, None)),
                },
                Some(checked(5, 0, 2)),
            ),
            make_node_with_disk(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
            make_node_with_disk(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Unknown,
                None,
            ),
        ]);

        let health = classify::classify(analyze(cluster));
        assert!(
            matches!(
                &health,
                ClusterHealth::Degraded {
                    reason: Reason::ReducedRedundancy,
                    ..
                }
            ),
            "expected Degraded ReducedRedundancy (pg reason preserved), got {health:?}"
        );
    }

    #[test]
    fn disk_critical_cluster_pg_reason_is_preserved() {
        // Cluster is already Critical (no primary) → filesystem errors don't change the reason
        let cluster = make_cluster(vec![
            make_node_with_disk(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 5, 0)),
            ),
            make_node_with_disk(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
            make_node_with_disk(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                Some(checked(0, 0, 0)),
            ),
        ]);

        let health = classify::classify(analyze(cluster));
        assert!(
            matches!(
                &health,
                ClusterHealth::Critical {
                    reason: Reason::NoPrimary,
                    ..
                }
            ),
            "expected Critical NoPrimary (pg reason preserved), got {health:?}"
        );
    }

    #[test]
    fn disk_failed_outcome_does_not_affect_verdict() {
        use crate::v2::scan::disk_check::DiskCheckOutcome;
        let cluster = make_cluster(vec![
            make_node_with_disk(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(2, None)),
                },
                Some(DiskCheckOutcome::Failed {
                    reason: "SSH timeout".to_owned(),
                }),
            ),
            make_node_with_disk(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                None,
            ),
            make_node_with_disk(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
                None,
            ),
        ]);

        let health = classify::classify(analyze(cluster));
        assert!(
            matches!(&health, ClusterHealth::Healthy { .. }),
            "expected Healthy (failed disk check ignored), got {health:?}"
        );
    }
}
