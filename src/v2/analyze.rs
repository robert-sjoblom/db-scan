use std::{collections::HashMap, sync::Arc};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::instrument;

use crate::{
    pipeline::PipelineContext,
    prometheus::FileSystemMetrics,
    v2::{
        analyze::split_brain::resolve_split_brain,
        cluster::Cluster,
        scan::{AnalyzedNode, Role, health_check_primary::PgSyncSettings},
    },
};

/// WAL generation rate in bytes per second (approximately 16MB/s under typical load).
const WAL_GENERATION_RATE_BYTES_PER_SEC: u64 = 16_000_000;
/// Maximum acceptable replication lag in seconds.
const LAG_THRESHOLD_SECONDS: u64 = 5;
/// Replication lag threshold in bytes.
const LAG_THRESHOLD_BYTES: u64 = WAL_GENERATION_RATE_BYTES_PER_SEC * LAG_THRESHOLD_SECONDS;

type Ip = String;

pub type SplitBrainInfo = crate::v2::analyze::split_brain::SplitBrainInfo;
pub type SplitBrainResolution = crate::v2::analyze::split_brain::SplitBrainResolution;

mod split_brain;

/// Async task that analyzes clusters and sends results through a channel.
///
/// This function receives [`Cluster`] instances from `cluster_rx`, enriches them with
/// backup progress data (if the prometheus feature is enabled), performs health analysis,
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
    ctx: Arc<PipelineContext>,
    mut cluster_rx: UnboundedReceiver<Cluster>,
    analyzed_tx: UnboundedSender<ClusterHealth>,
) {
    while let Some(cluster) = cluster_rx.recv().await {
        let analyzed = analyze_with_enrichment(cluster, &ctx.batch_data);

        match analyzed_tx.send(analyzed) {
            Ok(()) => tracing::trace!("sent analyzed cluster"),
            Err(e) => tracing::error!(error = %e, "failed to send analyzed cluster"),
        }
    }
}

/// Calculates backup progress before analyzing the cluster, allowing us to
/// test the `analyze` function without setting up file system metrics too.
fn analyze_with_enrichment(
    cluster: Cluster,
    batch_data: &HashMap<Ip, FileSystemMetrics>,
) -> ClusterHealth {
    let progress = calculate_backup_progress(&cluster, batch_data);
    let health = analyze(cluster, progress);
    apply_disk_verdict(health)
}

#[instrument(skip_all, level = "debug", fields(
    cluster = %cluster.name,
    batch_data_count = batch_data.len(),
    replication_connections = tracing::field::Empty,
    basebackup_count = tracing::field::Empty,
))]
/// Calculate backup progress for any `pg_basebackup` connections on the primary
fn calculate_backup_progress(
    cluster: &Cluster,
    batch_data: &HashMap<Ip, FileSystemMetrics>,
) -> HashMap<String, u16> {
    let mut progress = HashMap::new();

    let Some(replication_conns) = cluster.primary_replication_info() else {
        return progress;
    };

    let span = tracing::Span::current();
    span.record("replication_connections", replication_conns.len());

    let basebackup_count = replication_conns
        .iter()
        .filter(|c| c.application_name == "pg_basebackup")
        .count();
    span.record("basebackup_count", basebackup_count);

    tracing::debug!(
        replication_count = replication_conns.len(),
        "checking replication connections for pg_basebackup"
    );

    for conn in replication_conns {
        if conn.application_name != "pg_basebackup" {
            continue;
        }

        tracing::debug!(
            pid = conn.pid,
            state = %conn.state,
            client_addr = ?conn.client_addr,
            client_hostname = ?conn.client_hostname,
            "found pg_basebackup connection"
        );

        let Some(client_addr) = &conn.client_addr else {
            tracing::warn!(conn = ?conn, "conn has no client_addr");
            continue;
        };

        // This is the metrics for the replication/pg_basebackup
        let Some(conn_metrics) = batch_data.get(client_addr) else {
            tracing::warn!(
                client_addr = client_addr,
                "no file system metric for connection"
            );
            continue;
        };

        let Some(primary) = cluster.primary() else {
            continue;
        };

        let Some(primary_metrics) = batch_data.get(&primary.ip_address.to_string()) else {
            tracing::warn!(
                primary_conn = primary.ip_address.to_string(),
                "no file system metric for primary"
            );
            continue;
        };

        tracing::debug!(
            client_addr = client_addr,
            used_bytes = conn_metrics.used_bytes,
            primary_bytes = primary_metrics.size_bytes
        );

        let progress_pct =
            estimate_backup_progress(primary_metrics.used_bytes, conn_metrics.used_bytes);
        progress.insert(client_addr.clone(), progress_pct);
    }

    progress
}

fn analyze(cluster: Cluster, backup_progress: HashMap<String, u16>) -> ClusterHealth {
    let primaries: Vec<_> = cluster.primaries().collect();
    let replicas: Vec<_> = cluster.replicas().collect();

    let reachable_count = primaries.len() + replicas.len();

    // Zero nodes reachable - truly unknown state
    if reachable_count == 0 {
        return ClusterHealth::Unknown {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reachable_nodes: 0,
            reason: Reason::NoNodesReachable,
        };
    }

    // No primaries found - Critical state (even if we only see replicas)
    if primaries.is_empty() {
        return ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::NoPrimary,
        };
    }

    // Multiple primaries - Critical (split brain)
    if primaries.len() > 1 {
        let split_brain_info = resolve_split_brain(&primaries, &replicas);
        return ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::SplitBrain(split_brain_info),
        };
    }

    // At this point we have exactly 1 primary
    let primary = primaries[0];

    // Check for archive failure (archive_mode=on but never succeeded)
    if let Some((failed_count, last_failed_wal)) = check_archive_failure(primary) {
        return ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::ArchiveFailure {
                failed_count,
                last_failed_wal,
            },
        };
    }

    // Check if failover occurred (primary is not db001)
    let failover = is_failover_node(&primary.node_name);

    // Calculate max replication lag from primary's perspective
    let max_lag = calculate_max_lag(primary);

    // Count streaming replicas (replicas with active wal_receiver)
    let streaming_replicas: Vec<_> = replicas
        .iter()
        .filter(|r| is_replica_streaming(r))
        .collect();
    let rebuilding_count = replicas.len() - streaming_replicas.len();

    // Detect chained replication (replica replicating from another replica)
    let chained_replica = detect_chained_replica(primary, &replicas);

    // Pre-compute sync_commit status for no-replicas case (avoids borrow issues)
    let sync_commit_off = is_sync_commit_off(primary);

    // Replicas whose sync_state is not Quorum (we require quorum for all streaming replicas)
    let non_quorum_replicas = find_non_quorum_replicas(primary);

    let signals = ClusterSignals {
        failover,
        max_lag,
        rebuilding_count,
        chained_replica,
        sync_commit_off,
        non_quorum_replicas,
    };

    // Determine health based on replica count and lag
    match replicas.len() {
        2 => analyze_full_redundancy(cluster, backup_progress, signals),
        1 => analyze_one_replica_down(cluster, backup_progress, max_lag),
        0 => analyze_no_replicas(cluster, backup_progress, sync_commit_off),
        _ => ClusterHealth::Unknown {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reachable_nodes: reachable_count,
            reason: Reason::UnexpectedTopology,
        },
    }
}

/// Pre-computed signals fed into the per-topology analyzers.
struct ClusterSignals {
    failover: bool,
    max_lag: u64,
    rebuilding_count: usize,
    chained_replica: Option<ChainedReplicaInfo>,
    sync_commit_off: bool,
    non_quorum_replicas: Vec<String>,
}

/// Analyze a cluster with full redundancy (2 replicas visible).
///
/// Returns Healthy if all conditions are met, otherwise Degraded with appropriate reason.
fn analyze_full_redundancy(
    cluster: Cluster,
    backup_progress: HashMap<String, u16>,
    signals: ClusterSignals,
) -> ClusterHealth {
    let ClusterSignals {
        failover,
        max_lag,
        rebuilding_count,
        chained_replica,
        sync_commit_off,
        non_quorum_replicas,
    } = signals;

    // If ALL replicas are not streaming AND sync replication is disabled,
    // writes are unprotected - this is Critical, not just Degraded
    if rebuilding_count == 2 && sync_commit_off {
        return ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::WritesUnprotected,
        };
    }

    // Check for rebuilding replicas
    if rebuilding_count > 0 {
        return ClusterHealth::Degraded {
            lag: max_lag,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::RebuildingReplica,
        };
    }

    if max_lag > LAG_THRESHOLD_BYTES {
        return ClusterHealth::Degraded {
            lag: max_lag,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::HighReplicationLag,
        };
    }

    if let Some(chained) = chained_replica {
        // Chained replication is a degraded topology (less redundancy)
        return ClusterHealth::Degraded {
            lag: max_lag,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::ChainedReplica {
                chained_replica: chained.chained_replica,
                upstream_replica: chained.upstream_replica,
            },
        };
    }

    if !non_quorum_replicas.is_empty() {
        return ClusterHealth::Degraded {
            lag: max_lag,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress,
            },
            reason: Reason::NotInQuorum {
                replicas: non_quorum_replicas,
            },
        };
    }

    ClusterHealth::Healthy {
        failover,
        cluster: AnalyzedCluster {
            cluster,
            backup_progress,
        },
    }
}

/// Analyze a cluster with one replica down (1 replica visible).
///
/// Always returns Degraded with `OneReplicaDown` reason.
fn analyze_one_replica_down(
    cluster: Cluster,
    backup_progress: HashMap<String, u16>,
    max_lag: u64,
) -> ClusterHealth {
    ClusterHealth::Degraded {
        lag: max_lag,
        cluster: AnalyzedCluster {
            cluster,
            backup_progress,
        },
        reason: Reason::OneReplicaDown,
    }
}

/// Analyze a cluster with no replicas visible.
///
/// Returns Critical with either `WritesUnprotected` (`sync_commit=off`) or `WritesBlocked` (`sync_commit=on`).
fn analyze_no_replicas(
    cluster: Cluster,
    backup_progress: HashMap<String, u16>,
    sync_commit_off: bool,
) -> ClusterHealth {
    let reason = if sync_commit_off {
        Reason::WritesUnprotected
    } else {
        Reason::WritesBlocked
    };
    ClusterHealth::Critical {
        cluster: AnalyzedCluster {
            cluster,
            backup_progress,
        },
        reason,
    }
}

/// Check if this node is a failover node (not db001).
fn is_failover_node(node_name: &str) -> bool {
    // Node naming convention: env-pg-appXXX-dbYYY.zone.example.com
    // db001 is the original primary, db002/db003 are replicas
    // If db002 or db003 is primary, failover has occurred
    !node_name.contains("-db001")
}

/// Calculate maximum replication lag from primary health data
/// Only considers actual replica connections, excludes backup operations (`pg_basebackup`, etc.)
fn calculate_max_lag(primary: &AnalyzedNode) -> u64 {
    if let Role::Primary { health } = &primary.role {
        // Calculate lag from LSN differences (sent_lsn - replay_lsn)
        // This is the actual byte lag, not a time-based estimate
        health
            .replication
            .iter()
            .filter(|r| {
                // Only include actual streaming replicas, exclude backup operations
                r.state == "streaming"
                    && !matches!(
                        r.application_name.as_str(),
                        "pg_basebackup" | "pg_dump" | "pg_dumpall"
                    )
            })
            .filter_map(|r| {
                // For streaming replicas, use replay_lsn (or flush_lsn as fallback)
                let effective_lsn = r.replay_lsn.as_deref().or(r.flush_lsn.as_deref());
                if let (Some(sent), Some(replay)) = (r.sent_lsn.as_deref(), effective_lsn) {
                    pg_lsn_diff(sent, replay)
                } else {
                    None
                }
            })
            .max()
            .unwrap_or(0)
    } else {
        0
    }
}

/// Check if a replica is actively streaming (has `wal_receiver`).
fn is_replica_streaming(node: &AnalyzedNode) -> bool {
    if let Role::Replica { health } = &node.role {
        health.wal_receiver.is_some()
    } else {
        false
    }
}

/// Check if writes are unprotected (no synchronous replication).
/// This is true when:
/// - `synchronous_commit` is "off" or "local"
///   Check if archive command has never succeeded since this node became primary.
///
/// Returns `Some((failed_count`, `last_failed_wal`)) if:
/// - `archive_mode` = "on"
/// - `archived_count` = 0 (no successful archives)
/// - `failed_count` > 0 (there have been failures)
///
/// TODO: Consider adding Degraded state when `last_failed_time > last_archived_time`
/// (archive was working but most recent attempt failed). WAL archives at least every 15 min.
fn check_archive_failure(primary: &AnalyzedNode) -> Option<(i64, Option<String>)> {
    let Role::Primary { health } = &primary.role else {
        return None;
    };

    let archive_mode = health.configuration.get("archive_mode")?;
    if archive_mode != "on" {
        return None;
    }

    let archiver = &health.archiver;
    if archiver.archived_count == 0 && archiver.failed_count > 0 {
        Some((archiver.failed_count, archiver.last_failed_wal.clone()))
    } else {
        None
    }
}

/// - OR `synchronous_standby_names` is empty (even with `remote_apply`, writes won't block)
///
/// See: <https://postgresqlco.nf/doc/en/param/synchronous_commit>/.
fn is_sync_commit_off(primary: &AnalyzedNode) -> bool {
    if let Role::Primary { health } = &primary.role {
        let sync_commit_off = health
            .configuration
            .get("synchronous_commit")
            .is_some_and(|v| v == "off" || v == "local");

        let standby_names_empty = health
            .configuration
            .get("synchronous_standby_names")
            .is_some_and(std::string::String::is_empty);

        sync_commit_off || standby_names_empty
    } else {
        false
    }
}

/// Returns `application_names` of replicas whose `sync_state` is not Quorum.
fn find_non_quorum_replicas(primary: &AnalyzedNode) -> Vec<String> {
    let Role::Primary { health } = &primary.role else {
        return Vec::new();
    };
    health
        .replication
        .iter()
        .filter(|conn| !matches!(conn.sync_state, PgSyncSettings::Quorum))
        .map(|conn| conn.application_name.clone())
        .collect()
}

/// Information about a chained replica.
struct ChainedReplicaInfo {
    /// The replica that is chained.
    chained_replica: String,
    /// The upstream replica it's replicating from.
    upstream_replica: String,
}

/// Detect if any replica is replicating from another replica instead of the primary.
///
/// Returns information about the first chained replica found, if any.
fn detect_chained_replica(
    primary: &AnalyzedNode,
    replicas: &[&AnalyzedNode],
) -> Option<ChainedReplicaInfo> {
    let primary_ip = primary.ip_address.to_string();

    // Build a map of replica IPs to replica names for lookup
    let replica_ips: HashMap<String, &str> = replicas
        .iter()
        .map(|r| (r.ip_address.to_string(), r.node_name.as_str()))
        .collect();

    for replica in replicas {
        if let Role::Replica { health } = &replica.role
            && let Some(wal_receiver) = &health.wal_receiver
        {
            let sender_ip = &wal_receiver.sender_host;

            // If sender_host is not the primary's IP, check if it's another replica
            if sender_ip != &primary_ip
                && let Some(&upstream_name) = replica_ips.get(sender_ip)
            {
                return Some(ChainedReplicaInfo {
                    chained_replica: replica.node_name.clone(),
                    upstream_replica: upstream_name.to_owned(),
                });
            }
        }
    }

    None
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

/// Inspect `disk_check` results on each node and upgrade `ClusterHealth` if warranted.
///
/// Rules (in priority order):
/// - Unknown and Critical (pg-based): left unchanged
/// - Any node has `filesystem_errors` > 0: upgrade to Critical { `FilesystemErrors` }
/// - Any node has io/block errors, and current health is Healthy: upgrade to Degraded { `DiskIoErrors` }
fn apply_disk_verdict(health: ClusterHealth) -> ClusterHealth {
    use crate::v2::scan::disk_check::DiskCheckOutcome;

    if matches!(
        health,
        ClusterHealth::Unknown { .. } | ClusterHealth::Critical { .. }
    ) {
        return health;
    }

    let (worst_fs, worst_io) = {
        let cluster_ref = health.cluster();
        let mut worst_fs: Option<(String, u32)> = None;
        let mut worst_io: Option<(String, u32, u32)> = None;

        #[expect(clippy::needless_else, reason = "conflicting lints")]
        for node in &cluster_ref.cluster.nodes {
            if let Some(DiskCheckOutcome::Checked(result)) = &node.disk_check {
                if result.filesystem_errors > 0 {
                    if worst_fs
                        .as_ref()
                        .is_none_or(|(_, c)| result.filesystem_errors > *c)
                    {
                        worst_fs = Some((node.node_name.clone(), result.filesystem_errors));
                    }
                } else if result.io_errors > 0 || result.block_errors > 0 {
                    worst_io.get_or_insert((
                        node.node_name.clone(),
                        result.io_errors,
                        result.block_errors,
                    ));
                } else {
                }
            }
        }
        (worst_fs, worst_io)
    };

    if worst_fs.is_none() && worst_io.is_none() {
        return health;
    }

    if let Some((node, count)) = worst_fs {
        let cluster = match health {
            ClusterHealth::Healthy { cluster, .. } | ClusterHealth::Degraded { cluster, .. } => {
                cluster
            }
            ClusterHealth::Critical { .. } | ClusterHealth::Unknown { .. } => return health,
        };
        return ClusterHealth::Critical {
            cluster,
            reason: Reason::FilesystemErrors { node, count },
        };
    }

    if let ClusterHealth::Healthy { cluster, .. } = health {
        let (node, io_errors, block_errors) = worst_io.unwrap();
        return ClusterHealth::Degraded {
            lag: 0,
            cluster,
            reason: Reason::DiskIoErrors {
                node,
                io_errors,
                block_errors,
            },
        };
    }

    health
}

/// Calculate byte difference between two `PostgreSQL` LSNs
/// LSN format: "XXX/YYYYYYYY" where both parts are hexadecimal
/// Returns None if LSNs are invalid.
fn pg_lsn_diff(lsn1: &str, lsn2: &str) -> Option<u64> {
    fn parse_lsn(lsn: &str) -> Option<u64> {
        let parts: Vec<&str> = lsn.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let high = u64::from_str_radix(parts[0], 16).ok()?;
        let low = u64::from_str_radix(parts[1], 16).ok()?;
        Some((high << 32) | low)
    }

    let pos1 = parse_lsn(lsn1)?;
    let pos2 = parse_lsn(lsn2)?;

    // Return absolute difference
    Some(pos1.abs_diff(pos2))
}

/// Estimate `pg_basebackup` progress by comparing primary DB size vs replica filesystem usage
/// Returns progress as u16 (percentage * 100, e.g., 4156 = 41.56%).
///
/// This is a rough estimate assuming the used bytes on the replica are mostly from the backup.
/// This may be inaccurate if there's other data on the filesystem.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "it's an estimate, not the math olympiad"
)]
fn estimate_backup_progress(primary_used_bytes: u64, replica_used_bytes: u64) -> u16 {
    if primary_used_bytes == 0 {
        return 0;
    }

    let progress = (replica_used_bytes as f64 / primary_used_bytes as f64) * 10000.0;
    progress.min(10000.0) as u16
}

#[derive(Debug, Eq, PartialEq)]
pub struct AnalyzedCluster {
    pub cluster: Cluster,
    /// Backup progress for `pg_basebackup` connections, mapped by `client_addr`
    /// Key: client IP address, Value: progress (pct * 100, e.g., 4156 = 41.56%).
    pub backup_progress: HashMap<String, u16>,
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
    /// Returns a reference to the analyzed cluster.
    pub fn cluster(&self) -> &AnalyzedCluster {
        match self {
            ClusterHealth::Healthy { cluster, .. }
            | ClusterHealth::Degraded { cluster, .. }
            | ClusterHealth::Critical { cluster, .. }
            | ClusterHealth::Unknown { cluster, .. } => cluster,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Reason {
    // Degraded reasons
    OneReplicaDown,
    HighReplicationLag,
    /// A replica has `wal_receiver` = None, indicating it's rebuilding or disconnected.
    RebuildingReplica,
    /// A replica is replicating from another replica instead of the primary (cascading replication).
    ChainedReplica {
        /// The replica that is chained (replicating from another replica).
        chained_replica: String,
        /// The upstream replica it's replicating from.
        upstream_replica: String,
    },
    /// One or more streaming replicas have a `sync_state` other than `quorum`.
    NotInQuorum {
        /// `application_name` of each replica whose `sync_state` is not Quorum.
        replicas: Vec<String>,
    },

    // Critical reasons
    /// No primary found in the cluster.
    NoPrimary,
    /// Multiple nodes return `pg_is_in_recovery()` = false.
    SplitBrain(SplitBrainInfo),
    /// Primary has `sync_commit=on` but no sync replicas - writes are blocked.
    WritesBlocked,
    /// Primary has `sync_commit=off` with no replicas - DR mode, no redundancy.
    WritesUnprotected,
    /// Archive command has never succeeded since becoming primary.
    ArchiveFailure {
        failed_count: i64,
        last_failed_wal: Option<String>,
    },

    // Unknown reasons
    /// Cannot connect to any nodes in the cluster.
    NoNodesReachable,
    /// Cluster has unexpected topology (e.g., more than 3 nodes).
    UnexpectedTopology,

    // Disk reasons (from dmesg within the recency window)
    /// I/O or block-device errors found in dmesg.
    DiskIoErrors {
        node: String,
        io_errors: u32,
        block_errors: u32,
    },
    /// Filesystem-level errors found in dmesg.
    FilesystemErrors {
        node: String,
        count: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{
        tests_common::{healthy, unhealthy},
        writer::units::parse_lag_to_bytes,
    };

    use pretty_assertions::assert_eq;

    #[test]
    fn test_healthy_cluster() {
        let cluster = healthy::non_failover_cluster();

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Healthy {
            failover: false,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_degraded_cluster_one_replica_down() {
        let cluster = unhealthy::db001_unreachable_failover_with_replica();
        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Degraded {
            lag: 0,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::OneReplicaDown,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_degraded_cluster_rebuilding_replica() {
        // Scenario: db002 is primary (failover occurred), db003 is streaming replica,
        // db001 is online but rebuilding (wal_receiver = None, old last_transaction_replay_at)
        let cluster = unhealthy::db001_rebuilding_after_failover();
        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Degraded {
            lag: 0,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::RebuildingReplica,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_degraded_cluster_chained_replica() {
        // Scenario: db001 is primary, db002 replicates from db001, db003 replicates from db002 (chained)
        let cluster = unhealthy::chained_replica();
        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Degraded {
            lag: 0,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::ChainedReplica {
                chained_replica: "dev-pg-app001-db003.sto3.example.com".to_owned(),
                upstream_replica: "dev-pg-app001-db002.sto2.example.com".to_owned(),
            },
        };
        assert_eq!(actual, expected);
    }

    // ==================== Helper function tests ====================

    #[test]
    fn test_is_failover_node_db001_is_not_failover() {
        assert!(!is_failover_node("dev-pg-app001-db001.sto1.example.com"));
        assert!(!is_failover_node("prod-pg-app007-db001.sto2.example.com"));
    }

    #[test]
    fn test_is_failover_node_db002_is_failover() {
        assert!(is_failover_node("dev-pg-app001-db002.sto1.example.com"));
        assert!(is_failover_node("prod-pg-app007-db002.sto2.example.com"));
    }

    #[test]
    fn test_is_failover_node_db003_is_failover() {
        assert!(is_failover_node("dev-pg-app001-db003.sto1.example.com"));
        assert!(is_failover_node("prod-pg-app007-db003.sto3.example.com"));
    }

    #[test]
    fn test_parse_lag_to_bytes_zero_lag() {
        assert_eq!(parse_lag_to_bytes("00:00:00.001234"), Some(0));
    }

    #[test]
    fn test_parse_lag_to_bytes_one_second() {
        // 1 second * 16MB/s = 16,000,000 bytes
        assert_eq!(parse_lag_to_bytes("00:00:01.000000"), Some(16_000_000));
    }

    #[test]
    fn test_parse_lag_to_bytes_one_minute() {
        // 60 seconds * 16MB/s = 960,000,000 bytes
        assert_eq!(parse_lag_to_bytes("00:01:00.000000"), Some(960_000_000));
    }

    #[test]
    fn test_parse_lag_to_bytes_complex() {
        // 1h 30m 45s = 5445 seconds * 16MB/s = 87,120,000,000 bytes
        assert_eq!(parse_lag_to_bytes("01:30:45.123456"), Some(87_120_000_000));
    }

    #[test]
    fn test_parse_lag_to_bytes_invalid_format() {
        assert_eq!(parse_lag_to_bytes("invalid"), None);
        assert_eq!(parse_lag_to_bytes("00:00"), None);
        assert_eq!(parse_lag_to_bytes(""), None);
    }
}

#[cfg(test)]
mod cluster_state_tests {
    use super::*;
    use crate::v2::{
        cluster::Cluster,
        scan::{
            AnalyzedNode, Role,
            health_check_primary::{ArchiverStats, PrimaryHealthCheckResult},
            health_check_replica::{LagInfo, ReplicaHealthCheckResult},
        },
        tests_common::{ClusterBuilder, NodeBuilder, PrimaryHealthBuilder, ReplicaHealthBuilder},
    };
    use chrono::Utc;
    use std::net::Ipv4Addr;

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

    fn make_primary_health_with_config(
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

    pub fn make_primary_health_with_timeline(
        replication_count: usize,
        replay_lag: Option<&str>,
        timeline_id: i32,
    ) -> PrimaryHealthCheckResult {
        let mut builder = PrimaryHealthBuilder::new()
            .with_replication(replication_count)
            .with_timeline(timeline_id);
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

    pub fn make_node_with_ip(
        id: u32,
        name: &str,
        role: Role,
        ip_address: Ipv4Addr,
    ) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_ip(ip_address)
            .build_with_role(role)
    }

    pub fn make_cluster(nodes: Vec<AnalyzedNode>) -> Cluster {
        ClusterBuilder::new("dev-pg-app001")
            .with_nodes(nodes)
            .build()
    }

    // ==================== Unknown state tests ====================

    #[test]
    fn test_unknown_when_all_nodes_unreachable() {
        let cluster = make_cluster(vec![
            make_node(1, "dev-pg-app001-db001.sto1.example.com", Role::Unknown),
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Unknown {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reachable_nodes: 0,
            reason: Reason::NoNodesReachable,
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::WritesBlocked,
        };

        assert_eq!(actual, expected);
    }

    // ==================== Critical state tests ====================

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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::NoPrimary,
        };

        assert_eq!(actual, expected);
    }

    // ==================== Degraded state tests ====================

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

        let actual = analyze(cluster.clone(), HashMap::new());
        // LSN diff: 48F/6957B540 - 48F/6357B540 = 0x06000000 = 100,663,296 bytes (~96MB)
        let expected = ClusterHealth::Degraded {
            lag: 100_663_296,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::HighReplicationLag,
        };

        assert_eq!(actual, expected);
    }

    // ==================== Healthy with failover tests ====================

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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Healthy {
            failover: true,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Healthy {
            failover: true,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
        };

        assert_eq!(actual, expected);
    }

    // ==================== Critical: WritesBlocked and WritesUnprotected tests ====================

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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::WritesBlocked,
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::WritesUnprotected,
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::WritesUnprotected,
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::WritesUnprotected,
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::WritesUnprotected,
        };

        assert_eq!(actual, expected);
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::ArchiveFailure {
                failed_count: 16452,
                last_failed_wal: Some("000000120000058300000073".to_owned()),
            },
        };

        assert_eq!(actual, expected);
    }

    // ==================== NotInQuorum tests ====================

    #[test]
    fn test_degraded_when_replicas_are_async_with_empty_standby_names() {
        // Mirrors the real dump: synchronous_commit=on, synchronous_standby_names="",
        // both replicas streaming with sync_state=async. Cluster must be Degraded.
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

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Degraded {
            lag: 0,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::NotInQuorum {
                replicas: vec![
                    "dev_pg_app001_db002".to_owned(),
                    "dev_pg_app001_db003".to_owned(),
                ],
            },
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_degraded_when_one_replica_is_potential() {
        // Only one of the two replicas is in Quorum; the other is Potential.
        // Strict policy: any non-quorum replica → Degraded.
        let primary_health = PrimaryHealthBuilder::new()
            .with_replication(2)
            .with_sync_state(PgSyncSettings::Potential)
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

        let actual = analyze(cluster.clone(), HashMap::new());
        assert!(
            matches!(
                actual,
                ClusterHealth::Degraded {
                    reason: Reason::NotInQuorum { .. },
                    ..
                }
            ),
            "expected Degraded NotInQuorum, got {actual:?}"
        );
    }

    // ==================== is_sync_commit_off helper tests ====================

    #[test]
    fn test_is_sync_commit_off_returns_true_for_off() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "off".to_owned());

        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        );

        assert!(is_sync_commit_off(&node));
    }

    #[test]
    fn test_is_sync_commit_off_returns_true_for_local() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "local".to_owned());

        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        );

        assert!(is_sync_commit_off(&node));
    }

    #[test]
    fn test_is_sync_commit_off_returns_false_for_on() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "on".to_owned());

        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        );

        assert!(!is_sync_commit_off(&node));
    }

    #[test]
    fn test_is_sync_commit_off_returns_false_for_remote_write() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "remote_write".to_owned());

        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        );

        assert!(!is_sync_commit_off(&node));
    }

    #[test]
    fn test_is_sync_commit_off_returns_false_for_remote_apply() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "remote_apply".to_owned());

        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        );

        assert!(!is_sync_commit_off(&node));
    }

    #[test]
    fn test_is_sync_commit_off_returns_false_when_missing() {
        // When config is empty, default to assuming sync_commit is on
        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health(0, None)),
            },
        );

        assert!(!is_sync_commit_off(&node));
    }

    #[test]
    fn test_is_sync_commit_off_returns_false_for_replica() {
        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Replica {
                health: Box::new(make_replica_health()),
            },
        );

        assert!(!is_sync_commit_off(&node));
    }

    // ==================== Disk verdict promotion tests ====================

    fn make_node_with_disk(
        id: u32,
        name: &str,
        role: Role,
        disk: Option<crate::v2::scan::disk_check::DiskCheckOutcome>,
    ) -> AnalyzedNode {
        let mut n = NodeBuilder::new(name).with_id(id).build_with_role(role);
        n.disk_check = disk;
        n
    }

    fn checked(io: u32, fs: u32, blk: u32) -> crate::v2::scan::disk_check::DiskCheckOutcome {
        use crate::v2::scan::disk_check::{DiskCheckOutcome, DiskCheckResult};
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

        let health = apply_disk_verdict(analyze(cluster.clone(), HashMap::new()));
        assert!(
            matches!(
                &health,
                ClusterHealth::Degraded {
                    reason: Reason::DiskIoErrors {
                        io_errors: 2,
                        block_errors: 1,
                        ..
                    },
                    ..
                }
            ),
            "expected Degraded DiskIoErrors, got {health:?}"
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

        let health = apply_disk_verdict(analyze(cluster.clone(), HashMap::new()));
        assert!(
            matches!(
                &health,
                ClusterHealth::Critical {
                    reason: Reason::FilesystemErrors { count: 3, .. },
                    ..
                }
            ),
            "expected Critical FilesystemErrors, got {health:?}"
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

        let health = apply_disk_verdict(analyze(cluster.clone(), HashMap::new()));
        assert!(
            matches!(
                &health,
                ClusterHealth::Critical {
                    reason: Reason::FilesystemErrors { count: 2, .. },
                    ..
                }
            ),
            "expected Critical FilesystemErrors, got {health:?}"
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

        let health = apply_disk_verdict(analyze(cluster.clone(), HashMap::new()));
        assert!(
            matches!(
                &health,
                ClusterHealth::Degraded {
                    reason: Reason::OneReplicaDown,
                    ..
                }
            ),
            "expected Degraded OneReplicaDown (pg reason preserved), got {health:?}"
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

        let health = apply_disk_verdict(analyze(cluster.clone(), HashMap::new()));
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

        let health = apply_disk_verdict(analyze(cluster.clone(), HashMap::new()));
        assert!(
            matches!(&health, ClusterHealth::Healthy { .. }),
            "expected Healthy (failed disk check ignored), got {health:?}"
        );
    }

    #[test]
    #[cfg(feature = "prometheus")]
    fn test_estimate_backup_progress() {
        let replica_used_bytes = 415_626_584_064_u64; // ~415 GB
        let primary_db_size = 1_000_000_000_000_u64; // 1 TB

        let progress_pct = estimate_backup_progress(primary_db_size, replica_used_bytes);

        // Expected progress: (415626584064 / 1000000000000) * 10000 = ~4156 (41.56%)
        assert_eq!(progress_pct, 4156);
    }

    #[test]
    #[cfg(feature = "prometheus")]
    fn test_estimate_backup_progress_edge_cases() {
        // Zero primary size
        assert_eq!(estimate_backup_progress(0, 100), 0);

        // Zero replica usage
        assert_eq!(estimate_backup_progress(1000, 0), 0);

        // 100% complete (100% = 10000)
        assert_eq!(estimate_backup_progress(1000, 1000), 10000);

        // Over 100% (more data on replica than primary DB, clamped to 10000)
        assert_eq!(estimate_backup_progress(1000, 1500), 10000);
    }
}
