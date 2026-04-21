use std::{collections::HashMap, sync::Arc};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::instrument;

use crate::{
    pipeline::PipelineContext,
    prometheus::FileSystemMetrics,
    v2::{
        cluster::Cluster,
        scan::{AnalyzedNode, Role},
    },
};

/// WAL generation rate in bytes per second (approximately 16MB/s under typical load)
const WAL_GENERATION_RATE_BYTES_PER_SEC: u64 = 16_000_000;
/// Maximum acceptable replication lag in seconds
const LAG_THRESHOLD_SECONDS: u64 = 5;
/// Replication lag threshold in bytes
const LAG_THRESHOLD_BYTES: u64 = WAL_GENERATION_RATE_BYTES_PER_SEC * LAG_THRESHOLD_SECONDS;

type Ip = String;

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
            Ok(_) => tracing::trace!("sent analyzed cluster"),
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
    analyze(cluster, progress)
}

#[instrument(skip_all, level = "debug", fields(
    cluster = %cluster.name,
    batch_data_count = batch_data.len(),
    replication_connections = tracing::field::Empty,
    basebackup_count = tracing::field::Empty,
))]
/// Calculate backup progress for any pg_basebackup connections on the primary
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

    // Determine health based on replica count and lag
    match replicas.len() {
        2 => analyze_full_redundancy(
            cluster,
            backup_progress,
            failover,
            max_lag,
            rebuilding_count,
            chained_replica,
            sync_commit_off,
        ),
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

/// Analyze a cluster with full redundancy (2 replicas visible).
///
/// Returns Healthy if all conditions are met, otherwise Degraded with appropriate reason.
fn analyze_full_redundancy(
    cluster: Cluster,
    backup_progress: HashMap<String, u16>,
    failover: bool,
    max_lag: u64,
    rebuilding_count: usize,
    chained_replica: Option<ChainedReplicaInfo>,
    sync_commit_off: bool,
) -> ClusterHealth {
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
/// Always returns Degraded with OneReplicaDown reason.
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
/// Returns Critical with either WritesUnprotected (sync_commit=off) or WritesBlocked (sync_commit=on).
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

/// Check if this node is a failover node (not db001)
fn is_failover_node(node_name: &str) -> bool {
    // Node naming convention: env-pg-appXXX-dbYYY.zone.example.com
    // db001 is the original primary, db002/db003 are replicas
    // If db002 or db003 is primary, failover has occurred
    !node_name.contains("-db001")
}

/// Calculate maximum replication lag from primary health data
/// Only considers actual replica connections, excludes backup operations (pg_basebackup, etc.)
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

/// Check if a replica is actively streaming (has wal_receiver)
fn is_replica_streaming(node: &AnalyzedNode) -> bool {
    if let Role::Replica { health } = &node.role {
        health.wal_receiver.is_some()
    } else {
        false
    }
}

/// Check if writes are unprotected (no synchronous replication).
/// This is true when:
/// - synchronous_commit is "off" or "local"
///   Check if archive command has never succeeded since this node became primary.
///
/// Returns Some((failed_count, last_failed_wal)) if:
/// - archive_mode = "on"
/// - archived_count = 0 (no successful archives)
/// - failed_count > 0 (there have been failures)
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

/// - OR synchronous_standby_names is empty (even with remote_apply, writes won't block)
///
/// See: https://postgresqlco.nf/doc/en/param/synchronous_commit/
fn is_sync_commit_off(primary: &AnalyzedNode) -> bool {
    if let Role::Primary { health } = &primary.role {
        let sync_commit_off = health
            .configuration
            .get("synchronous_commit")
            .map(|v| v == "off" || v == "local")
            .unwrap_or(false);

        let standby_names_empty = health
            .configuration
            .get("synchronous_standby_names")
            .map(|v| v.is_empty())
            .unwrap_or(false);

        sync_commit_off || standby_names_empty
    } else {
        false
    }
}

/// Information about a chained replica
struct ChainedReplicaInfo {
    /// The replica that is chained
    chained_replica: String,
    /// The upstream replica it's replicating from
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
                    upstream_replica: upstream_name.to_string(),
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
        _ => None,
    }
}

/// Information extracted from timeline analysis of multiple primaries.
///
/// Used during split-brain resolution to categorize primaries by their timeline.
struct TimelineInfo<'a> {
    /// The highest timeline ID found among primaries
    highest_timeline: i32,
    /// The primary node with the highest timeline (first one if multiple)
    highest_timeline_node: &'a AnalyzedNode,
    /// Primaries that share the highest timeline (could be multiple if equal)
    primaries_with_highest_timeline: Vec<(&'a AnalyzedNode, i32)>,
    /// Primaries with timelines lower than the highest (stale primaries)
    primaries_with_lower_timeline: Vec<(&'a AnalyzedNode, i32)>,
}

/// Extract and categorize timeline information from primary nodes.
///
/// Sorts primaries by timeline (highest first) and partitions them into
/// those with the highest timeline and those with lower (stale) timelines.
fn extract_timeline_info<'a>(primaries: &[&'a AnalyzedNode]) -> TimelineInfo<'a> {
    // Extract timeline info from primaries
    let mut primary_timelines: Vec<(&AnalyzedNode, i32)> = primaries
        .iter()
        .filter_map(|p| get_timeline(p).map(|tl| (*p, tl)))
        .collect();

    // Sort by timeline descending (highest first)
    primary_timelines.sort_by_key(|b| std::cmp::Reverse(b.1));

    let highest_timeline = primary_timelines[0].1;
    let highest_timeline_node = primary_timelines[0].0;

    // Find primaries with the highest timeline (could be multiple if equal)
    let primaries_with_highest_timeline: Vec<_> = primary_timelines
        .iter()
        .filter(|(_, tl)| *tl == highest_timeline)
        .copied()
        .collect();

    // Find primaries with lower timelines
    let primaries_with_lower_timeline: Vec<_> = primary_timelines
        .iter()
        .filter(|(_, tl)| *tl < highest_timeline)
        .copied()
        .collect();

    TimelineInfo {
        highest_timeline,
        highest_timeline_node,
        primaries_with_highest_timeline,
        primaries_with_lower_timeline,
    }
}

/// Build a map of which replicas are following which primary.
///
/// For each primary, checks if any replica's WAL receiver is connected to that
/// primary's IP address (on port 5432). Returns a map from primary node name
/// to the list of replica node names following it.
fn build_replica_following_map<'a>(
    timeline_info: &TimelineInfo<'a>,
    replicas: &[&AnalyzedNode],
) -> HashMap<String, Vec<String>> {
    // First, build a map of sender (host:port) -> replica names
    let mut replicas_by_sender: HashMap<String, Vec<String>> = HashMap::new();

    for replica in replicas {
        if let Role::Replica { health } = &replica.role
            && let Some(wal_receiver) = &health.wal_receiver
        {
            let sender_key = format!("{}:{}", wal_receiver.sender_host, wal_receiver.sender_port);
            replicas_by_sender
                .entry(sender_key)
                .or_default()
                .push(replica.node_name.clone());
        }
    }

    // Match sender IPs to primaries (combine both highest and lower timeline primaries)
    let all_primaries: Vec<_> = timeline_info
        .primaries_with_highest_timeline
        .iter()
        .chain(timeline_info.primaries_with_lower_timeline.iter())
        .collect();

    let mut replicas_following = HashMap::new();

    for (primary, _) in all_primaries {
        let primary_ip = primary.ip_address.to_string();
        // Check for connections on standard PostgreSQL port
        let key_5432 = format!("{}:5432", primary_ip);
        if let Some(followers) = replicas_by_sender.get(&key_5432) {
            replicas_following.insert(primary.node_name.clone(), followers.clone());
        }
    }

    replicas_following
}

/// Determine the true primary based on timeline and replica evidence.
///
/// Resolution strategy:
/// 1. If one primary has a higher timeline, it's likely the true primary
/// 2. Check which primary the replicas are streaming from
/// 3. Replica evidence can override timeline if replicas follow a lower-timeline primary
///    (indicates the higher-timeline primary was isolated after promotion)
fn determine_true_primary(
    timeline_info: TimelineInfo<'_>,
    replicas_following: HashMap<String, Vec<String>>,
) -> SplitBrainInfo {
    if timeline_info.primaries_with_highest_timeline.len() == 1
        && !timeline_info.primaries_with_lower_timeline.is_empty()
    {
        resolve_with_different_timelines(&timeline_info, &replicas_following)
    } else if timeline_info.primaries_with_highest_timeline.len() > 1 {
        resolve_with_equal_timelines(&timeline_info, &replicas_following)
    } else {
        // Single primary with highest timeline, no stale ones (shouldn't happen with >= 2 primaries)
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .skip(1)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: timeline_info.highest_timeline_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Indeterminate,
        }
    }
}

/// Resolve split-brain when primaries have different timelines.
///
/// One primary has a higher timeline - but we still need to check if replicas disagree.
/// Replica evidence can override timeline analysis.
fn resolve_with_different_timelines(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<String, Vec<String>>,
) -> SplitBrainInfo {
    let stale_tl = timeline_info.primaries_with_lower_timeline[0].1;
    let highest_tl_node = timeline_info.highest_timeline_node;
    let highest_tl = timeline_info.highest_timeline;

    let replicas_following_highest = replicas_following
        .get(&highest_tl_node.node_name)
        .cloned()
        .unwrap_or_default();

    // Check if any replicas are following a lower-timeline primary instead
    let mut replicas_following_stale: Vec<String> = vec![];
    let mut stale_with_followers: Option<&AnalyzedNode> = None;

    for (stale_node, _) in &timeline_info.primaries_with_lower_timeline {
        if let Some(followers) = replicas_following.get(&stale_node.node_name)
            && !followers.is_empty()
        {
            replicas_following_stale = followers.clone();
            stale_with_followers = Some(*stale_node);
            break;
        }
    }

    if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty() {
        // Replicas are following the lower-timeline primary - it's the real primary
        // The higher-timeline was likely isolated after a failed promotion
        let stale_node = stale_with_followers.unwrap();

        SplitBrainInfo {
            true_primary: stale_node.node_name.clone(),
            stale_primaries: vec![highest_tl_node.node_name.clone()],
            resolution: SplitBrainResolution::ReplicaOverridesTimeline {
                true_primary_timeline: stale_tl,
                stale_timeline: highest_tl,
                replicas_following_true: replicas_following_stale,
            },
        }
    } else if !replicas_following_highest.is_empty() {
        // Both timeline and replica evidence agree
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_lower_timeline
            .iter()
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: highest_tl_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Both {
                true_primary_timeline: highest_tl,
                stale_timeline: stale_tl,
                replicas_following_true: replicas_following_highest,
            },
        }
    } else {
        // No replica evidence - trust timeline
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_lower_timeline
            .iter()
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: highest_tl_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::HigherTimeline {
                true_primary_timeline: highest_tl,
                stale_timeline: stale_tl,
            },
        }
    }
}

/// Resolve split-brain when primaries have equal timelines.
///
/// When timelines are equal, we need replica evidence to determine the true primary.
fn resolve_with_equal_timelines(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<String, Vec<String>>,
) -> SplitBrainInfo {
    // Find which primary has replicas following it
    let mut primary_with_followers: Option<&AnalyzedNode> = None;
    let mut followers_list: Vec<String> = vec![];

    for (primary, _) in &timeline_info.primaries_with_highest_timeline {
        if let Some(followers) = replicas_following.get(&primary.node_name)
            && !followers.is_empty()
        {
            primary_with_followers = Some(*primary);
            followers_list = followers.clone();
            break;
        }
    }

    if let Some(true_primary_node) = primary_with_followers {
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .filter(|(n, _)| n.node_name != true_primary_node.node_name)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: true_primary_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::ReplicaFollowing {
                replicas_following_true: followers_list,
            },
        }
    } else {
        // Cannot determine - mark first as "true" but resolution is indeterminate
        let first = timeline_info.primaries_with_highest_timeline[0].0;
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .skip(1)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: first.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Indeterminate,
        }
    }
}

/// Resolve a split-brain scenario by determining the true primary.
///
/// Resolution strategy:
/// 1. Compare timelines - higher timeline = more recent promotion
/// 2. Check which primary the replicas are streaming from (received_tli)
/// 3. If both agree, high confidence. If they disagree, prefer replica evidence.
fn resolve_split_brain(primaries: &[&AnalyzedNode], replicas: &[&AnalyzedNode]) -> SplitBrainInfo {
    assert!(
        primaries.len() >= 2,
        "resolve_split_brain requires at least 2 primaries"
    );

    let timeline_info = extract_timeline_info(primaries);
    let replicas_following = build_replica_following_map(&timeline_info, replicas);
    determine_true_primary(timeline_info, replicas_following)
}

/// Calculate byte difference between two PostgreSQL LSNs
/// LSN format: "XXX/YYYYYYYY" where both parts are hexadecimal
/// Returns None if LSNs are invalid
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

/// Estimate pg_basebackup progress by comparing primary DB size vs replica filesystem usage
/// Returns progress as u16 (percentage * 100, e.g., 4156 = 41.56%)
///
/// This is a rough estimate assuming the used bytes on the replica are mostly from the backup.
/// This may be inaccurate if there's other data on the filesystem.
fn estimate_backup_progress(primary_used_bytes: u64, replica_used_bytes: u64) -> u16 {
    if primary_used_bytes == 0 {
        return 0;
    }

    let progress = (replica_used_bytes as f64 / primary_used_bytes as f64) * 10000.0;
    progress.min(10000.0) as u16
}

#[derive(Debug, Eq, PartialEq)]
pub struct AnalyzedCluster {
    pub(crate) cluster: Cluster,
    /// Backup progress for pg_basebackup connections, mapped by client_addr
    /// Key: client IP address, Value: progress (pct * 100, e.g., 4156 = 41.56%)
    pub backup_progress: HashMap<String, u16>,
}

impl AnalyzedCluster {
    /// Get the cluster name
    pub fn name(&self) -> &str {
        &self.cluster.name
    }
}

#[derive(Debug, Eq, PartialEq)]
/// Represents the overall health of the PostgreSQL cluster.
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
    /// - **WritesBlocked:** Primary has sync_commit=on but no sync replicas to satisfy quorum.
    /// - **WritesUnprotected:** Primary has sync_commit=off with no replicas (DR mode).
    /// - **NoPrimary:** No primary found in the cluster.
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
    /// Returns a reference to the analyzed cluster
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
    /// A replica has wal_receiver = None, indicating it's rebuilding or disconnected
    RebuildingReplica,
    /// A replica is replicating from another replica instead of the primary (cascading replication)
    ChainedReplica {
        /// The replica that is chained (replicating from another replica)
        chained_replica: String,
        /// The upstream replica it's replicating from
        upstream_replica: String,
    },

    // Critical reasons
    /// No primary found in the cluster
    NoPrimary,
    /// Multiple nodes return pg_is_in_recovery() = false
    SplitBrain(SplitBrainInfo),
    /// Primary has sync_commit=on but no sync replicas - writes are blocked
    WritesBlocked,
    /// Primary has sync_commit=off with no replicas - DR mode, no redundancy
    WritesUnprotected,
    /// Archive command has never succeeded since becoming primary
    ArchiveFailure {
        failed_count: i64,
        last_failed_wal: Option<String>,
    },

    // Unknown reasons
    /// Cannot connect to any nodes in the cluster
    NoNodesReachable,
    /// Cluster has unexpected topology (e.g., more than 3 nodes)
    UnexpectedTopology,
}

/// Information about a split-brain scenario and its resolution
#[derive(Debug, Eq, PartialEq)]
pub struct SplitBrainInfo {
    /// The node determined to be the true primary based on timeline analysis
    pub true_primary: String,
    /// The node(s) that are stale primaries (should be demoted)
    pub stale_primaries: Vec<String>,
    /// How the true primary was determined
    pub resolution: SplitBrainResolution,
}

/// How split-brain was resolved
#[derive(Debug, Eq, PartialEq)]
pub enum SplitBrainResolution {
    /// Higher timeline indicates the true primary (most recent promotion)
    HigherTimeline {
        true_primary_timeline: i32,
        stale_timeline: i32,
    },
    /// Replicas are streaming from the true primary
    ReplicaFollowing {
        replicas_following_true: Vec<String>,
    },
    /// Both timeline and replica evidence agree
    Both {
        true_primary_timeline: i32,
        stale_timeline: i32,
        replicas_following_true: Vec<String>,
    },
    /// Replica evidence overrides timeline - replicas are following a lower-timeline primary
    /// This indicates the higher-timeline primary was likely isolated after promotion
    ReplicaOverridesTimeline {
        true_primary_timeline: i32,
        stale_timeline: i32,
        replicas_following_true: Vec<String>,
    },
    /// Cannot determine true primary - timelines equal, no replica evidence
    Indeterminate,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::{
        tests_common::{healthy, unhealthy},
        writer::parse_lag_to_bytes,
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
                chained_replica: "dev-pg-app001-db003.sto3.example.com".to_string(),
                upstream_replica: "dev-pg-app001-db002.sto2.example.com".to_string(),
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
            health_check_replica::{LagInfo, ReplicaHealthCheckResult, WalReceiverInfo},
        },
        tests_common::{ClusterBuilder, NodeBuilder, PrimaryHealthBuilder, ReplicaHealthBuilder},
    };
    use chrono::Utc;
    use std::net::Ipv4Addr;

    use pretty_assertions::assert_eq;

    // Convenience functions using the shared builders
    fn make_primary_health(
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

    fn make_primary_health_with_timeline(
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

    fn make_replica_health() -> ReplicaHealthCheckResult {
        ReplicaHealthBuilder::new().build()
    }

    fn make_node(id: u32, name: &str, role: Role) -> AnalyzedNode {
        NodeBuilder::new(name).with_id(id).build_with_role(role)
    }

    fn make_node_with_ip(id: u32, name: &str, role: Role, ip_address: Ipv4Addr) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_ip(ip_address)
            .build_with_role(role)
    }

    fn make_cluster(nodes: Vec<AnalyzedNode>) -> Cluster {
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

    #[test]
    fn test_critical_when_split_brain_two_primaries_same_timeline() {
        // Both primaries have same timeline (11), replica is following db001 (sender_host: 127.1.12.151)
        let cluster = make_cluster(vec![
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(1, Some("00:00:00.001"))),
                },
                Ipv4Addr::new(127, 1, 12, 151),
            ),
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(1, Some("00:00:00.001"))),
                },
                Ipv4Addr::new(127, 2, 12, 151),
            ),
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()), // sender_host: 127.1.12.151
                },
                Ipv4Addr::new(127, 3, 12, 151),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // Both primaries have same timeline, replica's sender_host matches db001's IP
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db001.sto1.example.com".to_string(),
                stale_primaries: vec!["dev-pg-app001-db002.sto2.example.com".to_string()],
                resolution: SplitBrainResolution::ReplicaFollowing {
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_string(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_no_replica_evidence() {
        // Scenario: 2 primaries detected (split brain), no replica to determine true primary
        // Both have same timeline, no replica evidence - resolution is indeterminate
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
                },
            ),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                // db001 is first in iteration order, but resolution is indeterminate
                true_primary: "dev-pg-app001-db001.sto1.example.com".to_string(),
                stale_primaries: vec!["dev-pg-app001-db002.sto2.example.com".to_string()],
                resolution: SplitBrainResolution::Indeterminate,
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_higher_timeline_wins() {
        // Scenario: db001 has timeline 11, db002 has timeline 12 (more recent promotion)
        // db002 should be identified as true primary
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 11)),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 12)),
                },
            ),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_string(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_string()],
                resolution: SplitBrainResolution::HigherTimeline {
                    true_primary_timeline: 12,
                    stale_timeline: 11,
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_both_timeline_and_replica_evidence() {
        // Scenario: db002 has higher timeline AND replica is following db002
        // Use different IPs so we can match replica to the correct primary
        let replica_health = ReplicaHealthCheckResult {
            timeline_id: 12,
            wal_receiver: Some(WalReceiverInfo {
                pid: 4053449,
                status: "streaming".to_string(),
                receive_start_lsn: "47F/67000000".to_string(),
                receive_start_tli: 12,
                written_lsn: "48F/6957B540".to_string(),
                flushed_lsn: "48F/6957B540".to_string(),
                received_tli: 12,
                last_msg_send_time: Some(Utc::now()),
                last_msg_receipt_time: Some(Utc::now()),
                latest_end_lsn: "48F/6957B540".to_string(),
                latest_end_time: Some(Utc::now()),
                slot_name: None,
                sender_host: "127.2.12.151".to_string(), // db002's IP
                sender_port: 5432,
                conninfo: "user=replicator host=127.2.12.151".to_string(),
            }),
            lag: LagInfo {
                apply_lag_bytes: Some(0),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 11)),
                },
                Ipv4Addr::new(127, 1, 12, 151),
            ),
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(1, None, 12)),
                },
                Ipv4Addr::new(127, 2, 12, 151),
            ),
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(replica_health),
                },
                Ipv4Addr::new(127, 3, 12, 151),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_string(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_string()],
                resolution: SplitBrainResolution::Both {
                    true_primary_timeline: 12,
                    stale_timeline: 11,
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_string(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_replica_overrides_higher_timeline() {
        // Scenario: db002 has higher timeline (12) but replica is following db001 (timeline 11)
        // This happens when db002 was promoted but then isolated, while db001 continued serving
        // The replica following db001 is the authoritative evidence of the true primary
        let replica_health = ReplicaHealthCheckResult {
            timeline_id: 11,
            wal_receiver: Some(WalReceiverInfo {
                pid: 4053449,
                status: "streaming".to_string(),
                receive_start_lsn: "47F/67000000".to_string(),
                receive_start_tli: 11, // Following timeline 11 (db001)
                written_lsn: "48F/6957B540".to_string(),
                flushed_lsn: "48F/6957B540".to_string(),
                received_tli: 11,
                last_msg_send_time: Some(Utc::now()),
                last_msg_receipt_time: Some(Utc::now()),
                latest_end_lsn: "48F/6957B540".to_string(),
                latest_end_time: Some(Utc::now()),
                slot_name: None,
                sender_host: "127.1.12.151".to_string(), // db001's IP
                sender_port: 5432,
                conninfo: "user=replicator host=127.1.12.151".to_string(),
            }),
            lag: LagInfo {
                apply_lag_bytes: Some(0),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(1, None, 11)), // Lower timeline but has replica
                },
                Ipv4Addr::new(127, 1, 12, 151),
            ),
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 12)), // Higher timeline but isolated
                },
                Ipv4Addr::new(127, 2, 12, 151),
            ),
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(replica_health),
                },
                Ipv4Addr::new(127, 3, 12, 151),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // db001 is the true primary because the replica is following it,
        // even though db002 has a higher timeline
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db001.sto1.example.com".to_string(),
                stale_primaries: vec!["dev-pg-app001-db002.sto2.example.com".to_string()],
                resolution: SplitBrainResolution::ReplicaOverridesTimeline {
                    true_primary_timeline: 11,
                    stale_timeline: 12,
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_string(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_after_hard_failover_replica_follows_new_primary() {
        // Scenario: Hard failover occurred, db002 was promoted to primary.
        // db001 came back online as a stale primary (same timeline, no replicas).
        // db003 correctly reconnected to db002 (the new primary).
        // Both primaries have the same timeline, so we rely on replica evidence.
        // db002 should be identified as the true primary because db003 follows it.
        let replica_health = ReplicaHealthCheckResult {
            timeline_id: 13,
            wal_receiver: Some(WalReceiverInfo {
                pid: 2727816,
                status: "streaming".to_string(),
                receive_start_lsn: "281/7D000000".to_string(),
                receive_start_tli: 13,
                written_lsn: "281/BAAA6510".to_string(),
                flushed_lsn: "281/BAAA6510".to_string(),
                received_tli: 13,
                last_msg_send_time: Some(Utc::now()),
                last_msg_receipt_time: Some(Utc::now()),
                latest_end_lsn: "281/BAAA6510".to_string(),
                latest_end_time: Some(Utc::now()),
                slot_name: None,
                sender_host: "127.2.12.162".to_string(), // db002's IP - following new primary
                sender_port: 5432,
                conninfo: "user=replicator host=127.2.12.162".to_string(),
            }),
            lag: LagInfo {
                apply_lag_bytes: Some(0),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            // db001: Stale primary that came back after failover (no replicas connected)
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 13)),
                },
                Ipv4Addr::new(127, 1, 12, 162),
            ),
            // db002: New primary (promoted during failover, has db003 streaming)
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(1, None, 13)),
                },
                Ipv4Addr::new(127, 2, 12, 162),
            ),
            // db003: Replica correctly following db002
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(replica_health),
                },
                Ipv4Addr::new(127, 3, 12, 162),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // db002 is the true primary because the replica is following it
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_string(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_string()],
                resolution: SplitBrainResolution::ReplicaFollowing {
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_string(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_primary_with_two_unreachable_replicas() {
        // Primary reachable + one replica reachable (but replica count is 0)
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
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
                Role::UnknownReplica,
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // 1 primary + 1 replica = Degraded with OneReplicaDown
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
        config.insert("synchronous_commit".to_string(), "on".to_string());

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
        config.insert("synchronous_commit".to_string(), "off".to_string());

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
        config.insert("synchronous_commit".to_string(), "local".to_string());

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
        config.insert("synchronous_commit".to_string(), "remote_apply".to_string());
        config.insert("synchronous_standby_names".to_string(), "".to_string());

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
        config.insert("synchronous_commit".to_string(), "remote_apply".to_string());
        config.insert("synchronous_standby_names".to_string(), "".to_string());

        // Replica health with no wal_receiver (disconnected)
        let disconnected_replica = ReplicaHealthCheckResult {
            timeline_id: 18,
            wal_receiver: None,
            lag: LagInfo {
                apply_lag_bytes: Some(131072),
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
        config.insert("archive_mode".to_string(), "on".to_string());
        config.insert(
            "archive_command".to_string(),
            "/usr/bin/pgbackrest --stanza=dev-pg-app001 archive-push %p".to_string(),
        );

        let archiver = ArchiverStats {
            archived_count: 0,
            failed_count: 16452,
            last_archived_wal: None,
            last_archived_time: None,
            last_failed_wal: Some("000000120000058300000073".to_string()),
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
                last_failed_wal: Some("000000120000058300000073".to_string()),
            },
        };

        assert_eq!(actual, expected);
    }

    // ==================== is_sync_commit_off helper tests ====================

    #[test]
    fn test_is_sync_commit_off_returns_true_for_off() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_string(), "off".to_string());

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
        config.insert("synchronous_commit".to_string(), "local".to_string());

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
        config.insert("synchronous_commit".to_string(), "on".to_string());

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
        config.insert("synchronous_commit".to_string(), "remote_write".to_string());

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
        config.insert("synchronous_commit".to_string(), "remote_apply".to_string());

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

    #[test]
    #[cfg(feature = "prometheus")]
    fn test_estimate_backup_progress() {
        let replica_used_bytes = 415_626_584_064u64; // ~415 GB
        let primary_db_size = 1_000_000_000_000u64; // 1 TB

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
