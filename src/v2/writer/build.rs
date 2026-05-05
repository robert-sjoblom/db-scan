use std::collections::{BTreeMap, HashMap};

use crate::v2::{
    analyze::{AnalyzedCluster, ClusterHealth, Reason, SplitBrainInfo, SplitBrainResolution},
    scan::{
        AnalyzedNode, disk_check::DiskCheckOutcome, health_check_primary::ReplicationConnection,
    },
};

use super::{
    units::{format_bytes, parse_lag_to_bytes},
    view::{ClusterView, NodeView, PrimaryView, ReasonView, ReplicaView, ReplicasView, Status},
};

pub(crate) fn build_cluster_view(health: &ClusterHealth) -> ClusterView {
    match health {
        ClusterHealth::Healthy { failover, cluster } => {
            let show_tl = timelines_differ(&cluster.cluster.nodes);
            let primary = build_primary_view(cluster, show_tl);
            let replicas = build_replicas_view(
                cluster.cluster.primary(),
                &cluster.backup_progress,
                &cluster.cluster.nodes,
                show_tl,
            );
            ClusterView {
                status: Status::Healthy,
                name: cluster.name().to_string(),
                primary,
                replicas,
                lag_bytes: None,
                disk: "-".to_string(),
                reason: ReasonView {
                    short: if *failover {
                        "Failover".to_string()
                    } else {
                        "-".to_string()
                    },
                    details_json: "{}".to_string(),
                },
                failover: *failover,
            }
        }
        ClusterHealth::Degraded {
            lag,
            cluster,
            reason,
        } => {
            let show_tl = timelines_differ(&cluster.cluster.nodes);
            let primary = build_primary_view(cluster, show_tl);
            let replicas = build_replicas_view(
                cluster.cluster.primary(),
                &cluster.backup_progress,
                &cluster.cluster.nodes,
                show_tl,
            );
            let reason_view = build_reason_view(reason);
            let disk = extract_disk_info(cluster);
            let failover = cluster
                .cluster
                .primary()
                .map(|n| !n.node_name.contains("-db001"))
                .unwrap_or(false);
            log_degraded(cluster.name(), &reason_view.short, *lag);
            ClusterView {
                status: Status::Degraded,
                name: cluster.name().to_string(),
                primary,
                replicas,
                lag_bytes: Some(*lag),
                disk,
                reason: reason_view,
                failover,
            }
        }
        ClusterHealth::Critical { cluster, reason } => {
            let (primary, replicas) = build_primary_replicas_for_critical(cluster, reason);
            let reason_view = build_reason_view(reason);
            let disk = extract_disk_info(cluster);
            log_critical(cluster.name(), reason, &reason_view.short);
            ClusterView {
                status: Status::Critical,
                name: cluster.name().to_string(),
                primary,
                replicas,
                lag_bytes: None,
                disk,
                reason: reason_view,
                failover: false,
            }
        }
        ClusterHealth::Unknown {
            cluster,
            reachable_nodes,
            reason,
        } => {
            let reason_view = build_reason_view(reason);
            let disk = extract_disk_info(cluster);
            tracing::warn!(
                cluster = %cluster.name(),
                reachable_nodes = reachable_nodes,
                reason = %reason_view.short,
                "cluster state unknown"
            );
            ClusterView {
                status: Status::Unknown,
                name: cluster.name().to_string(),
                primary: PrimaryView::Dash,
                replicas: ReplicasView::Unknown {
                    reachable: *reachable_nodes as u32,
                },
                lag_bytes: None,
                disk,
                reason: reason_view,
                failover: false,
            }
        }
    }
}

fn build_primary_view(cluster: &AnalyzedCluster, show_tl: bool) -> PrimaryView {
    match cluster.cluster.primary() {
        None => PrimaryView::None,
        Some(node) => {
            let tl = if show_tl {
                node.role.as_primary().map(|h| h.timeline_id)
            } else {
                None
            };
            PrimaryView::Single(NodeView {
                display: extract_db_number(&node.node_name),
                timeline: tl,
            })
        }
    }
}

fn build_replicas_view(
    primary: Option<&AnalyzedNode>,
    backup_progress: &HashMap<String, u16>,
    nodes: &[AnalyzedNode],
    show_tl: bool,
) -> ReplicasView {
    let Some(primary_health) = primary.and_then(|p| p.role.as_primary()) else {
        return ReplicasView::None;
    };

    if primary_health.replication.is_empty() {
        return ReplicasView::None;
    }

    let grouped = group_connections_by_identity(&primary_health.replication);
    let replicas: Vec<ReplicaView> = grouped
        .into_iter()
        .map(|((app_name, _), conns)| {
            let normalized = normalize_application_name(&app_name);
            let tl = if show_tl {
                find_replica_timeline(&app_name, nodes)
            } else {
                None
            };
            let backup_lag = compute_backup_lag_display(&app_name, &conns, backup_progress);
            ReplicaView {
                node: NodeView {
                    display: normalized,
                    timeline: tl,
                },
                conn_count: conns.len(),
                backup_lag,
            }
        })
        .collect();

    if replicas.is_empty() {
        ReplicasView::None
    } else {
        ReplicasView::List(replicas)
    }
}

fn build_primary_replicas_for_critical(
    cluster: &AnalyzedCluster,
    reason: &Reason,
) -> (PrimaryView, ReplicasView) {
    match reason {
        Reason::NoPrimary => (PrimaryView::None, ReplicasView::None),
        Reason::SplitBrain(info) => {
            let show_tl = timelines_differ(&cluster.cluster.nodes);
            let true_tl = if show_tl {
                find_node_timeline(&info.true_primary, &cluster.cluster.nodes)
            } else {
                None
            };
            let stale: Vec<NodeView> = info
                .stale_primaries
                .iter()
                .map(|s| {
                    let tl = if show_tl {
                        find_node_timeline(s, &cluster.cluster.nodes)
                    } else {
                        None
                    };
                    NodeView {
                        display: extract_db_number(s),
                        timeline: tl,
                    }
                })
                .collect();
            let primary = PrimaryView::SplitBrain {
                true_primary: NodeView {
                    display: extract_db_number(&info.true_primary),
                    timeline: true_tl,
                },
                stale,
            };
            let replicas = build_split_brain_replicas(info);
            (primary, replicas)
        }
        Reason::WritesBlocked | Reason::WritesUnprotected => {
            let show_tl = timelines_differ(&cluster.cluster.nodes);
            let primary = build_primary_view(cluster, show_tl);
            (primary, ReplicasView::None)
        }
        _ => {
            let show_tl = timelines_differ(&cluster.cluster.nodes);
            let primary = build_primary_view(cluster, show_tl);
            let replicas = build_replicas_view(
                cluster.cluster.primary(),
                &cluster.backup_progress,
                &cluster.cluster.nodes,
                show_tl,
            );
            (primary, replicas)
        }
    }
}

fn build_split_brain_replicas(info: &SplitBrainInfo) -> ReplicasView {
    match &info.resolution {
        SplitBrainResolution::ReplicaFollowing {
            replicas_following_true,
        }
        | SplitBrainResolution::Both {
            replicas_following_true,
            ..
        }
        | SplitBrainResolution::ReplicaOverridesTimeline {
            replicas_following_true,
            ..
        } => {
            let true_primary_display = extract_db_number(&info.true_primary);
            let pairs: Vec<(NodeView, NodeView)> = replicas_following_true
                .iter()
                .map(|r| {
                    (
                        NodeView {
                            display: extract_db_number(r),
                            timeline: None,
                        },
                        NodeView {
                            display: true_primary_display.clone(),
                            timeline: None,
                        },
                    )
                })
                .collect();
            if pairs.is_empty() {
                ReplicasView::None
            } else {
                ReplicasView::SplitBrainFollowing(pairs)
            }
        }
        SplitBrainResolution::HigherTimeline { .. } | SplitBrainResolution::Indeterminate => {
            ReplicasView::None
        }
    }
}

fn build_reason_view(reason: &Reason) -> ReasonView {
    let (short, details_json) = format_reason(reason);
    ReasonView {
        short,
        details_json,
    }
}

fn log_degraded(cluster: &str, reason: &str, lag: u64) {
    tracing::warn!(
        cluster = %cluster,
        reason = %reason,
        lag_bytes = lag,
        "cluster degraded"
    );
}

fn log_critical(cluster: &str, reason: &Reason, reason_str: &str) {
    match reason {
        Reason::SplitBrain(info) => {
            tracing::error!(
                cluster = %cluster,
                reason = %reason_str,
                true_primary = %info.true_primary,
                stale_primaries = ?info.stale_primaries,
                resolution = ?info.resolution,
                "SPLIT BRAIN DETECTED"
            );
        }
        Reason::WritesBlocked => {
            tracing::error!(
                cluster = %cluster,
                reason = %reason_str,
                "writes blocked - no sync replicas available"
            );
        }
        Reason::WritesUnprotected => {
            tracing::error!(
                cluster = %cluster,
                reason = %reason_str,
                "writes unprotected - no replication redundancy"
            );
        }
        Reason::NoPrimary => {
            tracing::error!(
                cluster = %cluster,
                reason = %reason_str,
                "no primary found in cluster"
            );
        }
        _ => {
            tracing::error!(
                cluster = %cluster,
                reason = %reason_str,
                "cluster critical"
            );
        }
    }
}

fn timelines_differ(nodes: &[AnalyzedNode]) -> bool {
    let mut seen: Option<i32> = None;
    for node in nodes {
        let tl = match &node.role {
            crate::v2::scan::Role::Primary { health } => Some(health.timeline_id),
            crate::v2::scan::Role::Replica { health } => Some(health.timeline_id),
            _ => None,
        };
        if let Some(tl) = tl {
            match seen {
                None => seen = Some(tl),
                Some(prev) if prev != tl => return true,
                _ => {}
            }
        }
    }
    false
}

fn find_replica_timeline(app_name: &str, nodes: &[AnalyzedNode]) -> Option<i32> {
    let normalized = normalize_application_name(app_name);
    nodes
        .iter()
        .find(|n| {
            n.node_name
                .split('-')
                .find(|p| p.starts_with("db"))
                .and_then(|p| p.split('.').next())
                == Some(normalized.as_str())
        })
        .and_then(|n| match &n.role {
            crate::v2::scan::Role::Replica { health } => Some(health.timeline_id),
            _ => None,
        })
}

fn find_node_timeline(node_name: &str, nodes: &[AnalyzedNode]) -> Option<i32> {
    nodes
        .iter()
        .find(|n| n.node_name == node_name)
        .and_then(|n| match &n.role {
            crate::v2::scan::Role::Primary { health } => Some(health.timeline_id),
            crate::v2::scan::Role::Replica { health } => Some(health.timeline_id),
            _ => None,
        })
}

fn extract_db_number(node_name: &str) -> String {
    // Node naming: env-pg-appXXX-dbYYY.zone.example.com
    let Some(db_part) = node_name.split('-').find(|p| p.starts_with("db")) else {
        return node_name.to_string();
    };

    let mut parts = db_part.splitn(3, '.');
    match (parts.next(), parts.next()) {
        (Some(db_num), Some(zone)) => format!("{}@{}", db_num, zone),
        (Some(db_num), None) => db_num.to_string(),
        _ => node_name.to_string(),
    }
}

fn normalize_application_name(app_name: &str) -> String {
    // Application names are like: dev_pg_app001_db002
    if let Some(db_part) = app_name.split('_').next_back()
        && db_part.starts_with("db")
    {
        return db_part.to_string();
    }
    app_name.to_string()
}

fn extract_disk_info(cluster: &AnalyzedCluster) -> String {
    let mut total_io = 0u32;
    let mut total_fs = 0u32;
    let mut total_block = 0u32;
    let mut checked = 0;
    let mut failed = 0;

    for node in &cluster.cluster.nodes {
        match &node.disk_check {
            Some(DiskCheckOutcome::Checked(result)) => {
                checked += 1;
                total_io += result.io_errors;
                total_fs += result.filesystem_errors;
                total_block += result.block_errors;
            }
            Some(DiskCheckOutcome::Failed { .. }) => {
                failed += 1;
            }
            None => {}
        }
    }

    if checked == 0 && failed == 0 {
        return "-".to_string();
    }

    let total_errors = total_io + total_fs + total_block;

    if failed > 0 && checked == 0 {
        return format!("check failed ({})", failed);
    }

    if total_errors == 0 {
        return "ok".to_string();
    }

    let mut parts = Vec::new();
    if total_io > 0 {
        parts.push(format!("{}io", total_io));
    }
    if total_fs > 0 {
        parts.push(format!("{}fs", total_fs));
    }
    if total_block > 0 {
        parts.push(format!("{}blk", total_block));
    }

    parts.join(",")
}

fn format_reason(reason: &Reason) -> (String, String) {
    match reason {
        Reason::OneReplicaDown => ("OneReplicaDown".to_string(), "{}".to_string()),
        Reason::HighReplicationLag => ("HighReplicationLag".to_string(), "{}".to_string()),
        Reason::RebuildingReplica => ("RebuildingReplica".to_string(), "{}".to_string()),
        Reason::ChainedReplica {
            chained_replica,
            upstream_replica,
        } => {
            let short = format!(
                "ChainedReplica: {}→{}",
                extract_db_number(chained_replica),
                extract_db_number(upstream_replica)
            );
            let details = serde_json::json!({
                "chained_replica": chained_replica,
                "upstream_replica": upstream_replica
            })
            .to_string();
            (short, details)
        }
        Reason::NotInQuorum { replicas } => {
            let short = format!("NotInQuorum: {}", replicas.join(", "));
            let details = serde_json::json!({ "replicas": replicas }).to_string();
            (short, details)
        }
        Reason::NoPrimary => ("NoPrimary".to_string(), "{}".to_string()),
        Reason::SplitBrain(info) => {
            let resolution_str = match &info.resolution {
                SplitBrainResolution::HigherTimeline {
                    true_primary_timeline,
                    stale_timeline,
                } => format!("timeline {} > {}", true_primary_timeline, stale_timeline),
                SplitBrainResolution::ReplicaFollowing { .. } => "replica evidence".to_string(),
                SplitBrainResolution::Both {
                    true_primary_timeline,
                    stale_timeline,
                    ..
                } => format!(
                    "timeline {} > {} + replica",
                    true_primary_timeline, stale_timeline
                ),
                SplitBrainResolution::ReplicaOverridesTimeline {
                    true_primary_timeline,
                    stale_timeline,
                    ..
                } => format!(
                    "replica overrides timeline ({} < {})",
                    true_primary_timeline, stale_timeline
                ),
                SplitBrainResolution::Indeterminate => "indeterminate".to_string(),
            };
            let short = format!("SplitBrain: {}", resolution_str);
            let details = serde_json::json!({
                "true_primary": info.true_primary,
                "stale_primaries": info.stale_primaries,
                "resolution": format!("{:?}", info.resolution)
            })
            .to_string();
            (short, details)
        }
        Reason::WritesBlocked => ("WritesBlocked".to_string(), "{}".to_string()),
        Reason::WritesUnprotected => ("WritesUnprotected".to_string(), "{}".to_string()),
        Reason::ArchiveFailure {
            failed_count,
            last_failed_wal,
        } => {
            let short = format!("ArchiveFailure: {} failures", failed_count);
            let details = serde_json::json!({
                "failed_count": failed_count,
                "last_failed_wal": last_failed_wal
            })
            .to_string();
            (short, details)
        }
        Reason::NoNodesReachable => ("NoNodesReachable".to_string(), "{}".to_string()),
        Reason::UnexpectedTopology => ("UnexpectedTopology".to_string(), "{}".to_string()),
        Reason::DiskIoErrors {
            node,
            io_errors,
            block_errors,
        } => {
            let short = format!(
                "disk I/O errors on {} (io={}, blk={})",
                extract_db_number(node),
                io_errors,
                block_errors
            );
            (short, "{}".to_string())
        }
        Reason::FilesystemErrors { node, count } => {
            let short = format!(
                "filesystem errors on {} ({})",
                extract_db_number(node),
                count
            );
            (short, "{}".to_string())
        }
    }
}

type ConnectionKey = (String, Option<String>);

fn group_connections_by_identity(
    replication: &[ReplicationConnection],
) -> BTreeMap<ConnectionKey, Vec<&ReplicationConnection>> {
    let mut grouped: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for conn in replication {
        let key = (conn.application_name.clone(), conn.client_addr.clone());
        grouped.entry(key).or_default().push(conn);
    }
    grouped
}

fn compute_backup_lag_display(
    app_name: &str,
    conns: &[&ReplicationConnection],
    backup_progress: &HashMap<String, u16>,
) -> Option<String> {
    const BACKUP_APPS: &[&str] = &["pg_basebackup", "pg_dump", "pg_dumpall"];

    if !BACKUP_APPS.contains(&app_name) {
        return None;
    }

    let progress_from_prometheus = conns
        .iter()
        .filter_map(|c| {
            c.client_addr
                .as_ref()
                .and_then(|addr| backup_progress.get(addr))
        })
        .max();

    if let Some(&progress_pct_100) = progress_from_prometheus {
        let pct = progress_pct_100 as f64 / 100.0;
        return Some(format!(" ~{:.1}%", pct));
    }

    conns
        .iter()
        .filter_map(|c| c.replay_lag.as_deref())
        .filter_map(parse_lag_to_bytes)
        .max()
        .map(|lag| format!(" ~{} behind", format_bytes(lag)))
}
