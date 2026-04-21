mod csv;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::IsTerminal,
    sync::Arc,
};

use csv::CsvWriter;

use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    pipeline::PipelineContext,
    v2::{
        analyze::{AnalyzedCluster, ClusterHealth, Reason, SplitBrainInfo, SplitBrainResolution},
        scan::{disk_check::DiskCheckOutcome, health_check_primary::ReplicationConnection},
    },
};

/// ANSI color codes for terminal output
mod colors {
    pub const RED: &str = "\x1b[31m";
    pub const YELLOW: &str = "\x1b[33m";
    pub const GREEN: &str = "\x1b[32m";
    pub const GRAY: &str = "\x1b[90m";
    pub const RESET: &str = "\x1b[0m";
}

/// Output options for the writer
#[derive(Debug, Default)]
pub struct WriterOptions {
    /// Show healthy clusters (default: false)
    pub show_healthy: bool,
    /// Show healthy clusters that have experienced failover (default: false)
    pub show_failover: bool,
    /// Path to write CSV output (optional)
    pub csv_path: Option<String>,
    /// Disable colors in terminal output
    pub no_color: bool,
}

/// Result of a scan, containing both the formatted output and clusters to rescan.
#[derive(Debug)]
pub struct ScanResult {
    /// Formatted terminal output string.
    pub output: String,
    /// Cluster names that need to be rescanned (Degraded, Critical, or Unknown).
    pub clusters_to_rescan: HashSet<String>,
}

/// A row of output data extracted from ClusterHealth
#[derive(Debug, Eq, PartialEq)]
pub(super) struct OutputRow {
    status: Status,
    cluster: String,
    primary: String,
    replicas: String,
    lag: Option<u64>,
    disk: String,
    reason: String,
    details_json: String,
}

impl Ord for OutputRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.status
            .cmp(&other.status)
            .then_with(|| self.cluster.cmp(&other.cluster))
    }
}

impl PartialOrd for OutputRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Status {
    Healthy = 0,
    Unknown = 1,
    Degraded = 2,
    Critical = 3,
}

impl Status {
    fn as_str(&self) -> &'static str {
        match self {
            Status::Critical => "CRITICAL",
            Status::Degraded => "DEGRADED",
            Status::Unknown => "UNKNOWN",
            Status::Healthy => "HEALTHY",
        }
    }

    fn color(&self) -> &'static str {
        match self {
            Status::Critical => colors::RED,
            Status::Degraded => colors::YELLOW,
            Status::Unknown => colors::GRAY,
            Status::Healthy => colors::GREEN,
        }
    }
}

/// Collects ClusterHealth results, streams to CSV, returns scan result.
///
/// Returns a `ScanResult` containing both the formatted terminal output and
/// the set of cluster names that need rescanning (Degraded, Critical, or Unknown).
pub async fn write_results(
    ctx: Arc<PipelineContext>,
    mut analyze_rx: UnboundedReceiver<ClusterHealth>,
) -> ScanResult {
    let mut rows: Vec<OutputRow> = Vec::new();
    let mut clusters_to_rescan: HashSet<String> = HashSet::new();

    // Initialize CSV writer if path provided
    let mut csv_writer =
        ctx.writer_options
            .csv_path
            .as_ref()
            .and_then(|path| match CsvWriter::new(path) {
                Ok(w) => Some(w),
                Err(e) => {
                    tracing::error!(path = %path, error = %e, "failed to create CSV file");
                    None
                }
            });

    // Collect results, streaming to CSV as they arrive
    while let Some(health) = analyze_rx.recv().await {
        // Track unhealthy clusters for watch mode rescanning
        clusters_to_rescan.extend(cluster_to_rescan(&health));

        if let Some(row) = extract_row(&health, &ctx.writer_options) {
            // Write to CSV immediately
            if let Some(ref mut writer) = csv_writer
                && let Err(e) = writer.write_row(&row)
            {
                tracing::error!(error = %e, "failed to write CSV row");
            }
            rows.push(row);
        }
    }

    // Flush and close CSV
    if let Some(ref mut writer) = csv_writer {
        if let Err(e) = writer.flush() {
            tracing::error!(error = %e, "failed to flush CSV");
        } else if let Some(ref path) = ctx.writer_options.csv_path {
            tracing::info!(path = %path, "CSV written successfully");
        }
    }

    // Sort by severity (Healthy first, then Unknown, Degraded, Critical), then cluster alphabetically
    rows.sort();

    let output = build_terminal_output(&rows, &ctx.writer_options);
    ScanResult {
        output,
        clusters_to_rescan,
    }
}

/// Returns the cluster name if it should be rescanned, None otherwise.
fn cluster_to_rescan(health: &ClusterHealth) -> Option<String> {
    match health {
        ClusterHealth::Healthy { .. } => None,
        ClusterHealth::Degraded { cluster, .. }
        | ClusterHealth::Critical { cluster, .. }
        | ClusterHealth::Unknown { cluster, .. } => Some(cluster.name().to_string()),
    }
}

/// Extract an OutputRow from ClusterHealth, returning None if it should be filtered out
fn extract_row(health: &ClusterHealth, options: &WriterOptions) -> Option<OutputRow> {
    match health {
        ClusterHealth::Healthy { failover, cluster } => {
            if !should_show_healthy_cluster(options, *failover) {
                return None;
            }
            let (primary, replicas) = extract_primary_and_replicas(cluster);
            Some(OutputRow {
                status: Status::Healthy,
                cluster: cluster.name().to_string(),
                primary: if *failover {
                    format!("{} (failover)", primary)
                } else {
                    primary
                },
                replicas,
                lag: None,
                disk: "-".to_string(),
                reason: if *failover {
                    "Failover".to_string()
                } else {
                    "-".to_string()
                },
                details_json: "{}".to_string(),
            })
        }
        ClusterHealth::Degraded {
            lag,
            cluster,
            reason,
        } => {
            let (primary, replicas) = extract_primary_and_replicas(cluster);
            let (reason_str, details) = format_reason(reason);
            let disk = extract_disk_info(cluster);
            log_degraded(cluster.name(), &reason_str, *lag);
            Some(OutputRow {
                status: Status::Degraded,
                cluster: cluster.name().to_string(),
                primary: format_primary_with_failover(&primary, cluster),
                replicas,
                lag: Some(*lag),
                disk,
                reason: reason_str,
                details_json: details,
            })
        }
        ClusterHealth::Critical { cluster, reason } => {
            let (primary, replicas) = extract_primary_and_replicas_for_critical(cluster, reason);
            let (reason_str, details) = format_reason(reason);
            let disk = extract_disk_info(cluster);
            log_critical(cluster.name(), reason, &reason_str);
            Some(OutputRow {
                status: Status::Critical,
                cluster: cluster.name().to_string(),
                primary,
                replicas,
                lag: None,
                disk,
                reason: reason_str,
                details_json: details,
            })
        }
        ClusterHealth::Unknown {
            cluster,
            reachable_nodes,
            reason,
        } => {
            let (reason_str, details) = format_reason(reason);
            let disk = extract_disk_info(cluster);
            tracing::warn!(
                cluster = %cluster.name(),
                reachable_nodes = reachable_nodes,
                reason = %reason_str,
                "cluster state unknown"
            );
            Some(OutputRow {
                status: Status::Unknown,
                cluster: cluster.name().to_string(),
                primary: "-".to_string(),
                replicas: format!("?/2 ({} reachable)", reachable_nodes),
                lag: None,
                disk,
                reason: reason_str,
                details_json: details,
            })
        }
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

/// Extract primary node short name and replica info from cluster
fn extract_primary_and_replicas(cluster: &AnalyzedCluster) -> (String, String) {
    // Find the primary
    let primary_node = cluster.cluster.primary();

    let primary_short = primary_node
        .map(|n| extract_db_number(&n.node_name))
        .unwrap_or_else(|| "(none)".to_string());

    let replicas = get_connected_replicas(primary_node, &cluster.backup_progress);

    (primary_short, replicas)
}

/// Get formatted list of connected replicas from a primary node.
///
/// Returns a comma-separated list of replica db numbers (e.g., "db002,db003")
/// or "-" if no primary or no connected replicas.
fn get_connected_replicas(
    primary: Option<&crate::v2::scan::AnalyzedNode>,
    backup_progress: &HashMap<String, u16>,
) -> String {
    primary
        .and_then(|p| p.role.as_primary())
        .map(|health| format_replica_list(&health.replication, backup_progress))
        .unwrap_or_else(|| "-".to_string())
}

/// Format a list of replication connections as a comma-separated string of db numbers.
/// Deduplicates connections with the same application_name and client_addr.
/// Shows lag for backup operations (pg_basebackup, etc.)
/// Output is sorted by (application_name, client_addr) for deterministic results.
fn format_replica_list(
    replication: &[crate::v2::scan::health_check_primary::ReplicationConnection],
    backup_progress: &HashMap<String, u16>,
) -> String {
    if replication.is_empty() {
        return "-".to_string();
    }

    let grouped = group_connections_by_identity(replication);

    let formatted: Vec<String> = grouped
        .into_iter()
        .map(|((app_name, _), conns)| format_replica_entry(&app_name, &conns, backup_progress))
        .collect();

    if formatted.is_empty() {
        "-".to_string()
    } else {
        formatted.join(",")
    }
}

type ConnectionKey = (String, Option<String>);

/// Group connections by (application_name, client_addr), sorted for deterministic output.
fn group_connections_by_identity(
    replication: &[ReplicationConnection],
) -> std::collections::BTreeMap<ConnectionKey, Vec<&ReplicationConnection>> {
    let mut grouped: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for conn in replication {
        let key = (conn.application_name.clone(), conn.client_addr.clone());
        grouped.entry(key).or_default().push(conn);
    }
    grouped
}

/// Format a single replica entry with optional count and backup lag info.
fn format_replica_entry(
    app_name: &str,
    conns: &[&ReplicationConnection],
    backup_progress: &HashMap<String, u16>,
) -> String {
    let normalized = normalize_application_name(app_name);
    let count = conns.len();
    let lag_info = compute_backup_lag_display(app_name, conns, backup_progress);

    match (count > 1, lag_info) {
        (true, Some(lag)) => format!("{}(×{}{})", normalized, count, lag),
        (true, None) => format!("{}(×{})", normalized, count),
        (false, Some(lag)) => format!("{}{}", normalized, lag),
        (false, None) => normalized,
    }
}

/// For backup operations, compute a human-readable lag estimate.
/// Returns None for non-backup operations or if no lag data is available.
/// When prometheus feature is enabled and backup progress is available, shows actual progress.
fn compute_backup_lag_display(
    app_name: &str,
    conns: &[&ReplicationConnection],
    backup_progress: &HashMap<String, u16>,
) -> Option<String> {
    const BACKUP_APPS: &[&str] = &["pg_basebackup", "pg_dump", "pg_dumpall"];

    if !BACKUP_APPS.contains(&app_name) {
        return None;
    }

    // Try to use actual backup progress from Prometheus (when available)
    // Progress is stored as percentage * 100 (e.g., 4156 = 41.56%)
    // The HashMap is keyed by client_addr (IP address) for consistent lookup
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

    // Fallback: use time-based estimate from replay_lag
    // Note: replay_lag shows time since backup started, NOT actual backup progress.
    // pg_stat_replication doesn't track file copy progress, only WAL streaming lag.
    // This is a rough estimate based on time elapsed since backup connection began.
    conns
        .iter()
        .filter_map(|c| c.replay_lag.as_deref())
        .filter_map(parse_lag_to_bytes)
        .max()
        .map(|lag| format!(" ~{} behind", format_bytes(lag)))
}

/// Parse PostgreSQL interval lag to estimated bytes
/// Used for backup operations where time-based lag is more accurate than LSN diff
pub fn parse_lag_to_bytes(lag: &str) -> Option<u64> {
    // Format: HH:MM:SS.microseconds
    let parts: Vec<&str> = lag.split(':').collect();
    if parts.len() != 3 {
        return None;
    }

    let hours: u64 = parts[0].parse().ok()?;
    let minutes: u64 = parts[1].parse().ok()?;
    let seconds_parts: Vec<&str> = parts[2].split('.').collect();
    let seconds: u64 = seconds_parts[0].parse().ok()?;

    let total_seconds = hours * 3600 + minutes * 60 + seconds;

    // Rough estimate: 16MB/s WAL generation rate
    Some(total_seconds * 16_000_000)
}

/// Format bytes in a human-readable format (KB, MB, GB)
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

/// Extract primary and replicas for Critical states (may have split-brain)
fn extract_primary_and_replicas_for_critical(
    cluster: &AnalyzedCluster,
    reason: &Reason,
) -> (String, String) {
    match reason {
        Reason::NoPrimary => ("(none)".to_string(), "-".to_string()),
        Reason::SplitBrain(info) => {
            let primary = format!(
                "{} vs {}",
                extract_db_number(&info.true_primary),
                info.stale_primaries
                    .iter()
                    .map(|s| extract_db_number(s))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let replicas = format_split_brain_replicas(info);
            (primary, replicas)
        }
        Reason::WritesBlocked | Reason::WritesUnprotected => {
            let (primary, _) = extract_primary_and_replicas(cluster);
            (primary, "-".to_string())
        }
        _ => extract_primary_and_replicas(cluster),
    }
}

/// Format replica info for split-brain scenario
fn format_split_brain_replicas(info: &SplitBrainInfo) -> String {
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
            let replicas: Vec<String> = replicas_following_true
                .iter()
                .map(|r| {
                    format!(
                        "{}→{}",
                        extract_db_number(r),
                        extract_db_number(&info.true_primary)
                    )
                })
                .collect();
            replicas.join(",")
        }
        SplitBrainResolution::HigherTimeline { .. } | SplitBrainResolution::Indeterminate => {
            "-".to_string()
        }
    }
}

/// Format the primary field, adding (failover) if not db001
fn format_primary_with_failover(primary: &str, cluster: &AnalyzedCluster) -> String {
    let primary_node = cluster.cluster.primary();

    if let Some(node) = primary_node
        && !node.node_name.contains("-db001")
    {
        return format!("{} (failover)", primary);
    }
    primary.to_string()
}

/// Extract db number with zone (e.g., "db002@sto1") from full node name
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

/// Normalize application_name from pg_stat_replication to db number
/// e.g., "dev_pg_app001_db002" -> "db002"
fn normalize_application_name(app_name: &str) -> String {
    // Application names are like: dev_pg_app001_db002
    if let Some(db_part) = app_name.split('_').next_back()
        && db_part.starts_with("db")
    {
        return db_part.to_string();
    }
    app_name.to_string()
}

/// Extract disk check summary from cluster nodes
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

/// Format reason enum to (short_string, json_details)
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
        Reason::NoPrimary => ("NoPrimary".to_string(), "{}".to_string()),
        Reason::SplitBrain(info) => {
            let resolution_str = match &info.resolution {
                SplitBrainResolution::HigherTimeline {
                    true_primary_timeline,
                    stale_timeline,
                } => {
                    format!("timeline {} > {}", true_primary_timeline, stale_timeline)
                }
                SplitBrainResolution::ReplicaFollowing { .. } => "replica evidence".to_string(),
                SplitBrainResolution::Both {
                    true_primary_timeline,
                    stale_timeline,
                    ..
                } => {
                    format!(
                        "timeline {} > {} + replica",
                        true_primary_timeline, stale_timeline
                    )
                }
                SplitBrainResolution::ReplicaOverridesTimeline {
                    true_primary_timeline,
                    stale_timeline,
                    ..
                } => {
                    format!(
                        "replica overrides timeline ({} < {})",
                        true_primary_timeline, stale_timeline
                    )
                }
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
    }
}

/// Format lag in human-readable form
fn format_lag(lag: Option<u64>) -> String {
    match lag {
        None => "-".to_string(),
        Some(0) => "0B".to_string(),
        Some(bytes) => {
            if bytes >= 1_000_000_000 {
                format!("{:.1}GB", bytes as f64 / 1_000_000_000.0)
            } else if bytes >= 1_000_000 {
                format!("{:.0}MB", bytes as f64 / 1_000_000.0)
            } else if bytes >= 1_000 {
                format!("{:.0}KB", bytes as f64 / 1_000.0)
            } else {
                format!("{}B", bytes)
            }
        }
    }
}

/// Build tab-separated terminal output string with colors
fn build_terminal_output(rows: &[OutputRow], options: &WriterOptions) -> String {
    if rows.is_empty() {
        return "No clusters to display.".to_string();
    }

    let has_disk_info = rows.iter().any(|r| r.disk != "-");

    let use_color = !options.no_color && std::io::stdout().is_terminal();

    // Calculate column widths
    let mut max_cluster = "CLUSTER".len();
    let mut max_primary = "PRIMARY".len();
    let mut max_replicas = "REPLICAS".len();
    let mut max_lag = "LAG".len();
    let mut max_disk = "DISK".len();
    let mut max_reason = "REASON".len();

    for row in rows {
        max_cluster = max_cluster.max(row.cluster.len());
        max_primary = max_primary.max(row.primary.len());
        max_replicas = max_replicas.max(row.replicas.len());
        max_lag = max_lag.max(format_lag(row.lag).len());
        max_disk = max_disk.max(row.disk.len());
        max_reason = max_reason.max(row.reason.len());
    }

    let mut output = String::new();

    // Header
    output.push_str(&format!(
        "{:<8} {:<width_cluster$} {:<width_primary$} {:<width_replicas$} {:<width_lag$} {:<width_disk$} {}\n",
        "STATUS",
        "CLUSTER",
        "PRIMARY",
        "REPLICAS",
        "LAG",
        "DISK",
        "REASON",
        width_cluster = max_cluster,
        width_primary = max_primary,
        width_replicas = max_replicas,
        width_lag = max_lag,
        width_disk = max_disk,
    ));

    // Rows
    for row in rows {
        let status_str = if use_color {
            format!(
                "{}{}{}",
                row.status.color(),
                row.status.as_str(),
                colors::RESET
            )
        } else {
            row.status.as_str().to_string()
        };

        // Add padding for color codes (they don't count toward visible width)
        let status_padding = if use_color {
            8 + row.status.color().len() + colors::RESET.len()
        } else {
            8
        };

        output.push_str(&format!(
            "{:<status_padding$} {:<width_cluster$} {:<width_primary$} {:<width_replicas$} {:<width_lag$} {:<width_disk$} {}\n",
            status_str,
            row.cluster,
            row.primary,
            row.replicas,
            format_lag(row.lag),
            row.disk,
            row.reason,
            status_padding = status_padding,
            width_cluster = max_cluster,
            width_primary = max_primary,
            width_replicas = max_replicas,
            width_lag = max_lag,
            width_disk = max_disk,
        ));
    }

    // Add disk legend if any disk info was shown
    if has_disk_info {
        output.push('\n');
        output.push_str("DISK: io=I/O errors, fs=filesystem errors, blk=block device errors\n");
    }

    output
}

/// Determine if a healthy cluster should be shown based on options
fn should_show_healthy_cluster(options: &WriterOptions, failover: bool) -> bool {
    options.show_healthy || (failover && options.show_failover)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_row_ordering() {
        let mut rows = [
            OutputRow {
                status: Status::Critical,
                cluster: "cluster_a".to_string(),
                primary: "db001".to_string(),
                replicas: "db002".to_string(),
                lag: None,
                disk: "-".to_string(),
                reason: "NoPrimary".to_string(),
                details_json: "{}".to_string(),
            },
            OutputRow {
                status: Status::Healthy,
                cluster: "cluster_z".to_string(),
                primary: "db001".to_string(),
                replicas: "db002".to_string(),
                lag: None,
                disk: "-".to_string(),
                reason: "-".to_string(),
                details_json: "{}".to_string(),
            },
            OutputRow {
                status: Status::Degraded,
                cluster: "cluster_b".to_string(),
                primary: "db001".to_string(),
                replicas: "db002".to_string(),
                lag: Some(1000),
                disk: "3io".to_string(),
                reason: "HighReplicationLag".to_string(),
                details_json: "{}".to_string(),
            },
            OutputRow {
                status: Status::Healthy,
                cluster: "cluster_a".to_string(),
                primary: "db001".to_string(),
                replicas: "db002".to_string(),
                lag: None,
                disk: "-".to_string(),
                reason: "-".to_string(),
                details_json: "{}".to_string(),
            },
            OutputRow {
                status: Status::Unknown,
                cluster: "cluster_c".to_string(),
                primary: "-".to_string(),
                replicas: "?/2 (1 reachable)".to_string(),
                lag: None,
                disk: "ok".to_string(),
                reason: "NoNodesReachable".to_string(),
                details_json: "{}".to_string(),
            },
            OutputRow {
                status: Status::Critical,
                cluster: "cluster_b".to_string(),
                primary: "db001".to_string(),
                replicas: "-".to_string(),
                lag: None,
                disk: "-".to_string(),
                reason: "WritesBlocked".to_string(),
                details_json: "{}".to_string(),
            },
        ];

        rows.sort();

        assert_eq!(rows[0].status, Status::Healthy);
        assert_eq!(rows[0].cluster, "cluster_a");

        assert_eq!(rows[1].status, Status::Healthy);
        assert_eq!(rows[1].cluster, "cluster_z");

        assert_eq!(rows[2].status, Status::Unknown);
        assert_eq!(rows[2].cluster, "cluster_c");

        assert_eq!(rows[3].status, Status::Degraded);
        assert_eq!(rows[3].cluster, "cluster_b");

        assert_eq!(rows[4].status, Status::Critical);
        assert_eq!(rows[4].cluster, "cluster_a");

        assert_eq!(rows[5].status, Status::Critical);
        assert_eq!(rows[5].cluster, "cluster_b");
    }
}
