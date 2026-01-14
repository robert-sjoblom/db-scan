use std::{
    fs::File,
    io::{BufWriter, IsTerminal, Write},
    path::Path,
};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::v2::analyze::{
    AnalyzedCluster, ClusterHealth, Reason, SplitBrainInfo, SplitBrainResolution,
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

/// A row of output data extracted from ClusterHealth
#[derive(Debug)]
struct OutputRow {
    status: Status,
    cluster: String,
    primary: String,
    replicas: String,
    lag: Option<u64>,
    reason: String,
    details_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Status {
    Critical = 0,
    Degraded = 1,
    Unknown = 2,
    Healthy = 3,
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

/// CSV writer that streams rows as they arrive
struct CsvWriter {
    writer: BufWriter<File>,
}

impl CsvWriter {
    fn new(path: &str) -> std::io::Result<Self> {
        let file = File::create(Path::new(path))?;
        let mut writer = BufWriter::new(file);
        // Write header
        writeln!(
            writer,
            "status,cluster,primary,replicas,lag_bytes,reason,details_json"
        )?;
        Ok(Self { writer })
    }

    fn write_row(&mut self, row: &OutputRow) -> std::io::Result<()> {
        writeln!(
            self.writer,
            "{},{},{},{},{},{},\"{}\"",
            row.status.as_str(),
            row.cluster,
            row.primary,
            row.replicas,
            row.lag.map(|l| l.to_string()).unwrap_or_default(),
            row.reason,
            row.details_json.replace('"', "\"\"")
        )
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// Collects ClusterHealth results, streams to CSV, returns terminal output string.
///
/// Returns a formatted string for terminal display. The caller should print this
/// after all other logging is complete.
pub async fn write_results(
    mut analyze_rx: UnboundedReceiver<ClusterHealth>,
    options: WriterOptions,
) -> String {
    let mut rows: Vec<OutputRow> = Vec::new();

    // Initialize CSV writer if path provided
    let mut csv_writer = options
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
        if let Some(row) = extract_row(&health, &options) {
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
        } else if let Some(ref path) = options.csv_path {
            tracing::info!(path = %path, "CSV written successfully");
        }
    }

    // Sort by severity (Critical first, then Degraded, Unknown, Healthy)
    rows.sort_by(|a, b| a.status.cmp(&b.status));

    build_terminal_output(&rows, &options)
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
            Some(OutputRow {
                status: Status::Degraded,
                cluster: cluster.name().to_string(),
                primary: format_primary_with_failover(&primary, cluster),
                replicas,
                lag: Some(*lag),
                reason: reason_str,
                details_json: details,
            })
        }
        ClusterHealth::Critical { cluster, reason } => {
            let (primary, replicas) = extract_primary_and_replicas_for_critical(cluster, reason);
            let (reason_str, details) = format_reason(reason);
            Some(OutputRow {
                status: Status::Critical,
                cluster: cluster.name().to_string(),
                primary,
                replicas,
                lag: None,
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
            Some(OutputRow {
                status: Status::Unknown,
                cluster: cluster.name().to_string(),
                primary: "-".to_string(),
                replicas: format!("?/2 ({} reachable)", reachable_nodes),
                lag: None,
                reason: reason_str,
                details_json: details,
            })
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

    let replicas = get_connected_replicas(primary_node);

    (primary_short, replicas)
}

/// Get formatted list of connected replicas from a primary node.
///
/// Returns a comma-separated list of replica db numbers (e.g., "db002,db003")
/// or "-" if no primary or no connected replicas.
fn get_connected_replicas(primary: Option<&crate::v2::scan::AnalyzedNode>) -> String {
    primary
        .and_then(|p| p.role.as_primary())
        .map(|health| format_replica_list(&health.replication))
        .unwrap_or_else(|| "-".to_string())
}

/// Format a list of replication connections as a comma-separated string of db numbers.
/// Deduplicates connections with the same application_name and client_addr.
/// Shows lag for backup operations (pg_basebackup, etc.)
fn format_replica_list(
    replication: &[crate::v2::scan::health_check_primary::ReplicationConnection],
) -> String {
    use std::collections::HashMap;

    // Group by (application_name, client_addr) and collect connections
    let mut grouped: HashMap<
        (String, Option<String>),
        Vec<&crate::v2::scan::health_check_primary::ReplicationConnection>,
    > = HashMap::new();
    let mut order: Vec<(String, Option<String>)> = Vec::new();

    for conn in replication {
        let key = (conn.application_name.clone(), conn.client_addr.clone());
        if !grouped.contains_key(&key) {
            order.push(key.clone());
        }
        grouped.entry(key).or_default().push(conn);
    }

    let connected: Vec<String> = order
        .iter()
        .map(|(app_name, client_addr)| {
            let normalized = normalize_application_name(app_name);
            let conns = &grouped[&(app_name.clone(), client_addr.clone())];
            let count = conns.len();

            // For backup operations, show lag based on time (more accurate for backup progress)
            let is_backup = matches!(
                app_name.as_str(),
                "pg_basebackup" | "pg_dump" | "pg_dumpall"
            );
            let lag_info = if is_backup {
                // For pg_basebackup, use time-based replay_lag to estimate data volume
                // LSN differences don't represent actual backup progress accurately
                let max_lag = conns
                    .iter()
                    .filter_map(|c| c.replay_lag.as_deref())
                    .filter_map(parse_lag_to_bytes)
                    .max();

                max_lag.map(|lag| format!(" ~{} behind", format_bytes(lag)))
            } else {
                None
            };

            if count > 1 {
                format!("{}(×{}{})", normalized, count, lag_info.unwrap_or_default())
            } else {
                format!("{}{}", normalized, lag_info.unwrap_or_default())
            }
        })
        .collect();

    if connected.is_empty() {
        "-".to_string()
    } else {
        connected.join(",")
    }
}

/// Parse PostgreSQL interval lag to estimated bytes
/// Used for backup operations where time-based lag is more accurate than LSN diff
fn parse_lag_to_bytes(lag: &str) -> Option<u64> {
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

/// Extract db number (e.g., "db002") from full node name
fn extract_db_number(node_name: &str) -> String {
    // Node naming: env-pg-appXXX-dbYYY.zone.example.com
    if let Some(db_part) = node_name.split('-').find(|p| p.starts_with("db")) {
        if let Some(dot_pos) = db_part.find('.') {
            return db_part[..dot_pos].to_string();
        }
        return db_part.to_string();
    }
    node_name.to_string()
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

    let use_color = !options.no_color && std::io::stdout().is_terminal();

    // Calculate column widths
    let mut max_cluster = "CLUSTER".len();
    let mut max_primary = "PRIMARY".len();
    let mut max_replicas = "REPLICAS".len();
    let mut max_lag = "LAG".len();
    let mut max_reason = "REASON".len();

    for row in rows {
        max_cluster = max_cluster.max(row.cluster.len());
        max_primary = max_primary.max(row.primary.len());
        max_replicas = max_replicas.max(row.replicas.len());
        max_lag = max_lag.max(format_lag(row.lag).len());
        max_reason = max_reason.max(row.reason.len());
    }

    let mut output = String::new();

    // Header
    output.push_str(&format!(
        "{:<8} {:<width_cluster$} {:<width_primary$} {:<width_replicas$} {:<width_lag$} {}\n",
        "STATUS",
        "CLUSTER",
        "PRIMARY",
        "REPLICAS",
        "LAG",
        "REASON",
        width_cluster = max_cluster,
        width_primary = max_primary,
        width_replicas = max_replicas,
        width_lag = max_lag,
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
            "{:<status_padding$} {:<width_cluster$} {:<width_primary$} {:<width_replicas$} {:<width_lag$} {}\n",
            status_str,
            row.cluster,
            row.primary,
            row.replicas,
            format_lag(row.lag),
            row.reason,
            status_padding = status_padding,
            width_cluster = max_cluster,
            width_primary = max_primary,
            width_replicas = max_replicas,
            width_lag = max_lag,
        ));
    }

    output
}

/// Determine if a healthy cluster should be shown based on options
fn should_show_healthy_cluster(options: &WriterOptions, failover: bool) -> bool {
    options.show_healthy || (failover && options.show_failover)
}
