mod build;
mod csv;
mod terminal;
pub mod units;
mod view;

use std::{collections::HashSet, sync::Arc};

use tokio::sync::mpsc::UnboundedReceiver;

use crate::{pipeline::PipelineContext, v2::analyze::ClusterHealth};

/// Output options for the writer.
#[derive(Debug, Default)]
pub struct WriterOptions {
    /// Show healthy clusters (default: false).
    pub show_healthy: bool,
    /// Show healthy clusters that have experienced failover (default: false).
    pub show_failover: bool,
    /// Path to write CSV output (optional).
    pub csv_path: Option<String>,
    /// Disable colors in terminal output.
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

/// Collects `ClusterHealth` results, streams to CSV, returns scan result.
pub async fn write_results(
    ctx: Arc<PipelineContext>,
    mut analyze_rx: UnboundedReceiver<ClusterHealth>,
) -> ScanResult {
    let mut views: Vec<view::ClusterView> = Vec::new();
    let mut clusters_to_rescan: HashSet<String> = HashSet::new();

    let mut csv = csv::CsvWriter::new(ctx.writer_options.csv_path.as_deref());

    while let Some(health) = analyze_rx.recv().await {
        clusters_to_rescan.extend(cluster_to_rescan(&health));

        if should_display(&health, &ctx.writer_options) {
            let view = build::build_cluster_view(&health);
            csv.write_row(&view);
            views.push(view);
        }
    }

    csv.flush();

    views.sort();

    let output = terminal::render_table(&views, &ctx.writer_options);
    ScanResult {
        output,
        clusters_to_rescan,
    }
}

fn cluster_to_rescan(health: &ClusterHealth) -> Option<String> {
    match health {
        ClusterHealth::Healthy { .. } => None,
        ClusterHealth::Degraded { cluster, .. }
        | ClusterHealth::Critical { cluster, .. }
        | ClusterHealth::Unknown { cluster, .. } => Some(cluster.name().to_owned()),
    }
}

fn should_display(health: &ClusterHealth, options: &WriterOptions) -> bool {
    match health {
        ClusterHealth::Healthy { failover, .. } => {
            options.show_healthy || (*failover && options.show_failover)
        }
        ClusterHealth::Degraded { .. }
        | ClusterHealth::Critical { .. }
        | ClusterHealth::Unknown { .. } => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        units::{display_width, to_superscript},
        view::{ClusterView, NodeView, PrimaryView, ReasonView, RenderMode, ReplicasView, Status},
    };

    #[test]
    fn test_to_superscript() {
        assert_eq!(to_superscript(0), "\u{2070}");
        assert_eq!(to_superscript(7), "\u{2077}");
        assert_eq!(to_superscript(10), "\u{b9}\u{2070}");
        assert_eq!(to_superscript(19), "\u{b9}\u{2079}");
    }

    #[test]
    fn test_format_node_sigils() {
        let n = NodeView {
            display: "db002@sto1".to_owned(),
            timeline: Some(7),
        };
        assert_eq!(n.render(RenderMode::WithSigils), "db002@sto1\u{2077}");

        let n = NodeView {
            display: "db002".to_owned(),
            timeline: Some(10),
        };
        assert_eq!(n.render(RenderMode::WithSigils), "db002\u{b9}\u{2070}");

        let n = NodeView {
            display: "db002".to_owned(),
            timeline: None,
        };
        assert_eq!(n.render(RenderMode::WithSigils), "db002");
    }

    #[test]
    fn test_display_width_with_sigils() {
        let n = NodeView {
            display: "db002".to_owned(),
            timeline: Some(7),
        };
        let s = n.render(RenderMode::WithSigils);
        assert_eq!(s.len(), 8); // byte count
        assert_eq!(display_width(&s), 6); // column count
        assert_eq!(display_width("db002"), 5);
        assert_eq!(display_width("db002\u{2077}*"), 7); // hypothetical, if * were added
    }

    #[test]
    fn test_output_row_ordering() {
        let make = |status: Status, name: &str| ClusterView {
            status,
            name: name.to_owned(),
            primary: PrimaryView::Single(NodeView {
                display: "db001".to_owned(),
                timeline: None,
            }),
            replicas: ReplicasView::None,
            lag_bytes: None,
            disk: "-".to_owned(),
            reason: ReasonView {
                short: "-".to_owned(),
                details_json: "{}".to_owned(),
            },
            failover: false,
        };

        let mut views = [
            make(Status::Critical, "cluster_a"),
            make(Status::Healthy, "cluster_z"),
            make(Status::Degraded, "cluster_b"),
            make(Status::Healthy, "cluster_a"),
            make(Status::Unknown, "cluster_c"),
            make(Status::Critical, "cluster_b"),
        ];

        views.sort();

        assert_eq!(views[0].status, Status::Healthy);
        assert_eq!(views[0].name, "cluster_a");

        assert_eq!(views[1].status, Status::Healthy);
        assert_eq!(views[1].name, "cluster_z");

        assert_eq!(views[2].status, Status::Unknown);
        assert_eq!(views[2].name, "cluster_c");

        assert_eq!(views[3].status, Status::Degraded);
        assert_eq!(views[3].name, "cluster_b");

        assert_eq!(views[4].status, Status::Critical);
        assert_eq!(views[4].name, "cluster_a");

        assert_eq!(views[5].status, Status::Critical);
        assert_eq!(views[5].name, "cluster_b");
    }
}
