use std::{collections::HashMap, path::PathBuf, sync::OnceLock, time::Instant};

use clap::Parser;
use redact::Secret;
use tracing::instrument;
use tracing_subscriber::EnvFilter;

use crate::{
    prometheus::FileSystemMetrics,
    timings::{Event, Stage},
    v2::{analyze::ClusterHealth, cluster::Cluster, node::Node, scan::AnalyzedNode},
};

mod database_portal;
mod logging;
mod prometheus;
mod timings;
mod v2;

static ARGS: OnceLock<Args> = OnceLock::new();

/// A tool to scan PostgreSQL clusters for configuration and health
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Your PG User
    #[arg(long, env = "PGUSER")]
    pguser: String,

    /// Your PG password
    #[arg(long, env = "PGPASSWORD", hide = true)]
    pgpassword: Secret<String>,

    /// Your ssl key file
    #[arg(long, env = "PGSSLKEY")]
    pgsslkey: PathBuf,

    /// Your ssl cert file
    #[arg(long, env = "PGSSLCERT")]
    pgsslcert: PathBuf,

    /// Your ssl root cert file
    #[arg(long, env = "PGSSLROOTCERT")]
    pgsslrootcert: PathBuf,

    /// Cluster to scan
    #[arg(short, long)]
    cluster: Option<String>,

    /// Log level
    #[arg(short, long, env = "RUST_LOG", default_value = "info")]
    log_level: EnvFilter,

    /// Show healthy clusters in output
    #[arg(long)]
    show_healthy: bool,

    /// Show healthy clusters that have experienced failover
    #[arg(long)]
    show_failover: bool,

    /// Silence tracing, useful when running a watch command
    #[arg(long, short)]
    silence_tracing: bool,

    /// Default user to use when not connecting with cert auth
    #[arg(long, env = "DEFAULT_USER")]
    default_user: String,

    /// Default password to use when not connecting with cert auth
    #[arg(long, env = "DEFAULT_PASS")]
    default_pass: String,

    /// Write CSV output to file
    #[arg(long)]
    csv: Option<String>,

    /// Disable colors in terminal output
    #[arg(long)]
    no_color: bool,
}

impl Args {
    fn cluster(&self) -> String {
        self.cluster
            .as_ref()
            .map(|s| format!("{s}.*"))
            .unwrap_or_else(|| ".*-(pg|ts)-.*".to_string())
    }
}

#[tokio::main]
async fn main() {
    let now = Instant::now();

    let args = Args::parse();

    if !args.silence_tracing {
        logging::setup(args.log_level.clone());
    }

    tracing::trace!(args = ?args, "parsed command line arguments");
    v2::db::setup(&args);

    // Extract writer options before moving args
    let writer_options = v2::writer::WriterOptions {
        show_healthy: args.show_healthy,
        show_failover: args.show_failover,
        csv_path: args.csv.clone(),
        no_color: args.no_color,
    };

    ARGS.set(args).unwrap();

    let (timings_tx, timings_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let (node_tx, node_rx) = tokio::sync::mpsc::unbounded_channel::<AnalyzedNode>();
    let (cluster_tx, cluster_rx) = tokio::sync::mpsc::unbounded_channel::<Cluster>();
    let (analyze_tx, analyze_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterHealth>();

    // Set up timings handle first, since others will send on the channel
    let timings_handle = tokio::spawn(timings::reporter(timings_rx));

    timings_tx.send(Event::Start(Stage::Prometheus)).ok();
    let batch_data = batch_filesystem_data().await;
    timings_tx.send(Event::End(Stage::Prometheus)).ok();

    tokio::spawn(v2::cluster::cluster_builder(
        node_rx,
        cluster_tx,
        timings_tx.clone(),
    ));

    timings_tx.send(Event::Start(Stage::DatabasePortal)).ok();
    let nodes = filter_nodes().await;
    timings_tx.send(Event::End(Stage::DatabasePortal)).ok();

    let scan_handle = tokio::spawn(v2::scan::scan_nodes(node_tx, nodes, timings_tx.clone()));
    let analyze_handle = tokio::spawn(v2::analyze::analyze_clusters(
        batch_data,
        cluster_rx,
        analyze_tx,
        timings_tx.clone(),
    ));
    let writer_handle = tokio::spawn(v2::writer::write_results(
        analyze_rx,
        writer_options,
        timings_tx.clone(),
    ));

    let (_, _, writer_result) =
        tokio::try_join!(scan_handle, analyze_handle, writer_handle).expect("Task failed");

    timings_tx.send(Event::Complete).ok();
    drop(timings_tx);

    let elapsed = now.elapsed();
    tracing::info!(
        duration_ms = elapsed.as_millis(),
        duration_secs = elapsed.as_secs_f64(),
        "scan completed"
    );

    match timings_handle.await {
        Ok(Some(s)) => print!("{s}"),
        Ok(None) => {}
        Err(e) => tracing::error!(error = %e, "timings_handle join error"),
    }

    print!("{}", writer_result);
}

#[instrument(level = "debug")]
async fn batch_filesystem_data() -> HashMap<String, FileSystemMetrics> {
    let data = prometheus::client::get_batch_filesystem_data(ARGS.get().unwrap().cluster()).await;

    if data.is_empty() {
        tracing::warn!("no prometheus metrics fetched, backup progress will be unavailable");
    } else {
        tracing::info!(
            metric_count = data.len(),
            "fetched prometheus filesystem metrics"
        );
    }
    data
}

// TODO! Fix this unwrap, it's not healthy
async fn filter_nodes() -> impl Iterator<Item = Node> {
    database_portal::nodes()
        .await
        .unwrap()
        .into_iter()
        .filter(|n| {
            if let Some(cluster) = &ARGS.get().unwrap().cluster {
                n.cluster_name().contains(cluster)
            } else {
                true
            }
        })
        .inspect(|n| tracing::trace!(node_id = n.id, node_name = %n.node_name, cluster_id = n.cluster_id, "fetched node"))
}
