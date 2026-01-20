use std::{collections::HashMap, time::Instant};

use clap::Parser;
use tracing::instrument;

use crate::{
    config::{CONFIG, get_config},
    prometheus::FileSystemMetrics,
    task_group::TaskGroup,
    timings::{Event, Stage},
    v2::{
        analyze::{ClusterHealth, analyze_clusters},
        cluster::{Cluster, cluster_builder},
        node::Node,
        scan::{AnalyzedNode, scan_nodes},
        writer::write_results,
    },
};

mod config;
mod database_portal;
mod logging;
mod pipeline;
mod prometheus;
mod task_group;
mod timings;
mod v2;

#[tokio::main]
async fn main() {
    let now = Instant::now();

    let args = config::DbScanConfig::parse();

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

    CONFIG.set(args).unwrap();

    let (timings_tx, timings_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let (node_tx, node_rx) = tokio::sync::mpsc::unbounded_channel::<AnalyzedNode>();
    let (cluster_tx, cluster_rx) = tokio::sync::mpsc::unbounded_channel::<Cluster>();
    let (analyze_tx, analyze_rx) = tokio::sync::mpsc::unbounded_channel::<ClusterHealth>();

    // TODO: we should move this to the task_group too
    // Set up timings handle first, since others will send on the channel
    let timings_handle = tokio::spawn(timings::reporter(timings_rx));

    let mut tasks = TaskGroup::new();

    timings_tx.send(Event::Start(Stage::Prometheus)).ok();
    let batch_data = batch_filesystem_data().await;
    timings_tx.send(Event::End(Stage::Prometheus)).ok();

    tasks.spawn(
        Stage::Clustering,
        cluster_builder(node_rx, cluster_tx, timings_tx.clone()),
    );

    timings_tx.send(Event::Start(Stage::DatabasePortal)).ok();
    let nodes = filter_nodes().await;
    timings_tx.send(Event::End(Stage::DatabasePortal)).ok();

    tasks.spawn(Stage::Scan, scan_nodes(node_tx, nodes, timings_tx.clone()));

    tasks.spawn(
        Stage::Analyze,
        analyze_clusters(batch_data, cluster_rx, analyze_tx, timings_tx.clone()),
    );

    tasks.spawn_returning(
        Stage::Write,
        write_results(analyze_rx, writer_options, timings_tx.clone()),
    );

    let results = tasks.run().await;

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

    match results {
        Ok(v) => {
            for (_, s) in v {
                print!("{s}");
            }
        }
        Err(e) => tracing::error!(error = %e, "task failed"),
    }
}

#[instrument(level = "debug")]
async fn batch_filesystem_data() -> HashMap<String, FileSystemMetrics> {
    let data = prometheus::client::get_batch_filesystem_data(get_config().cluster_pattern()).await;

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
            if let Some(cluster) = &get_config().cluster {
                n.cluster_name().contains(cluster)
            } else {
                true
            }
        })
        .inspect(|n| tracing::trace!(node_id = n.id, node_name = %n.node_name, cluster_id = n.cluster_id, "fetched node"))
}
