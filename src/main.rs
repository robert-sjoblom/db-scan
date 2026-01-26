use std::{collections::HashMap, sync::Arc, time::Instant};

use clap::Parser;
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;

use crate::{
    config::{CONFIG, get_config},
    pipeline::{Pipeline, PipelineContext},
    prometheus::FileSystemMetrics,
    timings::{Event, Stage},
    v2::{
        analyze::analyze_clusters, cluster::cluster_builder, node::Node, scan::scan_nodes,
        writer::write_results,
    },
};

mod config;
mod database_portal;
mod logging;
mod pipeline;
mod prometheus;
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

    // TODO: we should move this to the task_group too
    // Set up timings handle first, since others will send on the channel
    let timings_handle = tokio::spawn(timings::reporter(timings_rx));
    let batch_data = batch_filesystem_data(timings_tx.clone()).await;
    let pipeline_ctx = PipelineContext::new(timings_tx.clone(), batch_data, writer_options);

    let results = Pipeline::new(pipeline_ctx)
        .source(Stage::DatabasePortal, |ctx, tx| {
            filter_nodes(ctx.clone(), tx)
        })
        .stage(Stage::Scan, |ctx, rx, tx| scan_nodes(ctx.clone(), rx, tx))
        .stage(Stage::Clustering, |ctx, rx, tx| {
            cluster_builder(ctx.clone(), rx, tx)
        })
        .stage(Stage::Analyze, |ctx, rx, tx| {
            analyze_clusters(ctx.clone(), rx, tx)
        })
        .sink(Stage::Write, |ctx, rx| write_results(ctx.clone(), rx))
        .run()
        .await;

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
        Ok(s) => {
            print!("{s}");
        }
        Err(e) => tracing::error!(error = %e, "task failed"),
    }
}

#[instrument(level = "debug")]
async fn batch_filesystem_data(
    timings_tx: UnboundedSender<Event>,
) -> HashMap<String, FileSystemMetrics> {
    timings_tx.send(Event::Start(Stage::Prometheus)).ok();

    let data = prometheus::client::get_batch_filesystem_data(get_config().cluster_pattern()).await;

    if data.is_empty() {
        tracing::warn!("no prometheus metrics fetched, backup progress will be unavailable");
    } else {
        tracing::info!(
            metric_count = data.len(),
            "fetched prometheus filesystem metrics"
        );
    }
    timings_tx.send(Event::End(Stage::Prometheus)).ok();
    data
}

async fn filter_nodes(ctx: Arc<PipelineContext>, tx: UnboundedSender<Node>) {
    ctx.timings_tx
        .send(Event::Start(Stage::DatabasePortal))
        .ok();

    let nodes = match database_portal::nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            tracing::error!(error = %e, "failed to fetch nodes from database portal");
            ctx.timings_tx.send(Event::End(Stage::DatabasePortal)).ok();
            return;
        }
    };

    for node in nodes.into_iter().filter(|n| {
            if let Some(cluster) = &get_config().cluster {
                n.cluster_name().contains(cluster)
            } else {
                true
            }
        }).inspect(|n| tracing::trace!(node_id = n.id, node_name = %n.node_name, cluster_id = n.cluster_id, "fetched node")) {
            if tx.send(node).is_err() {
                tracing::warn!("receiver dropped, stopping node enumeration");
                break;
            }
        }

    ctx.timings_tx.send(Event::End(Stage::DatabasePortal)).ok();
}
