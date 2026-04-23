use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser;
use error_stack::Report;
use tokio::sync::mpsc::UnboundedSender;
use tracing::instrument;

use crate::{
    config::{CONFIG, get_config},
    pipeline::{Pipeline, PipelineContext, PipelineError},
    prometheus::FileSystemMetrics,
    timings::{Event, Stage},
    v2::{
        analyze::analyze_clusters,
        cluster::cluster_builder,
        node::Node,
        scan::scan_nodes,
        writer::{ScanResult, WriterOptions, write_results},
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
    let args = config::DbScanConfig::parse();

    if !args.silence_tracing {
        logging::setup(args.log_level.clone());
    }

    tracing::trace!(args = ?args, "parsed command line arguments");
    v2::db::setup(&args);

    // Extract options before moving args
    let writer_options = Arc::new(WriterOptions {
        show_healthy: args.show_healthy,
        show_failover: args.show_failover,
        csv_path: args.csv.clone(),
        no_color: args.no_color,
    });
    let watch_interval = args.watch.map(Duration::from_secs);

    CONFIG.set(args).unwrap();

    match watch_interval {
        Some(interval) => run_watch_mode(writer_options, interval).await,
        None => run_single_scan(writer_options).await,
    }
}

async fn run_single_scan(writer_options: Arc<WriterOptions>) {
    let now = Instant::now();

    let (timings_tx, timings_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let timings_handle = tokio::spawn(timings::reporter(timings_rx));
    let batch_data = batch_filesystem_data(timings_tx.clone()).await;

    let result = run_scan(&timings_tx, batch_data, writer_options, None).await;

    timings_tx.send(Event::Complete).ok();
    drop(timings_tx);

    let elapsed = now.elapsed();
    tracing::info!(
        duration_ms = elapsed.as_millis(),
        duration_secs = elapsed.as_secs_f64(),
        "scan completed"
    );

    if let Ok(Some(s)) = timings_handle.await {
        print!("{s}");
    }

    match result {
        Ok(scan_result) => print!("{}", scan_result.output),
        Err(e) => tracing::error!(error = %e, "scan failed"),
    }
}

async fn run_watch_mode(writer_options: Arc<WriterOptions>, interval: Duration) {
    let mut cluster_filter: Option<HashSet<String>> = None;
    let mut last_output = String::new();

    loop {
        // Clear screen
        print!("\x1B[2J\x1B[1;1H");

        // Timings are intentionally discarded in watch mode (screen is cleared anyway)
        let (timings_tx, _) = tokio::sync::mpsc::unbounded_channel::<Event>();
        let batch_data = batch_filesystem_data(timings_tx.clone()).await;

        let result = run_scan(
            &timings_tx,
            batch_data,
            writer_options.clone(),
            cluster_filter.as_ref(),
        )
        .await;

        match result {
            Ok(scan_result) => {
                print!("{}", scan_result.output);
                last_output = scan_result.output;

                if scan_result.clusters_to_rescan.is_empty() {
                    println!("\nAll clusters healthy. Exiting watch mode.");
                    break;
                }

                println!(
                    "\nRescanning {} unhealthy cluster(s) in {} seconds...",
                    scan_result.clusters_to_rescan.len(),
                    interval.as_secs()
                );
                cluster_filter = Some(scan_result.clusters_to_rescan);
            }
            Err(e) => {
                tracing::error!(error = %e, "scan failed");
                println!(
                    "\nScan failed. Retrying in {} seconds...",
                    interval.as_secs()
                );
            }
        }

        // Wait for interval or Ctrl+C
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n\nInterrupted. Last scan state:");
                print!("{}", last_output);
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

async fn run_scan(
    timings_tx: &UnboundedSender<Event>,
    batch_data: HashMap<String, FileSystemMetrics>,
    writer_options: Arc<WriterOptions>,
    cluster_filter: Option<&HashSet<String>>,
) -> Result<ScanResult, Report<PipelineError>> {
    let pipeline_ctx = PipelineContext::new(timings_tx.clone(), batch_data, writer_options);

    // Clone filter for the spawned task (needs 'static)
    let filter = cluster_filter.cloned();

    Pipeline::new(pipeline_ctx)
        .source(Stage::DatabasePortal, move |ctx, tx| {
            filter_nodes(ctx, tx, filter)
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
        .await
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

async fn filter_nodes(
    ctx: Arc<PipelineContext>,
    tx: UnboundedSender<Node>,
    cluster_filter: Option<HashSet<String>>,
) {
    let nodes = match database_portal::nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            tracing::error!(error = %e, "failed to fetch nodes from database portal");
            ctx.timings_tx.send(Event::End(Stage::DatabasePortal)).ok();
            return;
        }
    };

    for node in nodes
        .into_iter()
        .filter(|n| {
            // First, check CLI --cluster flag (substring match)
            let passes_cli_filter = match &get_config().cluster {
                Some(cluster) => n.cluster_name().contains(cluster),
                None => true,
            };
            if !passes_cli_filter {
                return false;
            }

            // Then, check watch mode cluster filter (exact match)
            match &cluster_filter {
                Some(filter) => filter.contains(&n.cluster_name()),
                None => true,
            }
        })
        .inspect(|n| {
            tracing::trace!(
                node_id = n.id,
                node_name = %n.node_name,
                cluster_id = n.cluster_id,
                "fetched node"
            )
        })
    {
        if tx.send(node).is_err() {
            tracing::warn!("receiver dropped, stopping node enumeration");
            break;
        }
    }
}
