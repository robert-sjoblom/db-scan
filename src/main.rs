use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::mpsc::UnboundedSender;

use crate::{
    config::{CONFIG, get_config},
    pipeline::{Pipeline, PipelineContext},
    timings::{Event, Stage},
    v2::{
        analyze::analyze_clusters,
        capture::capture,
        cluster::cluster_builder,
        db,
        node::Node,
        scan::scan_nodes,
        writer::{ScanResult, WriterOptions, write_results},
    },
};

mod config;
mod database_portal;
mod errors;
mod logging;
mod pipeline;
mod timings;
mod updater;
mod v2;

fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Install rustls default crypto provider");

    // Self-update fast path: bypass config validation (which requires PGPASSWORD etc).
    if std::env::args().nth(1).as_deref() == Some("self-update") {
        if let Err(e) = updater::update() {
            eprintln!("self-update failed: {e:#}");
            std::process::exit(1);
        }
        return;
    }
    let args = match config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e:#}");
            std::process::exit(2);
        }
    };

    if args.print_config {
        println!("{args:#?}");
        std::process::exit(0);
    }

    run(args);
}

#[tokio::main]
async fn run(args: config::DbScanConfig) {
    if !args.silence_tracing {
        logging::setup(args.log_level.clone());
    }

    if args.check_disks && args.ssh_user.is_none() {
        tracing::warn!(
            "--check-disks enabled but ssh_user is not set (CLI --ssh-user, SSH_USER env, or ssh.user in config); disk checks will be skipped"
        );
    }

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

    // Best-effort version check; capped so a slow/unreachable GitHub doesn't stall scans.
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::task::spawn_blocking(updater::nag_if_outdated),
    )
    .await;

    match watch_interval {
        Some(interval) => run_watch_mode(writer_options, interval).await,
        None => run_single_scan(writer_options).await,
    }
}

async fn run_single_scan(writer_options: Arc<WriterOptions>) {
    let now = Instant::now();

    let (timings_tx, timings_rx) = tokio::sync::mpsc::unbounded_channel::<Event>();
    let timings_handle = tokio::spawn(timings::reporter(timings_rx));

    let result = run_scan(&timings_tx, writer_options, None).await;

    let _ = timings_tx.send(Event::Complete);
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

        let result = run_scan(
            &timings_tx,
            Arc::clone(&writer_options),
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
            () = tokio::time::sleep(interval) => {}
        }
    }
}

async fn run_scan(
    timings_tx: &UnboundedSender<Event>,
    writer_options: Arc<WriterOptions>,
    cluster_filter: Option<&HashSet<String>>,
) -> anyhow::Result<ScanResult> {
    let cfg = get_config();

    // Set up capture DB connection before building the pipeline and moving args
    let capture_client = if let Some(pg_cfg) = &cfg.capture_cfg() {
        match db::connect_with(pg_cfg).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        tracing::error!(error = ?e, "capture connection closed");
                    }
                });
                Some(Arc::new(client))
            }
            Err(e) => {
                tracing::warn!(error = ?e, "capture setup failed; capture disabled");
                None
            }
        }
    } else {
        None
    };

    let pipeline_ctx = PipelineContext::new(timings_tx.clone(), writer_options, capture_client);

    // Clone filter for the spawned task (needs 'static)
    let filter = cluster_filter.cloned();

    Pipeline::new(pipeline_ctx)
        .source(Stage::DatabasePortal, move |ctx, tx| {
            filter_nodes(ctx, tx, filter)
        })
        .stage(Stage::Scan, |ctx, rx, tx| {
            scan_nodes(Arc::clone(&ctx), rx, tx)
        })
        .stage(Stage::Clustering, |ctx, rx, tx| {
            cluster_builder(Arc::clone(&ctx), rx, tx)
        })
        .stage(Stage::Analyze, |ctx, rx, tx| {
            analyze_clusters(Arc::clone(&ctx), rx, tx)
        })
        .stage(Stage::Capture, |ctx, rx, tx| {
            capture(Arc::clone(&ctx), rx, tx)
        })
        .sink(Stage::Write, |ctx, rx| write_results(Arc::clone(&ctx), rx))
        .run()
        .await
}

async fn filter_nodes(
    ctx: Arc<PipelineContext>,
    tx: UnboundedSender<Node>,
    cluster_filter: Option<HashSet<String>>,
) {
    let nodes = match database_portal::nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            tracing::error!(error = ?e, "failed to fetch nodes from database portal");
            let _ = ctx.timings_tx.send(Event::End(Stage::DatabasePortal));
            return;
        }
    };

    for node in nodes
        .into_iter()
        .filter(|n| {
            let passes_cli_filter = match &get_config().cluster {
                Some(re) => {
                    let matches = re.is_match(&n.cluster_name());
                    tracing::trace!(
                        cluster_name = %n.cluster_name(),
                        pattern = re.as_str(),
                        matches,
                        "cli filter"
                    );
                    matches
                }
                None => true,
            };
            if !passes_cli_filter {
                return false;
            }

            // Watch mode cluster filter (exact match)
            match &cluster_filter {
                Some(filter) => {
                    let matches = filter.contains(&n.cluster_name());
                    tracing::debug!(cluster_name = %n.cluster_name(), matches, "watch filter");
                    matches
                }
                None => true,
            }
        })
        .inspect(|n| {
            tracing::trace!(
                node_id = n.id,
                node_name = %n.name,
                cluster_id = n.cluster_id,
                "fetched node"
            );
        })
    {
        if tx.send(node).is_err() {
            tracing::warn!("receiver dropped, stopping node enumeration");
            break;
        }
    }
}
