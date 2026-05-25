use std::{sync::Arc, time::Duration};

use serde_json::json;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tracing::instrument;

use crate::{pipeline::PipelineContext, v2::analyze::ClusterHealth};

/// `capture` forwards each `ClusterHealth` downstream unchanged, however it
/// also buffers each one when it passes through. When the upstream channel closes,
/// it writes the buffered items to the capture DB.
#[instrument(skip_all, level = "info")]
pub async fn capture(
    ctx: Arc<PipelineContext>,
    mut cluster_rx: UnboundedReceiver<ClusterHealth>,
    analyzed_tx: UnboundedSender<ClusterHealth>,
) {
    let capture = ctx.capture_client.is_some();
    let mut buf = Vec::new();

    while let Some(cluster) = cluster_rx.recv().await {
        if capture {
            buf.push(cluster.clone());
        }

        match analyzed_tx.send(cluster) {
            Ok(()) => tracing::trace!("passed cluster downstream"),
            Err(e) => tracing::error!(error = %e, "failed to send analyzed cluster downstream"),
        }
    }

    if let Some(client) = &ctx.capture_client {
        // Best-effort write, time out fast if we can't write to capture DB.
        let _ = tokio::time::timeout(Duration::from_secs(5), flush_to_db(buf, Arc::clone(client)))
            .await;
    }
}

#[instrument(skip_all, level = "info")]
async fn flush_to_db(buf: Vec<ClusterHealth>, client: Arc<Client>) {
    let binary_version = env!("CARGO_PKG_VERSION");
    let (mut hostnames, mut blobs) = (Vec::with_capacity(buf.len()), Vec::with_capacity(buf.len()));

    for c in &buf {
        hostnames.push(c.cluster().name());
        blobs.push(json!(c));
    }

    let sql = "WITH r AS (SELECT gen_random_uuid() AS run_id)
  INSERT INTO db_scan.db_scan_captures
      (run_id, captured_at, binary_version, hostname, blob)
  SELECT
      r.run_id,
      NOW(),                       -- captured_at
      $1,                          -- binary_version
      t.hostname,                  -- hostname
      t.blob
  FROM r, UNNEST($2::text[], $3::jsonb[]) AS t(hostname, blob)";

    let res = client
        .execute(sql, &[&binary_version, &hostnames, &blobs])
        .await;

    match res {
        Ok(row_count) => tracing::info!(row_count = %row_count, "flushed to db"),
        Err(e) => tracing::error!(error = ?e, "failed to flush to db"),
    }
}
