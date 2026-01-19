use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::Client;
use tracing::instrument;

use crate::v2::{
    db::DbError,
    node::Node,
    scan::{AnalyzedNode, Role},
};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ReplicaHealthCheckResult {
    pub wal_receiver: Option<WalReceiverInfo>,
    pub lag: LagInfo,
    pub conflicts_by_db: HashMap<String, i32>,
    pub configuration: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct WalReceiverInfo {
    pub pid: i32,
    pub status: String,
    pub receive_start_lsn: String,
    pub receive_start_tli: i32,
    pub written_lsn: String,
    pub flushed_lsn: String,
    pub received_tli: i32,
    pub last_msg_send_time: Option<DateTime<Utc>>,
    pub last_msg_receipt_time: Option<DateTime<Utc>>,
    pub latest_end_lsn: String,
    pub latest_end_time: Option<DateTime<Utc>>,
    pub slot_name: Option<String>,
    pub sender_host: String,
    pub sender_port: i32,
    pub conninfo: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct LagInfo {
    pub apply_lag_bytes: Option<i64>,
    pub last_transaction_replay_at: Option<DateTime<Utc>>,
}

static HEALTH_CHECK_REPLICA_QUERY: &str = "SELECT jsonb_build_object(
    'wal_receiver', (
        SELECT COALESCE(to_jsonb(t), '{}'::jsonb)
        FROM (
            SELECT
                pid,
                status,
                receive_start_lsn::text,
                receive_start_tli,
                written_lsn::text,
                flushed_lsn::text,
                received_tli,
                last_msg_send_time,
                last_msg_receipt_time,
                latest_end_lsn::text,
                latest_end_time,
                slot_name,
                sender_host,
                sender_port,
                conninfo
            FROM
                pg_stat_wal_receiver
        ) t
    ),
    'lag', jsonb_build_object(
        'apply_lag_bytes', pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()),
        'last_transaction_replay_at', pg_last_xact_replay_timestamp()
    ),
    'conflicts_by_db', (
        SELECT COALESCE(jsonb_object_agg(datname, total_conflicts), '{}'::jsonb)
        FROM (
            SELECT
                datname,
                (confl_tablespace + confl_lock + confl_snapshot + confl_bufferpin + confl_deadlock) AS total_conflicts
            FROM
                pg_stat_database_conflicts
        ) AS t
        WHERE total_conflicts > 0
    ),
    'configuration', (
        SELECT jsonb_object_agg(name, setting)
        FROM pg_settings
        WHERE name IN (
            'hot_standby',
            'primary_conninfo',
            'primary_slot_name',
            'recovery_target_timeline'
        )
    )
)::text;";

#[instrument(skip(client, tx), level = "debug", fields(node_name = %node.node_name, node_id = node.id))]
pub(super) async fn check(client: Client, node: Arc<Node>, tx: UnboundedSender<AnalyzedNode>) {
    tracing::info!("starting replica health check");

    let analyzed = match execute_replica_health_check(&client).await {
        Ok(data) => {
            tracing::info!(
                wal_receiver_status = data.wal_receiver.as_ref().map(|w| &w.status),
                apply_lag_bytes = data.lag.apply_lag_bytes,
                conflicts_count = data.conflicts_by_db.len(),
                "replica health check completed"
            );

            AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.node_name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::Replica {
                    health: data.into(),
                },
                errors: vec![],
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "replica health check failed");

            AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.node_name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::UnknownReplica,
                errors: vec![e],
            }
        }
    };

    tracing::trace!(result = ?analyzed, "Replica health check raw result");

    match tx.send(analyzed) {
        Ok(_) => tracing::trace!(node_name = %node.node_name, "health checked replica node"),
        Err(e) => {
            tracing::error!(node_name = %node.node_name, error = %e, "failed to send health checked replica node")
        }
    };
}

#[instrument(skip(client), level = "trace")]
async fn execute_replica_health_check(
    client: &Client,
) -> Result<ReplicaHealthCheckResult, DbError> {
    tracing::debug!("executing replica health check query");

    let row = client.query_one(HEALTH_CHECK_REPLICA_QUERY, &[]).await?;
    tracing::debug!(row = ?row, "replica health check query executed");

    // Get JSONB as text and parse it
    let json_text: String = row.get(0);
    let json_value: serde_json::Value = serde_json::from_str(&json_text)?;

    tracing::trace!(json = %json_value, "Raw JSONB result");

    Ok(serde_json::from_value(json_value)?)
}
