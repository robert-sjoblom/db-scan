use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::Client;
use tracing::instrument;

use anyhow::Context as _;

use crate::{
    errors,
    v2::{
        node::Node,
        scan::{AnalyzedNode, Role},
    },
};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct ReplicaHealthCheckResult {
    pub current_time: DateTime<Utc>,
    pub timeline_id: i32,
    pub system_identifier: String,
    pub wal_receiver: Option<WalReceiverInfo>,
    pub lag: LagInfo,
    pub conflicts_by_db: HashMap<String, i32>,
    pub configuration: HashMap<String, String>,
    /// Applied position. Non-NULL on anything that has replayed, and can be
    /// stale -- frozen at promotion on a former standby. See ADR-002 §7.
    pub last_wal_replay_lsn: Option<String>,
    /// Received position. Survives walreceiver death and promotion within one
    /// postmaster, so `Some(_)` is a high-water mark, not a live position.
    pub last_wal_receive_lsn: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct LagInfo {
    pub apply_lag_bytes: Option<i64>,
    pub last_transaction_replay_at: Option<DateTime<Utc>>,
}

static HEALTH_CHECK_REPLICA_QUERY: &str = "SELECT jsonb_build_object(
    'current_time', (SELECT now()),
    'timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()),
    'system_identifier', (SELECT system_identifier::text FROM pg_control_system()),
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
    'last_wal_replay_lsn', pg_last_wal_replay_lsn()::text,
    'last_wal_receive_lsn', pg_last_wal_receive_lsn()::text,
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

#[instrument(skip(client, tx), level = "debug", fields(node_name = %node.name, node_id = node.id))]
pub(super) async fn check(client: Client, node: Arc<Node>, tx: UnboundedSender<AnalyzedNode>) {
    tracing::info!("starting replica health check");

    let analyzed = match execute_replica_health_check(&client).await {
        Ok(data) => {
            tracing::info!(
                timeline_id = data.timeline_id,
                wal_receiver_status = data.wal_receiver.as_ref().map(|w| &w.status),
                apply_lag_bytes = data.lag.apply_lag_bytes,
                conflicts_count = data.conflicts_by_db.len(),
                primary_conninfo = data.configuration.get("primary_conninfo"),
                "replica health check completed"
            );

            AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::Replica {
                    health: data.into(),
                },
                errors: vec![],
                disk_check: None,
            }
        }
        Err(err) => {
            let kind = errors::extract_kind(&err);
            tracing::error!(error = ?err, "replica health check failed");

            AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::UnknownReplica,
                errors: vec![kind],
                disk_check: None,
            }
        }
    };

    tracing::trace!(result = ?analyzed, "Replica health check raw result");

    match tx.send(analyzed) {
        Ok(()) => tracing::trace!(node_name = %node.name, "health checked replica node"),
        Err(e) => {
            tracing::error!(node_name = %node.name, error = %e, "failed to send health checked replica node");
        }
    }
}

#[instrument(skip(client), level = "trace")]
async fn execute_replica_health_check(client: &Client) -> anyhow::Result<ReplicaHealthCheckResult> {
    tracing::debug!("executing replica health check query");

    let row = client
        .query_one(HEALTH_CHECK_REPLICA_QUERY, &[])
        .await
        .map_err(errors::pg_err)
        .context("attempting: replica health check query")?;
    tracing::debug!(row = ?row, "replica health check query executed");

    let json_text: String = row.get(0);
    tracing::debug!(text = %json_text, "Raw JSONB text result");

    let json_value: serde_json::Value = serde_json::from_str(&json_text)
        .map_err(errors::serde_err)
        .context("attempting: parse health-check JSONB as JSON value")?;

    tracing::trace!(json = %json_value, "Raw JSONB result");

    serde_json::from_value(json_value)
        .map_err(errors::serde_err)
        .context("attempting: deserialize ReplicaHealthCheckResult")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use crate::v2::scan::health_check_replica::ReplicaHealthCheckResult;

    #[expect(
        clippy::unreadable_literal,
        reason = "verbatim capture from a live replica"
    )]
    fn replica_health_check_json() -> serde_json::Value {
        json!({
            "lag": {
                "apply_lag_bytes": 0,
                "last_transaction_replay_at": "2026-09-10T07:53:01.30842+02:00"
            },
            "timeline_id": 22,
            "current_time": "2026-09-10T07:53:01.345293+02:00",
            "wal_receiver": {
                "pid": 2709535,
                "status": "streaming",
                "conninfo": "user=replicator passfile=/var/lib/pgsql/.pgpass channel_binding=prefer connect_timeout=2 dbname=replication host=10.81.12.151 port=5432 application_name=dev_pg_app001_db002 fallback_application_name=walreceiver sslmode=prefer sslcompression=0 sslsni=1 ssl_min_protocol_version=TLSv1.2 gssencmode=prefer krbsrvname=postgres target_session_attrs=any",
                "slot_name": null,
                "flushed_lsn": "6FD/8F96BC00",
                "sender_host": "127.1.12.151",
                "sender_port": 5432,
                "written_lsn": "6FD/8F96BC00",
                "received_tli": 22,
                "latest_end_lsn": "6FD/8F96BC00",
                "latest_end_time": "2026-09-10T07:53:01.309884+02:00",
                "receive_start_lsn": "6FD/7D000000",
                "receive_start_tli": 22,
                "last_msg_send_time": "2026-09-10T07:53:01.309884+02:00",
                "last_msg_receipt_time": "2026-09-10T07:53:01.312387+02:00"
            },
            "configuration": {
                "hot_standby": "on",
                "primary_conninfo": "user=replicator connect_timeout=2 host=10.81.12.151 port=5432 application_name=dev_pg_app001_db002",
                "primary_slot_name": "",
                "recovery_target_timeline": "latest"
            },
            "conflicts_by_db": {},
            "system_identifier": "7233340535934352970",
            "last_wal_replay_lsn": "6FD/8F96BC00",
            "last_wal_receive_lsn": "6FD/8F96BC00"
        })
    }

    #[test]
    fn replica_health_check_result_deserializes() {
        let result: ReplicaHealthCheckResult =
            serde_json::from_value(replica_health_check_json()).unwrap();

        assert_eq!(result.last_wal_replay_lsn.as_deref(), Some("6FD/8F96BC00"));
        assert_eq!(result.last_wal_receive_lsn.as_deref(), Some("6FD/8F96BC00"));
    }

    #[test]
    fn replica_health_check_handles_null_wal_positions() {
        // PG17 returns SQL NULL, never 0/0, when the position is zero (xlogfuncs.c).
        // Rare in practice (see ADR-002 §7), but a NULL must not fail the scan.
        let mut json = replica_health_check_json();
        json["last_wal_receive_lsn"] = serde_json::Value::Null;

        let result: ReplicaHealthCheckResult = serde_json::from_value(json).unwrap();

        assert_eq!(result.last_wal_receive_lsn, None);
        assert_eq!(result.last_wal_replay_lsn.as_deref(), Some("6FD/8F96BC00"));
    }
}
