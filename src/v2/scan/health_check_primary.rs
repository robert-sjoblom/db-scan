use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::Client;
use tracing::instrument;

use crate::v2::{
    db::db_error::DbError,
    node::Node,
    scan::{AnalyzedNode, Role},
};

/// Per-replica replication state from `pg_stat_replication.sync_state`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PgSyncSettings {
    /// Replica is part of the synchronous quorum.
    Quorum,
    /// Replica is the single designated synchronous standby.
    Sync,
    /// Replica would become sync/quorum if a higher-priority one drops.
    Potential,
    /// Replica is asynchronous - primary does not wait for it before acking writes.
    Async,
}

impl From<String> for PgSyncSettings {
    fn from(value: String) -> Self {
        match value.as_str() {
            "quorum" => PgSyncSettings::Quorum,
            "sync" => PgSyncSettings::Sync,
            "potential" => PgSyncSettings::Potential,
            "async" => PgSyncSettings::Async,
            _ => {
                tracing::warn!(value = %value, "unknown pg_stat_replication.sync_state, defaulting to async");
                PgSyncSettings::Async
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub struct ArchiverStats {
    pub archived_count: i64,
    pub failed_count: i64,
    pub last_archived_wal: Option<String>,
    pub last_archived_time: Option<DateTime<Utc>>,
    pub last_failed_wal: Option<String>,
    pub last_failed_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub struct ReplicationSlot {
    pub slot_name: String,
    pub plugin: Option<String>,
    pub slot_type: String,
    pub active: bool,
    pub restart_lsn: Option<String>,
    pub wal_retained: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub struct PrimaryHealthCheckResult {
    pub timeline_id: i32,
    pub uptime: String,
    pub current_wal_lsn: String,
    pub configuration: HashMap<String, String>,
    pub replication: Vec<ReplicationConnection>,
    pub archiver: ArchiverStats,
    pub replication_slots: Vec<ReplicationSlot>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub struct ReplicationConnection {
    pub pid: i32,
    pub usesysid: i32,
    pub usename: String,
    pub application_name: String,
    pub client_addr: Option<String>,
    pub client_hostname: Option<String>,
    pub client_port: Option<i32>,
    pub backend_start: DateTime<Utc>,
    pub backend_xmin: Option<String>,
    pub state: String,
    /// LSN fields can be null for connections in "backup" state (e.g., `pg_basebackup`).
    pub sent_lsn: Option<String>,
    pub write_lsn: Option<String>,
    pub flush_lsn: Option<String>,
    pub replay_lsn: Option<String>,
    pub write_lag: Option<String>,
    pub flush_lag: Option<String>,
    pub replay_lag: Option<String>,
    pub sync_priority: i32,
    pub sync_state: PgSyncSettings,
    pub reply_time: Option<DateTime<Utc>>,
}

static HEALTH_CHECK_PRIMARY_QUERY: &str = "SELECT jsonb_build_object(
    'timeline_id', (SELECT timeline_id FROM pg_control_checkpoint()),
    'uptime', (SELECT (now() - pg_postmaster_start_time())::text),
    'current_wal_lsn', (SELECT pg_current_wal_lsn()::text),
    'configuration', (
        SELECT jsonb_object_agg(name, setting)
        FROM pg_settings
        WHERE name IN (
            'synchronous_standby_names',
            'synchronous_commit',
            'wal_level',
            'max_wal_senders',
            'wal_sender_timeout',
            'max_replication_slots',
            'archive_mode',
            'archive_command'
        )
    ),
    'replication', (
        SELECT COALESCE(jsonb_agg(t), '[]'::jsonb)
        FROM (
            SELECT
                pid,
                usesysid::int,
                usename,
                application_name,
                client_addr,
                client_hostname,
                client_port,
                backend_start,
                backend_xmin::text,
                state,
                sent_lsn::text,
                write_lsn::text,
                flush_lsn::text,
                replay_lsn::text,
                write_lag::text,
                flush_lag::text,
                replay_lag::text,
                sync_priority,
                sync_state,
                reply_time
            FROM
                pg_stat_replication
        ) t
    ),
    'archiver', (
        SELECT jsonb_build_object(
            'archived_count', archived_count,
            'failed_count', failed_count,
            'last_archived_wal', last_archived_wal,
            'last_archived_time', last_archived_time,
            'last_failed_wal', last_failed_wal,
            'last_failed_time', last_failed_time
        )
        FROM pg_stat_archiver
    ),
    'replication_slots', (
        SELECT COALESCE(jsonb_agg(t), '[]'::jsonb)
        FROM (
            SELECT
                slot_name,
                plugin,
                slot_type,
                active,
                restart_lsn::text,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS wal_retained
            FROM pg_replication_slots
        ) t
    )
)::text;";

#[instrument(skip(client, tx), level = "debug", fields(node_name = %node.node_name))]
pub(super) async fn check(client: Client, node: Arc<Node>, tx: UnboundedSender<AnalyzedNode>) {
    tracing::info!("starting primary health check");

    let analyzed = match execute_primary_health_check(&client).await {
        Ok(data) => {
            tracing::info!(
                timeline_id = data.timeline_id,
                uptime = %data.uptime,
                current_wal_lsn = %data.current_wal_lsn,
                replica_count = data.replication.len(),
                "primary health check completed"
            );

            AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.node_name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::Primary {
                    health: data.into(),
                },
                errors: vec![],
                disk_check: None,
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "primary health check failed");

            AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.node_name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::UnknownPrimary,
                errors: vec![e],
                disk_check: None,
            }
        }
    };

    tracing::trace!(result = ?analyzed, "Primary health check raw result");

    match tx.send(analyzed) {
        Ok(()) => tracing::trace!(node_name = %node.node_name, "health checked primary node"),
        Err(e) => {
            tracing::error!(node_name = %node.node_name, error = %e, "failed to send health checked primary node");
        }
    }
}

#[instrument(skip(client), level = "trace")]
async fn execute_primary_health_check(
    client: &Client,
) -> Result<PrimaryHealthCheckResult, DbError> {
    tracing::debug!("executing primary health check query");

    let row = client.query_one(HEALTH_CHECK_PRIMARY_QUERY, &[]).await?;

    tracing::debug!(row = ?row, "primary health check query executed");

    // Get JSONB as text and parse it
    let json_text: String = row.get(0);

    tracing::debug!(text = %json_text, "Raw JSONB text result");

    let json_value: serde_json::Value = serde_json::from_str(&json_text)?;

    tracing::trace!(json = %json_value, "Raw JSONB result");

    Ok(serde_json::from_value(json_value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_connection_handles_null_lsn_fields() {
        // pg_basebackup connections have null LSN fields - verify our schema handles this
        let json = serde_json::json!({
            "pid": 123,
            "usesysid": 0x4003,
            "usename": "replicator",
            "application_name": "pg_basebackup",
            "client_addr": "10.0.0.1",
            "client_hostname": null,
            "client_port": 5432,
            "backend_start": "2026-01-14T10:00:00Z",
            "backend_xmin": null,
            "state": "backup",
            "sent_lsn": null,
            "write_lsn": null,
            "flush_lsn": null,
            "replay_lsn": null,
            "write_lag": null,
            "flush_lag": null,
            "replay_lag": null,
            "sync_priority": 0,
            "sync_state": "async",
            "reply_time": null
        });

        let conn: ReplicationConnection = serde_json::from_value(json).unwrap();
        assert_eq!(conn.sent_lsn, None);
        assert_eq!(conn.replay_lsn, None);
        assert_eq!(conn.state, "backup");
    }
}
