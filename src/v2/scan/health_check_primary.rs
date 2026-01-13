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

/// PostgreSQL synchronous commit settings
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PgSyncSettings {
    /// One of the standby names must acknowledge writes
    Quorum,
    /// One of the standby names must confirm receipt of WAL (flush to durable storage)
    On,
    /// One of the standby names must confirm receipt of WAL written to disk
    RemoteWrite,
    /// Wait for local flush to disk, but not for replication
    Local,
    /// NO guarantees -- returns immediately
    Off,
}

impl From<String> for PgSyncSettings {
    fn from(value: String) -> Self {
        match value.as_str() {
            "remote_apply" => PgSyncSettings::Quorum,
            "on" => PgSyncSettings::On,
            "remote_write" => PgSyncSettings::RemoteWrite,
            "local" => PgSyncSettings::Local,
            "off" => PgSyncSettings::Off,
            _ => {
                tracing::warn!(value = %value, "unknown synchronous_commit setting, defaulting to off");
                PgSyncSettings::Off
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct PrimaryHealthCheckResult {
    pub timeline_id: i32,
    pub uptime: String,
    pub current_wal_lsn: String,
    pub configuration: HashMap<String, String>,
    pub replication: Vec<ReplicationConnection>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
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
    pub sent_lsn: String,
    pub write_lsn: String,
    pub flush_lsn: String,
    pub replay_lsn: String,
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
            'max_replication_slots'
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
            }
        }
    };

    tracing::trace!(result = ?analyzed, "Primary health check raw result");

    match tx.send(analyzed) {
        Ok(_) => tracing::trace!(node_name = %node.node_name, "health checked primary node"),
        Err(e) => {
            tracing::error!(node_name = %node.node_name, error = %e, "failed to send health checked primary node")
        }
    };
}

#[instrument(skip(client), level = "trace")]
async fn execute_primary_health_check(
    client: &Client,
) -> Result<PrimaryHealthCheckResult, DbError> {
    tracing::debug!("executing primary health check query");

    let row = client.query_one(HEALTH_CHECK_PRIMARY_QUERY, &[]).await?;

    // Get JSONB as text and parse it
    let json_text: String = row.get(0);
    let json_value: serde_json::Value = serde_json::from_str(&json_text)?;

    tracing::trace!(json = %json_value, "Raw JSONB result");

    Ok(serde_json::from_value(json_value)?)
}
