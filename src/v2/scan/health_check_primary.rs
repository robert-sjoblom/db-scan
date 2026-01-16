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
    /// Async replication (from pg_stat_replication.sync_state)
    Async,
}

impl From<String> for PgSyncSettings {
    fn from(value: String) -> Self {
        match value.as_str() {
            "remote_apply" => PgSyncSettings::Quorum,
            "on" => PgSyncSettings::On,
            "remote_write" => PgSyncSettings::RemoteWrite,
            "local" => PgSyncSettings::Local,
            "off" => PgSyncSettings::Off,
            "async" => PgSyncSettings::Async,
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
    /// LSN fields can be null for connections in "backup" state (e.g., pg_basebackup)
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

    /// Test deserialization of primary health check data with pg_basebackup connections
    /// that have null LSN fields (e.g., when state = "backup" or "streaming" during backup)
    #[test]
    fn test_deserialize_with_pg_basebackup_null_lsn() {
        let json_data = r#"{
            "uptime": "34 days 02:38:05.573646",
            "replication": [
                {
                    "pid": 1747896,
                    "state": "streaming",
                    "usename": "replicator",
                    "sent_lsn": "1198/5510EC20",
                    "usesysid": 16387,
                    "flush_lag": "00:00:31.634264",
                    "flush_lsn": "1198/54000000",
                    "write_lag": "00:00:00.001577",
                    "write_lsn": "1198/54FC1C70",
                    "replay_lag": "00:15:13.892236",
                    "replay_lsn": null,
                    "reply_time": "2026-01-14T11:32:53.073391+01:00",
                    "sync_state": "async",
                    "client_addr": "127.3.17.8",
                    "client_port": 26836,
                    "backend_xmin": null,
                    "backend_start": "2026-01-14T11:17:39.104304+01:00",
                    "sync_priority": 0,
                    "client_hostname": null,
                    "application_name": "pg_basebackup"
                },
                {
                    "pid": 630503,
                    "state": "streaming",
                    "usename": "replicator",
                    "sent_lsn": "1198/5510EC20",
                    "usesysid": 16387,
                    "flush_lag": null,
                    "flush_lsn": "1198/55107D00",
                    "write_lag": null,
                    "write_lsn": "1198/55107D00",
                    "replay_lag": null,
                    "replay_lsn": "1198/55107D00",
                    "reply_time": "2026-01-14T11:32:54.442266+01:00",
                    "sync_state": "quorum",
                    "client_addr": "10.82.17.8",
                    "client_port": 34024,
                    "backend_xmin": "641330620",
                    "backend_start": "2025-12-15T20:07:01.435786+01:00",
                    "sync_priority": 1,
                    "client_hostname": null,
                    "application_name": "prod_pg_app008_db002"
                },
                {
                    "pid": 1745964,
                    "state": "backup",
                    "usename": "replicator",
                    "sent_lsn": null,
                    "usesysid": 16387,
                    "flush_lag": null,
                    "flush_lsn": null,
                    "write_lag": null,
                    "write_lsn": null,
                    "replay_lag": null,
                    "replay_lsn": null,
                    "reply_time": null,
                    "sync_state": "async",
                    "client_addr": "127.3.17.8",
                    "client_port": 18238,
                    "backend_xmin": null,
                    "backend_start": "2026-01-14T11:09:57.350319+01:00",
                    "sync_priority": 0,
                    "client_hostname": null,
                    "application_name": "pg_basebackup"
                }
            ],
            "timeline_id": 4,
            "configuration": {
                "wal_level": "logical",
                "max_wal_senders": "35",
                "synchronous_commit": "remote_apply",
                "wal_sender_timeout": "300000",
                "max_replication_slots": "30",
                "synchronous_standby_names": "ANY 1 ( prod_pg_app008_db002, prod_pg_app008_db003 )"
            },
            "current_wal_lsn": "1198/5510EC20"
        }"#;

        let result: Result<PrimaryHealthCheckResult, _> = serde_json::from_str(json_data);

        assert!(
            result.is_ok(),
            "Should successfully deserialize: {:?}",
            result.err()
        );

        let data = result.unwrap();

        // Verify basic structure
        assert_eq!(data.timeline_id, 4);
        assert_eq!(data.current_wal_lsn, "1198/5510EC20");
        assert_eq!(data.replication.len(), 3);

        // Check first connection (streaming pg_basebackup with null replay_lsn)
        let conn1 = &data.replication[0];
        assert_eq!(conn1.pid, 1747896);
        assert_eq!(conn1.state, "streaming");
        assert_eq!(conn1.application_name, "pg_basebackup");
        assert_eq!(conn1.sent_lsn, Some("1198/5510EC20".to_string()));
        assert_eq!(conn1.write_lsn, Some("1198/54FC1C70".to_string()));
        assert_eq!(conn1.flush_lsn, Some("1198/54000000".to_string()));
        assert_eq!(conn1.replay_lsn, None); // null in JSON

        // Check second connection (normal streaming replica)
        let conn2 = &data.replication[1];
        assert_eq!(conn2.pid, 630503);
        assert_eq!(conn2.state, "streaming");
        assert_eq!(conn2.application_name, "prod_pg_app008_db002");
        assert_eq!(conn2.sent_lsn, Some("1198/5510EC20".to_string()));
        assert_eq!(conn2.write_lsn, Some("1198/55107D00".to_string()));
        assert_eq!(conn2.flush_lsn, Some("1198/55107D00".to_string()));
        assert_eq!(conn2.replay_lsn, Some("1198/55107D00".to_string()));
        assert_eq!(conn2.sync_state, PgSyncSettings::Quorum);

        // Check third connection (backup state with all nulls)
        let conn3 = &data.replication[2];
        assert_eq!(conn3.pid, 1745964);
        assert_eq!(conn3.state, "backup");
        assert_eq!(conn3.application_name, "pg_basebackup");
        assert_eq!(conn3.sent_lsn, None);
        assert_eq!(conn3.write_lsn, None);
        assert_eq!(conn3.flush_lsn, None);
        assert_eq!(conn3.replay_lsn, None);
        assert_eq!(conn3.reply_time, None);
    }

    /// Test that we can still deserialize connections with all LSN fields present
    #[test]
    fn test_deserialize_normal_replication_connection() {
        let json_data = r#"{
            "uptime": "1 day 00:00:00",
            "replication": [
                {
                    "pid": 12345,
                    "state": "streaming",
                    "usename": "replicator",
                    "sent_lsn": "0/3000000",
                    "usesysid": 16387,
                    "flush_lag": "00:00:00.001",
                    "flush_lsn": "0/3000000",
                    "write_lag": "00:00:00.001",
                    "write_lsn": "0/3000000",
                    "replay_lag": "00:00:00.002",
                    "replay_lsn": "0/2FFFFFF",
                    "reply_time": "2026-01-14T12:00:00+01:00",
                    "sync_state": "quorum",
                    "client_addr": "10.0.0.1",
                    "client_port": 5432,
                    "backend_xmin": "12345",
                    "backend_start": "2026-01-14T10:00:00+01:00",
                    "sync_priority": 1,
                    "client_hostname": "replica-1",
                    "application_name": "replica_1"
                }
            ],
            "timeline_id": 1,
            "configuration": {
                "wal_level": "replica",
                "synchronous_commit": "on"
            },
            "current_wal_lsn": "0/3000000"
        }"#;

        let result: Result<PrimaryHealthCheckResult, _> = serde_json::from_str(json_data);

        assert!(
            result.is_ok(),
            "Should successfully deserialize: {:?}",
            result.err()
        );

        let data = result.unwrap();
        let conn = &data.replication[0];

        assert_eq!(conn.sent_lsn, Some("0/3000000".to_string()));
        assert_eq!(conn.write_lsn, Some("0/3000000".to_string()));
        assert_eq!(conn.flush_lsn, Some("0/3000000".to_string()));
        assert_eq!(conn.replay_lsn, Some("0/2FFFFFF".to_string()));
    }
}
