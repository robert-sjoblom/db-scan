use std::{net::Ipv4Addr, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_postgres::Client;
use tracing::instrument;

use crate::{
    config::get_config,
    pipeline::PipelineContext,
    v2::{
        db::{self, DbError},
        node::Node,
        scan::{disk_check::DiskCheckOutcome, health_check_primary::PrimaryHealthCheckResult},
    },
};

pub mod disk_check;
pub mod health_check_primary;
pub mod health_check_replica;

#[instrument(skip_all, level = "info")]
pub async fn scan_nodes(
    _: Arc<PipelineContext>,
    mut rx: UnboundedReceiver<Node>,
    tx: UnboundedSender<AnalyzedNode>,
) {
    let mut handles = Vec::new();
    while let Some(node) = rx.recv().await {
        let tx = tx.clone();
        handles.push(tokio::spawn(async move { scan(node, tx).await }))
    }
    futures::future::join_all(handles).await;
}

#[instrument(skip(tx), level = "debug", fields(node_name = %node.node_name, node_id = node.id))]
async fn scan(node: Node, tx: UnboundedSender<AnalyzedNode>) {
    let node = Arc::from(node);
    let config = get_config();
    tracing::info!("starting node scan");

    // Start disk check in parallel if enabled — runs concurrently with PG health check
    let mut disk_check_handle = if config.check_disks && config.ssh_user.is_some() {
        let n = node.clone();
        let user = config.ssh_user.clone().unwrap();
        Some(tokio::spawn(async move {
            disk_check::check_disk_health(&n, &user).await
        }))
    } else {
        None
    };

    // Retry connection up to 3 times (initial attempt + 2 retries)
    let mut last_error = None;
    let (client, conn) = 'retry: {
        for attempt in 1..=3 {
            match db::connect(&node).await {
                Ok((client, conn)) => {
                    if attempt > 1 {
                        tracing::info!(
                            node_name = %node.node_name,
                            attempt = attempt,
                            max_attempts = 3,
                            "successfully connected after retry"
                        );
                    }
                    break 'retry (client, conn);
                }
                Err(e) => {
                    if attempt < 3 {
                        tracing::warn!(
                            node_name = %node.node_name,
                            attempt = attempt,
                            max_attempts = 3,
                            error = %e,
                            "connection attempt failed, retrying"
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                    } else {
                        tracing::error!(
                            node_name = %node.node_name,
                            attempt = attempt,
                            max_attempts = 3,
                            error = %e,
                            "connection failed after all retries"
                        );
                    }
                    last_error = Some(e);
                }
            }
        }

        // If we get here, all retries failed
        let e = last_error.unwrap();
        let disk_check = collect_disk_check(disk_check_handle.take()).await;
        match tx.send(AnalyzedNode {
            id: node.id,
            cluster_id: node.cluster_id,
            node_name: node.node_name.clone(),
            pg_version: node.pg_version.clone(),
            ip_address: node.ip_address,
            role: Role::Unknown,
            errors: vec![e],
            disk_check,
        }) {
            Ok(_) => {
                tracing::trace!(node_name = %node.node_name, "sent analyzed node after connection failure")
            }
            Err(_) => tracing::error!(node_name = %node.node_name, "failed to send analyzed node"),
        }
        return;
    };

    let conn_tx = tx.clone();
    let conn_node = node.clone();
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!(node_name = %conn_node.node_name, error = %e, "postgres connection closed with error");
            match conn_tx.send(AnalyzedNode {
                id: conn_node.id,
                cluster_id: conn_node.cluster_id,
                node_name: conn_node.node_name.clone(),
                pg_version: conn_node.pg_version.clone(),
                ip_address: conn_node.ip_address,
                role: Role::Unknown,
                errors: vec![e.into()],
                disk_check: None,
            }) {
                Ok(_) => {
                    tracing::trace!(node_name = %conn_node.node_name, "sent analyzed node after connection error")
                }
                Err(e) => {
                    tracing::error!(node_name = %conn_node.node_name, error = %e, "failed to send analyzed node")
                }
            }
        }
    });

    let primary = match is_primary(&client).await {
        Ok(r) => r,
        Err(e) => {
            let node_r = node.clone();
            let disk_check = collect_disk_check(disk_check_handle.take()).await;
            return match tx.send(AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.node_name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::Unknown,
                errors: vec![e],
                disk_check,
            }) {
                Ok(_) => {
                    tracing::trace!(node_name = %node_r.node_name, "sent node with unknown role")
                }
                Err(e) => {
                    tracing::error!(node_name = %node_r.node_name, error = %e, "failed to send node with unknown role")
                }
            };
        }
    };

    // Use an intermediate channel so we can patch the disk_check result in before forwarding
    let (hc_tx, mut hc_rx) = tokio::sync::mpsc::unbounded_channel::<AnalyzedNode>();
    let node_name = node.node_name.clone();

    if primary {
        tracing::trace!(node_name = %node_name, role = "primary", "spawning health check task");
        tokio::spawn(async move { health_check_primary::check(client, node, hc_tx).await });
    } else {
        tracing::trace!(node_name = %node_name, role = "replica", "spawning health check task");
        tokio::spawn(async move { health_check_replica::check(client, node, hc_tx).await });
    }

    // Await disk check concurrently with health check, then forward result
    let disk_check = collect_disk_check(disk_check_handle.take()).await;
    if let Some(mut analyzed_node) = hc_rx.recv().await {
        analyzed_node.disk_check = disk_check;
        if tx.send(analyzed_node).is_err() {
            tracing::error!(node_name = %node_name, "failed to send analyzed node");
        }
    }
}

async fn collect_disk_check(
    handle: Option<tokio::task::JoinHandle<DiskCheckOutcome>>,
) -> Option<DiskCheckOutcome> {
    match handle {
        None => None,
        Some(h) => h.await.ok(),
    }
}

#[instrument(skip(client), level = "trace")]
async fn is_primary(client: &Client) -> Result<bool, DbError> {
    match client.query_one("SELECT pg_is_in_recovery()", &[]).await {
        Ok(row) => {
            let in_recovery = row.get::<usize, bool>(0);
            let is_primary = !in_recovery;
            tracing::debug!(is_primary, in_recovery, "Determined node role");
            Ok(is_primary)
        }
        Err(e) => {
            tracing::error!(error = %e, "Failed to determine if node is primary");
            Err(e.into())
        }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone))]
pub struct AnalyzedNode {
    pub id: u32,
    pub cluster_id: u32,
    pub node_name: String,
    pub pg_version: String,
    pub ip_address: Ipv4Addr,
    pub role: Role,
    pub errors: Vec<DbError>,
    /// Disk health check result (populated for all nodes when --check-disks is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_check: Option<DiskCheckOutcome>,
}

impl AnalyzedNode {
    pub fn env(&self) -> String {
        self.node_name.split('-').next().unwrap().to_owned()
    }

    pub fn cluster_name(&self) -> String {
        self.node_name
            .split('-')
            .take(3)
            .collect::<Vec<&str>>()
            .join("-")
    }
}

/*
All nodes:
    Timeline (SELECT timeline_id FROM pg_control_checkpoint())

    Uptime (optional, helpful in failover analysis)

    Current WAL LSN (pg_current_wal_lsn() or pg_last_wal_receive_lsn()/pg_last_wal_replay_lsn() depending on role)

    System identifier (pg_control_system()) → helps validate all nodes belong to same cluster

    Connection status of replicas (pg_stat_replication from primary)?

If primary:
    pg_is_in_recovery() → false

    pg_current_wal_lsn() → to calculate replication lag

    SELECT pid, application_name, client_addr, state, sync_state, write_lag, flush_lag, replay_lag FROM pg_stat_replication

    This gives:

    Connected replicas

    Lag metrics

    Sync status (sync, async, etc.)

    IP mapping of replica clients

    Timeline ID (SELECT timeline_id FROM pg_control_checkpoint())

If replica:
    pg_is_in_recovery() → true

    pg_last_wal_receive_lsn() and pg_last_wal_replay_lsn()

    pg_stat_wal_receiver:

    status, receive_start_lsn, received_tli, sender_host, sync_priority, sync_state

    Crucial for understanding who the replica is following

    Timeline ID (to detect split-brain or divergence)

    Lag calculation (if possible: difference between replica LSN and primary LSN)

    Can't be calculated unless you know the primary’s current WAL LSN
*/

use crate::v2::scan::health_check_replica::ReplicaHealthCheckResult;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone))]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Unknown,
    /// If the primary health check fails
    UnknownPrimary,
    /// If the replica health check fails
    UnknownReplica,
    /// Primary node
    Primary {
        health: Box<PrimaryHealthCheckResult>,
    },
    /// Replica node
    Replica {
        health: Box<ReplicaHealthCheckResult>,
    },
}

impl Role {
    pub fn is_primary(&self) -> bool {
        matches!(self, Role::Primary { .. })
    }

    pub fn is_replica(&self) -> bool {
        matches!(self, Role::Replica { .. })
    }

    /// Returns the primary health data if this role is Primary, None otherwise.
    pub fn as_primary(&self) -> Option<&PrimaryHealthCheckResult> {
        match self {
            Role::Primary { health } => Some(health),
            _ => None,
        }
    }
}
