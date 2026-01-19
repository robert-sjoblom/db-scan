use std::{net::Ipv4Addr, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::Client;
use tracing::instrument;

use crate::{
    timings::{Event, Stage},
    v2::{
        db::{self, DbError},
        node::Node,
        scan::health_check_primary::PrimaryHealthCheckResult,
    },
};

pub mod health_check_primary;
pub mod health_check_replica;

#[instrument(skip_all, level = "info")]
pub async fn scan_nodes(
    tx: UnboundedSender<AnalyzedNode>,
    nodes: impl Iterator<Item = Node>,
    timings_tx: UnboundedSender<Event>,
) {
    timings_tx.send(Event::Start(Stage::Scan)).ok();
    let handles = nodes.into_iter().map(|node| {
        let tx = tx.clone();
        tokio::spawn(async move { scan(node, tx).await })
    });
    futures::future::join_all(handles).await;

    tracing::info!("node scanning completed");
    timings_tx.send(Event::End(Stage::Scan)).ok();
}

#[instrument(skip(tx), level = "debug", fields(node_name = %node.node_name, node_id = node.id))]
async fn scan(node: Node, tx: UnboundedSender<AnalyzedNode>) {
    let node = Arc::from(node);
    tracing::info!("starting node scan");

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
        match tx.send(AnalyzedNode {
            id: node.id,
            cluster_id: node.cluster_id,
            node_name: node.node_name.clone(),
            pg_version: node.pg_version.clone(),
            ip_address: node.ip_address,
            role: Role::Unknown,
            errors: vec![e],
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

    let mut handles = Vec::new();

    let primary = match is_primary(&client).await {
        Ok(r) => r,
        Err(e) => {
            let node_r = node.clone();
            return match tx.send(AnalyzedNode {
                id: node.id,
                cluster_id: node.cluster_id,
                node_name: node.node_name.clone(),
                pg_version: node.pg_version.clone(),
                ip_address: node.ip_address,
                role: Role::Unknown,
                errors: vec![e],
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

    if primary {
        tracing::trace!(node_name = %node.node_name, role = "primary", "spawning health check task");
        handles.push(tokio::spawn(async move {
            health_check_primary::check(client, node, tx).await
        }));
    } else {
        tracing::trace!(node_name = %node.node_name, role = "replica", "spawning health check task");
        handles.push(tokio::spawn(async move {
            health_check_replica::check(client, node, tx).await
        }));
    }

    // Wait for all health check tasks to complete
    for handle in handles {
        if let Err(e) = handle.await {
            tracing::error!(error = %e, "health check task failed");
        }
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
pub struct AnalyzedNode {
    pub id: u32,
    pub cluster_id: u32,
    pub node_name: String,
    pub pg_version: String,
    pub ip_address: Ipv4Addr,
    pub role: Role,
    pub errors: Vec<DbError>,
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
