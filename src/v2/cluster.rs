use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    pipeline::PipelineContext,
    v2::scan::{AnalyzedNode, health_check_primary::ReplicationConnection},
};

/// Listens for incoming Nodes, groups them by cluster_id, and sends complete Clusters
/// to the provided cluster channel. A Cluster is considered complete when it has 3 Nodes.
///
/// If the channel closes, the function will exit gracefully after logging any incomplete clusters.
pub async fn cluster_builder(
    _: Arc<PipelineContext>,
    mut node_rx: UnboundedReceiver<AnalyzedNode>,
    cluster_tx: UnboundedSender<Cluster>,
) {
    let mut nodes: HashMap<u32, Vec<AnalyzedNode>> = HashMap::new();

    while let Some(node) = node_rx.recv().await {
        tracing::trace!(
            sender_count = node_rx.sender_strong_count(),
            "node receiver active"
        );
        tracing::trace!(node_id = node.id, node_name = %node.node_name, cluster_id = node.cluster_id, "received node");
        tracing::trace!(clusters_count = nodes.len(), "current clusters in progress");

        let cluster_id = node.cluster_id;
        nodes.entry(cluster_id).or_default().push(node);

        // If we have received all nodes for a cluster, send the cluster to the cluster channel
        if nodes[&cluster_id].len() == 3 {
            let cluster_nodes = nodes.remove(&cluster_id).unwrap();
            let env = cluster_nodes[0].env();
            let name = cluster_nodes[0].cluster_name();
            let cluster = Cluster {
                id: cluster_id,
                name: name.clone(),
                nodes: cluster_nodes,
                env,
            };
            // TODO: don't use unwraps like this.
            cluster_tx.send(cluster).unwrap();
            tracing::info!(cluster_id = cluster_id, cluster_name = %name, "sent complete cluster");
        }
    }

    if !nodes.is_empty() {
        tracing::warn!(incomplete_clusters = nodes.len(), clusters = ?nodes, "node channel closed with incomplete clusters");
    } else {
        tracing::info!("node channel closed, all clusters processed");
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cluster {
    pub id: u32,
    pub name: String,
    pub env: String,
    pub nodes: Vec<AnalyzedNode>,
}

impl Cluster {
    /// Returns an iterator over all primary nodes in the cluster
    pub fn primaries(&self) -> impl Iterator<Item = &AnalyzedNode> {
        self.nodes.iter().filter(|n| n.role.is_primary())
    }

    /// Returns an iterator over all replica nodes in the cluster
    pub fn replicas(&self) -> impl Iterator<Item = &AnalyzedNode> {
        self.nodes.iter().filter(|n| n.role.is_replica())
    }

    /// Returns the primary node if exactly one exists
    pub fn primary(&self) -> Option<&AnalyzedNode> {
        let primaries: Vec<_> = self.primaries().collect();
        if primaries.len() == 1 {
            primaries.first().copied()
        } else {
            tracing::debug!(
                primary_count = primaries.len(),
                "no single primary found in cluster"
            );
            None
        }
    }
}

impl Cluster {
    /// Returns replication connections from the primary (if there's only one primary)
    pub fn primary_replication_info(&self) -> Option<&Vec<ReplicationConnection>> {
        use super::scan::Role;

        self.primary().and_then(|p| match &p.role {
            Role::Primary { health } => Some(&health.replication),
            _ => None,
        })
    }
}
