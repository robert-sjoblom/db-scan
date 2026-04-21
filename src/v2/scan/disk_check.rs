use std::sync::Arc;

use openssh::{KnownHosts, Session};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::instrument;

use crate::{
    config::get_config,
    pipeline::PipelineContext,
    v2::{analyze::ClusterHealth, node::Node},
};

use super::AnalyzedNode;

/// Result of checking dmesg for disk-related errors via SSH.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskCheckResult {
    /// Count of I/O errors (e.g., "I/O error", "Buffer I/O error")
    pub io_errors: u32,
    /// Count of filesystem errors (e.g., "EXT4-fs error", "XFS error")
    pub filesystem_errors: u32,
    /// Count of block device errors (e.g., "blk_update_request")
    pub block_errors: u32,
    /// Sample messages from dmesg (first N relevant lines)
    pub sample_messages: Vec<String>,
}

/// Outcome of a disk check attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DiskCheckOutcome {
    /// Check completed successfully
    Checked(DiskCheckResult),
    /// Check failed (SSH error, command error, etc.)
    Failed { reason: String },
}

const MAX_SAMPLE_MESSAGES: usize = 10;

/// Pipeline stage that enriches unhealthy clusters with disk check results.
/// Only runs disk checks if --check-disks flag is set and SSH_USER is configured.
#[instrument(skip_all, level = "info")]
pub async fn enrich_with_disk_checks(
    _ctx: Arc<PipelineContext>,
    mut rx: UnboundedReceiver<ClusterHealth>,
    tx: UnboundedSender<ClusterHealth>,
) {
    let config = get_config();
    let should_check = config.check_disks && config.ssh_user.is_some();

    if !should_check {
        // Pass through without disk checks
        while let Some(cluster_health) = rx.recv().await {
            if tx.send(cluster_health).is_err() {
                tracing::warn!("receiver dropped");
                break;
            }
        }
        return;
    }

    let ssh_user = config.ssh_user.as_ref().unwrap();
    tracing::info!(ssh_user = %ssh_user, "disk checks enabled for unhealthy clusters");

    while let Some(cluster_health) = rx.recv().await {
        let enriched = maybe_enrich_cluster(cluster_health, ssh_user).await;
        if tx.send(enriched).is_err() {
            tracing::warn!("receiver dropped");
            break;
        }
    }
}

async fn maybe_enrich_cluster(cluster_health: ClusterHealth, ssh_user: &str) -> ClusterHealth {
    if matches!(cluster_health, ClusterHealth::Healthy { .. }) {
        return cluster_health;
    }

    let cluster_name = cluster_health.cluster().name();
    tracing::info!(cluster = %cluster_name, "running disk checks on unhealthy cluster");

    // Extract, enrich, and rebuild
    match cluster_health {
        ClusterHealth::Healthy { .. } => unreachable!(),
        ClusterHealth::Degraded {
            lag,
            cluster,
            reason,
        } => {
            let enriched_cluster = enrich_analyzed_cluster(cluster, ssh_user).await;
            ClusterHealth::Degraded {
                lag,
                cluster: enriched_cluster,
                reason,
            }
        }
        ClusterHealth::Critical { cluster, reason } => {
            let enriched_cluster = enrich_analyzed_cluster(cluster, ssh_user).await;
            ClusterHealth::Critical {
                cluster: enriched_cluster,
                reason,
            }
        }
        ClusterHealth::Unknown {
            cluster,
            reachable_nodes,
            reason,
        } => {
            let enriched_cluster = enrich_analyzed_cluster(cluster, ssh_user).await;
            ClusterHealth::Unknown {
                cluster: enriched_cluster,
                reachable_nodes,
                reason,
            }
        }
    }
}

async fn enrich_analyzed_cluster(
    analyzed_cluster: crate::v2::analyze::AnalyzedCluster,
    ssh_user: &str,
) -> crate::v2::analyze::AnalyzedCluster {
    let crate::v2::analyze::AnalyzedCluster {
        cluster,
        backup_progress,
    } = analyzed_cluster;

    let crate::v2::cluster::Cluster {
        id,
        name,
        env,
        nodes,
    } = cluster;

    // Run disk checks on all nodes in parallel
    let mut handles = Vec::new();
    for node in nodes {
        let ssh_user = ssh_user.to_string();
        handles.push(tokio::spawn(
            async move { enrich_node(node, &ssh_user).await },
        ));
    }

    let enriched_nodes: Vec<AnalyzedNode> = futures::future::join_all(handles)
        .await
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();

    let enriched_cluster = crate::v2::cluster::Cluster {
        id,
        name,
        env,
        nodes: enriched_nodes,
    };

    crate::v2::analyze::AnalyzedCluster {
        cluster: enriched_cluster,
        backup_progress,
    }
}

async fn enrich_node(mut node: AnalyzedNode, ssh_user: &str) -> AnalyzedNode {
    tracing::debug!(node_name = %node.node_name, "running disk check");

    let node_ref = Arc::new(Node {
        id: node.id,
        cluster_id: node.cluster_id,
        node_name: node.node_name.clone(),
        pg_version: node.pg_version.clone(),
        ip_address: node.ip_address,
    });

    let result = check_disk_health(&node_ref, ssh_user).await;
    node.disk_check = Some(result);
    node
}

#[instrument(skip(ssh_user), level = "debug", fields(node_name = %node.node_name))]
async fn check_disk_health(node: &Arc<Node>, ssh_user: &str) -> DiskCheckOutcome {
    let destination = format!("{}@{}", ssh_user, node.ip_address);
    tracing::debug!(destination = %destination, "connecting via SSH for disk check");

    let session = match Session::connect_mux(&destination, KnownHosts::Accept).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "SSH connection failed");
            return DiskCheckOutcome::Failed {
                reason: format!("SSH connection failed: {e}"),
            };
        }
    };

    let output = match session
        .command("dmesg")
        .arg("-T")
        .raw_arg("2>/dev/null")
        .raw_arg("|")
        .arg("grep")
        .arg("-iE")
        .arg("I/O error|Buffer I/O|EXT4-fs error|XFS.*error|blk_update_request")
        .raw_arg("||")
        .arg("true")
        .output()
        .await
    {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(error = %e, "dmesg command failed");
            return DiskCheckOutcome::Failed {
                reason: format!("dmesg command failed: {e}"),
            };
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();

    let result = parse_dmesg_output(&lines);

    tracing::info!(
        node_name = %node.node_name,
        io_errors = result.io_errors,
        filesystem_errors = result.filesystem_errors,
        block_errors = result.block_errors,
        total_lines = lines.len(),
        "disk check completed"
    );

    DiskCheckOutcome::Checked(result)
}

fn parse_dmesg_output(lines: &[&str]) -> DiskCheckResult {
    let mut io_errors = 0u32;
    let mut filesystem_errors = 0u32;
    let mut block_errors = 0u32;
    let mut sample_messages = Vec::new();

    for line in lines {
        let lower = line.to_lowercase();

        // A line can match multiple categories
        if lower.contains("i/o error") || lower.contains("buffer i/o") {
            io_errors += 1;
        }
        if lower.contains("ext4-fs error")
            || lower.contains("ext4_")
            || (lower.contains("xfs") && lower.contains("error"))
        {
            filesystem_errors += 1;
        }
        if lower.contains("blk_update_request") {
            block_errors += 1;
        }

        if sample_messages.len() < MAX_SAMPLE_MESSAGES {
            sample_messages.push((*line).to_string());
        }
    }

    DiskCheckResult {
        io_errors,
        filesystem_errors,
        block_errors,
        sample_messages,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_output() {
        let result = parse_dmesg_output(&[]);
        assert_eq!(result.io_errors, 0);
        assert_eq!(result.filesystem_errors, 0);
        assert_eq!(result.block_errors, 0);
        assert!(result.sample_messages.is_empty());
    }

    #[test]
    fn parse_io_errors() {
        let lines = vec![
            "[Mon Apr 21 10:00:00 2025] blk_update_request: I/O error, dev sda, sector 123",
            "[Mon Apr 21 10:00:01 2025] Buffer I/O error on dev sda1",
        ];
        let result = parse_dmesg_output(&lines);

        assert_eq!(result.io_errors, 2);
        assert_eq!(result.block_errors, 1); // blk_update_request also counts
    }

    #[test]
    fn parse_filesystem_errors() {
        let lines = vec!["[Mon Apr 21 10:00:00 2025] EXT4-fs error (device sda1): ext4_lookup"];
        let result = parse_dmesg_output(&lines);

        assert_eq!(result.filesystem_errors, 1);
        assert_eq!(result.io_errors, 0);
    }

    #[test]
    fn sample_messages_limited() {
        let lines: Vec<&str> = (0..20).map(|_| "Buffer I/O error on dev sda1").collect();
        let result = parse_dmesg_output(&lines);

        assert_eq!(result.io_errors, 20);
        assert_eq!(result.sample_messages.len(), MAX_SAMPLE_MESSAGES);
    }
}
