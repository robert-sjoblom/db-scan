use std::sync::Arc;

use openssh::{KnownHosts, Session};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{config::get_config, v2::node::Node};

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

#[instrument(skip(ssh_user), level = "debug", fields(node_name = %node.node_name))]
pub(super) async fn check_disk_health(node: &Arc<Node>, ssh_user: &str) -> DiskCheckOutcome {
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

    let window = get_config().disk_check_window_minutes;
    let output = match session
        .command("dmesg")
        .arg("-T")
        .arg("--since")
        .arg(format!("{window} minutes ago"))
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
