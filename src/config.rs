use std::{path::PathBuf, sync::OnceLock};

use clap::Parser;
use redact::Secret;
use regex_lite::Regex;
use tracing_subscriber::EnvFilter;

pub(crate) static CONFIG: OnceLock<DbScanConfig> = OnceLock::new();

pub(crate) fn get_config() -> &'static DbScanConfig {
    CONFIG.get().expect("CONFIG not initialized")
}

/// A tool to scan PostgreSQL clusters for configuration and health
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub(crate) struct DbScanConfig {
    /// Your PG User
    #[arg(long, env = "PGUSER")]
    pub(crate) pguser: String,

    /// Your PG password
    #[arg(long, env = "PGPASSWORD", hide = true)]
    pub(crate) pgpassword: Secret<String>,

    /// Your ssl key file
    #[arg(long, env = "PGSSLKEY")]
    pub(crate) pgsslkey: PathBuf,

    /// Your ssl cert file
    #[arg(long, env = "PGSSLCERT")]
    pub(crate) pgsslcert: PathBuf,

    /// Your ssl root cert file
    #[arg(long, env = "PGSSLROOTCERT")]
    pub(crate) pgsslrootcert: PathBuf,

    /// Cluster to scan (regex)
    #[arg(short, long, value_parser = parse_cluster_regex)]
    pub(crate) cluster: Option<Regex>,

    /// Log level
    #[arg(short, long, env = "RUST_LOG", default_value = "info")]
    pub(crate) log_level: EnvFilter,

    /// Show healthy clusters in output
    #[arg(long)]
    pub(crate) show_healthy: bool,

    /// Show healthy clusters that have experienced failover
    #[arg(long)]
    pub(crate) show_failover: bool,

    /// Silence tracing, useful when running a watch command
    #[arg(long, short)]
    pub(crate) silence_tracing: bool,

    /// Default user to use when not connecting with cert auth
    #[arg(long, env = "DEFAULT_USER")]
    pub(crate) default_user: String,

    /// Default password to use when not connecting with cert auth
    #[arg(long, env = "DEFAULT_PASS")]
    pub(crate) default_pass: String,

    /// Write CSV output to file
    #[arg(long)]
    pub(crate) csv: Option<String>,

    /// Disable colors in terminal output
    #[arg(long)]
    pub(crate) no_color: bool,

    /// Watch mode: continuously rescan unhealthy clusters at the specified interval (seconds).
    /// Defaults to 60 seconds when flag is present without a value.
    #[arg(long, default_missing_value = "60", num_args = 0..=1)]
    pub(crate) watch: Option<u64>,

    /// SSH user for disk health checks (e.g., "first_last" format)
    #[arg(long, env = "SSH_USER")]
    pub(crate) ssh_user: Option<String>,

    /// Enable disk health checks via SSH on unhealthy nodes
    #[arg(long)]
    pub(crate) check_disks: bool,

    /// Recency window in minutes for dmesg entries to count against health.
    /// Older entries are ignored.
    #[arg(long, env = "DISK_CHECK_WINDOW_MINUTES", default_value = "60")]
    pub(crate) disk_check_window_minutes: u64,
}

fn parse_cluster_regex(s: &str) -> Result<Regex, regex_lite::Error> {
    Regex::new(s)
}

impl DbScanConfig {
    pub(crate) fn cluster_pattern(&self) -> String {
        self.cluster
            .as_ref()
            .map(|r| r.as_str().to_string())
            .unwrap_or_else(|| ".*-(pg|ts)-.*".to_string())
    }
}
