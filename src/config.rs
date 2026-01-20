use std::{path::PathBuf, sync::OnceLock};

use clap::Parser;
use redact::Secret;
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

    /// Cluster to scan
    #[arg(short, long)]
    pub(crate) cluster: Option<String>,

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
}

impl DbScanConfig {
    pub(crate) fn cluster_pattern(&self) -> String {
        self.cluster
            .as_ref()
            .map(|s| format!("{s}.*"))
            .unwrap_or_else(|| ".*-(pg|ts)-.*".to_string())
    }
}
