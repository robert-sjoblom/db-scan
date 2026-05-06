use std::{fs, path::PathBuf, sync::OnceLock};

use anyhow::{Context as _, anyhow};
use clap::Parser;
use redact::Secret;
use regex_lite::Regex;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

pub(crate) static CONFIG: OnceLock<DbScanConfig> = OnceLock::new();

pub(crate) fn get_config() -> &'static DbScanConfig {
    CONFIG.get().expect("CONFIG not initialized")
}

/// Resolved configuration consumed by the rest of the program.
#[expect(clippy::struct_excessive_bools, reason = "independent CLI flags")]
#[derive(Debug)]
pub(crate) struct DbScanConfig {
    pub(crate) pguser: String,
    pub(crate) pgpassword: Secret<String>,
    pub(crate) pgsslkey: PathBuf,
    pub(crate) pgsslcert: PathBuf,
    pub(crate) pgsslrootcert: PathBuf,
    pub(crate) cluster: Option<Regex>,
    pub(crate) log_level: EnvFilter,
    pub(crate) show_healthy: bool,
    pub(crate) show_failover: bool,
    pub(crate) silence_tracing: bool,
    pub(crate) default_user: String,
    pub(crate) default_pass: String,
    pub(crate) csv: Option<String>,
    pub(crate) no_color: bool,
    pub(crate) watch: Option<u64>,
    pub(crate) ssh_user: Option<String>,
    pub(crate) check_disks: bool,
    pub(crate) disk_check_window_minutes: u64,
}

/// A tool to scan `PostgreSQL` clusters for configuration and health.
#[expect(clippy::struct_excessive_bools, reason = "independent CLI flags")]
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub(crate) struct CliArgs {
    /// Path to config file. Defaults to `$XDG_CONFIG_HOME/db-scan/config.yml`
    /// (or `~/.config/db-scan/config.yml`).
    #[arg(long)]
    pub(crate) config: Option<PathBuf>,

    /// Skip loading the config file.
    #[arg(long)]
    pub(crate) no_config: bool,

    /// Your PG User.
    #[arg(long, env = "PGUSER")]
    pub(crate) pguser: Option<String>,

    /// Your PG password (env-only; not read from config file).
    #[arg(long, env = "PGPASSWORD", hide = true)]
    pub(crate) pgpassword: Secret<String>,

    /// Your ssl key file.
    #[arg(long, env = "PGSSLKEY")]
    pub(crate) pgsslkey: Option<PathBuf>,

    /// Your ssl cert file.
    #[arg(long, env = "PGSSLCERT")]
    pub(crate) pgsslcert: Option<PathBuf>,

    /// Your ssl root cert file.
    #[arg(long, env = "PGSSLROOTCERT")]
    pub(crate) pgsslrootcert: Option<PathBuf>,

    /// Cluster to scan (regex).
    #[arg(short, long, value_parser = parse_cluster_regex)]
    pub(crate) cluster: Option<Regex>,

    /// Log level.
    #[arg(short, long, env = "RUST_LOG")]
    pub(crate) log_level: Option<String>,

    /// Show healthy clusters in output.
    #[arg(long)]
    pub(crate) show_healthy: bool,

    /// Show healthy clusters that have experienced failover.
    #[arg(long)]
    pub(crate) show_failover: bool,

    /// Silence tracing, useful when running a watch command.
    #[arg(long, short)]
    pub(crate) silence_tracing: bool,

    /// Default user to use when not connecting with cert auth.
    #[arg(long, env = "DEFAULT_USER")]
    pub(crate) default_user: Option<String>,

    /// Default password to use when not connecting with cert auth.
    #[arg(long, env = "DEFAULT_PASS")]
    pub(crate) default_pass: Option<String>,

    /// Write CSV output to file.
    #[arg(long)]
    pub(crate) csv: Option<String>,

    /// Disable colors in terminal output.
    #[arg(long)]
    pub(crate) no_color: bool,

    /// Watch mode: continuously rescan unhealthy clusters at the specified interval (seconds).
    /// Defaults to 60 seconds when flag is present without a value.
    #[arg(long, default_missing_value = "60", num_args = 0..=1)]
    pub(crate) watch: Option<u64>,

    /// SSH user for disk health checks (e.g., "`first_last`" format).
    #[arg(long, env = "SSH_USER")]
    pub(crate) ssh_user: Option<String>,

    /// Enable disk health checks via SSH on unhealthy nodes.
    #[arg(long)]
    pub(crate) check_disks: bool,

    /// Recency window in minutes for dmesg entries to count against health.
    /// Older entries are ignored.
    #[arg(long, env = "DISK_CHECK_WINDOW_MINUTES")]
    pub(crate) disk_check_window_minutes: Option<u64>,
}

impl DbScanConfig {
    pub(crate) fn cluster_pattern(&self) -> String {
        self.cluster
            .as_ref()
            .map_or_else(|| ".*-(pg|ts)-.*".to_owned(), |r| r.as_str().to_owned())
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    #[serde(default)]
    postgres: PostgresFile,
    #[serde(default)]
    defaults: DefaultsFile,
    #[serde(default)]
    ssh: SshFile,
    #[serde(default)]
    display: DisplayFile,
    #[serde(default)]
    disk_check: DiskCheckFile,
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct PostgresFile {
    user: Option<String>,
    sslkey: Option<PathBuf>,
    sslcert: Option<PathBuf>,
    sslrootcert: Option<PathBuf>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct DefaultsFile {
    user: Option<String>,
    password: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct SshFile {
    user: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct DisplayFile {
    log_level: Option<String>,
    no_color: Option<bool>,
}

#[derive(Deserialize, Default, Debug)]
#[serde(deny_unknown_fields)]
struct DiskCheckFile {
    window_minutes: Option<u64>,
}

fn parse_cluster_regex(s: &str) -> Result<Regex, regex_lite::Error> {
    Regex::new(s)
}

fn default_config_path() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))?;
    Some(base.join("db-scan").join("config.yml"))
}

fn load_file(explicit: Option<&PathBuf>, no_config: bool) -> anyhow::Result<FileConfig> {
    if no_config {
        return Ok(FileConfig::default());
    }
    let (path, required) = match explicit {
        Some(p) => (p.clone(), true),
        None => match default_config_path() {
            Some(p) => (p, false),
            None => return Ok(FileConfig::default()),
        },
    };
    match fs::read_to_string(&path) {
        Ok(s) => serde_yaml::from_str(&s)
            .with_context(|| format!("parsing config file {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => {
            Ok(FileConfig::default())
        }
        Err(e) => Err(e).with_context(|| format!("reading config file {}", path.display())),
    }
}

/// Parse CLI args, load the config file, and merge into the final `DbScanConfig`.
pub(crate) fn load() -> anyhow::Result<DbScanConfig> {
    let cli = CliArgs::parse();
    let file = load_file(cli.config.as_ref(), cli.no_config)?;

    let pguser = cli
        .pguser
        .or(file.postgres.user)
        .ok_or_else(|| anyhow!("pguser not set (CLI --pguser, PGUSER env, or postgres.user)"))?;
    let pgsslkey = cli.pgsslkey.or(file.postgres.sslkey).ok_or_else(|| {
        anyhow!("pgsslkey not set (CLI --pgsslkey, PGSSLKEY env, or postgres.sslkey)")
    })?;
    let pgsslcert = cli.pgsslcert.or(file.postgres.sslcert).ok_or_else(|| {
        anyhow!("pgsslcert not set (CLI --pgsslcert, PGSSLCERT env, or postgres.sslcert)")
    })?;
    let pgsslrootcert = cli.pgsslrootcert.or(file.postgres.sslrootcert).ok_or_else(|| {
        anyhow!(
            "pgsslrootcert not set (CLI --pgsslrootcert, PGSSLROOTCERT env, or postgres.sslrootcert)"
        )
    })?;
    let default_user = cli.default_user.or(file.defaults.user).ok_or_else(|| {
        anyhow!("default_user not set (CLI --default-user, DEFAULT_USER env, or defaults.user)")
    })?;
    let default_pass = cli.default_pass.or(file.defaults.password).ok_or_else(|| {
        anyhow!("default_pass not set (CLI --default-pass, DEFAULT_PASS env, or defaults.password)")
    })?;
    let log_level_str = cli
        .log_level
        .or(file.display.log_level)
        .unwrap_or_else(|| "info".to_owned());
    let log_level =
        EnvFilter::try_new(&log_level_str).context("parsing log_level as tracing EnvFilter")?;
    let no_color = cli.no_color || file.display.no_color.unwrap_or(false);
    let ssh_user = cli.ssh_user.or(file.ssh.user);
    let disk_check_window_minutes = cli
        .disk_check_window_minutes
        .or(file.disk_check.window_minutes)
        .unwrap_or(60);

    Ok(DbScanConfig {
        pguser,
        pgpassword: cli.pgpassword,
        pgsslkey,
        pgsslcert,
        pgsslrootcert,
        cluster: cli.cluster,
        log_level,
        show_healthy: cli.show_healthy,
        show_failover: cli.show_failover,
        silence_tracing: cli.silence_tracing,
        default_user,
        default_pass,
        csv: cli.csv,
        no_color,
        watch: cli.watch,
        ssh_user,
        check_disks: cli.check_disks,
        disk_check_window_minutes,
    })
}
