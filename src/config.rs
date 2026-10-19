use std::{fs, path::PathBuf, sync::OnceLock, time::Duration};

use anyhow::{Context as _, anyhow};
use clap::Parser;
use redact::Secret;
use regex_lite::Regex;
use serde::Deserialize;
use tokio_postgres::config::SslMode;
use tracing_subscriber::EnvFilter;

pub(crate) static CONFIG: OnceLock<DbScanConfig> = OnceLock::new();

pub(crate) fn get_config() -> &'static DbScanConfig {
    CONFIG.get().expect("CONFIG not initialized")
}

/// Resolved configuration consumed by the rest of the program.
#[expect(clippy::struct_excessive_bools, reason = "independent CLI flags")]
#[derive(Debug)]
pub(crate) struct DbScanConfig {
    pub(crate) print_config: bool,
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
    pub(crate) max_concurrency: usize,
    /// Deadline for a whole node connect: TCP, TLS handshake, startup and auth.
    pub(crate) connect_timeout: Duration,
    pub(crate) database_portal_url: String,
    pub(crate) capture: Option<CaptureFile>,
}

impl DbScanConfig {
    pub fn capture_cfg(&self) -> Option<tokio_postgres::Config> {
        let Some(pg_cfg) = &self.capture else {
            return None;
        };

        if !pg_cfg.enabled {
            return None;
        }

        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&pg_cfg.postgres.host)
            .port(pg_cfg.postgres.port)
            .dbname(&pg_cfg.postgres.dbname)
            .user(&pg_cfg.postgres.user)
            .password(self.pgpassword.expose_secret())
            .ssl_mode(SslMode::Prefer)
            .connect_timeout(Duration::from_secs(5));

        Some(cfg)
    }
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

    /// Print the resolved configuration (after merging CLI, env, and file) to stdout and exit.
    #[arg(long)]
    pub(crate) print_config: bool,

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

    /// Maximum number of nodes scanned concurrently. Higher values finish faster
    /// but can saturate shared SSH/Postgres targets and cause spurious failures.
    #[arg(long, env = "DB_SCAN_MAX_CONCURRENCY")]
    pub(crate) max_concurrency: Option<usize>,
}

#[derive(Deserialize, Default, Debug)]
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
    scan: ScanFile,
    #[serde(default)]
    database_portal: DatabasePortalFile,
    #[serde(default)]
    capture: Option<CaptureFile>,
}

#[derive(Deserialize, Default, Debug)]
pub(crate) struct CaptureFile {
    /// Lets the block stay in the file while capture is switched off.
    #[serde(default = "enabled_by_default")]
    pub(crate) enabled: bool,
    pub(crate) postgres: PostgresCapture,
}

fn enabled_by_default() -> bool {
    true
}

#[derive(Deserialize, Default, Debug)]
pub(crate) struct PostgresCapture {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) dbname: String,
    pub(crate) user: String,
}

#[derive(Deserialize, Default, Debug)]
struct DatabasePortalFile {
    url: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
struct PostgresFile {
    user: Option<String>,
    sslkey: Option<PathBuf>,
    sslcert: Option<PathBuf>,
    sslrootcert: Option<PathBuf>,
}

#[derive(Deserialize, Default, Debug)]
struct DefaultsFile {
    user: Option<String>,
    password: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
struct SshFile {
    user: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
struct DisplayFile {
    log_level: Option<String>,
    no_color: Option<bool>,
}

#[derive(Deserialize, Default, Debug)]
struct ScanFile {
    max_concurrency: Option<usize>,
    connect_timeout_secs: Option<u64>,
}

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 15;

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
        Ok(s) => parse_file(&s, |key| {
            eprintln!(
                "warning: config file {}: unknown field `{key}` ignored",
                path.display()
            );
        })
        .with_context(|| format!("parsing config file {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !required => {
            Ok(FileConfig::default())
        }
        Err(e) => Err(e).with_context(|| format!("reading config file {}", path.display())),
    }
}

/// Parse the YAML config. Keys the schema does not know are reported to
/// `on_unknown` as dotted paths (`scan.new_knob`) and otherwise ignored, so a
/// config written for another release still loads.
fn parse_file(
    yaml: &str,
    mut on_unknown: impl FnMut(String),
) -> Result<FileConfig, serde_yaml::Error> {
    let de = serde_yaml::Deserializer::from_str(yaml);
    serde_ignored::deserialize(de, |path| on_unknown(key_path(&path)))
}

/// Render an ignored key as the operator wrote it, `capture.extra`, skipping
/// the `Option` and newtype layers `serde_ignored` would otherwise show as `?`.
fn key_path(path: &serde_ignored::Path<'_>) -> String {
    use serde_ignored::Path;

    match path {
        Path::Root => String::new(),
        Path::Seq { parent, index } => format!("{}[{index}]", key_path(parent)),
        Path::Map { parent, key } => match key_path(parent) {
            parent if parent.is_empty() => key.clone(),
            parent => format!("{parent}.{key}"),
        },
        Path::Some { parent }
        | Path::NewtypeStruct { parent }
        | Path::NewtypeVariant { parent } => key_path(parent),
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
    let max_concurrency = cli
        .max_concurrency
        .or(file.scan.max_concurrency)
        .unwrap_or(256)
        .max(1);
    let connect_timeout = Duration::from_secs(
        file.scan
            .connect_timeout_secs
            .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS)
            .max(1),
    );
    let database_portal_url = file
        .database_portal
        .url
        .ok_or_else(|| anyhow!("database_portal.url not set in config"))?;
    let capture = file.capture;

    let config = DbScanConfig {
        print_config: cli.print_config,
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
        max_concurrency,
        connect_timeout,
        database_portal_url,
        capture,
    };

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> (FileConfig, Vec<String>) {
        let mut unknown = Vec::new();
        let file = parse_file(yaml, |path| unknown.push(path)).expect("config parses");
        (file, unknown)
    }

    #[test]
    fn unknown_top_level_key_is_reported_and_ignored() {
        let (file, unknown) = parse("future_section: {a: 1}\nscan: {max_concurrency: 4}\n");

        assert_eq!(unknown, ["future_section"]);
        assert_eq!(file.scan.max_concurrency, Some(4));
    }

    #[test]
    fn unknown_nested_key_is_reported_with_its_section() {
        let (file, unknown) = parse("scan: {max_concurrency: 4, new_knob: 2}\n");

        assert_eq!(unknown, ["scan.new_knob"]);
        assert_eq!(file.scan.max_concurrency, Some(4));
    }

    #[test]
    fn known_keys_are_not_reported() {
        let (file, unknown) = parse(
            "postgres: {user: u, sslkey: /k, sslcert: /c, sslrootcert: /r}
defaults: {user: d, password: p}
ssh: {user: s}
display: {log_level: info, no_color: true}
scan: {max_concurrency: 4, connect_timeout_secs: 30}
database_portal: {url: http://x}
",
        );

        assert!(unknown.is_empty(), "reported {unknown:?}");
        assert_eq!(file.postgres.user.as_deref(), Some("u"));
        assert_eq!(file.display.no_color, Some(true));
        assert_eq!(file.scan.connect_timeout_secs, Some(30));
        assert_eq!(file.database_portal.url.as_deref(), Some("http://x"));
    }

    #[test]
    fn unknown_key_inside_optional_section_keeps_a_plain_path() {
        let (_, unknown) = parse(
            "capture: {enabled: false, postgres: {host: h, port: 1, dbname: d, user: u}, extra: 1}\n",
        );

        assert_eq!(unknown, ["capture.extra"]);
    }

    #[test]
    fn capture_block_without_enabled_key_is_on() {
        let (file, unknown) =
            parse("capture: {postgres: {host: h, port: 1, dbname: d, user: u}}\n");

        assert!(file.capture.expect("capture block parsed").enabled);
        assert!(unknown.is_empty(), "reported {unknown:?}");
    }

    #[test]
    fn enabled_false_keeps_the_block_and_turns_capture_off() {
        let (file, unknown) =
            parse("capture: {enabled: false, postgres: {host: h, port: 1, dbname: d, user: u}}\n");

        assert!(!file.capture.expect("capture block parsed").enabled);
        assert!(unknown.is_empty(), "reported {unknown:?}");
    }

    #[test]
    fn capture_block_absent_turns_capture_off() {
        let (file, _) = parse("scan: {max_concurrency: 4}\n");

        assert!(file.capture.is_none());
    }

    #[test]
    fn capture_block_without_postgres_is_an_error() {
        let err = parse_file("capture: {}\n", |_| {}).expect_err("postgres is required");

        assert!(err.to_string().contains("postgres"), "{err}");
    }

    #[test]
    fn capture_cfg_is_none_when_the_block_is_disabled() {
        let config = DbScanConfig {
            print_config: false,
            pguser: "u".into(),
            pgpassword: Secret::new("pw".into()),
            pgsslkey: PathBuf::from("/k"),
            pgsslcert: PathBuf::from("/c"),
            pgsslrootcert: PathBuf::from("/r"),
            cluster: None,
            log_level: EnvFilter::new("info"),
            show_healthy: false,
            show_failover: false,
            silence_tracing: false,
            default_user: "d".into(),
            default_pass: "p".into(),
            csv: None,
            no_color: false,
            watch: None,
            ssh_user: None,
            check_disks: false,
            max_concurrency: 1,
            connect_timeout: Duration::from_secs(15),
            database_portal_url: "http://x".into(),
            capture: Some(CaptureFile {
                enabled: false,
                postgres: PostgresCapture {
                    host: "h".into(),
                    port: 1,
                    dbname: "d".into(),
                    user: "u".into(),
                },
            }),
        };

        assert!(config.capture_cfg().is_none());
    }
}
