use std::{fs, sync::OnceLock, time::Duration};

use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::{Client, Config, Connection, Socket, config::SslMode};
use tracing::instrument;

use crate::{ARGS, Args, v2::node::Node};

// TODO: we shouldn't really lose the actual error types here, but `Deserialize` and `Serialize` can't be implemented
// on external types orz

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum DbError {
    #[error("TLS Connector Error: {0}")]
    TlsConnector(String),
    #[error("Postgres Error: {0}")]
    Postgres(String),
    #[error("IO Error: {0}")]
    Io(String),
    #[error("Serde JSON Error: {0}")]
    SerdeJson(String),
}

impl From<native_tls::Error> for DbError {
    fn from(err: native_tls::Error) -> Self {
        DbError::TlsConnector(err.to_string())
    }
}

impl From<tokio_postgres::Error> for DbError {
    fn from(err: tokio_postgres::Error) -> Self {
        DbError::Postgres(err.to_string())
    }
}

impl From<std::io::Error> for DbError {
    fn from(err: std::io::Error) -> Self {
        DbError::Io(err.to_string())
    }
}

impl From<serde_json::Error> for DbError {
    fn from(err: serde_json::Error) -> Self {
        DbError::SerdeJson(err.to_string())
    }
}

impl Eq for DbError {}
impl PartialEq for DbError {
    fn eq(&self, other: &Self) -> bool {
        use DbError::*;
        match (self, other) {
            (TlsConnector(a), TlsConnector(b)) => a == b,
            (Postgres(a), Postgres(b)) => a == b,
            (Io(a), Io(b)) => a == b,
            (SerdeJson(a), SerdeJson(b)) => a == b,
            _ => false,
        }
    }
}

static CONNECTOR: OnceLock<MakeTlsConnector> = OnceLock::new();
type PgConnection = Connection<Socket, postgres_native_tls::TlsStream<Socket>>;

pub async fn connect(node: &Node) -> Result<(Client, PgConnection), DbError> {
    tracing::trace!(node_name = %node.node_name, node_id = node.id, "connecting to node");
    let cfg = pg_cfg(node);
    let connector = if node.requires_cert() {
        connector()
    } else {
        &MakeTlsConnector::new(
            TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()?,
        )
    };

    let (client, conn) = cfg.connect(connector.clone()).await?;
    Ok((client, conn))
}

fn pg_cfg(node: &Node) -> Config {
    tracing::trace!(node_name = %node.node_name, "building pg config");
    let args = ARGS.get().expect("Args initialized");
    let mut cfg = Config::new();

    cfg.host(&node.node_name)
        .port(5432)
        .dbname("postgres")
        .connect_timeout(Duration::from_secs(10));

    if node.requires_cert() {
        cfg.ssl_mode(SslMode::Require)
            .user(&args.pguser)
            .password(args.pgpassword.expose_secret());
    } else {
        cfg.ssl_mode(SslMode::Prefer)
            .user("postgres")
            .password("fortnox1");
    }

    cfg
}

#[instrument(skip_all, level = "TRACE")]
pub fn connector() -> &'static MakeTlsConnector {
    tracing::trace!("getting TLS connector");
    CONNECTOR.get().expect("Connector initialized")
}

pub fn setup(cfg: &Args) {
    tracing::info!("setting up TLS connector");
    CONNECTOR.get_or_init(|| {
        let identity = Identity::from_pkcs8(
            &fs::read(&cfg.pgsslcert).expect("SSL cert exists"),
            &fs::read(&cfg.pgsslkey).expect("SSL key exists"),
        )
        .unwrap();

        let ca_cert =
            Certificate::from_pem(&fs::read(&cfg.pgsslrootcert).expect("SSL root cert exists"))
                .unwrap();

        let connector = TlsConnector::builder()
            .add_root_certificate(ca_cert)
            .identity(identity)
            .build()
            .expect("Build TLS connector");

        postgres_native_tls::MakeTlsConnector::new(connector)
    });
    tracing::info!("TLS connector set up");
}
