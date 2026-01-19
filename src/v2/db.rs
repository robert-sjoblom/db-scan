use std::{fs, sync::OnceLock, time::Duration};

use native_tls::{Certificate, Identity, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, Config, Connection, Socket, config::SslMode};
use tracing::instrument;

use crate::{ARGS, Args, v2::node::Node};
pub use db_error::DbError;

mod db_error;

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
