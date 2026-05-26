use std::{fs, io::BufReader, sync::Arc, sync::OnceLock, time::Duration};

use rustls::{
    ClientConfig, RootCertStore, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, ring},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
};
use tokio_postgres::{Client, Config, Connection, Socket, config::SslMode, tls::MakeTlsConnect};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::instrument;

use anyhow::Context as _;

use crate::{CONFIG, config::DbScanConfig, errors, v2::node::Node};

static CONNECTOR: OnceLock<MakeRustlsConnect> = OnceLock::new();
static INSECURE_CONNECTOR: OnceLock<MakeRustlsConnect> = OnceLock::new();
pub type PgConnection = Connection<Socket, <MakeRustlsConnect as MakeTlsConnect<Socket>>::Stream>;

/// Connect using a caller-supplied [`Config`] and the global cert-based TLS
/// connector. Returns `(Client, PgConnection)`; the caller is responsible for
/// spawning the connection driver.
///
/// Used by features that need to connect to a Postgres that isn't part of the
/// scanned node fleet (e.g. capture uploads).
pub async fn connect_with(cfg: &Config) -> anyhow::Result<(Client, PgConnection)> {
    cfg.connect(connector().clone())
        .await
        .map_err(errors::pg_err)
        .context("attempting: postgres connect")
}

pub async fn connect(node: &Node) -> anyhow::Result<(Client, PgConnection)> {
    tracing::trace!(node_name = %node.name, node_id = node.id, "connecting to node");
    let cfg = pg_cfg(node);
    let connector = if node.requires_cert() {
        connector()
    } else {
        insecure_connector()
    };

    let (client, conn) = cfg
        .connect(connector.clone())
        .await
        .map_err(errors::pg_err)
        .with_context(|| format!("requires_cert: {}", node.requires_cert()))
        .context("attempting: postgres connect")?;
    Ok((client, conn))
}

fn pg_cfg(node: &Node) -> Config {
    tracing::trace!(node_name = %node.name, "building pg config");
    let args = CONFIG.get().expect("Args initialized");
    let mut cfg = Config::new();

    cfg.host(&node.name)
        .port(5432)
        .dbname("postgres")
        .connect_timeout(Duration::from_secs(10));

    if node.requires_cert() {
        cfg.ssl_mode(SslMode::Require)
            .user(&args.pguser)
            .password(args.pgpassword.expose_secret());
    } else {
        cfg.ssl_mode(SslMode::Prefer)
            .user(&args.default_user)
            .password(&args.default_pass);
    }

    cfg
}

#[instrument(skip_all, level = "TRACE")]
pub fn connector() -> &'static MakeRustlsConnect {
    tracing::trace!("getting TLS connector");
    CONNECTOR.get().expect("Connector initialized")
}

fn insecure_connector() -> &'static MakeRustlsConnect {
    INSECURE_CONNECTOR.get_or_init(|| {
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("Build TLS protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(
                NoCertVerification(ring::default_provider()),
            ))
            .with_no_client_auth();
        MakeRustlsConnect::new(config)
    })
}

pub fn setup(cfg: &DbScanConfig) {
    tracing::info!("setting up TLS connector");
    CONNECTOR.get_or_init(|| {
        let provider = Arc::new(ring::default_provider());

        let cert_pem = fs::read(&cfg.pgsslcert).expect("SSL cert exists");
        let key_pem = fs::read(&cfg.pgsslkey).expect("SSL key exists");
        let ca_pem = fs::read(&cfg.pgsslrootcert).expect("SSL root cert exists");

        let cert_chain: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(&*cert_pem))
                .collect::<Result<_, _>>()
                .expect("Parse client cert PEM");

        let key: PrivateKeyDer<'static> =
            rustls_pemfile::private_key(&mut BufReader::new(&*key_pem))
                .expect("Parse client key PEM")
                .expect("Client key present");

        let mut roots = RootCertStore::empty();
        let ca_certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut BufReader::new(&*ca_pem))
                .collect::<Result<_, _>>()
                .expect("Parse root CA PEM");
        for ca in ca_certs {
            roots.add(ca).expect("Add root CA");
        }

        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("Build TLS protocol versions")
            .with_root_certificates(roots)
            .with_client_auth_cert(cert_chain, key)
            .expect("Build TLS client config");

        MakeRustlsConnect::new(config)
    });
    tracing::info!("TLS connector set up");
}

#[derive(Debug)]
struct NoCertVerification(CryptoProvider);

impl ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
