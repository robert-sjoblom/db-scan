use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum DbErrorKind {
    ConnectionRefused,
    ConnectionClosed,
    ConnectionTimeout,
    AuthenticationFailed,
    TlsHandshakeFailed,
    SslCertificateInvalid,
    InsufficientPrivileges,
    QuerySyntaxError,
    QueryFailed,
    InvalidResponse,
    Other,
}

impl std::fmt::Display for DbErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match *self {
            DbErrorKind::ConnectionRefused => "connection refused",
            DbErrorKind::ConnectionClosed => "connection closed",
            DbErrorKind::ConnectionTimeout => "connection timeout",
            DbErrorKind::AuthenticationFailed => "authentication failed",
            DbErrorKind::TlsHandshakeFailed => "TLS handshake failed",
            DbErrorKind::SslCertificateInvalid => "SSL certificate invalid",
            DbErrorKind::InsufficientPrivileges => "insufficient privileges",
            DbErrorKind::QuerySyntaxError => "query syntax error",
            DbErrorKind::QueryFailed => "query failed",
            DbErrorKind::InvalidResponse => "invalid response",
            DbErrorKind::Other => "other error",
        };
        f.write_str(s)
    }
}

pub fn classify_postgres(err: &tokio_postgres::Error) -> DbErrorKind {
    if let Some(db_err) = err.as_db_error() {
        return match db_err.code().code() {
            "28000" | "28P01" => DbErrorKind::AuthenticationFailed,
            "42501" => DbErrorKind::InsufficientPrivileges,
            "42601" => DbErrorKind::QuerySyntaxError,
            _ => DbErrorKind::QueryFailed,
        };
    }
    // Server hung up after the socket was open (mid-handshake, during auth,
    // or on an established connection) — not a refused TCP connect.
    match io_error_kind(err) {
        Some(std::io::ErrorKind::ConnectionRefused) => return DbErrorKind::ConnectionRefused,
        Some(std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe) => {
            return DbErrorKind::ConnectionClosed;
        }
        _ => {}
    }
    if err.is_closed() {
        return DbErrorKind::ConnectionClosed;
    }
    if err.to_string().contains("timeout") {
        return DbErrorKind::ConnectionTimeout;
    }
    DbErrorKind::Other
}

fn io_error_kind(err: &tokio_postgres::Error) -> Option<std::io::ErrorKind> {
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            return Some(io.kind());
        }
        source = e.source();
    }
    None
}

/// Lift a `tokio_postgres::Error` into `anyhow::Error` with a classified
/// [`DbErrorKind`] attached as context so callers can recover it via
/// [`extract_kind`].
pub fn pg_err(e: tokio_postgres::Error) -> anyhow::Error {
    let kind = classify_postgres(&e);
    anyhow::Error::new(e).context(kind)
}

pub fn serde_err(e: serde_json::Error) -> anyhow::Error {
    anyhow::Error::new(e).context(DbErrorKind::InvalidResponse)
}

/// Walk an `anyhow::Error`'s context chain for the [`DbErrorKind`] attached by
/// `*_err` helpers. Falls back to [`DbErrorKind::Other`] if none was attached.
pub fn extract_kind(err: &anyhow::Error) -> DbErrorKind {
    err.downcast_ref::<DbErrorKind>()
        .copied()
        .unwrap_or(DbErrorKind::Other)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_cfg(port: u16) -> tokio_postgres::Config {
        let mut cfg = tokio_postgres::Config::new();
        cfg.host("127.0.0.1")
            .port(port)
            .user("x")
            .ssl_mode(tokio_postgres::config::SslMode::Disable);
        cfg
    }

    #[tokio::test]
    async fn connect_to_closed_port_is_refused() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };

        let err = local_cfg(port)
            .connect(tokio_postgres::NoTls)
            .await
            .err()
            .expect("closed port must not connect");
        assert_eq!(classify_postgres(&err), DbErrorKind::ConnectionRefused);
    }

    #[tokio::test]
    async fn server_hangup_mid_startup_is_closed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let _server = tokio::spawn(async move {
            // Read the startup message first so the close is a clean FIN.
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0_u8; 256];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await;
            drop(sock);
        });

        let err = local_cfg(port)
            .connect(tokio_postgres::NoTls)
            .await
            .err()
            .expect("hung-up server must not connect");
        assert_eq!(classify_postgres(&err), DbErrorKind::ConnectionClosed);
    }

    #[test]
    fn extract_kind_finds_attached_kind() {
        let err = anyhow::Error::msg("boom")
            .context(DbErrorKind::ConnectionRefused)
            .context("attempting: connect to node");
        assert_eq!(extract_kind(&err), DbErrorKind::ConnectionRefused);
    }

    #[test]
    fn extract_kind_defaults_to_other() {
        let err = anyhow::anyhow!("bare error");
        assert_eq!(extract_kind(&err), DbErrorKind::Other);
    }
}
