use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum DbErrorKind {
    ConnectionRefused,
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
    if err.is_closed() {
        return DbErrorKind::ConnectionRefused;
    }
    if err.to_string().contains("timeout") {
        return DbErrorKind::ConnectionTimeout;
    }
    DbErrorKind::Other
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
