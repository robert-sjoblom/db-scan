use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub enum DbErrorKind {
    ConnectionRefused,
    ConnectionTimeout,
    HostUnreachable,
    AuthenticationFailed,
    TlsHandshakeFailed,
    SslCertificateInvalid,
    InsufficientPrivileges,
    QuerySyntaxError,
    QueryFailed,
    InvalidResponse,
    StartingUp,
    ShuttingDown,
    NotAcceptingConnections,
    Other,
}

impl std::fmt::Display for DbErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match *self {
            DbErrorKind::ConnectionRefused => "connection refused",
            DbErrorKind::ConnectionTimeout => "connection timeout",
            DbErrorKind::HostUnreachable => "host unreachable",
            DbErrorKind::AuthenticationFailed => "authentication failed",
            DbErrorKind::TlsHandshakeFailed => "TLS handshake failed",
            DbErrorKind::SslCertificateInvalid => "SSL certificate invalid",
            DbErrorKind::InsufficientPrivileges => "insufficient privileges",
            DbErrorKind::QuerySyntaxError => "query syntax error",
            DbErrorKind::QueryFailed => "query failed",
            DbErrorKind::InvalidResponse => "invalid response",
            DbErrorKind::StartingUp => "starting up",
            DbErrorKind::ShuttingDown => "shutting down",
            DbErrorKind::NotAcceptingConnections => "not accepting connections",
            DbErrorKind::Other => "other error",
        };
        f.write_str(s)
    }
}

pub fn classify_postgres(err: &tokio_postgres::Error) -> DbErrorKind {
    if let Some(db_err) = err.as_db_error() {
        return classify_db_error(db_err.code().code(), db_err.message());
    }
    if err.is_closed() {
        return DbErrorKind::ConnectionRefused;
    }
    // tokio-postgres keeps the socket error in `source()` and leaves it out of `Display`.
    if let Some(io_err) =
        std::error::Error::source(err).and_then(|source| source.downcast_ref::<std::io::Error>())
    {
        return classify_io_error(io_err);
    }
    if err.to_string().contains("timeout") {
        return DbErrorKind::ConnectionTimeout;
    }
    DbErrorKind::Other
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "io::ErrorKind is non_exhaustive; unlisted kinds stay Other"
)]
fn classify_io_error(err: &std::io::Error) -> DbErrorKind {
    match err.kind() {
        std::io::ErrorKind::TimedOut => DbErrorKind::ConnectionTimeout,
        std::io::ErrorKind::ConnectionRefused => DbErrorKind::ConnectionRefused,
        std::io::ErrorKind::HostUnreachable | std::io::ErrorKind::NetworkUnreachable => {
            DbErrorKind::HostUnreachable
        }
        _ => DbErrorKind::Other,
    }
}

fn classify_db_error(code: &str, message: &str) -> DbErrorKind {
    match code {
        "28000" | "28P01" => DbErrorKind::AuthenticationFailed,
        "42501" => DbErrorKind::InsufficientPrivileges,
        "42601" => DbErrorKind::QuerySyntaxError,
        "57P03" => classify_cannot_connect_now(message),
        _ => DbErrorKind::QueryFailed,
    }
}

/// SQLSTATE 57P03 covers every phase where the postmaster refuses new
/// connections; only the message says which one.
fn classify_cannot_connect_now(message: &str) -> DbErrorKind {
    if message.contains("starting up") || message.contains("not yet accepting connections") {
        DbErrorKind::StartingUp
    } else if message.contains("shutting down") {
        DbErrorKind::ShuttingDown
    } else {
        DbErrorKind::NotAcceptingConnections
    }
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

    #[rstest::rstest]
    #[case::timed_out(std::io::ErrorKind::TimedOut, DbErrorKind::ConnectionTimeout)]
    #[case::refused(std::io::ErrorKind::ConnectionRefused, DbErrorKind::ConnectionRefused)]
    #[case::host_unreachable(std::io::ErrorKind::HostUnreachable, DbErrorKind::HostUnreachable)]
    #[case::network_unreachable(
        std::io::ErrorKind::NetworkUnreachable,
        DbErrorKind::HostUnreachable
    )]
    #[case::unmapped(std::io::ErrorKind::PermissionDenied, DbErrorKind::Other)]
    fn connect_io_errors_are_classified_by_kind(
        #[case] kind: std::io::ErrorKind,
        #[case] expected: DbErrorKind,
    ) {
        assert_eq!(classify_io_error(&std::io::Error::from(kind)), expected);
    }

    #[tokio::test]
    async fn refused_tcp_connect_is_connection_refused() {
        let closed_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();

        let Err(err) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(closed_port)
            .user("db-scan")
            .connect(tokio_postgres::NoTls)
            .await
        else {
            panic!("connect to a closed port succeeded");
        };

        assert_eq!(classify_postgres(&err), DbErrorKind::ConnectionRefused);
    }

    #[rstest::rstest]
    #[case::starting_up("the database system is starting up", DbErrorKind::StartingUp)]
    #[case::not_consistent(
        "the database system is not yet accepting connections",
        DbErrorKind::StartingUp
    )]
    #[case::shutting_down("the database system is shutting down", DbErrorKind::ShuttingDown)]
    #[case::crash_recovery(
        "the database system is in recovery mode",
        DbErrorKind::NotAcceptingConnections
    )]
    fn cannot_connect_now_is_split_by_message(
        #[case] message: &str,
        #[case] expected: DbErrorKind,
    ) {
        assert_eq!(classify_db_error("57P03", message), expected);
    }
}
