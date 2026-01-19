use serde::{Deserialize, Serialize};
use thiserror::Error;
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
