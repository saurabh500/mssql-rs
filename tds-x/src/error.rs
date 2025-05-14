use thiserror::Error;
use tokio::time::error::Elapsed;

#[derive(Debug, Error)]
pub enum TimeoutErrorType {
    #[error("Elapsed: {0}")]
    Elapsed(Elapsed),

    #[error("{0}")]
    String(String),
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Server redirected the connection: {host}:{port} times")]
    Redirection { host: String, port: u16 },

    #[error("Protocol Error: {0}")]
    ProtocolError(String),

    #[error("TLS Error: {0}")]
    TlsError(#[from] native_tls::Error),

    #[error("Timeout Error: {0}")]
    TimeoutError(TimeoutErrorType),

    #[error("Sql Error: {number}: {class}: {state}: {message} on {} in {} at line {}",
            server_name.clone().unwrap(), proc_name.clone().unwrap(), line_number.unwrap())]
    SqlServerError {
        message: String,
        state: u8,
        class: i32,
        number: u32,
        server_name: Option<String>,
        proc_name: Option<String>,
        line_number: Option<i32>,
    },

    #[error("Usage Error: {0}")]
    UsageError(String),
}
