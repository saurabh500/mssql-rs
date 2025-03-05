use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Server redirected the connection: {host}:{port} {count} times")]
    Redirection { host: String, port: i32, count: i32 },

    #[error("Protocol Error: {0}")]
    ProtocolError(String),

    #[error("TLS Error: {0}")]
    TlsError(#[from] native_tls::Error),

    #[error("Sql Error: {number}: {class}: {state}: {message} on {} in {} at line {}",
            server_name.clone().unwrap(), proc_name.clone().unwrap(), line_number.unwrap())]
    SqlServerError {
        message: String,
        state: String,
        class: i32,
        number: i32,
        server_name: Option<String>,
        proc_name: Option<String>,
        line_number: Option<i32>,
    },
}
