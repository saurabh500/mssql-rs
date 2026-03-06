// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io;

/// Errors returned by the `mssql-rs` client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("invalid connection string: {0}")]
    ConnectionStringInvalid(String),

    #[error("query failed: {0}")]
    QueryFailed(String),

    #[error("SQL Server error {number} (state {state}): {message}")]
    SqlServer {
        message: String,
        state: u8,
        number: u32,
    },

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("query cancelled")]
    Cancelled,

    #[error("type conversion: {0}")]
    TypeConversion(String),

    #[error("protocol error: {0}")]
    Protocol(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<mssql_tds::error::Error> for Error {
    fn from(e: mssql_tds::error::Error) -> Self {
        match e {
            mssql_tds::error::Error::Io(io_err) => Error::Io(io_err),
            mssql_tds::error::Error::ConnectionError(msg) => Error::ConnectionFailed(msg),
            mssql_tds::error::Error::ConnectionClosed(msg) => Error::ConnectionFailed(msg),
            mssql_tds::error::Error::TimeoutError(t) => Error::Timeout(t.to_string()),
            mssql_tds::error::Error::OperationCancelledError(_) => Error::Cancelled,
            mssql_tds::error::Error::SqlServerError {
                message,
                state,
                number,
                ..
            } => Error::SqlServer {
                message,
                state,
                number,
            },
            mssql_tds::error::Error::TypeConversionError(msg) => Error::TypeConversion(msg),
            mssql_tds::error::Error::UsageError(msg) => Error::QueryFailed(msg),
            other => Error::Protocol(Box::new(other)),
        }
    }
}
