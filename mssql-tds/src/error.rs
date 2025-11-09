// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

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

    #[error("Operation Cancelled Error: {0}")]
    OperationCancelledError(String),

    #[error("Sql Error: {number}: Class {class}: State {state}: {message} on {} in {} at line {}",
            server_name.clone().unwrap_or_else(|| "Unknown".into()), proc_name.clone().unwrap_or_else(|| "Unknown".into()), line_number.unwrap_or_default())]
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

    #[error("Unexpected Implementation Error: {0}")]
    ImplementationError(String),

    #[error("Unimplemented Feature: {feature} - {context}")]
    UnimplementedFeature { feature: String, context: String },
}
