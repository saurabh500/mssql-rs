// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

pub mod bulk_copy_errors;

pub use bulk_copy_errors::{BulkCopyAttentionTimeoutError, BulkCopyError, BulkCopyTimeoutError};

use crate::security::SecurityError;
use thiserror::Error;
use tokio::time::error::Elapsed;

/// A single SQL Server error, analogous to SqlClient's `SqlError`.
///
/// SQL Server can return multiple errors for a single batch execution.
/// This struct represents one error from the stream. The full collection
/// is available via `Error::SqlServerError { errors }`.
#[derive(Debug, Clone)]
pub struct SqlErrorInfo {
    /// Error message text returned by the server.
    pub message: String,
    /// Error state, used by the server to indicate specific error conditions.
    pub state: u8,
    /// Severity class of the error (maps to TDS `Class` field).
    pub class: i32,
    /// Server-defined error number.
    pub number: u32,
    /// Name of the server that generated the error.
    pub server_name: Option<String>,
    /// Name of the stored procedure that generated the error.
    pub proc_name: Option<String>,
    /// Line number in the batch or procedure where the error occurred.
    pub line_number: Option<i32>,
}

impl std::fmt::Display for SqlErrorInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Sql Error: {}: Class {}: State {}: {} on {} in {} at line {}",
            self.number,
            self.class,
            self.state,
            self.message,
            self.server_name.as_deref().unwrap_or("Unknown"),
            self.proc_name.as_deref().unwrap_or("Unknown"),
            self.line_number.unwrap_or_default()
        )
    }
}

impl From<&crate::token::tokens::ErrorToken> for SqlErrorInfo {
    fn from(token: &crate::token::tokens::ErrorToken) -> Self {
        Self {
            message: token.message.clone(),
            state: token.state,
            class: token.severity as i32,
            number: token.number,
            server_name: Some(token.server_name.clone()),
            proc_name: Some(token.proc_name.clone()),
            line_number: Some(token.line_number as i32),
        }
    }
}

/// The source of a timeout: either a Tokio `Elapsed` or a descriptive string.
#[derive(Debug, Error)]
pub enum TimeoutErrorType {
    #[error("Elapsed: {0}")]
    Elapsed(Elapsed),

    #[error("{0}")]
    String(String),
}

/// All errors produced by the TDS client.
#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Server redirected the connection: {host}:{port} times")]
    Redirection { host: String, port: u16 },

    #[error("Connection Error: {0}")]
    ConnectionError(String),

    #[error("Protocol Error: {0}")]
    ProtocolError(String),

    #[error("TLS Error: {0}")]
    TlsError(#[from] native_tls::Error),

    #[error(
        "TLS handshake failed while connecting to '{expected_host}': {source}. Certificate SANs: {cert_sans}"
    )]
    TlsHandshakeError {
        source: native_tls::Error,
        expected_host: String,
        cert_sans: String,
    },

    #[error("Timeout Error: {0}")]
    TimeoutError(TimeoutErrorType),

    #[error("Operation Cancelled Error: {0}")]
    OperationCancelledError(String),

    #[error("{}", SqlServerError::format_errors(errors))]
    SqlServerError { errors: Vec<SqlErrorInfo> },

    #[error("Usage Error: {0}")]
    UsageError(String),

    #[error("Unexpected Implementation Error: {0}")]
    ImplementationError(String),

    #[error("Unimplemented Feature: {feature} - {context}")]
    UnimplementedFeature { feature: String, context: String },

    #[error("Type Conversion Error: {0}")]
    TypeConversionError(String),

    #[error("Connection closed: {0}")]
    ConnectionClosed(String),

    #[error(
        "Unsupported Encoding: LCID {lcid} (0x{lcid:04X}). Consider using NVARCHAR instead of VARCHAR/TEXT for better compatibility."
    )]
    UnsupportedEncoding { lcid: u32 },

    #[error(
        "Certificate file not found: {path}. Verify the ServerCertificate path is correct and the file exists."
    )]
    CertificateNotFound { path: String },

    #[error(
        "Invalid certificate format in file: {path}. Ensure the file contains a valid DER or PEM encoded X.509 certificate."
    )]
    InvalidCertificateFormat { path: String },

    #[error(
        "Server certificate has expired. The server's certificate is no longer valid. Contact your administrator."
    )]
    CertificateExpired,

    #[error(
        "Server certificate validation failed: Certificate mismatch. The server presented a different certificate than expected. Verify you are connecting to the correct server."
    )]
    CertificateMismatch,

    #[error(
        "Failed to read certificate file: {path}. Error: {error}. Check file permissions and ensure the file is not locked by another process."
    )]
    CertificateFileIoError { path: String, error: String },

    #[error("No server certificate available during TLS handshake.")]
    NoServerCertificate,

    #[error("Bulk Copy Error: {0}")]
    BulkCopyError(#[from] BulkCopyError),

    #[error("Security error: {0}")]
    Security(#[from] SecurityError),
}

/// Helper for `SqlServerError` display formatting.
struct SqlServerError;

impl SqlServerError {
    fn format_errors(errors: &[SqlErrorInfo]) -> String {
        match errors.len() {
            0 => "Sql Error: (no error details)".to_string(),
            1 => errors[0].to_string(),
            _ => errors
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }
}

impl Error {
    /// Create a `SqlServerError` from a single `SqlErrorInfo`.
    pub fn from_sql_error(error: SqlErrorInfo) -> Self {
        Error::SqlServerError {
            errors: vec![error],
        }
    }

    /// Create a `SqlServerError` from multiple `SqlErrorInfo`s.
    pub fn from_sql_errors(errors: Vec<SqlErrorInfo>) -> Self {
        Error::SqlServerError { errors }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_timeout_error_type_string() {
        let error = TimeoutErrorType::String("Test timeout".to_string());
        assert_eq!(error.to_string(), "Test timeout");
    }

    #[test]
    fn test_timeout_error_type_elapsed() {
        // Create an Elapsed error by timing out a sleep
        let rt = tokio::runtime::Runtime::new().unwrap();
        let elapsed = rt.block_on(async {
            tokio::time::timeout(std::time::Duration::from_millis(1), async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            })
            .await
            .unwrap_err()
        });
        let error = TimeoutErrorType::Elapsed(elapsed);
        assert!(error.to_string().contains("Elapsed"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_error = io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused");
        let error = Error::from(io_error);
        match error {
            Error::Io(e) => assert_eq!(e.kind(), io::ErrorKind::ConnectionRefused),
            _ => panic!("Expected IO error"),
        }
    }

    #[test]
    fn test_redirection_error() {
        let error = Error::Redirection {
            host: "example.com".to_string(),
            port: 1433,
        };
        assert!(error.to_string().contains("example.com"));
        assert!(error.to_string().contains("1433"));
    }

    #[test]
    fn test_protocol_error() {
        let error = Error::ProtocolError("Invalid packet".to_string());
        assert_eq!(error.to_string(), "Protocol Error: Invalid packet");
    }

    #[test]
    fn test_timeout_error() {
        let timeout_type = TimeoutErrorType::String("Query timeout".to_string());
        let error = Error::TimeoutError(timeout_type);
        assert!(error.to_string().contains("Query timeout"));
    }

    #[test]
    fn test_operation_cancelled_error() {
        let error = Error::OperationCancelledError("User cancelled".to_string());
        assert!(error.to_string().contains("User cancelled"));
    }

    #[test]
    fn test_sql_server_error_full() {
        let error = Error::from_sql_error(SqlErrorInfo {
            message: "Login failed".to_string(),
            state: 1,
            class: 14,
            number: 18456,
            server_name: Some("SQLSERVER01".to_string()),
            proc_name: Some("sp_login".to_string()),
            line_number: Some(42),
        });
        let err_str = error.to_string();
        assert!(err_str.contains("18456"));
        assert!(err_str.contains("Login failed"));
        assert!(err_str.contains("SQLSERVER01"));
        assert!(err_str.contains("sp_login"));
        assert!(err_str.contains("42"));
    }

    #[test]
    fn test_sql_server_error_with_none_values() {
        let error = Error::from_sql_error(SqlErrorInfo {
            message: "Error occurred".to_string(),
            state: 2,
            class: 16,
            number: 50000,
            server_name: None,
            proc_name: None,
            line_number: None,
        });
        let err_str = error.to_string();
        assert!(err_str.contains("50000"));
        assert!(err_str.contains("Error occurred"));
        assert!(err_str.contains("Unknown"));
    }

    #[test]
    fn test_sql_server_error_multiple() {
        let error = Error::from_sql_errors(vec![
            SqlErrorInfo {
                message: "First error".to_string(),
                state: 1,
                class: 16,
                number: 50000,
                server_name: Some("SRV".to_string()),
                proc_name: None,
                line_number: Some(1),
            },
            SqlErrorInfo {
                message: "Second error".to_string(),
                state: 1,
                class: 16,
                number: 50001,
                server_name: Some("SRV".to_string()),
                proc_name: None,
                line_number: Some(2),
            },
        ]);
        let err_str = error.to_string();
        assert!(err_str.contains("First error"));
        assert!(err_str.contains("Second error"));
        assert!(err_str.contains("50000"));
        assert!(err_str.contains("50001"));
    }

    #[test]
    fn test_usage_error() {
        let error = Error::UsageError("Invalid connection string".to_string());
        assert_eq!(error.to_string(), "Usage Error: Invalid connection string");
    }

    #[test]
    fn test_implementation_error() {
        let error = Error::ImplementationError("Not implemented yet".to_string());
        assert_eq!(
            error.to_string(),
            "Unexpected Implementation Error: Not implemented yet"
        );
    }

    #[test]
    fn test_unimplemented_feature() {
        let error = Error::UnimplementedFeature {
            feature: "Always Encrypted".to_string(),
            context: "Column encryption not supported".to_string(),
        };
        let err_str = error.to_string();
        assert!(err_str.contains("Always Encrypted"));
        assert!(err_str.contains("Column encryption not supported"));
    }

    #[test]
    fn test_type_conversion_error() {
        let error = Error::TypeConversionError("Cannot convert VARCHAR to INT".to_string());
        assert_eq!(
            error.to_string(),
            "Type Conversion Error: Cannot convert VARCHAR to INT"
        );
    }

    #[test]
    fn test_error_debug_format() {
        let error = Error::ProtocolError("Test".to_string());
        let debug_str = format!("{error:?}");
        assert!(debug_str.contains("ProtocolError"));
    }
}
