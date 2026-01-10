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

    #[error("Type Conversion Error: {0}")]
    TypeConversionError(String),

    #[error(
        "Unsupported Encoding: LCID {lcid} (0x{lcid:04X}). Consider using NVARCHAR instead of VARCHAR/TEXT for better compatibility."
    )]
    UnsupportedEncoding { lcid: u32 },
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
        let error = Error::SqlServerError {
            message: "Login failed".to_string(),
            state: 1,
            class: 14,
            number: 18456,
            server_name: Some("SQLSERVER01".to_string()),
            proc_name: Some("sp_login".to_string()),
            line_number: Some(42),
        };
        let err_str = error.to_string();
        assert!(err_str.contains("18456"));
        assert!(err_str.contains("Login failed"));
        assert!(err_str.contains("SQLSERVER01"));
        assert!(err_str.contains("sp_login"));
        assert!(err_str.contains("42"));
    }

    #[test]
    fn test_sql_server_error_with_none_values() {
        let error = Error::SqlServerError {
            message: "Error occurred".to_string(),
            state: 2,
            class: 16,
            number: 50000,
            server_name: None,
            proc_name: None,
            line_number: None,
        };
        let err_str = error.to_string();
        assert!(err_str.contains("50000"));
        assert!(err_str.contains("Error occurred"));
        assert!(err_str.contains("Unknown"));
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
