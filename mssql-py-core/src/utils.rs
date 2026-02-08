// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Utility functions for the PyO3 bindings

use mssql_tds::error::{BulkCopyError, Error as TdsError};
use pyo3::Python;
use pyo3::prelude::*;

/// Emit a Python warning for unimplemented features.
///
/// Use this when a parameter is accepted for compatibility but not yet functional.
/// The warning is silently ignored if the Python warnings module cannot be imported.
///
/// # Arguments
/// * `py` - The Python GIL token
/// * `param_name` - Name of the parameter that's not implemented
pub fn emit_unimplemented_warning(py: Python<'_>, param_name: &str) {
    if let Ok(warnings) = py.import("warnings") {
        let message = format!(
            "The '{}' parameter is not yet supported and will be ignored",
            param_name
        );
        let _ = warnings.call_method1(
            "warn",
            (message, py.get_type::<pyo3::exceptions::PyUserWarning>()),
        );
    }
}

/// Convert error types from mssql-tds to Python exceptions
/// This will be used when implementing actual Core TDS connection logic
#[allow(dead_code)]
pub fn convert_error(error: anyhow::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}

/// Convert TDS errors to appropriate Python exceptions
pub fn convert_tds_error(error: TdsError) -> PyErr {
    match error {
        TdsError::UsageError(msg) => {
            // UsageError includes client-side validation failures like:
            // - NULL to non-nullable column conversion
            // - Invalid string-to-int parsing
            // - Out of range values
            pyo3::exceptions::PyValueError::new_err(msg)
        }
        TdsError::TypeConversionError(msg) => {
            // Type conversion errors (e.g., decimal parsing, type mismatches)
            pyo3::exceptions::PyValueError::new_err(msg)
        }
        TdsError::ProtocolError(msg) => {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Protocol Error: {}", msg))
        }
        TdsError::BulkCopyError(bc_err) => {
            // Handle bulk copy specific errors with appropriate Python exceptions
            match bc_err {
                BulkCopyError::Timeout(timeout_err) => {
                    // Bulk copy operation timed out - raise TimeoutError
                    pyo3::exceptions::PyTimeoutError::new_err(format!(
                        "Bulk copy operation timed out after {} seconds ({} rows copied){}",
                        timeout_err.timeout_seconds,
                        timeout_err.rows_copied,
                        timeout_err
                            .context
                            .as_ref()
                            .map(|c| format!(". {}", c))
                            .unwrap_or_default()
                    ))
                }
                BulkCopyError::AttentionTimeout(attn_err) => {
                    // Attention ACK not received - connection is broken
                    let msg = if attn_err.connection_broken {
                        "Attention acknowledgment not received within 5 seconds. Connection has been broken."
                    } else {
                        "Attention acknowledgment not received within 5 seconds."
                    };
                    pyo3::exceptions::PyTimeoutError::new_err(msg)
                }
                BulkCopyError::ConnectionBroken(msg) => pyo3::exceptions::PyRuntimeError::new_err(
                    format!("Connection is broken: {}", msg),
                ),
            }
        }
        _ => pyo3::exceptions::PyRuntimeError::new_err(error.to_string()),
    }
}
