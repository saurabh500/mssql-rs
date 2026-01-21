// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Utility functions for the PyO3 bindings

use mssql_tds::error::Error as TdsError;
use pyo3::prelude::*;
use pyo3::Python;

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
        _ => pyo3::exceptions::PyRuntimeError::new_err(error.to_string()),
    }
}
