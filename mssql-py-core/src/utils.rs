// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Utility functions for the PyO3 bindings

use mssql_tds::error::Error as TdsError;
use pyo3::prelude::*;

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
        _ => pyo3::exceptions::PyRuntimeError::new_err(error.to_string()),
    }
}
