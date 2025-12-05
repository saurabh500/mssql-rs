// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Utility functions for the PyO3 bindings

use pyo3::prelude::*;

/// Convert error types from mssql-tds to Python exceptions
/// This will be used when implementing actual Core TDS connection logic
#[allow(dead_code)]
pub fn convert_error(error: anyhow::Error) -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
}
