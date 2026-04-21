// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::prelude::*;
use std::sync::OnceLock;

use mssql_tds::connection::client_context::DriverVersion;

mod bulkcopy;
mod connection;
mod cursor;
mod python_logger_adapter;
mod row_writer;
mod tracing_init;
mod types;
mod utils;

pub use python_logger_adapter::{init_tracing_bridge, scoped_tracing_bridge};

/// Module-level driver version, set once by the host Python package.
/// Falls back to mssql-tds crate version if never set.
pub(crate) static DRIVER_VERSION: OnceLock<DriverVersion> = OnceLock::new();

/// Module-level runtime details, set once by the host Python package.
pub(crate) static RUNTIME_DETAILS: OnceLock<String> = OnceLock::new();

/// Returns the driver version, falling back to the mssql-tds crate version.
pub(crate) fn get_driver_version() -> DriverVersion {
    DRIVER_VERSION
        .get()
        .copied()
        .unwrap_or_else(DriverVersion::from_cargo_version)
}

/// Python module for Core TDS connectivity
#[pymodule(name = "mssql_py_core")]
fn mssql_py_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing on module load (via MSSQL_TDS_TRACE env var)
    tracing_init::init_tracing();

    // Statically capture the Python version once during module initialization
    let py_version = m.py().version();
    let _ = RUNTIME_DETAILS.set(format!("Python {}", py_version));

    // Statically capture the mssql_python driver version during module initialization
    if let Ok(ver_str) = m
        .py()
        .import("mssql_python")
        .and_then(|module| module.getattr("__version__"))
        .and_then(|v| v.extract::<String>())
    {
        let parts: Vec<&str> = ver_str.split('.').collect();
        if parts.len() >= 3 {
            #[allow(clippy::collapsible_if)]
            if let (Ok(major), Ok(minor), Ok(build)) = (
                parts[0].parse::<u8>(),
                parts[1].parse::<u8>(),
                parts[2].parse::<u16>(),
            ) {
                let _ = DRIVER_VERSION.set(DriverVersion::new(major, minor, build));
            }
        }
    }

    m.add_class::<connection::PyCoreConnection>()?;
    m.add_class::<cursor::PyCoreCursor>()?;
    Ok(())
}
