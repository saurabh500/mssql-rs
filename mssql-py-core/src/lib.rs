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
static DRIVER_VERSION: OnceLock<DriverVersion> = OnceLock::new();

/// Returns the driver version, falling back to the mssql-tds crate version.
pub(crate) fn get_driver_version() -> DriverVersion {
    DRIVER_VERSION
        .get()
        .copied()
        .unwrap_or_else(DriverVersion::from_cargo_version)
}

/// Python function to set the driver version once at module init.
/// Called by the host package (e.g. mssql-python) before creating any connections.
///
/// # Arguments
/// * `major` - Major version number (0-255)
/// * `minor` - Minor version number (0-255)
/// * `build` - Build number (0-65535)
#[pyfunction]
fn set_driver_version(major: u8, minor: u8, build: u16) {
    let _ = DRIVER_VERSION.set(DriverVersion::new(major, minor, build));
}

/// Python module for Core TDS connectivity
#[pymodule(name = "mssql_py_core")]
fn mssql_py_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing on module load (via MSSQL_TDS_TRACE env var)
    tracing_init::init_tracing();

    m.add_function(wrap_pyfunction!(set_driver_version, m)?)?;
    m.add_class::<connection::PyCoreConnection>()?;
    m.add_class::<cursor::PyCoreCursor>()?;
    Ok(())
}
