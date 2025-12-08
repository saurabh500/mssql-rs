// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::prelude::*;

mod connection;
mod cursor;
mod types;
mod utils;
mod bulkcopy;

/// Python module for Core TDS connectivity
#[pymodule(name = "mssql_py_core")]
fn mssql_py_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<connection::PyCoreConnection>()?;
    m.add_class::<cursor::PyCoreCursor>()?;
    Ok(())
}
