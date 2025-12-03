use pyo3::prelude::*;

mod connection;
mod cursor;
mod types;
mod utils;

/// Python module for Core TDS connectivity
#[pymodule]
fn mssql_core_tds(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<connection::DdbcConnection>()?;
    m.add_class::<cursor::DdbcCursor>()?;
    Ok(())
}
