// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use pyo3::prelude::*;
use std::sync::Once;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

mod bulkcopy;
mod connection;
mod cursor;
mod types;
mod utils;

static INIT: Once = Once::new();

/// Initialize tracing if ENABLE_TRACE environment variable is set to true
fn init_tracing() {
    let enable_trace = std::env::var("ENABLE_TRACE")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);

    if enable_trace {
        INIT.call_once(|| {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
    }
}

/// Python module for Core TDS connectivity
#[pymodule(name = "mssql_py_core")]
fn mssql_py_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize tracing on module load
    init_tracing();

    m.add_class::<connection::PyCoreConnection>()?;
    m.add_class::<cursor::PyCoreCursor>()?;
    Ok(())
}
