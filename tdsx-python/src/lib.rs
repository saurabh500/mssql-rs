pub mod batch_result;
pub mod client_context;
pub mod connection;
pub mod query_result;
pub mod result_set;
pub mod row;

use client_context::PyClientContext;
use connection::{PyTdsConnection, create_connection_sync};
use once_cell::sync::Lazy;
use pyo3::prelude::*;
use query_result::PyQueryResultStream;
use std::sync::Once;
use tokio::runtime::Runtime;
use tracing::Level;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

// Shared Tokio runtime (global)
pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

static INIT: Once = Once::new();

/// Module initialization
/// This is the entry point for the Python module which lists all the types and functions that the module exposes.
#[pymodule]
fn pytdslib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    INIT.call_once(|| {
        let env_filter = EnvFilter::new("tds_x::query::result=trace");

        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .with_env_filter(env_filter)
            .finish();
        if std::env::var("TDSXTRACE").unwrap_or_else(|_| "false".to_string()) == "True" {
            tracing::subscriber::set_global_default(subscriber)
                .expect("Setting default subscriber failed");
        }
    });

    m.add_class::<PyClientContext>()?;
    m.add_class::<PyTdsConnection>()?;
    m.add_class::<PyQueryResultStream>()?;
    m.add_function(wrap_pyfunction!(create_connection_sync, m.py())?)?;
    Ok(())
}
