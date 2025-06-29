use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::sync::{Arc, Mutex};
use tds_x::connection::client_context::ClientContext;
use tds_x::connection::tds_connection::TdsConnection;
use tds_x::connection_provider::tds_connection_provider::TdsConnectionProvider;
use tds_x::query::result::BatchResult;

use crate::{batch_result::PyBatchResult, client_context::PyClientContext, RUNTIME};

/// Represents a Python TDS connection.
///
/// This struct is unsendable and holds a reference to the client context
/// and the TDS connection.
#[pyclass(unsendable)]
pub struct PyTdsConnection {
    /// Stores the client context to ensure it outlives the connection.
    #[allow(dead_code)]
    context: Arc<ClientContext>,

    /// Stores the TDS connection inside a Mutex for interior mutability.
    connection: Option<Arc<Mutex<TdsConnection>>>,
}

#[pymethods]
impl PyTdsConnection {
    /// Executes a SQL command synchronously.
    ///
    /// # Arguments
    ///
    /// * `sql_command` - A string slice that holds the SQL command to execute.
    ///
    /// # Returns
    ///
    /// * `PyResult<PyBatchResult>` - The result of the SQL execution.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the connection is dropped or if there is an error executing the SQL.
    #[pyo3(text_signature = "(self, sql_command)")]
    fn execute_sync(&mut self, sql_command: String) -> PyResult<PyBatchResult> {
        let Some(ref conn) = self.connection else {
            return Err(PyRuntimeError::new_err("Connection already dropped"));
        };

        // Lock the connection to get a mutable reference
        let mut conn = conn
            .lock()
            .map_err(|_| PyRuntimeError::new_err("Failed to lock connection"))?;

        // Use shared runtime to execute the async function
        let batch_res = RUNTIME.block_on(async { conn.execute(sql_command, None, None).await });

        match batch_res {
            Ok(batch_result) => {
                use std::mem;
                let batch_result_static: BatchResult<'static> = unsafe {
                    mem::transmute::<BatchResult<'_>, BatchResult<'static>>(batch_result)
                };

                Ok(PyBatchResult {
                    batch_result: Some(batch_result_static),
                })
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("Error executing SQL: {e}"))),
        }
    }
}

/// Creates a TDS connection using the shared Tokio runtime.
///
/// # Arguments
///
/// * `py_ctx` - A `PyClientContext` that is converted into a `ClientContext`.
///
/// # Returns
///
/// * `PyResult<PyTdsConnection>` - The created TDS connection.
///
/// # Errors
///
/// Returns a `PyRuntimeError` if there is a connection error.
#[pyfunction]
pub fn create_connection_sync(py_ctx: PyClientContext) -> PyResult<PyTdsConnection> {
    let context: Arc<ClientContext> = Arc::new(py_ctx.into());

    let provider = TdsConnectionProvider {};

    let conn_result =
        RUNTIME.block_on(async { provider.create_connection((*context).clone(), None).await });

    match conn_result {
        Ok(tds_conn) => Ok(PyTdsConnection {
            context: Arc::clone(&context),
            connection: Some(Arc::new(Mutex::new(tds_conn))),
        }),
        Err(e) => Err(PyRuntimeError::new_err(format!(
            "Connection error: {:?}",
            e
        ))),
    }
}
