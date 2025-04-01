use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tds_x::query::result::QueryResultType;

use crate::row::PyRowStream;

#[pyclass(unsendable)]
pub struct PyResultSet {
    pub inner: Option<QueryResultType<'static>>,
}

#[pymethods]
impl PyResultSet {
    fn __enter__(_slf: PyRefMut<Self>) -> PyRefMut<Self> {
        _slf
    }

    fn __exit__(
        mut _slf: PyRefMut<Self>,
        _args: PyObject,
        _nargs: PyObject,
        _kwnames: PyObject,
    ) -> PyResult<()> {
        // This will automatically call the drop implementation
        _slf.cleanup();
        Ok(())
    }

    fn has_resultset(&self) -> PyResult<bool> {
        let inner = self.inner.as_ref();

        // If there is no result set, return false
        match inner {
            None => Ok(false),
            Some(inner) => match inner {
                QueryResultType::ResultSet(_) => Ok(true),
                QueryResultType::Update(_) => Ok(false),
            },
        }
    }

    fn get_row_stream(&mut self) -> PyResult<PyRowStream> {
        let inner = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("ResultSet already consumed"))?;
        match inner {
            QueryResultType::ResultSet(result_set) => {
                let row_stream = result_set.into_row_stream();
                match row_stream {
                    Ok(row_stream) => Ok(PyRowStream {
                        inner: Some(row_stream),
                    }),
                    Err(e) => Err(PyRuntimeError::new_err(format!(
                        "Error creating row stream: {}",
                        e
                    ))),
                }
            }
            QueryResultType::Update(_) => {
                unreachable!(" Get Row Stream called without a query result type of rowstream")
            }
        }
    }

    fn cleanup(&mut self) {
        // Logic to clean up resources
        if self.inner.take().is_some() {
            // The inner object's drop will automatically be called here.
        }
    }
}

impl Drop for PyResultSet {
    fn drop(&mut self) {
        // Call cleanup logic
        self.cleanup();
    }
}
