use crate::query_result::PyQueryResultStream;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tds_x::query::result::{BatchResult, QueryResultTypeStream};

/// A Python class representing the result of a batch query execution.
///
/// This class is used to encapsulate the result of executing a batch query
/// in the TDS (Tabular Data Stream) protocol. It provides a method to
/// stream the query results.
///
/// # Fields
///
/// * `batch_result` - An optional `BatchResult` that holds the result of
///   the batch query execution. The `BatchResult` is consumed when the
///   `stream` method is called.
///
/// # Methods
///
/// * `stream` - Consumes the `batch_result` and returns a `PyQueryResultStream`
///   that can be used to iterate over the query results. If the `batch_result`
///   has already been consumed, an error is returned.
#[pyclass(unsendable)]
pub struct PyBatchResult {
    pub batch_result: Option<BatchResult<'static>>,
}

#[pymethods]
impl PyBatchResult {
    /// Streams the query results from the batch result.
    ///
    /// This method consumes the `batch_result` and returns a `PyQueryResultStream`
    /// that can be used to iterate over the query results. If the `batch_result`
    /// has already been consumed, an error is returned.
    ///
    /// # Returns
    ///
    /// A `PyQueryResultStream` for iterating over the query results.
    ///
    /// # Errors
    ///
    /// Returns a `PyRuntimeError` if the `batch_result` has already been consumed.
    fn stream(&mut self) -> PyResult<PyQueryResultStream> {
        let vr = self
            .batch_result
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Batch result already consumed"))?;
        let stream = vr.stream_results();
        let boxed: QueryResultTypeStream<'static> = unsafe { std::mem::transmute::<_, _>(stream) };
        Ok(PyQueryResultStream { inner: Some(boxed) })
    }
}
