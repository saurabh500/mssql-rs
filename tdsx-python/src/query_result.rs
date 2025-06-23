use crate::{result_set::PyResultSet, RUNTIME};
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tds_x::query::result::{QueryResultType, QueryResultTypeStream};

#[pyclass(unsendable)]
pub struct PyQueryResultStream {
    pub inner: Option<QueryResultTypeStream<'static>>,
}

#[pymethods]
impl PyQueryResultStream {
    fn __iter__(&mut self) -> PyResult<PyResultSet> {
        let result = RUNTIME.block_on(async {
            let stream = self
                .inner
                .as_mut()
                .ok_or_else(|| PyRuntimeError::new_err("Stream already consumed"))?;
            if let Some(query_result_type) = stream.next().await {
                let qrt = query_result_type.unwrap();
                match qrt {
                    QueryResultType::ResultSet(result_set) => {
                        return Ok(PyResultSet {
                            inner: Some(QueryResultType::ResultSet(result_set)),
                        });
                    }
                    QueryResultType::DmlResult(rows_affected) => {
                        return Ok(PyResultSet {
                            inner: Some(QueryResultType::DmlResult(rows_affected)),
                        });
                    }
                }
            }
            Ok(PyResultSet { inner: None })
        });
        result
    }

    fn next_result(&mut self) -> PyResult<Option<PyResultSet>> {
        let result = {
            let stream = self.inner.as_mut();
            if stream.is_none() {
                return Ok(None);
            }
            let mut_stream = stream.unwrap();
            if let Some(query_result_type) = RUNTIME.block_on(mut_stream.next()) {
                let qrt = query_result_type.unwrap();
                match qrt {
                    QueryResultType::ResultSet(result_set) => {
                        return Ok(Some(PyResultSet {
                            inner: Some(QueryResultType::ResultSet(result_set)),
                        }));
                    }
                    QueryResultType::DmlResult(rows_affected) => {
                        return Ok(Some(PyResultSet {
                            inner: Some(QueryResultType::DmlResult(rows_affected)),
                        }));
                    }
                }
            }
            Ok(None)
        };
        result
    }
}
