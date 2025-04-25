use crate::RUNTIME;
use futures::StreamExt;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use tds_x::{
    datatypes::decoder::ColumnValues,
    query::result::{RowData, RowStream},
};

#[pyclass(unsendable)]
pub struct PyRowStream {
    #[allow(dead_code)]
    pub inner: Option<RowStream<'static>>,
}

#[pymethods]
impl PyRowStream {
    fn next(&mut self) -> PyResult<Option<PyRow>> {
        let inner = self.inner.as_mut().unwrap();

        let row = RUNTIME.block_on(inner.next());

        match row {
            Some(row) => {
                let row_data: RowData = row.unwrap();
                Ok(Some(PyRow { inner: row_data }))
            }
            None => Ok(None),
        }
    }

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

    fn cleanup(&mut self) {
        // Logic to clean up resources
        if self.inner.take().is_some() {
            // The inner object's drop will automatically be called here.
        }
    }
}

macro_rules! handle_py_result {
    ($py_result:expr) => {
        match $py_result {
            Ok(value) => value.into(),
            Err(e) => return Err(PyRuntimeError::new_err(e.to_string())),
        }
    };
}

#[pyclass(unsendable)]
pub struct PyRow {
    inner: RowData,
}

#[pymethods]
impl PyRow {
    fn get_value(&mut self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        let column_values = &mut self.inner;
        let mut results: Vec<Py<PyAny>> = Vec::new();
        while let Some(col_val) = RUNTIME.block_on(column_values.next()) {
            // Map the error to a Python Runtime error.
            let col_val = col_val.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let py_value: PyObject = match col_val.get_value() {
                ColumnValues::Int(i) => handle_py_result!(i.into_pyobject(py)),
                ColumnValues::String(s) => handle_py_result!(s.to_utf8_string().into_pyobject(py)),
                ColumnValues::Float(f) => handle_py_result!(f.into_pyobject(py)),
                ColumnValues::TinyInt(ti) => handle_py_result!(ti.into_pyobject(py)),
                ColumnValues::SmallInt(si) => handle_py_result!(si.into_pyobject(py)),
                ColumnValues::BigInt(bi) => handle_py_result!(bi.into_pyobject(py)),
                ColumnValues::Real(re) => handle_py_result!(re.into_pyobject(py)),
                ColumnValues::Decimal(_decimal_parts) => todo!(),
                ColumnValues::Numeric(_decimal_parts) => todo!(),
                ColumnValues::Bit(bit) => {
                    let bitpy = bit.into_pyobject(py);
                    match bitpy {
                        Ok(bit) => bit.to_owned().into(),
                        Err(e) => return Err(PyRuntimeError::new_err(e.to_string())),
                    }
                }
                ColumnValues::DateTime(dt) => handle_py_result!((dt).into_pyobject(py)),
                ColumnValues::Bytes(items) => handle_py_result!(items.into_pyobject(py)),
                ColumnValues::Null => py.None(),
                ColumnValues::Uuid(uuid) => handle_py_result!(uuid.to_string().into_pyobject(py)),
            };
            results.push(py_value);
        }
        Ok(results)
    }
}
