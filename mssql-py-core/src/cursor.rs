// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use mssql_tds::connection::bulk_copy::{
    BulkCopy, ColumnMapping as TdsColumnMapping, ColumnMappingSource,
};
use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
use mssql_tds::datatypes::column_values::ColumnValues;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyTuple};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::bulkcopy::PythonRowAdapter;
use crate::utils::convert_tds_error;

/// Python Cursor class for Core TDS backend
#[pyclass]
pub struct PyCoreCursor {
    tds_client: Arc<Mutex<TdsClient>>,
    runtime_handle: Handle,
    has_resultset: bool,
}

#[pymethods]
impl PyCoreCursor {
    #[pyo3(signature = (query, params=None))]
    #[allow(unused_variables)]
    fn execute(
        &mut self,
        py: Python,
        query: String,
        params: Option<Vec<Py<PyAny>>>,
    ) -> PyResult<()> {
        info!("execute: Executing query: {}", query);

        let tds_client = self.tds_client.clone();
        let runtime_handle = self.runtime_handle.clone();

        // Execute query asynchronously
        py.detach(|| {
            runtime_handle.block_on(async {
                let mut client = tds_client.lock().await;
                info!("execute: Locked TDS client, calling execute");

                // Execute with 30 second timeout
                client.execute(query, Some(30), None).await.map_err(|e| {
                    error!("execute: Failed to execute query: {}", e);
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Query execution failed: {}",
                        e
                    ))
                })?;

                info!("execute: Query executed successfully");
                Ok::<_, PyErr>(())
            })
        })?;

        self.has_resultset = true;
        Ok(())
    }

    fn fetchone(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        if !self.has_resultset {
            return Ok(None);
        }

        info!("fetchone: Fetching one row");

        let tds_client = self.tds_client.clone();
        let runtime_handle = self.runtime_handle.clone();

        // Fetch one row asynchronously
        let result = py.detach(|| {
            runtime_handle.block_on(async {
                let mut client = tds_client.lock().await;
                info!("fetchone: Locked TDS client");

                if let Some(resultset) = client.get_current_resultset() {
                    info!("fetchone: Got resultset, fetching next row");
                    if let Some(row) = resultset.next_row().await.map_err(|e| {
                        error!("fetchone: Failed to fetch row: {}", e);
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Failed to fetch row: {}",
                            e
                        ))
                    })? {
                        info!("fetchone: Got row with {} columns", row.len());
                        return Ok(Some(row));
                    }
                }

                info!("fetchone: No more rows");
                Ok::<_, PyErr>(None)
            })
        })?;

        // Convert row to Python tuple
        if let Some(row) = result {
            Python::attach(|py| {
                let py_list: Vec<Bound<'_, PyAny>> = row
                    .iter()
                    .map(|col_val| Self::column_value_to_python(py, col_val))
                    .collect();
                let py_tuple = PyTuple::new(py, py_list.iter())?;
                Ok(Some(py_tuple.into()))
            })
        } else {
            Ok(None)
        }
    }

    fn fetchall(&mut self, py: Python) -> PyResult<Vec<Py<PyAny>>> {
        if !self.has_resultset {
            return Ok(vec![]);
        }

        info!("fetchall: Fetching all rows");

        let mut results = Vec::new();
        while let Some(row) = self.fetchone(py)? {
            results.push(row);
        }

        info!("fetchall: Fetched {} rows", results.len());
        Ok(results)
    }

    fn fetchmany(&mut self, py: Python, size: Option<usize>) -> PyResult<Vec<Py<PyAny>>> {
        let fetch_size = size.unwrap_or(1);
        let mut results = Vec::new();

        for _ in 0..fetch_size {
            if let Some(row) = self.fetchone(py)? {
                results.push(row);
            } else {
                break;
            }
        }

        Ok(results)
    }

    fn close(&mut self) -> PyResult<()> {
        // TODO: Might need to drain the results.
        self.has_resultset = false;
        Ok(())
    }

    /// Perform bulk copy operation to insert data into a SQL Server table.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the destination table (can include schema: "schema.table")
    /// * `data_source` - Python iterator yielding tuples of data to insert
    /// * `kwargs` - Optional keyword arguments for bulk copy options:
    ///   - `batch_size` (int): Number of rows per batch (default: 0)
    ///   - `timeout` (int): Timeout in seconds (default: 30)
    ///   - `column_mappings` (list): Optional list of (source, dest) tuples for column mapping.
    ///     Source can be an integer (0-based ordinal) or string (column name).
    ///     Destination is a string column name.
    ///     If not provided, automatic ordinal-based mapping is used (0→0, 1→1, etc.).
    ///     Example: [(0, 'id'), (1, 'name')] or [('src_id', 'dest_id')]
    ///   - `keep_identity` (bool): Preserve source identity values. When not specified, identity values are assigned by the destination.
    ///   - `check_constraints` (bool): Check constraints while data is being inserted. By default, constraints are not checked.
    ///   - `table_lock` (bool): Obtain a bulk update lock for the duration of the bulk copy operation. When not specified, row locks are used.
    ///   - `keep_nulls` (bool): Preserve null values in the destination table regardless of the settings for default values. When not specified, null values are replaced by default values where applicable.
    ///   - `fire_triggers` (bool): When specified, cause the server to fire the insert triggers for the rows being inserted into the database.
    ///
    /// # Returns
    ///
    /// Dictionary containing:
    /// - `rows_copied` (int): Number of rows successfully copied
    /// - `batch_count` (int): Number of batches sent
    /// - `elapsed_time` (float): Time taken in seconds
    ///
    /// # Example
    ///
    /// ```python
    /// cursor = connection.cursor()
    /// data = [(1, 'Alice'), (2, 'Bob')]
    /// result = cursor.bulkcopy('Users', iter(data), batch_size=1000)
    /// print(f"Copied {result['rows_copied']} rows")
    /// ```
    #[pyo3(signature = (table_name, data_source, kwargs=None))]
    fn bulkcopy(
        &mut self,
        py: Python,
        table_name: String,
        data_source: &Bound<'_, PyIterator>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyDict>> {
        info!("bulkcopy: Starting bulkcopy to table: {}", table_name);

        // Parse kwargs with defaults
        let options = Self::parse_bulkcopy_kwargs(kwargs)?;
        info!(
            "bulkcopy: Parsed options - batch_size={}, timeout={:?}",
            options.batch_size, options.timeout
        );

        // Clone the TdsClient Arc for async execution
        let tds_client = self.tds_client.clone();

        // Collect Python iterator into a Rust iterator adapter
        // We need to convert the Python iterator to owned Py<PyAny> objects
        info!("bulkcopy: Collecting rows from Python iterator");
        let mut rows = Vec::new();
        for item in data_source {
            let tuple = item?;
            rows.push(tuple.unbind());
        }
        info!("bulkcopy: Collected {} rows", rows.len());

        // Track whether we need to auto-generate mappings
        let auto_generate_mappings = options.column_mappings.is_empty() && !rows.is_empty();
        if auto_generate_mappings {
            info!("bulkcopy: No column mappings provided, will auto-generate after retrieving metadata");
        }

        // Release GIL and execute async bulk copy
        info!("bulkcopy: Releasing GIL and starting async execution");
        let runtime_handle = self.runtime_handle.clone();
        let result = py.detach(|| {
            info!("bulkcopy: Inside detached thread");
            // Use the connection's runtime handle instead of creating a new runtime
            info!("bulkcopy: Using existing runtime handle");

            runtime_handle.block_on(async {
                info!("bulkcopy: Inside async block, attempting to lock TDS client");
                // Lock the TDS client
                let mut client = tds_client.lock().await;
                info!("bulkcopy: Successfully locked TDS client");

                // Create BulkCopy instance
                info!("bulkcopy: Creating BulkCopy instance");
                let mut bulk_copy = BulkCopy::new(&mut client, table_name)
                    .batch_size(options.batch_size)
                    .timeout(options.timeout)
                    .check_constraints(options.check_constraints)
                    .fire_triggers(options.fire_triggers)
                    .keep_identity(options.keep_identity)
                    .keep_nulls(options.keep_nulls)
                    .table_lock(options.table_lock)
                    .use_internal_transaction(options.use_internal_transaction);
                info!("bulkcopy: BulkCopy instance created");

                // Auto-generate column mappings if needed
                let mut column_mappings = options.column_mappings;
                let destination_metadata = if auto_generate_mappings || column_mappings.is_empty() {
                    info!("bulkcopy: Retrieving destination metadata for auto-mapping or type coercion");
                    Some(bulk_copy.retrieve_destination_metadata().await
                        .map_err(|e| {
                            error!("bulkcopy: Failed to retrieve destination metadata: {}", e);
                            pyo3::exceptions::PyRuntimeError::new_err(format!(
                                "Failed to retrieve destination metadata: {}", e
                            ))
                        })?)
                } else {
                    None
                };

                if auto_generate_mappings {
                    let metadata = destination_metadata.as_ref().unwrap();
                    info!("bulkcopy: Retrieved {} columns from destination table", metadata.len());

                    // Get the number of columns in the first row
                    let num_columns = Python::attach(|py| {
                        let first_row = rows[0].bind(py);
                        if let Ok(tuple) = first_row.cast::<PyTuple>() {
                            tuple.len()
                        } else {
                            0
                        }
                    });

                    info!("bulkcopy: First row has {} columns", num_columns);

                    // Auto-generate ordinal mappings for available columns
                    let mapping_count = std::cmp::min(num_columns, metadata.len());
                    for (i, col_meta) in metadata.iter().enumerate().take(mapping_count) {
                        column_mappings.push(ColumnMapping::ByOrdinal {
                            source: i,
                            destination: col_meta.name.clone(),
                        });
                    }
                    info!("bulkcopy: Auto-generated {} column mappings", mapping_count);
                }

                // Add column mappings
                info!(
                    "bulkcopy: Adding {} column mappings",
                    column_mappings.len()
                );
                for mapping in column_mappings {
                    let tds_mapping = match mapping {
                        ColumnMapping::ByName {
                            source,
                            destination,
                        } => TdsColumnMapping {
                            source: ColumnMappingSource::Name(source),
                            destination,
                        },
                        ColumnMapping::ByOrdinal {
                            source,
                            destination,
                        } => TdsColumnMapping {
                            source: ColumnMappingSource::Ordinal(source),
                            destination,
                        },
                    };
                    bulk_copy = bulk_copy.add_column_mapping(tds_mapping);
                }
                info!("bulkcopy: Column mappings added");

                // Create iterator of PythonRowAdapter
                info!("bulkcopy: Creating PythonRowAdapter iterators");
                let row_adapters: Vec<PythonRowAdapter> = if let Some(metadata) = destination_metadata {
                    info!("bulkcopy: Creating adapters with metadata for type coercion");
                    let metadata_arc = Arc::new(metadata);
                    rows.into_iter()
                        .map(|row| PythonRowAdapter::with_metadata(row, Arc::clone(&metadata_arc)))
                        .collect()
                } else {
                    info!("bulkcopy: Creating adapters without metadata");
                    rows.into_iter()
                        .map(PythonRowAdapter::new)
                        .collect()
                };
                info!("bulkcopy: Created {} row adapters", row_adapters.len());

                // Execute bulk copy with zero-copy streaming
                info!("bulkcopy: Calling write_to_server_zerocopy");
                let bulk_result = bulk_copy
                    .write_to_server_zerocopy(row_adapters.into_iter())
                    .await
                    .map_err(|e| {
                        error!("bulkcopy: write_to_server_zerocopy failed: {}", e);
                        convert_tds_error(e)
                    })?;
                info!(
                    "bulkcopy: write_to_server_zerocopy completed successfully, rows_affected={}",
                    bulk_result.rows_affected
                );

                Ok::<_, PyErr>(bulk_result)
            })
        })?;

        // Convert result to Python dict
        let py_result = PyDict::new(py);
        py_result.set_item("rows_copied", result.rows_affected)?;

        // Calculate batch count
        let batch_count = if options.batch_size > 0 {
            result.rows_affected.div_ceil(options.batch_size as u64)
        } else {
            1
        };
        py_result.set_item("batch_count", batch_count)?;

        py_result.set_item("elapsed_time", result.elapsed.as_secs_f64())?;

        let rows_per_second = if result.elapsed.as_secs_f64() > 0.0 {
            result.rows_affected as f64 / result.elapsed.as_secs_f64()
        } else {
            0.0
        };
        py_result.set_item("rows_per_second", rows_per_second)?;

        Ok(py_result.into())
    }
}

impl PyCoreCursor {
    /// Convert a TDS ColumnValue to a Python object
    fn column_value_to_python<'py>(py: Python<'py>, col_val: &ColumnValues) -> Bound<'py, PyAny> {
        use pyo3::types::{PyBytes, PyString};

        match col_val {
            ColumnValues::Null => py.None().into_bound(py),
            ColumnValues::Bit(b) => (*b).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::TinyInt(i) => (*i).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::SmallInt(i) => (*i).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::Int(i) => (*i).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::BigInt(i) => (*i).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::Float(f) => (*f).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::Real(f) => (*f as f64).into_pyobject(py).unwrap().to_owned().into_any(),
            ColumnValues::String(s) => s
                .to_utf8_string()
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            ColumnValues::Uuid(u) => u
                .to_string()
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            ColumnValues::Bytes(b) => PyBytes::new(py, b).into_any(),
            ColumnValues::Numeric(n) | ColumnValues::Decimal(n) => format!("{:?}", n)
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            ColumnValues::DateTime(dt) => format!("{:?}", dt)
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            ColumnValues::SmallDateTime(dt) => format!("{:?}", dt)
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            ColumnValues::Money(m) => format!("{:?}", m)
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            ColumnValues::SmallMoney(m) => format!("{:?}", m)
                .into_pyobject(py)
                .unwrap()
                .to_owned()
                .into_any(),
            _ => PyString::new(py, &format!("{:?}", col_val)).into_any(),
        }
    }

    /// Parse bulk copy keyword arguments from Python dict
    fn parse_bulkcopy_kwargs(kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<BulkCopyOptions> {
        let mut options = BulkCopyOptions::default();

        if let Some(dict) = kwargs {
            // Performance settings
            if let Some(batch_size) = dict.get_item("batch_size")? {
                options.batch_size = batch_size.extract::<usize>()?;
            }

            if let Some(timeout) = dict.get_item("timeout")? {
                let timeout_secs = timeout.extract::<u64>()?;
                options.timeout = Duration::from_secs(timeout_secs);
            }

            // Column mappings
            if let Some(mappings) = dict.get_item("column_mappings")? {
                options.column_mappings = Self::parse_column_mappings(&mappings)?;
            }

            // Bulk copy options
            if let Some(keep_identity) = dict.get_item("keep_identity")? {
                options.keep_identity = keep_identity.extract::<bool>()?;
            }

            if let Some(check_constraints) = dict.get_item("check_constraints")? {
                options.check_constraints = check_constraints.extract::<bool>()?;
            }

            if let Some(table_lock) = dict.get_item("table_lock")? {
                options.table_lock = table_lock.extract::<bool>()?;
            }

            if let Some(keep_nulls) = dict.get_item("keep_nulls")? {
                options.keep_nulls = keep_nulls.extract::<bool>()?;
            }

            if let Some(fire_triggers) = dict.get_item("fire_triggers")? {
                options.fire_triggers = fire_triggers.extract::<bool>()?;
            }

            if let Some(use_internal_transaction) = dict.get_item("use_internal_transaction")? {
                options.use_internal_transaction = use_internal_transaction.extract::<bool>()?;
            }
        }

        Ok(options)
    }

    /// Parse column mappings from Python list of tuples
    fn parse_column_mappings(mappings_obj: &Bound<'_, PyAny>) -> PyResult<Vec<ColumnMapping>> {
        use pyo3::exceptions::PyTypeError;
        use pyo3::types::PyList;

        // Check if it's a list
        if !mappings_obj.is_instance_of::<PyList>() {
            return Err(PyTypeError::new_err("column_mappings must be a list"));
        }

        let mut result = Vec::new();

        // Iterate through list items
        let list_len = mappings_obj.len()?;
        for i in 0..list_len {
            let item = mappings_obj.get_item(i)?;

            // Check if it's a tuple
            if !item.is_instance_of::<PyTuple>() {
                return Err(PyTypeError::new_err("Each mapping must be a tuple"));
            }

            let tuple_len = item.len()?;
            if tuple_len != 2 {
                return Err(PyTypeError::new_err(
                    "Each mapping tuple must have exactly 2 elements: (source, destination)",
                ));
            }

            let source = item.get_item(0)?;
            let destination = item.get_item(1)?.extract::<String>()?;

            let mapping = if let Ok(source_name) = source.extract::<String>() {
                // Name-based mapping
                ColumnMapping::ByName {
                    source: source_name,
                    destination,
                }
            } else if let Ok(source_ordinal) = source.extract::<usize>() {
                // Ordinal-based mapping
                ColumnMapping::ByOrdinal {
                    source: source_ordinal,
                    destination,
                }
            } else {
                return Err(PyTypeError::new_err(
                    "Source must be either a string (name) or int (ordinal)",
                ));
            };

            result.push(mapping);
        }

        Ok(result)
    }

    fn __repr__(&self) -> String {
        "PyCoreCursor()".to_string()
    }
}

impl PyCoreCursor {
    pub fn new(tds_client: Arc<Mutex<TdsClient>>, runtime_handle: Handle) -> Self {
        Self {
            tds_client,
            runtime_handle,
            has_resultset: false,
        }
    }
}

/// Bulk copy options parsed from Python kwargs
#[derive(Debug, Clone)]
#[allow(dead_code)] // Will be used when implementing actual bulk copy
struct BulkCopyOptions {
    // Performance settings
    batch_size: usize,
    timeout: Duration,

    // Column mappings
    column_mappings: Vec<ColumnMapping>,

    // Bulk copy options
    keep_identity: bool,
    check_constraints: bool,
    table_lock: bool,
    keep_nulls: bool,
    fire_triggers: bool,
    use_internal_transaction: bool,
}

impl Default for BulkCopyOptions {
    fn default() -> Self {
        Self {
            batch_size: 0,
            timeout: Duration::from_secs(30),
            column_mappings: Vec::new(),
            keep_identity: false,
            check_constraints: false,
            table_lock: false,
            keep_nulls: false,
            fire_triggers: false,
            use_internal_transaction: false,
        }
    }
}

/// Column mapping from source to destination
#[derive(Debug, Clone)]
enum ColumnMapping {
    /// Map by column name
    ByName { source: String, destination: String },
    /// Map by column ordinal (0-based)
    ByOrdinal { source: usize, destination: String },
}
