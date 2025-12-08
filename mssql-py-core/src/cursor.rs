// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use mssql_tds::connection::bulk_copy::{BulkCopy, ColumnMappingSource, ColumnMapping as TdsColumnMapping};
use mssql_tds::connection::tds_client::TdsClient;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyIterator, PyTuple};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::runtime::Handle;
use tracing::{info, error};

use crate::bulkcopy::PythonRowAdapter;

/// Python Cursor class for Core TDS backend
#[pyclass]
pub struct PyCoreCursor {
    tds_client: Arc<Mutex<TdsClient>>,
    runtime_handle: Handle,
    has_resultset: bool,
}

#[pymethods]
impl PyCoreCursor {
    fn execute(&mut self, _query: String, _params: Option<Vec<Py<PyAny>>>) -> PyResult<()> {
        // TODO: Implement execute with actual TDS query execution
        Ok(())
    }

    fn fetchone(&mut self) -> PyResult<Option<Py<PyAny>>> {
        // TODO: Implement fetchone
        Ok(None)
    }

    fn fetchall(&mut self) -> PyResult<Vec<Py<PyAny>>> {
        // TODO: Implement fetchall
        Ok(vec![])
    }

    fn fetchmany(&mut self, _size: Option<usize>) -> PyResult<Vec<Py<PyAny>>> {
        // TODO: Implement fetchmany
        Ok(vec![])
    }

    fn close(&mut self) -> PyResult<()> {
        // Cursor close - no action needed for now
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
    ///   - `column_mappings` (list): List of (source, dest) tuples for column mapping
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
        info!("bulkcopy: Parsed options - batch_size={}, timeout={:?}", options.batch_size, options.timeout);

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
                let mut bulk_copy = BulkCopy::new(&mut *client, table_name)
                    .batch_size(options.batch_size)
                    .timeout(options.timeout)
                    .check_constraints(options.check_constraints)
                    .fire_triggers(options.fire_triggers)
                    .keep_identity(options.keep_identity)
                    .keep_nulls(options.keep_nulls)
                    .table_lock(options.table_lock)
                    .use_internal_transaction(options.use_internal_transaction);
                info!("bulkcopy: BulkCopy instance created");

                // Add column mappings
                info!("bulkcopy: Adding {} column mappings", options.column_mappings.len());
                for mapping in options.column_mappings {
                    let tds_mapping = match mapping {
                        ColumnMapping::ByName { source, destination } => {
                            TdsColumnMapping {
                                source: ColumnMappingSource::Name(source),
                                destination,
                            }
                        }
                        ColumnMapping::ByOrdinal { source, destination } => {
                            TdsColumnMapping {
                                source: ColumnMappingSource::Ordinal(source),
                                destination,
                            }
                        }
                    };
                    bulk_copy = bulk_copy.add_column_mapping(tds_mapping);
                }
                info!("bulkcopy: Column mappings added");

                // Create iterator of PythonRowAdapter
                info!("bulkcopy: Creating PythonRowAdapter iterators");
                let row_adapters: Vec<PythonRowAdapter> = rows
                    .into_iter()
                    .map(|py_obj| PythonRowAdapter::new(py_obj))
                    .collect();
                info!("bulkcopy: Created {} row adapters", row_adapters.len());

                // Execute bulk copy with zero-copy streaming
                info!("bulkcopy: Calling write_to_server_zerocopy");
                let bulk_result = bulk_copy
                    .write_to_server_zerocopy(row_adapters.into_iter())
                    .await
                    .map_err(|e| {
                        error!("bulkcopy: write_to_server_zerocopy failed: {}", e);
                        pyo3::exceptions::PyRuntimeError::new_err(format!("Bulk copy failed: {}", e))
                    })?;
                info!("bulkcopy: write_to_server_zerocopy completed successfully, rows_affected={}", bulk_result.rows_affected);

                Ok::<_, PyErr>(bulk_result)
            })
        })?;

        // Convert result to Python dict
        let py_result = PyDict::new(py);
        py_result.set_item("rows_copied", result.rows_affected)?;
        
        // Calculate batch count
        let batch_count = if options.batch_size > 0 {
            (result.rows_affected + options.batch_size as u64 - 1) / options.batch_size as u64
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
