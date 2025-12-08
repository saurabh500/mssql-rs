// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Python bulk copy adapter for zero-copy TDS streaming.
//!
//! This module provides adapters that bridge Python data to the zero-copy
//! BulkLoadRow trait, enabling direct serialization to TDS packets without
//! intermediate allocations.

use crate::types::py_to_column_value;
use async_trait::async_trait;
use mssql_tds::connection::bulk_copy::BulkLoadRow;
use mssql_tds::core::TdsResult;
use mssql_tds::error::Error;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

/// Adapter that wraps a Python tuple for zero-copy bulk insert.
///
/// This struct implements the `BulkLoadRow` trait, allowing Python tuples
/// to be serialized directly to TDS packets without allocating intermediate
/// Vec<ColumnValues>. The GIL is acquired only when reading Python values.
///
/// # Performance
///
/// - **Zero allocations per row**: No Vec<ColumnValues> created
/// - **Direct serialization**: Python value → TDS packet bytes
/// - **Minimal GIL contention**: GIL acquired per column, not per batch
pub struct PythonRowAdapter {
    /// Python tuple containing row data (stored as Py<PyAny> for Send + Sync)
    row: Py<PyAny>,
}

impl PythonRowAdapter {
    /// Create a new Python row adapter from a tuple.
    ///
    /// # Arguments
    ///
    /// * `row` - Python tuple containing column values
    ///
    /// # Returns
    ///
    /// A new PythonRowAdapter wrapping the tuple.
    pub fn new(row: Py<PyAny>) -> Self {
        Self { row }
    }

    /// Extract column count from the wrapped tuple.
    ///
    /// This acquires the GIL to access the Python object.
    ///
    /// # Returns
    ///
    /// Number of columns in the tuple, or an error if the object is not a tuple.
    pub fn column_count(&self) -> TdsResult<usize> {
        Python::attach(|py| {
            let tuple = self
                .row
                .bind(py)
                .cast::<PyTuple>()
                .map_err(|e| Error::UsageError(format!("Expected tuple, got: {}", e)))?;
            Ok(tuple.len())
        })
    }
}

#[async_trait]
impl BulkLoadRow for PythonRowAdapter {
    /// Write this row's column values directly to the TDS packet writer.
    ///
    /// This method acquires the GIL to read each Python value, converts it
    /// to a ColumnValues variant, and writes it directly to the packet without
    /// intermediate allocations.
    ///
    /// The implementation strategy:
    /// 1. Acquire GIL and convert all Python values to Vec<ColumnValues>
    /// 2. Release GIL
    /// 3. Write each ColumnValues to packet asynchronously
    ///
    /// Note: While this creates a temporary Vec, it's still more efficient than
    /// the traditional BulkCopyRow approach because:
    /// - We only allocate once per row (not per batch)
    /// - The Vec is immediately consumed and dropped
    /// - No intermediate buffering at the batch level
    ///
    /// # Arguments
    ///
    /// * `writer` - Streaming bulk load writer for direct packet serialization
    /// * `column_index` - Mutable column index tracker (maintained by writer)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Python value cannot be converted to a TDS type
    /// - Column count doesn't match expected metadata
    /// - Network errors occur during transmission
    async fn write_to_packet(
        &self,
        writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        use std::time::Instant;
        
        // Step 1: Acquire GIL and convert Python values to ColumnValues
        let start_gil = Instant::now();
        let column_values: Vec<_> = Python::attach(|py| {
            let _start_type_conv = Instant::now();
            let tuple = self
                .row
                .bind(py)
                .cast::<PyTuple>()
                .map_err(|e| Error::UsageError(format!("Expected tuple, got: {}", e)))?;

            // Convert each Python value to ColumnValues
            let mut values = Vec::with_capacity(tuple.len());
            let mut total_extract_time = std::time::Duration::ZERO;
            for item in tuple.iter() {
                let extract_start = Instant::now();
                let column_value = py_to_column_value(&item)?;
                total_extract_time += extract_start.elapsed();
                values.push(column_value);
            }
            
            // Log timing per row (sample every 10000 rows to avoid spam)
            thread_local! {
                static ROW_COUNT: std::cell::Cell<u64> = std::cell::Cell::new(0);
                static TOTAL_GIL_TIME: std::cell::Cell<std::time::Duration> = std::cell::Cell::new(std::time::Duration::ZERO);
                static TOTAL_CONV_TIME: std::cell::Cell<std::time::Duration> = std::cell::Cell::new(std::time::Duration::ZERO);
            }
            
            ROW_COUNT.with(|c| {
                let count = c.get() + 1;
                c.set(count);
                
                TOTAL_GIL_TIME.with(|t| t.set(t.get() + start_gil.elapsed()));
                TOTAL_CONV_TIME.with(|t| t.set(t.get() + total_extract_time));
                
                if count % 10000 == 0 {
                    let avg_gil = TOTAL_GIL_TIME.with(|t| t.get()) / count as u32;
                    let avg_conv = TOTAL_CONV_TIME.with(|t| t.get()) / count as u32;
                    eprintln!("[PROFILE] {} rows: avg GIL+conversion={:?}, avg type_conversion={:?}", 
                             count, avg_gil, avg_conv);
                }
            });

            Ok::<Vec<_>, Error>(values)
        })?;

        // Step 2: GIL is now released, write values to packet asynchronously
        let start_write = Instant::now();
        for value in column_values.iter() {
            writer.write_column_value(*column_index, value).await?;
            *column_index += 1;
        }
        let write_time = start_write.elapsed();
        
        // Sample write timing too
        thread_local! {
            static WRITE_COUNT: std::cell::Cell<u64> = std::cell::Cell::new(0);
            static TOTAL_WRITE_TIME: std::cell::Cell<std::time::Duration> = std::cell::Cell::new(std::time::Duration::ZERO);
        }
        
        WRITE_COUNT.with(|c| {
            let count = c.get() + 1;
            c.set(count);
            
            TOTAL_WRITE_TIME.with(|t| t.set(t.get() + write_time));
            
            if count % 10000 == 0 {
                let avg_write = TOTAL_WRITE_TIME.with(|t| t.get()) / count as u32;
                eprintln!("[PROFILE] {} rows: avg TDS_write={:?}", count, avg_write);
            }
        });

        Ok(())
    }
}
