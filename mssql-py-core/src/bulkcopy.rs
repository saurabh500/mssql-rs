// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Python bulk copy adapter for zero-copy TDS streaming.
//!
//! This module provides adapters that bridge Python data to the zero-copy
//! BulkLoadRow trait, enabling direct serialization to TDS packets without
//! intermediate allocations.

use std::cell::Cell;
use std::time::{Duration, Instant};

use crate::types::py_to_column_value;
use async_trait::async_trait;
use mssql_tds::connection::bulk_copy::{BulkLoadRow, DestinationColumnMetadata};
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::bulk_copy_metadata::SqlDbType;
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::error::Error;
use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};

/// Adapter that wraps a Python tuple for zero-copy bulk insert.
///
/// This struct implements the `BulkLoadRow` trait, allowing Python tuples
/// to be serialized directly to TDS packets without allocating intermediate
/// Vec<ColumnValues>. The GIL is acquired only when reading Python values.
///
/// When destination metadata is provided, the adapter will attempt to perform
/// type conversions (e.g., string to int) to match the target column types.
pub struct PythonRowAdapter {
    /// Python tuple containing row data (stored as Py<PyAny> for Send + Sync)
    row: Py<PyAny>,
    /// Optional destination column metadata for type coercion
    destination_metadata: Option<Vec<DestinationColumnMetadata>>,
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
        Self {
            row,
            destination_metadata: None,
        }
    }

    /// Create a new Python row adapter with destination metadata for type coercion.
    ///
    /// # Arguments
    ///
    /// * `row` - Python tuple containing column values
    /// * `destination_metadata` - Destination column metadata for type conversion
    ///
    /// # Returns
    ///
    /// A new PythonRowAdapter with type coercion support.
    pub fn with_metadata(row: Py<PyAny>, destination_metadata: Vec<DestinationColumnMetadata>) -> Self {
        Self {
            row,
            destination_metadata: Some(destination_metadata),
        }
    }

    /// Convert a Python value to a ColumnValue, attempting type coercion if metadata is available.
    fn convert_with_coercion(
        py_obj: &Bound<'_, PyAny>,
        target_metadata: Option<&DestinationColumnMetadata>,
    ) -> TdsResult<ColumnValues> {
        // First check if value is None and validate against nullable constraint
        if py_obj.is_none() {
            if let Some(meta) = target_metadata {
                if !meta.is_nullable {
                    return Err(Error::UsageError(format!(
                        "Cannot insert NULL value into non-nullable column '{}'. Conversion not possible for NULL to non-nullable column",
                        meta.name
                    )));
                }
            }
            // If nullable or no metadata, return NULL
            return Ok(ColumnValues::Null);
        }
        
        // If we have target metadata, check if we need type coercion
        if let Some(meta) = target_metadata {
            // Check if the Python value is a string and target is an integer type
            if py_obj.is_instance_of::<PyString>() {
                match meta.sql_type {
                    SqlDbType::Int | SqlDbType::BigInt | SqlDbType::SmallInt | SqlDbType::TinyInt => {
                        // Try type coercion for string-to-integer conversion
                        return Self::try_coerce_type(py_obj, meta.sql_type);
                    }
                    _ => {
                        // Fall through to normal conversion
                    }
                }
            }
        }
        
        // Try direct conversion
        py_to_column_value(py_obj)
    }

    /// Attempt to coerce a Python value to match the target SQL type.
    fn try_coerce_type(py_obj: &Bound<'_, PyAny>, target_type: SqlDbType) -> TdsResult<ColumnValues> {
        match target_type {
            SqlDbType::Int | SqlDbType::BigInt | SqlDbType::SmallInt | SqlDbType::TinyInt => {
                // Try to convert string to integer
                if let Ok(py_str) = py_obj.cast::<PyString>() {
                    let s = py_str.to_str().map_err(|e| {
                        Error::UsageError(format!("Failed to extract string: {}", e))
                    })?;
                    
                    // Try parsing as i64 to cover all integer types
                    let parsed = s.parse::<i64>().map_err(|e| {
                        Error::UsageError(format!(
                            "Cannot convert string '{}' to integer: {}",
                            s, e
                        ))
                    })?;
                    
                    // Return appropriate integer type based on target
                    match target_type {
                        SqlDbType::TinyInt => {
                            if parsed >= 0 && parsed <= 255 {
                                Ok(ColumnValues::TinyInt(parsed as u8))
                            } else {
                                Err(Error::UsageError(format!(
                                    "Value {} out of range for TINYINT (0-255)",
                                    parsed
                                )))
                            }
                        }
                        SqlDbType::SmallInt => {
                            if parsed >= i16::MIN as i64 && parsed <= i16::MAX as i64 {
                                Ok(ColumnValues::SmallInt(parsed as i16))
                            } else {
                                Err(Error::UsageError(format!(
                                    "Value {} out of range for SMALLINT",
                                    parsed
                                )))
                            }
                        }
                        SqlDbType::Int => {
                            if parsed >= i32::MIN as i64 && parsed <= i32::MAX as i64 {
                                Ok(ColumnValues::Int(parsed as i32))
                            } else {
                                Err(Error::UsageError(format!(
                                    "Value {} out of range for INT",
                                    parsed
                                )))
                            }
                        }
                        SqlDbType::BigInt => Ok(ColumnValues::BigInt(parsed)),
                        _ => unreachable!(),
                    }
                } else {
                    Err(Error::UsageError(format!(
                        "Cannot coerce type to integer, expected string but got: {:?}",
                        py_obj.get_type().name()
                    )))
                }
            }
            _ => {
                // No coercion available for this type yet
                Err(Error::UsageError(format!(
                    "Type coercion not implemented for target type: {:?}",
                    target_type
                )))
            }
        }
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
        writer: &mut StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        // Step 1: Acquire GIL and convert Python values to ColumnValues
        let start_gil = Instant::now();
        let column_values: Vec<_> = Python::attach(|py| {
            let tuple = self
                .row
                .bind(py)
                .cast::<PyTuple>()
                .map_err(|e| Error::UsageError(format!("Expected tuple, got: {}", e)))?;

            // Convert each Python value to ColumnValues
            let mut values = Vec::with_capacity(tuple.len());
            let mut total_extract_time = Duration::ZERO;
            for (i, item) in tuple.iter().enumerate() {
                let extract_start = Instant::now();
                
                // Get target metadata if available
                let target_metadata = self.destination_metadata
                    .as_ref()
                    .and_then(|meta| meta.get(i));
                
                // Try conversion with type coercion and null validation
                let column_value = Self::convert_with_coercion(&item, target_metadata)?;
                
                total_extract_time += extract_start.elapsed();
                values.push(column_value);
            }

            // Log timing per row (sample every 10000 rows to avoid spam)
            thread_local! {
                static ROW_COUNT: Cell<u64> = const { Cell::new(0) };
                static TOTAL_GIL_TIME: Cell<Duration> = const { Cell::new(Duration::ZERO) };
                static TOTAL_CONV_TIME: Cell<Duration> = const { Cell::new(Duration::ZERO) };
            }

            ROW_COUNT.with(|c| {
                let count = c.get() + 1;
                c.set(count);

                TOTAL_GIL_TIME.with(|t| t.set(t.get() + start_gil.elapsed()));
                TOTAL_CONV_TIME.with(|t| t.set(t.get() + total_extract_time));

                if count % 10000 == 0 {
                    let avg_gil = TOTAL_GIL_TIME.with(|t| t.get()) / count as u32;
                    let avg_conv = TOTAL_CONV_TIME.with(|t| t.get()) / count as u32;
                    eprintln!(
                        "[PROFILE] {} rows: avg GIL+conversion={:?}, avg type_conversion={:?}",
                        count, avg_gil, avg_conv
                    );
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
            static WRITE_COUNT: Cell<u64> = const { Cell::new(0) };
            static TOTAL_WRITE_TIME: Cell<Duration> = const { Cell::new(Duration::ZERO) };
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
