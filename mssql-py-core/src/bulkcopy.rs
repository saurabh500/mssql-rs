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

/// Represents the source Python type for conversion mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourcePythonType {
    None,
    String,
    Int,
    Float,
    Bytes,
    Bool,
    Other,
}

impl SourcePythonType {
    /// Detect the Python type from a PyAny object (fast type checking).
    #[inline]
    fn detect(py_obj: &Bound<'_, PyAny>) -> Self {
        if py_obj.is_none() {
            SourcePythonType::None
        } else if py_obj.is_instance_of::<PyString>() {
            SourcePythonType::String
        } else if py_obj.is_instance_of::<pyo3::types::PyInt>() {
            SourcePythonType::Int
        } else if py_obj.is_instance_of::<pyo3::types::PyFloat>() {
            SourcePythonType::Float
        } else if py_obj.is_instance_of::<pyo3::types::PyBytes>() {
            SourcePythonType::Bytes
        } else if py_obj.is_instance_of::<pyo3::types::PyBool>() {
            SourcePythonType::Bool
        } else {
            SourcePythonType::Other
        }
    }
}

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

    /// Convert a Python value to a ColumnValue with type coercion based on target SQL type.
    ///
    /// This function implements a source-to-target type mapping strategy:
    /// 1. Detect the source Python type (fast type check)
    /// 2. Validate NULL handling against target column nullability
    /// 3. Check if coercion is needed based on (source_type, target_sql_type) pair
    /// 4. Apply appropriate conversion or fall back to default conversion
    ///
    /// # Type Coercion Matrix (extensible)
    ///
    /// | Source Python Type | Target SQL Type(s)              | Coercion Strategy           |
    /// |--------------------|----------------------------------|------------------------------|
    /// | None               | Any non-nullable                | Error: NULL constraint       |
    /// | None               | Any nullable                    | ColumnValues::Null           |
    /// | String             | INT/BIGINT/SMALLINT/TINYINT     | Parse string → integer       |
    /// | String             | (future: DECIMAL/NUMERIC)       | Parse string → decimal       |
    /// | String             | (future: DATETIME/DATE)         | Parse string → datetime      |
    /// | (default)          | Any                             | py_to_column_value()         |
    fn convert_with_coercion(
        py_obj: &Bound<'_, PyAny>,
        target_metadata: Option<&DestinationColumnMetadata>,
    ) -> TdsResult<ColumnValues> {
        // Step 1: Fast source type detection
        let source_type = SourcePythonType::detect(py_obj);

        // Step 2: Handle NULL values with validation
        if source_type == SourcePythonType::None {
            return Self::handle_null_value(target_metadata);
        }

        // Step 3: Check if we need type coercion based on source → target mapping
        if let Some(meta) = target_metadata {
            if let Some(coerced_value) = Self::try_type_coercion(py_obj, source_type, meta)? {
                return Ok(coerced_value);
            }
        }

        // Step 4: Fall back to default Python → TDS conversion
        py_to_column_value(py_obj)
    }

    /// Handle NULL value insertion with nullability validation.
    #[inline]
    fn handle_null_value(
        target_metadata: Option<&DestinationColumnMetadata>,
    ) -> TdsResult<ColumnValues> {
        if let Some(meta) = target_metadata {
            if !meta.is_nullable {
                return Err(Error::UsageError(format!(
                    "Cannot insert NULL value into non-nullable column '{}'. Conversion not possible for NULL to non-nullable column",
                    meta.name
                )));
            }
        }
        Ok(ColumnValues::Null)
    }

    /// Try type coercion based on source Python type and target SQL type.
    ///
    /// Returns Some(ColumnValues) if coercion was applied, None if no coercion needed.
    ///
    /// This method encapsulates the type coercion mapping logic, making it easy to add
    /// new conversions by pattern matching on (source_type, target_sql_type) pairs.
    fn try_type_coercion(
        py_obj: &Bound<'_, PyAny>,
        source_type: SourcePythonType,
        target_meta: &DestinationColumnMetadata,
    ) -> TdsResult<Option<ColumnValues>> {
        // Type coercion dispatch based on (source → target) mapping
        match (source_type, target_meta.sql_type) {
            // String → Integer types: Parse string as integer
            (
                SourcePythonType::String,
                SqlDbType::Int | SqlDbType::BigInt | SqlDbType::SmallInt | SqlDbType::TinyInt,
            ) => {
                let result = Self::coerce_string_to_integer(py_obj, target_meta.sql_type)?;
                Ok(Some(result))
            }

            // TODO: Add more coercion mappings as needed:
            // (SourcePythonType::String, SqlDbType::Decimal | SqlDbType::Numeric) => {...}
            // (SourcePythonType::String, SqlDbType::DateTime | SqlDbType::Date) => {...}
            // (SourcePythonType::Int, SqlDbType::Bit) => {...}

            // No coercion needed - use default conversion
            _ => Ok(None),
        }
    }

    /// Coerce a Python string to a SQL Server integer type.
    ///
    /// Parses the string as i64 and validates it fits within the target integer type's range.
    fn coerce_string_to_integer(
        py_obj: &Bound<'_, PyAny>,
        target_type: SqlDbType,
    ) -> TdsResult<ColumnValues> {
        let py_str = py_obj.cast::<PyString>().map_err(|e| {
            Error::UsageError(format!("Failed to cast to string: {}", e))
        })?;

        let s = py_str.to_str().map_err(|e| {
            Error::UsageError(format!("Failed to extract string: {}", e))
        })?;

        // Parse as i64 to cover all integer types
        let parsed = s.parse::<i64>().map_err(|e| {
            Error::UsageError(format!(
                "Cannot convert string '{}' to integer: {}",
                s, e
            ))
        })?;

        // Convert to appropriate TDS integer type with range validation
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
                        "Value {} out of range for SMALLINT ({} to {})",
                        parsed,
                        i16::MIN,
                        i16::MAX
                    )))
                }
            }
            SqlDbType::Int => {
                if parsed >= i32::MIN as i64 && parsed <= i32::MAX as i64 {
                    Ok(ColumnValues::Int(parsed as i32))
                } else {
                    Err(Error::UsageError(format!(
                        "Value {} out of range for INT ({} to {})",
                        parsed,
                        i32::MIN,
                        i32::MAX
                    )))
                }
            }
            SqlDbType::BigInt => Ok(ColumnValues::BigInt(parsed)),
            _ => unreachable!("coerce_string_to_integer called with non-integer target type"),
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
