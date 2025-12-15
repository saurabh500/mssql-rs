// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Python bulk copy adapter for zero-copy TDS streaming.
//!
//! This module provides adapters that bridge Python data to the zero-copy
//! BulkLoadRow trait, enabling direct serialization to TDS packets without
//! intermediate allocations.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::types::py_to_column_value;
use async_trait::async_trait;
use mssql_tds::connection::bulk_copy::{BulkLoadRow, ResolvedColumnMapping};
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType};
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::decoder::DecimalParts;
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
    Decimal,
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
            // Check for decimal.Decimal
            let py = py_obj.py();
            if let Ok(decimal_module) = pyo3::types::PyModule::import(py, "decimal") {
                if let Ok(decimal_class) = decimal_module.getattr("Decimal") {
                    if let Ok(is_instance) = py_obj.is_instance(&decimal_class) {
                        if is_instance {
                            return SourcePythonType::Decimal;
                        }
                    }
                }
            }
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
    /// Optional destination column metadata for type coercion (wrapped in Arc for efficient sharing across rows)
    destination_metadata: Option<Arc<Vec<BulkCopyColumnMetadata>>>,
    /// Optional resolved column mappings for reordering columns (wrapped in Arc for efficient sharing across rows)
    resolved_mappings: Option<Arc<Vec<ResolvedColumnMapping>>>,
}

impl PythonRowAdapter {
    /// Create a new Python row adapter with destination metadata for type coercion.
    ///
    /// # Arguments
    ///
    /// * `row` - Python tuple containing column values
    /// * `destination_metadata` - Destination column metadata for type conversion (wrapped in Arc for efficient sharing)
    /// * `resolved_mappings` - Optional resolved column mappings for reordering (wrapped in Arc for efficient sharing)
    ///
    /// # Returns
    ///
    /// A new PythonRowAdapter with type coercion support.
    pub fn with_metadata(
        row: Py<PyAny>,
        destination_metadata: Arc<Vec<BulkCopyColumnMetadata>>,
        resolved_mappings: Option<Arc<Vec<ResolvedColumnMapping>>>,
    ) -> Self {
        Self {
            row,
            destination_metadata: Some(destination_metadata),
            resolved_mappings,
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
        target_metadata: Option<&BulkCopyColumnMetadata>,
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
        target_metadata: Option<&BulkCopyColumnMetadata>,
    ) -> TdsResult<ColumnValues> {
        if let Some(meta) = target_metadata {
            if !meta.is_nullable {
                return Err(Error::UsageError(format!(
                    "Cannot insert NULL value into non-nullable column '{}'. Conversion not possible for NULL to non-nullable column",
                    meta.column_name
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
        target_meta: &BulkCopyColumnMetadata,
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

            // Int → Integer types: Validate range for target type
            // This ensures Python integers that exceed the target type's range are rejected
            // instead of being silently converted to a different integer type
            (
                SourcePythonType::Int,
                SqlDbType::Int | SqlDbType::BigInt | SqlDbType::SmallInt | SqlDbType::TinyInt,
            ) => {
                let result = Self::coerce_python_int_to_integer(py_obj, target_meta.sql_type)?;
                Ok(Some(result))
            }

            // Int → Bit: Convert Python integer to boolean
            // 0 = false, non-zero = true (matches SQL Server's implicit conversion)
            (SourcePythonType::Int, SqlDbType::Bit) => {
                let value = py_obj.extract::<i64>().map_err(|e| {
                    Error::UsageError(format!("Cannot extract Python integer: {}", e))
                })?;
                Ok(Some(ColumnValues::Bit(value != 0)))
            }

            // String → Bit: Parse string as boolean
            // Accepts: "0"/"1", "true"/"false", "True"/"False", "TRUE"/"FALSE"
            (SourcePythonType::String, SqlDbType::Bit) => {
                let py_str = py_obj
                    .cast::<PyString>()
                    .map_err(|e| Error::UsageError(format!("Failed to cast to string: {}", e)))?;

                let s = py_str
                    .to_str()
                    .map_err(|e| Error::UsageError(format!("Failed to extract string: {}", e)))?;

                let bit_value = match s.to_lowercase().as_str() {
                    "0" | "false" => false,
                    "1" | "true" => true,
                    _ => {
                        return Err(Error::UsageError(format!(
                            "Cannot convert string '{}' to BIT. Valid values: '0', '1', 'true', 'false'",
                            s
                        )))
                    }
                };
                Ok(Some(ColumnValues::Bit(bit_value)))
            }

            // String → Decimal/Numeric: Parse string as decimal
            (SourcePythonType::String, SqlDbType::Decimal | SqlDbType::Numeric) => {
                let result = Self::coerce_string_to_decimal(py_obj, target_meta)?;
                Ok(Some(result))
            }

            // Int → Decimal/Numeric: Convert integer to decimal
            (SourcePythonType::Int, SqlDbType::Decimal | SqlDbType::Numeric) => {
                let result = Self::coerce_int_to_decimal(py_obj, target_meta)?;
                Ok(Some(result))
            }

            // Float → Decimal/Numeric: Convert float to decimal
            (SourcePythonType::Float, SqlDbType::Decimal | SqlDbType::Numeric) => {
                let result = Self::coerce_float_to_decimal(py_obj, target_meta)?;
                Ok(Some(result))
            }

            // Decimal → Decimal/Numeric: Validate precision/scale
            (SourcePythonType::Decimal, SqlDbType::Decimal | SqlDbType::Numeric) => {
                let result = Self::coerce_decimal_to_decimal(py_obj, target_meta)?;
                Ok(Some(result))
            }

            // String → Money/SmallMoney: Parse string as money
            (SourcePythonType::String, SqlDbType::Money | SqlDbType::SmallMoney) => {
                let result = Self::coerce_string_to_money(py_obj, target_meta.sql_type)?;
                Ok(Some(result))
            }

            // Int → Money/SmallMoney: Convert integer to money
            (SourcePythonType::Int, SqlDbType::Money | SqlDbType::SmallMoney) => {
                let result = Self::coerce_int_to_money(py_obj, target_meta.sql_type)?;
                Ok(Some(result))
            }

            // Float → Money/SmallMoney: Convert float to money
            (SourcePythonType::Float, SqlDbType::Money | SqlDbType::SmallMoney) => {
                let result = Self::coerce_float_to_money(py_obj, target_meta.sql_type)?;
                Ok(Some(result))
            }

            // Decimal → Money/SmallMoney: Convert decimal to money
            (SourcePythonType::Decimal, SqlDbType::Money | SqlDbType::SmallMoney) => {
                let result = Self::coerce_decimal_to_money(py_obj, target_meta.sql_type)?;
                Ok(Some(result))
            }

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
        let py_str = py_obj
            .cast::<PyString>()
            .map_err(|e| Error::UsageError(format!("Failed to cast to string: {}", e)))?;

        let s = py_str
            .to_str()
            .map_err(|e| Error::UsageError(format!("Failed to extract string: {}", e)))?;

        // Parse as i64 to cover all integer types
        let parsed = s.parse::<i64>().map_err(|e| {
            Error::UsageError(format!("Cannot convert string '{}' to integer: {}", s, e))
        })?;

        // Convert to appropriate TDS integer type with range validation
        match target_type {
            SqlDbType::TinyInt => {
                if (0..=255).contains(&parsed) {
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

    /// Coerce a Python integer to a SQL Server integer type with range validation.
    ///
    /// Extracts the Python int and validates it fits within the target integer type's range.
    /// This prevents Python integers that exceed the target type's range from being silently
    /// converted to a different SQL integer type.
    ///
    /// For BIGINT targets, this function will attempt to extract the value as i64. If the Python
    /// integer is too large to fit in i64 (exceeds i64::MAX or i64::MIN), PyO3's extract will
    /// fail with an OverflowError, which we catch and convert to a meaningful error message.
    fn coerce_python_int_to_integer(
        py_obj: &Bound<'_, PyAny>,
        target_type: SqlDbType,
    ) -> TdsResult<ColumnValues> {
        // Extract as i64 to cover all integer types
        // For Python integers larger than i64::MAX, this will raise OverflowError
        let value = py_obj.extract::<i64>().map_err(|e| {
            // Check if it's an overflow error (Python int too large for i64)
            if e.to_string().contains("OverflowError") || e.to_string().contains("overflow") {
                Error::UsageError(format!(
                    "Python integer out of range for BIGINT column (valid range: {} to {})",
                    i64::MIN,
                    i64::MAX
                ))
            } else {
                Error::UsageError(format!("Cannot extract Python integer as i64: {}", e))
            }
        })?;

        // Convert to appropriate TDS integer type with range validation
        match target_type {
            SqlDbType::TinyInt => {
                if (0..=255).contains(&value) {
                    Ok(ColumnValues::TinyInt(value as u8))
                } else {
                    Err(Error::UsageError(format!(
                        "Python integer {} out of range for TINYINT column (valid range: 0-255)",
                        value
                    )))
                }
            }
            SqlDbType::SmallInt => {
                if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
                    Ok(ColumnValues::SmallInt(value as i16))
                } else {
                    Err(Error::UsageError(format!(
                        "Python integer {} out of range for SMALLINT column (valid range: {} to {})",
                        value,
                        i16::MIN,
                        i16::MAX
                    )))
                }
            }
            SqlDbType::Int => {
                if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
                    Ok(ColumnValues::Int(value as i32))
                } else {
                    Err(Error::UsageError(format!(
                        "Python integer {} out of range for INT column (valid range: {} to {})",
                        value,
                        i32::MIN,
                        i32::MAX
                    )))
                }
            }
            SqlDbType::BigInt => {
                // Value already validated to fit in i64 by extract() above
                Ok(ColumnValues::BigInt(value))
            }
            _ => unreachable!("coerce_python_int_to_integer called with non-integer target type"),
        }
    }

    /// Coerce a Python string to a SQL Server DECIMAL/NUMERIC type.
    fn coerce_string_to_decimal(
        py_obj: &Bound<'_, PyAny>,
        target_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<ColumnValues> {
        let s = py_obj
            .extract::<String>()
            .map_err(|e| Error::UsageError(format!("Failed to extract string: {}", e)))?;

        let decimal_parts =
            DecimalParts::from_string(&s, target_meta.precision, target_meta.scale)?;

        if target_meta.sql_type == SqlDbType::Numeric {
            Ok(ColumnValues::Numeric(decimal_parts))
        } else {
            Ok(ColumnValues::Decimal(decimal_parts))
        }
    }

    /// Coerce a Python integer to a SQL Server DECIMAL/NUMERIC type.
    fn coerce_int_to_decimal(
        py_obj: &Bound<'_, PyAny>,
        target_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<ColumnValues> {
        let value = py_obj
            .extract::<i64>()
            .map_err(|e| Error::UsageError(format!("Failed to extract integer: {}", e)))?;

        let decimal_parts =
            DecimalParts::from_i64(value, target_meta.precision, target_meta.scale)?;

        if target_meta.sql_type == SqlDbType::Numeric {
            Ok(ColumnValues::Numeric(decimal_parts))
        } else {
            Ok(ColumnValues::Decimal(decimal_parts))
        }
    }

    /// Coerce a Python float to a SQL Server DECIMAL/NUMERIC type.
    fn coerce_float_to_decimal(
        py_obj: &Bound<'_, PyAny>,
        target_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<ColumnValues> {
        let value = py_obj
            .extract::<f64>()
            .map_err(|e| Error::UsageError(format!("Failed to extract float: {}", e)))?;

        let decimal_parts =
            DecimalParts::from_f64(value, target_meta.precision, target_meta.scale)?;

        if target_meta.sql_type == SqlDbType::Numeric {
            Ok(ColumnValues::Numeric(decimal_parts))
        } else {
            Ok(ColumnValues::Decimal(decimal_parts))
        }
    }

    /// Coerce a Python Decimal to a SQL Server DECIMAL/NUMERIC type.
    /// Validates that the Python Decimal's precision and scale match the target.
    fn coerce_decimal_to_decimal(
        py_obj: &Bound<'_, PyAny>,
        target_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<ColumnValues> {
        // Extract Decimal as string
        if let Ok(decimal_str) = py_obj.call_method0("__str__") {
            if let Ok(s) = decimal_str.extract::<String>() {
                let decimal_parts =
                    DecimalParts::from_string(&s, target_meta.precision, target_meta.scale)?;

                if target_meta.sql_type == SqlDbType::Numeric {
                    return Ok(ColumnValues::Numeric(decimal_parts));
                } else {
                    return Ok(ColumnValues::Decimal(decimal_parts));
                }
            }
        }

        Err(Error::UsageError(
            "Failed to extract Decimal value as string".to_string(),
        ))
    }

    /// Coerce a Python string to a SQL Server MONEY/SMALLMONEY type.
    fn coerce_string_to_money(
        py_obj: &Bound<'_, PyAny>,
        target_type: SqlDbType,
    ) -> TdsResult<ColumnValues> {
        let py_str = py_obj
            .cast::<PyString>()
            .map_err(|e| Error::UsageError(format!("Failed to cast to string: {}", e)))?;

        let s = py_str
            .to_str()
            .map_err(|e| Error::UsageError(format!("Failed to extract string: {}", e)))?;

        // Parse the string as f64, then convert to money
        let value = s.parse::<f64>().map_err(|e| {
            Error::UsageError(format!(
                "Failed to parse string '{}' as money value: {}",
                s, e
            ))
        })?;

        Self::float_to_money(value, target_type)
    }

    /// Coerce a Python integer to a SQL Server MONEY/SMALLMONEY type.
    fn coerce_int_to_money(
        py_obj: &Bound<'_, PyAny>,
        target_type: SqlDbType,
    ) -> TdsResult<ColumnValues> {
        let value = py_obj
            .extract::<i64>()
            .map_err(|e| Error::UsageError(format!("Failed to extract integer: {}", e)))?;

        // Convert to f64, then to money (money values are scaled by 10,000)
        let money_value = value as f64;
        Self::float_to_money(money_value, target_type)
    }

    /// Coerce a Python float to a SQL Server MONEY/SMALLMONEY type.
    fn coerce_float_to_money(
        py_obj: &Bound<'_, PyAny>,
        target_type: SqlDbType,
    ) -> TdsResult<ColumnValues> {
        let value = py_obj
            .extract::<f64>()
            .map_err(|e| Error::UsageError(format!("Failed to extract float: {}", e)))?;

        Self::float_to_money(value, target_type)
    }

    /// Coerce a Python Decimal to a SQL Server MONEY/SMALLMONEY type.
    fn coerce_decimal_to_money(
        py_obj: &Bound<'_, PyAny>,
        target_type: SqlDbType,
    ) -> TdsResult<ColumnValues> {
        // Extract Decimal as string, then parse as f64
        if let Ok(decimal_str) = py_obj.call_method0("__str__") {
            if let Ok(s) = decimal_str.extract::<String>() {
                let value = s.parse::<f64>().map_err(|e| {
                    Error::UsageError(format!(
                        "Failed to parse Decimal '{}' as money value: {}",
                        s, e
                    ))
                })?;

                return Self::float_to_money(value, target_type);
            }
        }

        Err(Error::UsageError(
            "Failed to extract Decimal value for money conversion".to_string(),
        ))
    }

    /// Helper: Convert f64 to MONEY or SMALLMONEY with range validation.
    ///
    /// Money values are stored as scaled integers (scaled by 10,000):
    /// - MONEY: 8-byte integer (two 4-byte parts: MSB, LSB)
    /// - SMALLMONEY: 4-byte integer
    fn float_to_money(value: f64, target_type: SqlDbType) -> TdsResult<ColumnValues> {
        // Money types have 4 decimal places and are stored as integers scaled by 10,000
        const MONEY_SCALE: f64 = 10000.0;

        // MONEY range: -922,337,203,685,477.5808 to 922,337,203,685,477.5807
        const MONEY_MIN: f64 = -922_337_203_685_477.6;
        const MONEY_MAX: f64 = 922_337_203_685_477.6;

        // SMALLMONEY range: -214,748.3648 to 214,748.3647
        const SMALLMONEY_MIN: f64 = -214_748.364_8;
        const SMALLMONEY_MAX: f64 = 214_748.364_7;

        match target_type {
            SqlDbType::SmallMoney => {
                // Validate range for SMALLMONEY
                if !(SMALLMONEY_MIN..=SMALLMONEY_MAX).contains(&value) {
                    return Err(Error::UsageError(format!(
                        "Value {} exceeds SMALLMONEY range ({} to {})",
                        value, SMALLMONEY_MIN, SMALLMONEY_MAX
                    )));
                }

                // Scale by 10,000 and convert to i32
                let scaled = (value * MONEY_SCALE).round() as i32;

                Ok(ColumnValues::SmallMoney(
                    mssql_tds::datatypes::column_values::SqlSmallMoney { int_val: scaled },
                ))
            }
            SqlDbType::Money => {
                // Validate range for MONEY
                if !(MONEY_MIN..=MONEY_MAX).contains(&value) {
                    return Err(Error::UsageError(format!(
                        "Value {} exceeds MONEY range ({} to {})",
                        value, MONEY_MIN, MONEY_MAX
                    )));
                }

                // Scale by 10,000 and convert to i64
                let scaled = (value * MONEY_SCALE).round() as i64;

                // Split into MSB (high 32 bits) and LSB (low 32 bits)
                let lsb_part = (scaled & 0xFFFFFFFF) as i32;
                let msb_part = (scaled >> 32) as i32;

                Ok(ColumnValues::Money(
                    mssql_tds::datatypes::column_values::SqlMoney { lsb_part, msb_part },
                ))
            }
            _ => Err(Error::UsageError(format!(
                "Invalid target type {:?} for money conversion",
                target_type
            ))),
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
        let column_values: Vec<_> = Python::attach(|py| {
            let tuple = self
                .row
                .bind(py)
                .cast::<PyTuple>()
                .map_err(|e| Error::UsageError(format!("Expected tuple, got: {}", e)))?;

            // If we have resolved mappings, use them to determine column order and indices
            if let Some(mappings) = &self.resolved_mappings {
                // Use mappings to read columns in the correct order
                let mut values = Vec::with_capacity(mappings.len());
                let mut total_extract_time = Duration::ZERO;

                for mapping in mappings.iter() {
                    let extract_start = Instant::now();

                    // Read from source column index specified in the mapping
                    let item = tuple.get_item(mapping.source_index).map_err(|e| {
                        Error::UsageError(format!(
                            "Source column index {} out of bounds (tuple has {} columns): {}",
                            mapping.source_index,
                            tuple.len(),
                            e
                        ))
                    })?;

                    // Get target metadata from destination_metadata using destination_index
                    let target_metadata = self
                        .destination_metadata
                        .as_ref()
                        .and_then(|meta| meta.get(mapping.destination_index));

                    // Try conversion with type coercion and null validation
                    let column_value = Self::convert_with_coercion(&item, target_metadata)?;

                    total_extract_time += extract_start.elapsed();
                    values.push(column_value);
                }

                Ok::<Vec<_>, Error>(values)
            } else {
                // No mappings - use sequential reading (original behavior)
                let mut values = Vec::with_capacity(tuple.len());
                let mut total_extract_time = Duration::ZERO;

                for (i, item) in tuple.iter().enumerate() {
                    let extract_start = Instant::now();

                    // Get target metadata if available
                    let target_metadata = self
                        .destination_metadata
                        .as_ref()
                        .and_then(|meta| meta.get(i));

                    // Try conversion with type coercion and null validation
                    let column_value = Self::convert_with_coercion(&item, target_metadata)?;

                    total_extract_time += extract_start.elapsed();
                    values.push(column_value);
                }

                Ok::<Vec<_>, Error>(values)
            }
        })?;

        // Step 2: GIL is now released, write values to packet asynchronously
        for value in column_values.iter() {
            writer.write_column_value(*column_index, value).await?;
            *column_index += 1;
        }

        Ok(())
    }
}
