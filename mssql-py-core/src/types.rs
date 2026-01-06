// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Type conversion utilities between Python and SQL Server types

use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType};
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::error::Error;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDate, PyDateTime, PyInt, PyModule, PyString, PyTime};

/// Convert a Python object to ColumnValues for TDS serialization
///
/// This function handles direct conversion from Python types to TDS column values,
/// supporting the most common SQL Server data types.
///
/// # Supported Types
///
/// - `None` → `ColumnValues::Null`
/// - `int` → `ColumnValues::Int` or `ColumnValues::BigInt`
/// - `float` → `ColumnValues::Float`
/// - `str` → `ColumnValues::String`
/// - `bool` → `ColumnValues::Bit`
/// - `bytes` → `ColumnValues::Binary`
/// - `datetime.datetime` → `ColumnValues::DateTime2`
/// - `datetime.date` → `ColumnValues::Date`
/// - `datetime.time` → `ColumnValues::Time`
///
/// # Arguments
///
/// * `py_obj` - Python object to convert
/// * `target_metadata` - Optional target column metadata for type validation
///
/// # Returns
///
/// `TdsResult<ColumnValues>` - The converted column value
///
/// # Errors
///
/// Returns an error if:
/// - The Python type is not supported or conversion fails
/// - When target_metadata is provided, if the converted type doesn't match the target SQL type
///
/// Fast-path converter that checks type once and extracts directly
///
/// This avoids the expensive fallback chain of trying bool→i32→i64→str
/// on every single value. Instead, we check the Python type name once
/// and use direct extraction.
///
/// # Type Validation
///
/// When `target_metadata` is provided, this function validates that the converted
/// ColumnValues type is compatible with the target SQL type. This prevents silent
/// type mismatches that could occur when try_type_coercion() returns None but
/// the type mapping isn't properly maintained.
pub fn py_to_column_value(
    py_obj: &Bound<'_, PyAny>,
    target_metadata: Option<&BulkCopyColumnMetadata>,
) -> TdsResult<ColumnValues> {
    let result = py_to_column_value_internal(py_obj, target_metadata)?;

    // Validate type compatibility if metadata provided
    if let Some(meta) = target_metadata {
        validate_type_compatibility(&result, meta)?;
    }

    Ok(result)
}

/// Internal conversion function without validation.
///
/// This is the core type conversion logic extracted to a separate function
/// to keep the validation step clean and maintainable.
fn py_to_column_value_internal(
    py_obj: &Bound<'_, PyAny>,
    target_metadata: Option<&BulkCopyColumnMetadata>,
) -> TdsResult<ColumnValues> {
    // Handle None (NULL) - most common check
    if py_obj.is_none() {
        return Ok(ColumnValues::Null);
    }

    // Fast path: check instance type directly
    // This is much faster than trying extract::<T>() in sequence

    // Check for int (most common in bulk copy)
    if py_obj.is_instance_of::<PyInt>() {
        // Try i32 first (most common range)
        if let Ok(val) = py_obj.extract::<i32>() {
            return Ok(ColumnValues::Int(val));
        }
        // Fallback to i64 for large integers
        if let Ok(val) = py_obj.extract::<i64>() {
            return Ok(ColumnValues::BigInt(val));
        }
    }

    // Check for string (second most common)
    if py_obj.is_instance_of::<PyString>() {
        // Direct string extraction - no fallback needed
        let val = py_obj
            .extract::<String>()
            .map_err(|e| Error::UsageError(format!("Failed to extract string: {}", e)))?;
        let sql_string = SqlString::from_utf8_string(val);
        return Ok(ColumnValues::String(sql_string));
    }

    // Check for bool (must be before int check in fallback, but after PyInt instance check)
    if py_obj.is_instance_of::<PyBool>() {
        let val = py_obj
            .extract::<bool>()
            .map_err(|e| Error::UsageError(format!("Failed to extract bool: {}", e)))?;
        return Ok(ColumnValues::Bit(val));
    }

    // Check for float
    if py_obj.is_exact_instance_of::<pyo3::types::PyFloat>() {
        let val = py_obj
            .extract::<f64>()
            .map_err(|e| Error::UsageError(format!("Failed to extract float: {}", e)))?;

        // Check if target metadata specifies REAL vs FLOAT
        if let Some(meta) = target_metadata {
            match meta.sql_type {
                SqlDbType::Real => {
                    // Convert to Real (f32) - may lose precision
                    return Ok(ColumnValues::Real(val as f32));
                }
                SqlDbType::Float => {
                    // Keep as Float (f64)
                    return Ok(ColumnValues::Float(val));
                }
                _ => {
                    // For other target types, use Float and let coercion handle it
                    return Ok(ColumnValues::Float(val));
                }
            }
        }

        // No metadata provided - default to Float (f64)
        return Ok(ColumnValues::Float(val));
    }

    // Check for bytes
    if py_obj.is_instance_of::<PyBytes>() {
        let bytes = py_obj
            .extract::<Vec<u8>>()
            .map_err(|e| Error::UsageError(format!("Failed to extract bytes: {}", e)))?;
        return Ok(ColumnValues::Bytes(bytes));
    }

    // Check for datetime types (must check PyDateTime before PyDate since datetime is a subclass of date)
    if py_obj.is_instance_of::<PyDateTime>() {
        // Extract components from Python datetime
        let year = py_obj
            .getattr("year")
            .and_then(|v| v.extract::<i32>())
            .map_err(|e| Error::UsageError(format!("Failed to get year from datetime: {}", e)))?;

        let month = py_obj
            .getattr("month")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get month from datetime: {}", e)))?;

        let day = py_obj
            .getattr("day")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get day from datetime: {}", e)))?;

        let hour = py_obj
            .getattr("hour")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get hour from datetime: {}", e)))?;

        let minute = py_obj
            .getattr("minute")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get minute from datetime: {}", e)))?;

        let second = py_obj
            .getattr("second")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get second from datetime: {}", e)))?;

        let microsecond = py_obj
            .getattr("microsecond")
            .and_then(|v| v.extract::<u32>())
            .map_err(|e| {
                Error::UsageError(format!("Failed to get microsecond from datetime: {}", e))
            })?;

        // Calculate days since 1900-01-01
        // Use Python's date.toordinal() which gives days since 0001-01-01 (1-based)
        // Then subtract the ordinal of 1900-01-01
        let py = py_obj.py();
        let datetime_module = PyModule::import(py, "datetime")
            .map_err(|e| Error::UsageError(format!("Failed to import datetime module: {}", e)))?;

        let date_class = datetime_module
            .getattr("date")
            .map_err(|e| Error::UsageError(format!("Failed to get date class: {}", e)))?;

        // Create date for 1900-01-01
        let base_date = date_class
            .call1((1900, 1, 1))
            .map_err(|e| Error::UsageError(format!("Failed to create base date: {}", e)))?;

        let base_ordinal = base_date
            .call_method0("toordinal")
            .and_then(|v| v.extract::<i32>())
            .map_err(|e| Error::UsageError(format!("Failed to get base ordinal: {}", e)))?;

        // Get ordinal of the current datetime's date part
        let current_date = date_class
            .call1((year, month, day))
            .map_err(|e| Error::UsageError(format!("Failed to create current date: {}", e)))?;

        let current_ordinal = current_date
            .call_method0("toordinal")
            .and_then(|v| v.extract::<i32>())
            .map_err(|e| Error::UsageError(format!("Failed to get current ordinal: {}", e)))?;

        let days = current_ordinal - base_ordinal;

        // Check if target is DateTimeOffset, SmallDateTime or DateTime2 to determine which format to use
        if let Some(meta) = target_metadata {
            if meta.sql_type == SqlDbType::DateTimeOffset {
                // Convert to DATETIMEOFFSET format
                // DATETIMEOFFSET uses DATETIME2 + timezone offset (i16, minutes from UTC)
                // Calculate days from year 1 using Python's toordinal
                let current_ordinal = current_date
                    .call_method0("toordinal")
                    .and_then(|v| v.extract::<u32>())
                    .map_err(|e| {
                        Error::UsageError(format!(
                            "Failed to get current ordinal for DATETIMEOFFSET: {}",
                            e
                        ))
                    })?;

                // Python's toordinal() returns 1 for 0001-01-01, so subtract 1 to get 0-based days
                let days_dto = current_ordinal.checked_sub(1).ok_or_else(|| {
                    Error::UsageError(
                        "Date ordinal is 0, expected >= 1 for DATETIMEOFFSET".to_string(),
                    )
                })?;

                // Convert to 100-nanosecond units (DATETIME2/TIME uses 100ns precision)
                let time_nanoseconds = (hour as u64) * 36_000_000_000
                    + (minute as u64) * 600_000_000
                    + (second as u64) * 10_000_000
                    + (microsecond as u64) * 10;

                // Use the scale from metadata, defaulting to 7 (max precision)
                let scale = meta.scale;

                // Extract timezone offset
                let offset_minutes = match py_obj.call_method0("utcoffset") {
                    Ok(offset_delta) if !offset_delta.is_none() => {
                        // Get offset in seconds and convert to minutes
                        let offset_seconds = offset_delta
                            .call_method0("total_seconds")
                            .and_then(|v| v.extract::<f64>())
                            .map_err(|e| {
                                Error::UsageError(format!(
                                    "Failed to get timezone offset seconds: {}",
                                    e
                                ))
                            })?;
                        (offset_seconds / 60.0).round() as i16
                    }
                    _ => {
                        // No timezone info, default to UTC (0 offset)
                        0
                    }
                };

                // Validate offset range: -840 to +840 minutes (-14:00 to +14:00)
                if !(-840..=840).contains(&offset_minutes) {
                    return Err(Error::UsageError(format!(
                        "Timezone offset {} minutes out of valid range for DATETIMEOFFSET (-840 to +840)",
                        offset_minutes
                    )));
                }

                return Ok(ColumnValues::DateTimeOffset(
                    mssql_tds::datatypes::column_values::SqlDateTimeOffset {
                        datetime2: mssql_tds::datatypes::column_values::SqlDateTime2 {
                            days: days_dto,
                            time: mssql_tds::datatypes::column_values::SqlTime {
                                time_nanoseconds,
                                scale,
                            },
                        },
                        offset: offset_minutes,
                    },
                ));
            } else if meta.sql_type == SqlDbType::DateTime2 {
                // Convert to DATETIME2 format
                // DATETIME2 uses days since 0001-01-01 (0-based) instead of days since 1900-01-01
                // Calculate days from year 1 using Python's toordinal
                let current_ordinal = current_date
                    .call_method0("toordinal")
                    .and_then(|v| v.extract::<u32>())
                    .map_err(|e| {
                        Error::UsageError(format!(
                            "Failed to get current ordinal for DATETIME2: {}",
                            e
                        ))
                    })?;

                // Python's toordinal() returns 1 for 0001-01-01, so subtract 1 to get 0-based days
                let days_dt2 = current_ordinal.checked_sub(1).ok_or_else(|| {
                    Error::UsageError("Date ordinal is 0, expected >= 1 for DATETIME2".to_string())
                })?;

                // Convert to 100-nanosecond units (DATETIME2/TIME uses 100ns precision)
                let time_nanoseconds = (hour as u64) * 36_000_000_000
                    + (minute as u64) * 600_000_000
                    + (second as u64) * 10_000_000
                    + (microsecond as u64) * 10;

                // Use the scale from metadata, defaulting to 7 (max precision)
                let scale = meta.scale;

                return Ok(ColumnValues::DateTime2(
                    mssql_tds::datatypes::column_values::SqlDateTime2 {
                        days: days_dt2,
                        time: mssql_tds::datatypes::column_values::SqlTime {
                            time_nanoseconds,
                            scale,
                        },
                    },
                ));
            } else if meta.sql_type == SqlDbType::SmallDateTime {
                // Validate SMALLDATETIME range: 1900-01-01 00:00:00 to 2079-06-06 23:59:59
                if !(0..=65535).contains(&days) {
                    return Err(Error::UsageError(format!(
                        "DateTime value {}-{:02}-{:02} out of range for SMALLDATETIME column '{}' (valid range: 1900-01-01 to 2079-06-06)",
                        year, month, day, meta.column_name
                    )));
                }

                // Calculate time in minutes since midnight with proper rounding
                // SMALLDATETIME uses minute precision - round seconds >= 30 up to next minute
                // This matches SQL Server's client-side behavior: add 30 seconds before converting
                let mut rounded_minute = minute;
                let mut rounded_hour = hour;
                let mut rounded_days = days;

                if second >= 30 {
                    rounded_minute += 1;
                    if rounded_minute >= 60 {
                        rounded_minute = 0;
                        rounded_hour += 1;
                        if rounded_hour >= 24 {
                            rounded_hour = 0;
                            rounded_days += 1;
                        }
                    }
                }

                // Validate again after rounding (could overflow into next day beyond max date)
                if !(0..=65535).contains(&rounded_days) {
                    return Err(Error::UsageError(format!(
                        "DateTime value {}-{:02}-{:02} {hour:02}:{minute:02}:{second:02} out of range for SMALLDATETIME column '{}' after rounding (valid range: 1900-01-01 to 2079-06-06)",
                        year, month, day, meta.column_name
                    )));
                }

                let time_minutes = (rounded_hour as u16) * 60 + (rounded_minute as u16);

                return Ok(ColumnValues::SmallDateTime(
                    mssql_tds::datatypes::column_values::SqlSmallDateTime {
                        days: rounded_days as u16,
                        time: time_minutes,
                    },
                ));
            }
        }

        // Default to DATETIME format
        // Calculate time in 1/300th seconds (SQL Server DATETIME precision ~3.33ms)
        // Total milliseconds = hour*3600000 + minute*60000 + second*1000 + microsecond/1000
        let total_ms = (hour as u64) * 3_600_000
            + (minute as u64) * 60_000
            + (second as u64) * 1_000
            + (microsecond as u64) / 1_000;

        // Convert to 1/300th seconds
        // SQL Server rounds to nearest multiple of 3.33ms
        let time_ticks = ((total_ms * 300) / 1000) as u32;

        return Ok(ColumnValues::DateTime(
            mssql_tds::datatypes::column_values::SqlDateTime {
                days,
                time: time_ticks,
            },
        ));
    }

    if py_obj.is_instance_of::<PyDate>() {
        // Convert Python date object to SqlDate
        // Python's toordinal() is 1-based (date(1,1,1).toordinal() == 1)
        // SQL Server DATE needs 0-based days since 0001-01-01, so subtract 1
        match py_obj.call_method0("toordinal") {
            Ok(ordinal_obj) => {
                if let Ok(ordinal) = ordinal_obj.extract::<u32>() {
                    // Convert from 1-based ordinal to 0-based days
                    let days = ordinal.checked_sub(1).ok_or_else(|| {
                        Error::UsageError("Date ordinal is 0, expected >= 1".to_string())
                    })?;
                    return Ok(ColumnValues::Date(
                        mssql_tds::datatypes::column_values::SqlDate::create(days)?,
                    ));
                }
            }
            Err(e) => {
                return Err(Error::UsageError(format!(
                    "Failed to get ordinal from date object: {}",
                    e
                )));
            }
        }
    }

    if py_obj.is_instance_of::<PyTime>() {
        // Convert Python time object to SqlTime
        // Extract hour, minute, second, microsecond from Python time
        let hour = py_obj
            .getattr("hour")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get hour from time: {}", e)))?;

        let minute = py_obj
            .getattr("minute")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get minute from time: {}", e)))?;

        let second = py_obj
            .getattr("second")
            .and_then(|v| v.extract::<u8>())
            .map_err(|e| Error::UsageError(format!("Failed to get second from time: {}", e)))?;

        let microsecond = py_obj
            .getattr("microsecond")
            .and_then(|v| v.extract::<u32>())
            .map_err(|e| {
                Error::UsageError(format!("Failed to get microsecond from time: {}", e))
            })?;

        // Convert to 100-nanosecond units (SQL Server TIME uses 100ns precision)
        // time_100ns = hour * 3600 * 10^7 + minute * 60 * 10^7 + second * 10^7 + microsecond * 10
        let time_nanoseconds = (hour as u64) * 36_000_000_000
            + (minute as u64) * 600_000_000
            + (second as u64) * 10_000_000
            + (microsecond as u64) * 10;

        // Use the scale from target metadata if available, otherwise default to 7 (max precision)
        let scale = target_metadata.map(|meta| meta.scale).unwrap_or(7);

        return Ok(ColumnValues::Time(
            mssql_tds::datatypes::column_values::SqlTime {
                time_nanoseconds,
                scale,
            },
        ));
    }

    // Check for decimal.Decimal type
    // We need to check if the object is an instance of decimal.Decimal
    let py = py_obj.py();
    if let Ok(decimal_module) = PyModule::import(py, "decimal") {
        if let Ok(decimal_class) = decimal_module.getattr("Decimal") {
            if let Ok(is_instance) = py_obj.is_instance(&decimal_class) {
                if is_instance {
                    // Extract Decimal as string and parse it
                    if let Ok(decimal_str) = py_obj.call_method0("__str__") {
                        if let Ok(s) = decimal_str.extract::<String>() {
                            // Use a reasonable precision and scale for default conversion
                            // This will be validated/adjusted during bulk copy if metadata is available
                            match DecimalParts::from_string(&s, 38, 10) {
                                Ok(decimal_parts) => {
                                    return Ok(ColumnValues::Decimal(decimal_parts))
                                }
                                Err(e) => {
                                    return Err(Error::UsageError(format!(
                                        "Failed to convert Python Decimal '{}': {}",
                                        s, e
                                    )));
                                }
                            }
                        }
                    }
                    return Err(Error::UsageError(
                        "Failed to extract Decimal value as string".to_string(),
                    ));
                }
            }
        }
    }

    // Unsupported type
    let type_name = py_obj
        .get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());

    Err(Error::UsageError(format!(
        "Unsupported Python type for bulk copy: {}",
        type_name
    )))
}

/// Validate that the converted ColumnValues type matches the target SQL type.
///
/// This function ensures type safety by verifying that the result of py_to_column_value()
/// is compatible with the target column metadata provided. This prevents silent type
/// mismatches that could occur if try_type_coercion() incorrectly returns None.
///
/// # Arguments
///
/// * `result` - The ColumnValues produced by conversion
/// * `target_metadata` - The target column metadata to validate against
///
/// # Returns
///
/// `TdsResult<()>` - Ok if types are compatible, Err with descriptive message otherwise
fn validate_type_compatibility(
    result: &ColumnValues,
    target_metadata: &BulkCopyColumnMetadata,
) -> TdsResult<()> {
    // Check if result type matches target type
    let result_matches_target = match (&result, target_metadata.sql_type) {
        // Integer types
        (ColumnValues::TinyInt(_), SqlDbType::TinyInt) => true,
        (ColumnValues::SmallInt(_), SqlDbType::SmallInt) => true,
        (ColumnValues::Int(_), SqlDbType::Int) => true,
        (ColumnValues::BigInt(_), SqlDbType::BigInt) => true,

        // Float/Decimal
        (ColumnValues::Float(_), SqlDbType::Float) => true,
        (ColumnValues::Real(_), SqlDbType::Real) => true,
        (ColumnValues::Numeric(_), SqlDbType::Numeric | SqlDbType::Decimal) => true,

        // String
        (
            ColumnValues::String(_),
            SqlDbType::VarChar | SqlDbType::NVarChar | SqlDbType::Char | SqlDbType::NChar,
        ) => true,

        // Binary
        (ColumnValues::Bytes(_), SqlDbType::VarBinary | SqlDbType::Binary) => true,

        // Boolean
        (ColumnValues::Bit(_), SqlDbType::Bit) => true,

        // Date/Time
        (ColumnValues::Date(_), SqlDbType::Date) => true,
        (ColumnValues::DateTime2(_), SqlDbType::DateTime2) => true,
        (ColumnValues::DateTimeOffset(_), SqlDbType::DateTimeOffset) => true,
        (ColumnValues::DateTime(_), SqlDbType::DateTime) => true,
        (ColumnValues::SmallDateTime(_), SqlDbType::SmallDateTime) => true,
        (ColumnValues::Time(_), SqlDbType::Time) => true,

        // Money
        (ColumnValues::Money(_), SqlDbType::Money) => true,
        (ColumnValues::SmallMoney(_), SqlDbType::SmallMoney) => true,

        // NULL is always compatible
        (ColumnValues::Null, _) => true,

        // No match - type mismatch
        _ => false,
    };

    if !result_matches_target {
        return Err(Error::UsageError(format!(
            "Type mismatch for column '{}': converted to {:?} but target SQL type is {:?}. \
             This indicates try_type_coercion() returned None for an incompatible type pair.",
            target_metadata.column_name, result, target_metadata.sql_type
        )));
    }

    Ok(())
}
