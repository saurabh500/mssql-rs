// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TDS value serialization utilities for RPC parameters and bulk copy operations.
//!
//! This module provides shared value encoding logic that can be used by both
//! RPC parameter serialization (SqlType) and bulk copy ROW token serialization.
//! It separates the concerns of type metadata encoding from value encoding.

use crate::core::TdsResult;
use crate::datatypes::column_values::{ColumnValues, SqlXml};
use crate::datatypes::sql_json::SqlJson;
use crate::error::Error;
use crate::io::packet_writer::{PacketWriter, TdsPacketWriter, TdsPacketWriterUnchecked};

// NULL markers for different type classes
const NULL_LENGTH: u8 = 0x00;
const VARNULL: u16 = 0xFFFF;

// PLP (Partial Length Prefix) constants - made public for reuse in bulk_load.rs
pub const PLP_NULL: u64 = 0xFFFFFFFFFFFFFFFF;
pub const PLP_UNKNOWN_LEN: u64 = 0xFFFFFFFFFFFFFFFE; // -2 in signed i64, used when total length is unknown
pub const PLP_TERMINATOR: u32 = 0x00000000;

/// Context for value serialization, containing type metadata needed for encoding.
///
/// This struct encapsulates the TDS type information required to properly encode
/// a value without duplicating the type metadata itself.
#[derive(Debug, Clone)]
pub struct TdsTypeContext {
    /// TDS type byte (e.g., 0x26 for INTN, 0xE7 for NVARCHAR)
    pub tds_type: u8,

    /// Maximum type size (for nullable types: 1/2/4/8 for INTN, 4/8 for FLTN, etc.)
    pub max_size: u8,

    /// Whether this is a PLP (Partial Length Prefix) type (MAX types)
    pub is_plp: bool,

    /// Whether this is a fixed-length type (e.g., BINARY(n) vs VARBINARY(n))
    /// Fixed-length types write exactly max_size bytes with no length prefix in ROW tokens
    pub is_fixed_length: bool,

    /// For Decimal/Numeric: precision
    pub precision: Option<u8>,

    /// For Decimal/Numeric/Time/DateTime2/DateTimeOffset: scale
    pub scale: Option<u8>,

    /// Whether the type is nullable (affects NULL encoding)
    pub is_nullable: bool,
}

impl TdsTypeContext {
    /// Check if this is a fixed-length type (no length prefix needed in ROW data).
    pub fn is_fixed_type(&self) -> bool {
        matches!(
            self.tds_type,
            // Fixed types: INT1-INT8, BIT, FLT4, FLT8, DATETIME, MONEY, etc.
            0x30..=0x3F | 0x7F
        )
    }
}

/// Main value serializer - writes ONLY the value bytes (no type metadata).
pub struct TdsValueSerializer;

impl TdsValueSerializer {
    /// Serialize a value using the provided type context.
    ///
    /// This writes ONLY the value portion:
    /// - For nullable types (INTN, FLTN): length byte + value bytes
    /// - For fixed types (INT4, FLT8): value bytes only
    /// - For variable-length types: length prefix + value bytes
    /// - For PLP types: total_length + chunks + terminator
    ///
    /// Type metadata (TDS type byte, max_size, precision, scale, collation)
    /// must be written separately by the caller.
    #[inline]
    pub async fn serialize_value<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &ColumnValues,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        match value {
            ColumnValues::Null => Self::serialize_null(writer, ctx).await,
            ColumnValues::Bit(v) => Self::serialize_bit(writer, *v, ctx).await,
            ColumnValues::TinyInt(v) => Self::serialize_tinyint(writer, *v, ctx).await,
            ColumnValues::SmallInt(v) => Self::serialize_smallint(writer, *v, ctx).await,
            ColumnValues::Int(v) => Self::serialize_int(writer, *v, ctx).await,
            ColumnValues::BigInt(v) => Self::serialize_bigint(writer, *v, ctx).await,
            ColumnValues::Real(v) => Self::serialize_real(writer, *v, ctx).await,
            ColumnValues::Float(v) => Self::serialize_float(writer, *v, ctx).await,
            ColumnValues::Decimal(v) | ColumnValues::Numeric(v) => {
                Self::serialize_decimal(writer, v, ctx).await
            }
            ColumnValues::SmallMoney(v) => Self::serialize_smallmoney(writer, v, ctx).await,
            ColumnValues::Money(v) => Self::serialize_money(writer, v, ctx).await,
            ColumnValues::Date(v) => Self::serialize_date(writer, v, ctx).await,
            ColumnValues::Time(v) => Self::serialize_time(writer, v, ctx).await,
            ColumnValues::DateTime(v) => Self::serialize_datetime(writer, v, ctx).await,
            ColumnValues::DateTime2(v) => Self::serialize_datetime2(writer, v, ctx).await,
            ColumnValues::DateTimeOffset(v) => Self::serialize_datetimeoffset(writer, v, ctx).await,
            ColumnValues::SmallDateTime(v) => Self::serialize_smalldatetime(writer, v, ctx).await,
            ColumnValues::Bytes(v) => Self::serialize_bytes(writer, v, ctx).await,
            ColumnValues::Json(v) => Self::serialize_json(writer, v, ctx).await,
            _ => Err(Error::UnimplementedFeature {
                feature: format!("Value serialization not implemented for type: {:?}", value),
                context: "serialization".to_string(),
            }),
        }
    }

    /// Serialize a NULL value using the appropriate NULL marker for the type.
    #[inline(always)]
    pub async fn serialize_null<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Check type class and write appropriate NULL marker
        match ctx.tds_type {
            // Nullable types (INTN, FLTN, BITN, MONEYN, DATETIMEN, DecimalN, NumericN, Guid, DateN, TimeN, DateTime2N, DateTimeOffsetN) use length = 0x00
            0x26 | 0x6D | 0x68 | 0x6E | 0x6F | 0x6A | 0x6C | 0x24 | 0x28 | 0x29 | 0x2A | 0x2B => {
                writer.write_byte_async(NULL_LENGTH).await?;
            }
            // Fixed BIT type (0x32) cannot be NULL - must use BitN (0x68) for nullable
            0x32 => {
                return Err(Error::UsageError(
                    "Cannot serialize NULL for fixed BIT type 0x32. Use BitN (0x68) for nullable BIT columns.".to_string()
                ));
            }
            _ => {
                // Other types depend on length classification
                if ctx.is_plp {
                    // PLP NULL: 8 bytes of 0xFF
                    writer.write_u64_async(PLP_NULL).await?;
                } else if ctx.is_fixed_type() {
                    // Fixed-length types cannot be NULL - must use nullable variant (INTN, FLTN, etc.)
                    return Err(Error::UsageError(format!(
                        "Cannot serialize NULL for fixed-length type 0x{:02X}.",
                        ctx.tds_type
                    )));
                } else {
                    // Variable-length NULL: 0xFFFF
                    writer.write_u16_async(VARNULL).await?;
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_bit<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: bool,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        let byte_value = if value { 1u8 } else { 0u8 };

        if !ctx.is_fixed_type() {
            // Nullable BIT (BITN with length 1): length byte + value (2 bytes total)
            match writer.has_space(2) {
                false => {
                    writer.write_byte_async(1).await?; // Length for BITN (1 byte)
                    writer.write_byte_async(byte_value).await?;
                }
                true => {
                    writer.write_byte_unchecked(1); // Length for BITN (1 byte)
                    writer.write_byte_unchecked(byte_value);
                }
            }
        } else {
            // Fixed type (BIT, 0x32) - just write value (1 byte)
            match writer.has_space(1) {
                false => {
                    writer.write_byte_async(byte_value).await?;
                }
                true => {
                    writer.write_byte_unchecked(byte_value);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_tinyint<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: u8,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Nullable TINYINT (INTN with length 1): length byte + value (2 bytes total)
            match writer.has_space(2) {
                false => {
                    writer.write_byte_async(1).await?; // Length for INTN (1 byte)
                    writer.write_byte_async(value).await?;
                }
                true => {
                    writer.write_byte_unchecked(1); // Length for INTN (1 byte)
                    writer.write_byte_unchecked(value);
                }
            }
        } else {
            // Fixed type (INT1, 0x30) - just write value (1 byte)
            match writer.has_space(1) {
                false => {
                    writer.write_byte_async(value).await?;
                }
                true => {
                    writer.write_byte_unchecked(value);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_smallint<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: i16,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Nullable SMALLINT (INTN with length 2): length byte + value (3 bytes total)
            match writer.has_space(3) {
                false => {
                    writer.write_byte_async(2).await?; // Length for INTN (2 bytes)
                    writer.write_i16_async(value).await?;
                }
                true => {
                    writer.write_byte_unchecked(2); // Length for INTN (2 bytes)
                    writer.write_i16_unchecked(value);
                }
            }
        } else {
            // Fixed type (INT2, 0x34) - just write value (2 bytes)
            match writer.has_space(2) {
                false => {
                    writer.write_i16_async(value).await?;
                }
                true => {
                    writer.write_i16_unchecked(value);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_int<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: i32,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Ensure space for length byte + value (5 bytes total)
            match writer.has_space(5) {
                false => {
                    writer.write_byte_async(4).await?; // Length for INTN
                    writer.write_i32_async(value).await?;
                }
                true => {
                    writer.write_byte_unchecked(4); // Length for INTN
                    writer.write_i32_unchecked(value);
                }
            }
        } else {
            // Fixed type - just write value (4 bytes)
            match writer.has_space(4) {
                false => {
                    writer.write_i32_async(value).await?;
                }
                true => {
                    writer.write_i32_unchecked(value);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_bigint<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: i64,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Nullable BIGINT (INTN with length 8): length byte + value (9 bytes total)
            match writer.has_space(9) {
                false => {
                    writer.write_byte_async(8).await?; // Length for INTN (8 bytes)
                    writer.write_i64_async(value).await?;
                }
                true => {
                    writer.write_byte_unchecked(8); // Length for INTN (8 bytes)
                    writer.write_i64_unchecked(value);
                }
            }
        } else {
            // Fixed type (INT8, 0x7F) - just write value (8 bytes)
            match writer.has_space(8) {
                false => {
                    writer.write_i64_async(value).await?;
                }
                true => {
                    writer.write_i64_unchecked(value);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_real<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: f32,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // REAL is 4-byte IEEE 754 float
        // TDS FloatN format: length byte + IEEE 754 bytes (little-endian)
        if !ctx.is_fixed_type() {
            // Nullable REAL (FLOATN with length 4): length byte + value (5 bytes total)
            match writer.has_space(5) {
                false => {
                    writer.write_byte_async(4).await?; // Length for FLOATN (4 bytes)
                    // Write f32 as i32 bytes (same bit pattern)
                    writer.write_i32_async(value.to_bits() as i32).await?;
                }
                true => {
                    writer.write_byte_unchecked(4); // Length for FLOATN (4 bytes)
                    // Write f32 as i32 bytes (same bit pattern)
                    writer.write_i32_unchecked(value.to_bits() as i32);
                }
            }
        } else {
            // Fixed type (FLT4, 0x3B) - just write value (4 bytes)
            match writer.has_space(4) {
                false => {
                    // Write f32 as i32 bytes (same bit pattern)
                    writer.write_i32_async(value.to_bits() as i32).await?;
                }
                true => {
                    // Write f32 as i32 bytes (same bit pattern)
                    writer.write_i32_unchecked(value.to_bits() as i32);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_float<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: f64,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // FLOAT is 8-byte IEEE 754 double
        // TDS FloatN format: length byte + IEEE 754 bytes (little-endian)
        if !ctx.is_fixed_type() {
            // Nullable FLOAT (FLOATN with length 8): length byte + value (9 bytes total)
            match writer.has_space(9) {
                false => {
                    writer.write_byte_async(8).await?; // Length for FLOATN (8 bytes)
                    writer.write_f64_unchecked(value);
                }
                true => {
                    writer.write_byte_unchecked(8); // Length for FLOATN (8 bytes)
                    writer.write_f64_unchecked(value);
                }
            }
        } else {
            // Fixed type (FLT8, 0x3E) - just write value (8 bytes)
            match writer.has_space(8) {
                false => {
                    // Write f64 as i64 bytes (same bit pattern)
                    writer.write_i64_async(value.to_bits() as i64).await?;
                }
                true => {
                    writer.write_f64_unchecked(value);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_decimal<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::decoder::DecimalParts,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Decimal/Numeric format in TDS:
        // - Length byte (number of bytes in the value, excluding the length byte)
        // - Sign byte (0 = negative, 1 = positive)
        // - Little-endian bytes representing the value
        //
        // Length is determined by precision (from metadata):
        // Precision 1-9:   5 bytes (1 sign + 4 value bytes)
        // Precision 10-19: 9 bytes (1 sign + 8 value bytes)
        // Precision 20-28: 13 bytes (1 sign + 12 value bytes)
        // Precision 29-38: 17 bytes (1 sign + 16 value bytes)

        let precision = ctx.precision.unwrap_or(38);

        // Determine required byte length based on precision
        let value_bytes = match precision {
            1..=9 => 4,
            10..=19 => 8,
            20..=28 => 12,
            29..=38 => 16,
            _ => {
                return Err(Error::ProtocolError(format!(
                    "Invalid precision {} for DECIMAL/NUMERIC type",
                    precision
                )));
            }
        };

        let total_length = 1 + value_bytes; // sign byte + value bytes

        // Write length byte
        writer.write_byte_async(total_length as u8).await?;

        // Write sign byte
        writer
            .write_byte_async(if value.is_positive { 1 } else { 0 })
            .await?;

        // Write value bytes in little-endian order
        // Pad with zeros if value has fewer chunks than needed
        let chunks_needed = value_bytes / 4;
        for i in 0..chunks_needed {
            let chunk = value.int_parts.get(i).copied().unwrap_or(0);
            writer.write_i32_async(chunk).await?;
        }

        Ok(())
    }

    #[inline(always)]
    async fn serialize_smallmoney<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlSmallMoney,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // SMALLMONEY format in TDS:
        // - For MoneyN (nullable, 0x6E): length byte (4) + 4-byte integer
        // - For Money4 (fixed, 0x7A): 4-byte integer only
        // The value is stored as a 4-byte signed integer scaled by 10,000
        // (e.g., $123.4567 is stored as 1234567)

        if !ctx.is_fixed_type() {
            // MoneyN with length 4: length byte + value (5 bytes total)
            match writer.has_space(5) {
                false => {
                    writer.write_byte_async(4).await?; // Length for MoneyN (4 bytes)
                    writer.write_i32_async(value.int_val).await?;
                }
                true => {
                    writer.write_byte_unchecked(4); // Length for MoneyN (4 bytes)
                    writer.write_i32_unchecked(value.int_val);
                }
            }
        } else {
            // Fixed type (Money4, 0x7A) - just write value (4 bytes)
            match writer.has_space(4) {
                false => {
                    writer.write_i32_async(value.int_val).await?;
                }
                true => {
                    writer.write_i32_unchecked(value.int_val);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_money<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlMoney,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // MONEY format in TDS:
        // - For MoneyN (nullable, 0x6E): length byte (8) + 8 bytes (two 4-byte integers)
        // - For Money (fixed, 0x3C): 8 bytes (two 4-byte integers) only
        // The value is stored as two 4-byte signed integers in mixed endian format:
        //   - First 4 bytes: MSB (most significant 4 bytes)
        //   - Second 4 bytes: LSB (least significant 4 bytes)
        // The combined value is scaled by 10,000 (e.g., $123.4567 is stored as 1234567)

        if !ctx.is_fixed_type() {
            // MoneyN with length 8: length byte + value (9 bytes total)
            match writer.has_space(9) {
                false => {
                    writer.write_byte_async(8).await?; // Length for MoneyN (8 bytes)
                    writer.write_i32_async(value.msb_part).await?; // MSB first
                    writer.write_i32_async(value.lsb_part).await?; // LSB second
                }
                true => {
                    writer.write_byte_unchecked(8); // Length for MoneyN (8 bytes)
                    writer.write_i32_unchecked(value.msb_part); // MSB first
                    writer.write_i32_unchecked(value.lsb_part); // LSB second
                }
            }
        } else {
            // Fixed type (Money, 0x3C) - just write value (8 bytes)
            match writer.has_space(8) {
                false => {
                    writer.write_i32_async(value.msb_part).await?; // MSB first
                    writer.write_i32_async(value.lsb_part).await?; // LSB second
                }
                true => {
                    writer.write_i32_unchecked(value.msb_part); // MSB first
                    writer.write_i32_unchecked(value.lsb_part); // LSB second
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_date<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlDate,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // DATE format in TDS:
        // - For DateN (nullable, 0x28): length byte (3) + 3-byte unsigned integer
        // - For Date (fixed, 0x2A): 3-byte unsigned integer only
        // The value is stored as a 3-byte unsigned integer representing days since 0001-01-01
        // Valid range: 1 (0001-01-01) to 3,652,059 (9999-12-31)
        // The 3-byte unsigned integer can hold values up to 0xFFFFFF (16,777,215),
        // but SQL Server DATE type has a more restricted range.

        if !ctx.is_fixed_type() {
            // DateN with length 3: length byte + value (4 bytes total)
            let days = value.get_days();
            match writer.has_space(4) {
                false => {
                    writer.write_byte_async(3).await?; // Length for DateN (3 bytes)
                    // Write 3 bytes in little-endian format (u32 as 3 bytes)
                    writer.write_byte_async((days & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 8) & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 16) & 0xFF) as u8).await?;
                }
                true => {
                    writer.write_byte_unchecked(3); // Length for DateN (3 bytes)
                    // Write 3 bytes in little-endian format (u32 as 3 bytes)
                    writer.write_byte_unchecked((days & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 8) & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 16) & 0xFF) as u8);
                }
            }
        } else {
            // Fixed type (Date, 0x2A) - just write value (3 bytes)
            let days = value.get_days();

            match writer.has_space(3) {
                false => {
                    writer.write_byte_async((days & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 8) & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 16) & 0xFF) as u8).await?;
                }
                true => {
                    writer.write_byte_unchecked((days & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 8) & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 16) & 0xFF) as u8);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_time<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlTime,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // TIME format in TDS:
        // - For TimeN (nullable, 0x29): length byte + time_nanoseconds (3, 4, or 5 bytes)
        // - For Time (fixed, 0x2A): time_nanoseconds only (3, 4, or 5 bytes)
        // The value is stored as little-endian unsigned integer representing 100-nanosecond units
        // Length depends on scale:
        //   - Scale 0-2: 3 bytes
        //   - Scale 3-4: 4 bytes
        //   - Scale 5-7: 5 bytes

        // Determine the byte length based on scale
        let time_length = match value.scale {
            0..=2 => 3u8,
            3 | 4 => 4u8,
            5..=7 => 5u8,
            _ => {
                return Err(Error::UsageError(format!(
                    "Invalid scale for Time type: {}. Valid range: 0-7",
                    value.scale
                )));
            }
        };

        // Scale the time value based on the scale
        // The time_nanoseconds is always in 100-nanosecond units internally
        // But SQL Server expects the value to be in units appropriate for the scale:
        // Scale 0: seconds (divide by 10^7)
        // Scale 1: tenths of seconds (divide by 10^6)
        // Scale 2: hundredths of seconds (divide by 10^5)
        // Scale 3: milliseconds (divide by 10^4)
        // Scale 4: ten-thousandths (divide by 10^3)
        // Scale 5: hundred-thousandths (divide by 10^2)
        // Scale 6: microseconds (divide by 10^1)
        // Scale 7: 100-nanoseconds (divide by 10^0 = no scaling)
        let time_value = match value.scale {
            0 => value.time_nanoseconds / 10_000_000, // Seconds
            1 => value.time_nanoseconds / 1_000_000,  // Tenths
            2 => value.time_nanoseconds / 100_000,    // Hundredths
            3 => value.time_nanoseconds / 10_000,     // Milliseconds
            4 => value.time_nanoseconds / 1_000,      // Ten-thousandths
            5 => value.time_nanoseconds / 100,        // Hundred-thousandths
            6 => value.time_nanoseconds / 10,         // Microseconds
            7 => value.time_nanoseconds,              // 100-nanoseconds (no scaling)
            _ => value.time_nanoseconds,
        };

        if !ctx.is_fixed_type() {
            // TimeN with length prefix
            let total_size = (1 + time_length) as usize;
            match writer.has_space(total_size) {
                false => {
                    writer.write_byte_async(time_length).await?; // Length byte
                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        let byte_val = ((time_value >> (i * 8)) & 0xFF) as u8;
                        writer.write_byte_async(byte_val).await?;
                    }
                }
                true => {
                    writer.write_byte_unchecked(time_length); // Length byte
                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        let byte_val = ((time_value >> (i * 8)) & 0xFF) as u8;
                        writer.write_byte_unchecked(byte_val);
                    }
                }
            }
        } else {
            // Fixed type (Time, 0x2A) - just write value (3, 4, or 5 bytes)
            match writer.has_space(time_length as usize) {
                false => {
                    for i in 0..time_length {
                        writer
                            .write_byte_async(((time_value >> (i * 8)) & 0xFF) as u8)
                            .await?;
                    }
                }
                true => {
                    for i in 0..time_length {
                        writer.write_byte_unchecked(((time_value >> (i * 8)) & 0xFF) as u8);
                    }
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_datetime<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlDateTime,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // DATETIME format in TDS:
        // - For DateTimeN (nullable, 0x6F): length byte (8) + 4-byte days + 4-byte time
        // - For DateTime (fixed, 0x3D): 4-byte days + 4-byte time (8 bytes total)
        //
        // Days: Signed 32-bit integer representing days since January 1, 1900
        //       (negative for dates before 1900, back to January 1, 1753)
        // Time: Unsigned 32-bit integer representing 1/300th of a second since midnight
        //       (300 ticks per second, range 0 to 25,919,999 for 23:59:59.997)

        if !ctx.is_fixed_type() {
            // DateTimeN with length 8: length byte + value (9 bytes total)
            match writer.has_space(9) {
                false => {
                    writer.write_byte_async(8).await?; // Length for DateTimeN (8 bytes)
                    writer.write_i32_async(value.days).await?;
                    writer.write_u32_async(value.time).await?;
                }
                true => {
                    writer.write_byte_unchecked(8); // Length for DateTimeN (8 bytes)
                    writer.write_i32_unchecked(value.days);
                    // Note: No unchecked version for u32, use the bytes directly
                    let time_bytes = value.time.to_le_bytes();
                    writer.write_byte_unchecked(time_bytes[0]);
                    writer.write_byte_unchecked(time_bytes[1]);
                    writer.write_byte_unchecked(time_bytes[2]);
                    writer.write_byte_unchecked(time_bytes[3]);
                }
            }
        } else {
            // Fixed type (DateTime, 0x3D) - just write value (8 bytes)
            match writer.has_space(8) {
                false => {
                    writer.write_i32_async(value.days).await?;
                    writer.write_u32_async(value.time).await?;
                }
                true => {
                    writer.write_i32_unchecked(value.days);
                    // Note: No unchecked version for u32, use the bytes directly
                    let time_bytes = value.time.to_le_bytes();
                    writer.write_byte_unchecked(time_bytes[0]);
                    writer.write_byte_unchecked(time_bytes[1]);
                    writer.write_byte_unchecked(time_bytes[2]);
                    writer.write_byte_unchecked(time_bytes[3]);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_smalldatetime<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlSmallDateTime,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // SMALLDATETIME format in TDS:
        // - For DateTimeN (nullable, 0x6F): length byte (4) + 2-byte days + 2-byte time
        // - SMALLDATETIME always uses DateTimeN (0x6F) with length 4
        //
        // Days: Unsigned 16-bit integer representing days since January 1, 1900
        //       (range 0 to 65535 for dates from 1900-01-01 to 2079-06-06)
        // Time: Unsigned 16-bit integer representing minutes since midnight
        //       (range 0 to 1439 for times from 00:00 to 23:59)

        if !ctx.is_fixed_type() {
            // DateTimeN with length 4: length byte + value (5 bytes total)
            match writer.has_space(5) {
                false => {
                    writer.write_byte_async(4).await?; // Length for SmallDateTime (4 bytes)
                    writer.write_u16_async(value.days).await?;
                    writer.write_u16_async(value.time).await?;
                }
                true => {
                    writer.write_byte_unchecked(4); // Length for SmallDateTime (4 bytes)
                    writer.write_u16_unchecked(value.days);
                    writer.write_u16_unchecked(value.time);
                }
            }
        } else {
            // Fixed type - just write value (4 bytes)
            match writer.has_space(4) {
                false => {
                    writer.write_u16_async(value.days).await?;
                    writer.write_u16_async(value.time).await?;
                }
                true => {
                    writer.write_u16_unchecked(value.days);
                    writer.write_u16_unchecked(value.time);
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_datetime2<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlDateTime2,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // DATETIME2 format in TDS:
        // - For DateTime2N (nullable, 0x2A): length byte + time_nanoseconds + 3-byte days
        // - Time portion: 3, 4, or 5 bytes (same encoding as TIME type)
        // - Date portion: 3 bytes (unsigned, days since 0001-01-01)
        // Total length depends on scale:
        //   - Scale 0-2: 6 bytes (3 for time + 3 for date)
        //   - Scale 3-4: 7 bytes (4 for time + 3 for date)
        //   - Scale 5-7: 8 bytes (5 for time + 3 for date)

        // Determine the time length based on scale
        let time_length = match value.time.scale {
            0..=2 => 3u8,
            3 | 4 => 4u8,
            5..=7 => 5u8,
            _ => {
                return Err(Error::UsageError(format!(
                    "Invalid scale for DateTime2 type: {}. Valid range: 0-7",
                    value.time.scale
                )));
            }
        };

        let date_length = 3u8;
        let total_value_length = time_length + date_length;

        // Scale the time value based on the scale (same logic as serialize_time)
        let time_value = match value.time.scale {
            0 => value.time.time_nanoseconds / 10_000_000, // Seconds
            1 => value.time.time_nanoseconds / 1_000_000,  // Tenths
            2 => value.time.time_nanoseconds / 100_000,    // Hundredths
            3 => value.time.time_nanoseconds / 10_000,     // Milliseconds
            4 => value.time.time_nanoseconds / 1_000,      // Ten-thousandths
            5 => value.time.time_nanoseconds / 100,        // Hundred-thousandths
            6 => value.time.time_nanoseconds / 10,         // Microseconds
            7 => value.time.time_nanoseconds,              // 100-nanoseconds (no scaling)
            _ => value.time.time_nanoseconds,
        };

        if !ctx.is_fixed_type() {
            // DateTime2N with length prefix
            let total_size = (1 + total_value_length) as usize;
            match writer.has_space(total_size) {
                false => {
                    writer.write_byte_async(total_value_length).await?; // Length byte

                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        let byte_val = ((time_value >> (i * 8)) & 0xFF) as u8;
                        writer.write_byte_async(byte_val).await?;
                    }

                    // Write date as 3-byte little-endian unsigned integer
                    let date_bytes = value.days.to_le_bytes();
                    writer.write_byte_async(date_bytes[0]).await?;
                    writer.write_byte_async(date_bytes[1]).await?;
                    writer.write_byte_async(date_bytes[2]).await?;
                }
                true => {
                    writer.write_byte_unchecked(total_value_length); // Length byte

                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        let byte_val = ((time_value >> (i * 8)) & 0xFF) as u8;
                        writer.write_byte_unchecked(byte_val);
                    }

                    // Write date as 3-byte little-endian unsigned integer
                    let date_bytes = value.days.to_le_bytes();
                    writer.write_byte_unchecked(date_bytes[0]);
                    writer.write_byte_unchecked(date_bytes[1]);
                    writer.write_byte_unchecked(date_bytes[2]);
                }
            }
        } else {
            // Fixed type - just write value (time + 3-byte date)
            match writer.has_space(total_value_length as usize) {
                false => {
                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        writer
                            .write_byte_async(((time_value >> (i * 8)) & 0xFF) as u8)
                            .await?;
                    }

                    // Write date as 3-byte little-endian unsigned integer
                    let date_bytes = value.days.to_le_bytes();
                    writer.write_byte_async(date_bytes[0]).await?;
                    writer.write_byte_async(date_bytes[1]).await?;
                    writer.write_byte_async(date_bytes[2]).await?;
                }
                true => {
                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        writer.write_byte_unchecked(((time_value >> (i * 8)) & 0xFF) as u8);
                    }

                    // Write date as 3-byte little-endian unsigned integer
                    let date_bytes = value.days.to_le_bytes();
                    writer.write_byte_unchecked(date_bytes[0]);
                    writer.write_byte_unchecked(date_bytes[1]);
                    writer.write_byte_unchecked(date_bytes[2]);
                }
            }
        }
        Ok(())
    }

    /// Serialize a DATETIMEOFFSET value to the TDS stream.
    ///
    /// DATETIMEOFFSET wire format:
    /// - 1 byte: length (0 for NULL, or time_length + 3 + 2 for date + offset)
    /// - time_length bytes: time component (3, 4, or 5 bytes based on scale)
    /// - 3 bytes: date component (days since 0001-01-01, little-endian)
    /// - 2 bytes: timezone offset in minutes (little-endian, signed i16)
    ///
    /// Time length by scale:
    /// - Scale 0-2: 3 bytes
    /// - Scale 3-4: 4 bytes
    /// - Scale 5-7: 5 bytes
    async fn serialize_datetimeoffset<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &crate::datatypes::column_values::SqlDateTimeOffset,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // DATETIMEOFFSET format in TDS:
        // - For DateTimeOffsetN (nullable, 0x2B): length byte + time + 3-byte days + 2-byte offset
        // - Time portion: 3, 4, or 5 bytes (same encoding as TIME/DATETIME2 type)
        // - Date portion: 3 bytes (unsigned, days since 0001-01-01)
        // - Offset portion: 2 bytes (signed i16, minutes from UTC)
        // Total length depends on scale:
        //   - Scale 0-2: 8 bytes (3 for time + 3 for date + 2 for offset)
        //   - Scale 3-4: 9 bytes (4 for time + 3 for date + 2 for offset)
        //   - Scale 5-7: 10 bytes (5 for time + 3 for date + 2 for offset)

        // Determine the time length based on scale
        let time_length = match value.datetime2.time.scale {
            0..=2 => 3u8,
            3 | 4 => 4u8,
            5..=7 => 5u8,
            _ => {
                return Err(Error::UsageError(format!(
                    "Invalid scale for DateTimeOffset type: {}. Valid range: 0-7",
                    value.datetime2.time.scale
                )));
            }
        };

        let date_length = 3u8;
        let offset_length = 2u8;
        let total_value_length = time_length + date_length + offset_length;

        // Scale the time value based on the scale (same logic as serialize_time)
        let time_value = match value.datetime2.time.scale {
            0 => value.datetime2.time.time_nanoseconds / 10_000_000, // Seconds
            1 => value.datetime2.time.time_nanoseconds / 1_000_000,  // Tenths
            2 => value.datetime2.time.time_nanoseconds / 100_000,    // Hundredths
            3 => value.datetime2.time.time_nanoseconds / 10_000,     // Milliseconds
            4 => value.datetime2.time.time_nanoseconds / 1_000,      // Ten-thousandths
            5 => value.datetime2.time.time_nanoseconds / 100,        // Hundred-thousandths
            6 => value.datetime2.time.time_nanoseconds / 10,         // Microseconds
            7 => value.datetime2.time.time_nanoseconds,              // 100-nanoseconds (no scaling)
            _ => value.datetime2.time.time_nanoseconds,
        };

        if !ctx.is_fixed_type() {
            // DateTimeOffsetN with length prefix
            let total_size = (1 + total_value_length) as usize;
            match writer.has_space(total_size) {
                false => {
                    writer.write_byte_async(total_value_length).await?; // Length byte

                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        let byte_val = ((time_value >> (i * 8)) & 0xFF) as u8;
                        writer.write_byte_async(byte_val).await?;
                    }

                    // Write date as 3-byte little-endian unsigned integer
                    let date_bytes = value.datetime2.days.to_le_bytes();
                    writer.write_byte_async(date_bytes[0]).await?;
                    writer.write_byte_async(date_bytes[1]).await?;
                    writer.write_byte_async(date_bytes[2]).await?;

                    // Write timezone offset as 2-byte little-endian signed integer
                    let offset_bytes = value.offset.to_le_bytes();
                    writer.write_byte_async(offset_bytes[0]).await?;
                    writer.write_byte_async(offset_bytes[1]).await?;
                }
                true => {
                    writer.write_byte_unchecked(total_value_length); // Length byte

                    // Write time_value in little-endian format (variable bytes)
                    for i in 0..time_length {
                        let byte_val = ((time_value >> (i * 8)) & 0xFF) as u8;
                        writer.write_byte_unchecked(byte_val);
                    }

                    // Write date as 3-byte little-endian unsigned integer
                    let date_bytes = value.datetime2.days.to_le_bytes();
                    writer.write_byte_unchecked(date_bytes[0]);
                    writer.write_byte_unchecked(date_bytes[1]);
                    writer.write_byte_unchecked(date_bytes[2]);

                    // Write timezone offset as 2-byte little-endian signed integer
                    let offset_bytes = value.offset.to_le_bytes();
                    writer.write_byte_unchecked(offset_bytes[0]);
                    writer.write_byte_unchecked(offset_bytes[1]);
                }
            }
        }
        Ok(())
    }

    /// Serialize BINARY/VARBINARY data to the TDS stream.
    ///
    /// BINARY/VARBINARY wire format:
    /// - For variable-length types (BINARY/VARBINARY):
    ///   - If length <= 8000:
    ///     - 2 bytes: actual length (0xFFFF for NULL)
    ///     - n bytes: actual byte data
    ///   - If length > 8000 (MAX types, not common for BINARY):
    ///     - 8 bytes: total length (0xFFFFFFFFFFFFFFFF for NULL)
    ///     - chunks of data with 4-byte length prefixes
    ///     - 4-byte terminator (0x00000000)
    ///
    /// For BINARY(n): If data length < n, pad with zeros to reach fixed length
    /// For VARBINARY(n): Use exact data length, no padding
    ///
    /// TDS types:
    /// - 0xAD: BINARY(n) / VARBINARY(n) - fixed/variable-length binary
    /// - 0xA5: VARBINARY(MAX) - variable-length binary with PLP encoding
    #[inline(always)]
    async fn serialize_bytes<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &[u8],
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        let data_len = value.len();
        let schema_size = ctx.max_size as usize;

        // Check for size overflow (skip for PLP types which support up to 2GB)
        if !ctx.is_plp && data_len > schema_size {
            return Err(Error::UsageError(format!(
                "Binary data length ({}) exceeds schema size ({})",
                data_len, schema_size
            )));
        }

        // For PLP types (MAX types), use PLP encoding
        if ctx.is_plp {
            // Write PLP_UNKNOWN_LEN (0xFFFFFFFFFFFFFFFE) to indicate total length is unknown
            // This matches .NET SqlBulkCopy behavior
            writer.write_u64_async(PLP_UNKNOWN_LEN).await?;

            // Write chunk length (4 bytes)
            writer.write_u32_async(data_len as u32).await?;

            // Write actual data
            writer.write_async(value).await?;

            // Write terminator (4 bytes of 0x00)
            writer.write_u32_async(PLP_TERMINATOR).await?;
        } else if ctx.is_fixed_length {
            // Fixed-length BINARY(n): Write exactly n bytes (no length prefix)
            // Write actual data
            for &byte in value {
                writer.write_byte_async(byte).await?;
            }

            // Pad with zeros to reach fixed size
            let padding_needed = schema_size.saturating_sub(data_len);
            for _ in 0..padding_needed {
                writer.write_byte_async(0u8).await?;
            }
        } else {
            // Variable-length VARBINARY(n): Write 2-byte length prefix + data (no padding)
            writer.write_u16_async(data_len as u16).await?;

            // Write actual data
            for &byte in value {
                writer.write_byte_async(byte).await?;
            }
        }
        Ok(())
    }

    /// Serialize a JSON value using PLP encoding.
    ///
    /// JSON type (0xF4) uses UTF-8 encoding and PLP (Partially Length Prefixed) structure.
    /// Format: PLP_UNKNOWN_LEN (8 bytes) + chunk_len (4 bytes) + data + terminator (4 bytes)
    async fn serialize_json<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &SqlJson,
        _ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // CRITICAL: JSON bulk copy uses NVARCHAR (0xE7) encoding, which requires UTF-16LE
        // Convert UTF-8 JSON to UTF-16LE like .NET SqlBulkCopy does
        let json_str = std::str::from_utf8(&value.bytes).map_err(|e| {
            Error::TypeConversionError(format!("Invalid UTF-8 in JSON data: {}", e))
        })?;

        // Encode as UTF-16LE (no BOM needed for NVARCHAR)
        let utf16_data: Vec<u16> = json_str.encode_utf16().collect();
        let byte_len = utf16_data.len() * 2; // Each u16 is 2 bytes

        // JSON uses PLP encoding with UTF-16LE for NVARCHAR type
        // Write PLP_UNKNOWN_LEN (0xFFFFFFFFFFFFFFFE)
        writer.write_u64_async(PLP_UNKNOWN_LEN).await?;

        // Write chunk length (4 bytes)
        writer.write_u32_async(byte_len as u32).await?;

        // Write UTF-16LE encoded data
        for code_unit in utf16_data {
            writer.write_u16_async(code_unit).await?;
        }

        // Write terminator (4 bytes of 0x00)
        writer.write_u32_async(PLP_TERMINATOR).await?;

        Ok(())
    }
}
