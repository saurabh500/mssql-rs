// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TDS value serialization utilities for RPC parameters and bulk copy operations.
//!
//! This module provides shared value encoding logic that can be used by both
//! RPC parameter serialization (SqlType) and bulk copy ROW token serialization.
//! It separates the concerns of type metadata encoding from value encoding.

use crate::core::TdsResult;
use crate::datatypes::column_values::ColumnValues;
use crate::error::Error;
use crate::io::packet_writer::{PacketWriter, TdsPacketWriter, TdsPacketWriterUnchecked};

// NULL markers for different type classes
const NULL_LENGTH: u8 = 0x00;
const VARNULL: u16 = 0xFFFF;
const PLP_NULL: u64 = 0xFFFFFFFFFFFFFFFF;
const PLP_TERMINATOR: u32 = 0x00000000;

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
            ColumnValues::Decimal(v) | ColumnValues::Numeric(v) => {
                Self::serialize_decimal(writer, v, ctx).await
            }
            ColumnValues::SmallMoney(v) => Self::serialize_smallmoney(writer, v, ctx).await,
            ColumnValues::Money(v) => Self::serialize_money(writer, v, ctx).await,
            ColumnValues::Date(v) => Self::serialize_date(writer, v, ctx).await,
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
            // Nullable types (INTN, FLTN, BITN, MONEYN, DATETIMEN, DecimalN, NumericN, Guid, DateN) use length = 0x00
            0x26 | 0x6D | 0x68 | 0x6E | 0x6F | 0x6A | 0x6C | 0x24 | 0x28 => {
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
            match writer.has_space(4) {
                false => {
                    writer.write_byte_async(3).await?; // Length for DateN (3 bytes)
                    // Write 3 bytes in little-endian format (u32 as 3 bytes)
                    let days = value.get_days();
                    writer.write_byte_async((days & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 8) & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 16) & 0xFF) as u8).await?;
                }
                true => {
                    writer.write_byte_unchecked(3); // Length for DateN (3 bytes)
                    // Write 3 bytes in little-endian format (u32 as 3 bytes)
                    let days = value.get_days();
                    writer.write_byte_unchecked((days & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 8) & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 16) & 0xFF) as u8);
                }
            }
        } else {
            // Fixed type (Date, 0x2A) - just write value (3 bytes)
            match writer.has_space(3) {
                false => {
                    let days = value.get_days();
                    writer.write_byte_async((days & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 8) & 0xFF) as u8).await?;
                    writer.write_byte_async(((days >> 16) & 0xFF) as u8).await?;
                }
                true => {
                    let days = value.get_days();
                    writer.write_byte_unchecked((days & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 8) & 0xFF) as u8);
                    writer.write_byte_unchecked(((days >> 16) & 0xFF) as u8);
                }
            }
        }
        Ok(())
    }
}
