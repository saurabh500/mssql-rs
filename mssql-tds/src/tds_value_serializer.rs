// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TDS value serialization utilities for RPC parameters and bulk copy operations.
//!
//! This module provides shared value encoding logic that can be used by both
//! RPC parameter serialization (SqlType) and bulk copy ROW token serialization.
//! It separates the concerns of type metadata encoding from value encoding.

use crate::core::TdsResult;
use crate::datatypes::column_values::{
    ColumnValues, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlSmallDateTime,
};
use crate::datatypes::decoder::DecimalParts;
use crate::datatypes::sqldatatypes::TdsDataType;
use crate::datatypes::sqltypes::{self, SqlType};
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
    /// Create type context from SqlType (for RPC parameters).
    pub fn from_sql_type(sql_type: &SqlType) -> Self {
        let tds_type = Self::sql_type_to_tds_byte(sql_type);
        let max_size = Self::infer_max_size(sql_type);
        let is_plp = Self::is_sql_type_plp(sql_type);
        let (precision, scale) = Self::extract_precision_scale(sql_type);
        let is_nullable = true; // RPC parameters are always nullable in TDS

        Self {
            tds_type,
            max_size,
            is_plp,
            precision,
            scale,
            is_nullable,
        }
    }

    /// Create type context from BulkCopyColumnMetadata (for bulk copy operations).
    pub fn from_bulk_copy_metadata(
        metadata: &crate::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata,
    ) -> Self {
        let tds_type = metadata.tds_type;
        let max_size = metadata.length as u8;
        let is_plp = metadata.length_type.is_plp();
        let precision = if metadata.precision > 0 {
            Some(metadata.precision)
        } else {
            None
        };
        let scale = if metadata.scale > 0 {
            Some(metadata.scale)
        } else {
            None
        };
        let is_nullable = metadata.is_nullable;

        Self {
            tds_type,
            max_size,
            is_plp,
            precision,
            scale,
            is_nullable,
        }
    }

    /// Check if this is a fixed-length type (no length prefix needed).
    pub fn is_fixed_type(&self) -> bool {
        // Fixed types: INT1-INT8, BIT, FLT4, FLT8, DATETIME, MONEY, etc.
        // NOTE: GUIDTYPE (0x24) is NOT included here because it DOES require
        // a length prefix in bulk copy ROW tokens, unlike truly fixed-length types
        matches!(self.tds_type, 0x30..=0x3F | 0x7F)
    }

    fn sql_type_to_tds_byte(sql_type: &SqlType) -> u8 {
        match sql_type {
            SqlType::Bit(_) => TdsDataType::IntN as u8,
            SqlType::TinyInt(_) => TdsDataType::IntN as u8,
            SqlType::SmallInt(_) => TdsDataType::IntN as u8,
            SqlType::Int(_) => TdsDataType::IntN as u8,
            SqlType::BigInt(_) => TdsDataType::IntN as u8,
            SqlType::Real(_) => TdsDataType::FltN as u8,
            SqlType::Float(_) => TdsDataType::FltN as u8,
            SqlType::Decimal(_) => TdsDataType::NumericN as u8,
            SqlType::Numeric(_) => TdsDataType::NumericN as u8,
            SqlType::Money(_) => TdsDataType::MoneyN as u8,
            SqlType::SmallMoney(_) => TdsDataType::MoneyN as u8,
            SqlType::Time(_) => TdsDataType::TimeN as u8,
            SqlType::DateTime2(_) => TdsDataType::DateTime2N as u8,
            SqlType::DateTimeOffset(_) => TdsDataType::DateTimeOffsetN as u8,
            SqlType::SmallDateTime(_) => TdsDataType::DateTimeN as u8,
            SqlType::DateTime(_) => TdsDataType::DateTimeN as u8,
            SqlType::Date(_) => TdsDataType::DateN as u8,
            SqlType::NVarchar(_, _) => TdsDataType::NVarChar as u8,
            SqlType::NVarcharMax(_) => TdsDataType::NVarChar as u8,
            SqlType::Varchar(_, _) => TdsDataType::BigVarChar as u8,
            SqlType::VarcharMax(_) => TdsDataType::BigVarChar as u8,
            SqlType::VarBinary(_, _) => TdsDataType::BigVarBinary as u8,
            SqlType::VarBinaryMax(_) => TdsDataType::BigVarBinary as u8,
            SqlType::Binary(_, _) => TdsDataType::BigBinary as u8,
            SqlType::Char(_, _) => TdsDataType::BigChar as u8,
            SqlType::NChar(_, _) => TdsDataType::NChar as u8,
            SqlType::Xml(_) => TdsDataType::Xml as u8,
            SqlType::Uuid(_) => TdsDataType::Guid as u8,
            SqlType::Json(_) => TdsDataType::Json as u8,
            SqlType::Text(_) => TdsDataType::Text as u8,
            SqlType::NText(_) => TdsDataType::NText as u8,
        }
    }

    fn infer_max_size(sql_type: &SqlType) -> u8 {
        match sql_type {
            SqlType::Bit(_) => 1,
            SqlType::TinyInt(_) => 1,
            SqlType::SmallInt(_) => 2,
            SqlType::Int(_) => 4,
            SqlType::BigInt(_) => 8,
            SqlType::Real(_) => 4,
            SqlType::Float(_) => 8,
            SqlType::SmallMoney(_) => 4,
            SqlType::Money(_) => 8,
            SqlType::SmallDateTime(_) => 4,
            SqlType::DateTime(_) => 8,
            _ => 0,
        }
    }

    fn is_sql_type_plp(sql_type: &SqlType) -> bool {
        matches!(
            sql_type,
            SqlType::NVarcharMax(_)
                | SqlType::VarcharMax(_)
                | SqlType::VarBinaryMax(_)
                | SqlType::Xml(_)
        )
    }

    fn extract_precision_scale(sql_type: &SqlType) -> (Option<u8>, Option<u8>) {
        match sql_type {
            SqlType::Decimal(Some(parts)) | SqlType::Numeric(Some(parts)) => {
                (Some(parts.precision), Some(parts.scale))
            }
            SqlType::Time(Some(time)) => (None, Some(time.scale)),
            SqlType::DateTime2(Some(dt2)) => (None, Some(dt2.time.scale)),
            SqlType::DateTimeOffset(Some(dto)) => (None, Some(dto.datetime2.time.scale)),
            _ => (None, None),
        }
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
            ColumnValues::TinyInt(v) => Self::serialize_tinyint(writer, *v, ctx).await,
            ColumnValues::SmallInt(v) => Self::serialize_smallint(writer, *v, ctx).await,
            ColumnValues::Int(v) => Self::serialize_int(writer, *v, ctx).await,
            ColumnValues::BigInt(v) => Self::serialize_bigint(writer, *v, ctx).await,
            ColumnValues::Real(v) => Self::serialize_real(writer, *v, ctx).await,
            ColumnValues::Float(v) => Self::serialize_float(writer, *v, ctx).await,
            ColumnValues::Bit(v) => Self::serialize_bit(writer, *v, ctx).await,
            ColumnValues::Decimal(parts) | ColumnValues::Numeric(parts) => {
                Self::serialize_decimal(writer, parts, ctx).await
            }
            ColumnValues::String(sql_string) => {
                // Optimization: If SqlString is already UTF-16, use cached bytes directly
                if let Some(utf16_bytes) = sql_string.as_utf16_bytes() {
                    Self::serialize_string_cached(writer, utf16_bytes, ctx).await
                } else {
                    let s = sql_string.to_utf8_string();
                    Self::serialize_string(writer, &s, ctx).await
                }
            }
            ColumnValues::Bytes(bytes) => Self::serialize_bytes(writer, bytes, ctx).await,
            ColumnValues::Uuid(uuid) => Self::serialize_uuid(writer, uuid, ctx).await,
            ColumnValues::Xml(xml) => Self::serialize_plp_bytes(writer, &xml.bytes).await,
            ColumnValues::Json(json) => {
                let json_str = json.as_string();
                let bytes = json_str.as_bytes();
                Self::serialize_plp_bytes(writer, bytes).await
            }
            ColumnValues::Date(date) => Self::serialize_date(writer, date).await,
            ColumnValues::Time(time) => Self::serialize_time(writer, time, ctx).await,
            ColumnValues::DateTime2(dt2) => Self::serialize_datetime2(writer, dt2, ctx).await,
            ColumnValues::DateTimeOffset(dto) => {
                Self::serialize_datetimeoffset(writer, dto, ctx).await
            }
            ColumnValues::DateTime(dt) => Self::serialize_datetime(writer, dt).await,
            ColumnValues::SmallDateTime(sdt) => Self::serialize_smalldatetime(writer, sdt).await,
            ColumnValues::Money(money) => Self::serialize_money(writer, money, ctx).await,
            ColumnValues::SmallMoney(sm) => Self::serialize_smallmoney(writer, sm, ctx).await,
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
            // Nullable types (INTN, FLTN, BITN, MONEYN, DATETIMEN, NumericN, Guid, DateN) use length = 0x00
            0x26 | 0x6D | 0x68 | 0x6E | 0x6F | 0x6C | 0x24 | 0x28 => {
                writer.write_byte_async(NULL_LENGTH).await?;
            }
            _ => {
                // Other types depend on length classification
                if ctx.is_plp {
                    // PLP NULL: 8 bytes of 0xFF
                    writer.write_u64_async(PLP_NULL).await?;
                } else if ctx.is_fixed_type() {
                    // Fixed-length NULL: 0x00
                    writer.write_byte_async(NULL_LENGTH).await?;
                } else {
                    // Variable-length NULL: 0xFFFF
                    writer.write_u16_async(VARNULL).await?;
                }
            }
        }
        Ok(())
    }

    // Integer types
    #[inline(always)]
    async fn serialize_tinyint<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: u8,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        if !ctx.is_fixed_type() {
            writer.write_byte_async(1).await?; // Length for INTN
        }
        writer.write_byte_async(value).await?;
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
        if !ctx.is_fixed_type() {
            writer.write_byte_async(2).await?; // Length for INTN
        }
        writer.write_i16_async(value).await?;
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
            if writer.has_space(5) {
                writer.write_byte_unchecked(4); // Length for INTN
                writer.write_i32_unchecked(value);
            } else {
                writer.write_byte_async(4).await?; // Length for INTN
                writer.write_i32_async(value).await?;
            }
        } else {
            // Fixed type - just write value (4 bytes)
            if writer.has_space(4) {
                writer.write_i32_unchecked(value);
            } else {
                writer.write_i32_async(value).await?;
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
            // Ensure space for length byte + value (9 bytes total)
            if writer.has_space(9) {
                writer.write_byte_unchecked(8); // Length for INTN
                writer.write_i64_unchecked(value);
            } else {
                writer.write_byte_async(8).await?; // Length for INTN
                writer.write_i64_async(value).await?;
            }
        } else {
            // Fixed type - just write value (8 bytes)
            if writer.has_space(8) {
                writer.write_i64_unchecked(value);
            } else {
                writer.write_i64_async(value).await?;
            }
        }
        Ok(())
    }

    // Floating point types
    #[inline(always)]
    async fn serialize_real<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: f32,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        if !ctx.is_fixed_type() {
            writer.write_byte_async(4).await?; // Length for FLTN
        }
        let bytes = value.to_le_bytes();
        writer.write_async(&bytes).await?;
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
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Ensure space for length byte + value (9 bytes total)
            if writer.has_space(9) {
                writer.write_byte_unchecked(8); // Length for FLTN
                writer.write_f64_unchecked(value);
            } else {
                writer.write_byte_async(8).await?; // Length for FLTN
                let bytes = value.to_le_bytes();
                writer.write_async(&bytes).await?;
            }
        } else {
            // Fixed type - just write value (8 bytes)
            if writer.has_space(8) {
                writer.write_f64_unchecked(value);
            } else {
                let bytes = value.to_le_bytes();
                writer.write_async(&bytes).await?;
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
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Ensure space for length byte + value (2 bytes total)
            if writer.has_space(2) {
                writer.write_byte_unchecked(1); // Length for BITN
                writer.write_byte_unchecked(if value { 1 } else { 0 });
            } else {
                writer.write_byte_async(1).await?; // Length for BITN
                writer.write_byte_async(if value { 1 } else { 0 }).await?;
            }
        } else {
            // Fixed type - just write value (1 byte)
            if writer.has_space(1) {
                writer.write_byte_unchecked(if value { 1 } else { 0 });
            } else {
                writer.write_byte_async(if value { 1 } else { 0 }).await?;
            }
        }
        Ok(())
    }

    // Decimal/Numeric
    #[inline]
    async fn serialize_decimal<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        parts: &DecimalParts,
        _ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Length byte (always 17 for max precision)
        writer.write_byte_async(sqltypes::DECIMAL_FIXED_SIZE).await?;

        // Sign byte
        writer
            .write_byte_async(if parts.is_positive { 0x01 } else { 0x00 })
            .await?;

        // Write up to 3 int_parts, pad with zeros if fewer
        for i in 0..3 {
            let part = parts.int_parts.get(i).copied().unwrap_or(0);
            writer.write_i32_async(part).await?;
        }
        // The fourth part is always 0
        writer.write_i32_async(0).await?;

        Ok(())
    }

    // String types
    #[inline]
    async fn serialize_string<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        s: &str,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Determine encoding based on TDS type
        let is_unicode = matches!(
            ctx.tds_type,
            0xE7 | 0xEF | 0x63 // NVARCHAR, NCHAR, NTEXT
        );

        if ctx.is_plp {
            // PLP string - still needs Vec for length calculation
            let bytes: Vec<u8> = if is_unicode {
                s.encode_utf16().flat_map(|c| c.to_le_bytes()).collect()
            } else {
                s.as_bytes().to_vec()
            };
            Self::serialize_plp_bytes(writer, &bytes).await?;
        } else {
            // Non-PLP: Optimized zero-copy path for Unicode, direct write for ASCII
            if is_unicode {
                // UTF-16LE: For non-PLP, we need the byte length first
                // Temporarily collect to get accurate length (most strings are small in bulk copy)
                let utf16_bytes: Vec<u8> = s.encode_utf16().flat_map(|c| c.to_le_bytes()).collect();
                writer.write_u16_async(utf16_bytes.len() as u16).await?;
                writer.write_async(&utf16_bytes).await?;
            } else {
                // UTF-8 encoding (for VARCHAR) - already zero-copy
                writer.write_u16_async(s.len() as u16).await?;
                writer.write_async(s.as_bytes()).await?;
            }
        }

        Ok(())
    }

    /// Optimized string serialization for pre-encoded UTF-16 bytes (from SqlString cache).
    /// This avoids re-encoding strings that are already in UTF-16 format.
    #[inline]
    async fn serialize_string_cached<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        utf16_bytes: &[u8],
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        if ctx.is_plp {
            // PLP string - write cached bytes directly
            Self::serialize_plp_bytes(writer, utf16_bytes).await?;
        } else {
            // Non-PLP: Write length + cached UTF-16 bytes (zero-copy)
            writer.write_u16_async(utf16_bytes.len() as u16).await?;
            writer.write_async(utf16_bytes).await?;
        }

        Ok(())
    }

    // Binary types
    #[inline]
    async fn serialize_bytes<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        bytes: &[u8],
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        if ctx.is_plp {
            // PLP binary
            Self::serialize_plp_bytes(writer, bytes).await?;
        } else {
            // Variable-length binary with 2-byte length prefix
            writer.write_u16_async(bytes.len() as u16).await?;
            writer.write_async(bytes).await?;
        }

        Ok(())
    }

    // UUID
    #[inline(always)]
    async fn serialize_uuid<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        uuid: &uuid::Uuid,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // UNIQUEIDENTIFIER (GUIDTYPE 0x24) serialization:
        // ALWAYS write 1-byte length prefix (0x10) followed by 16 bytes of data.
        // This applies to both RPC parameters and bulk copy ROW tokens.
        // Even though COLMETADATA includes the length byte for type info,
        // each ROW token still requires the length prefix (verified against .NET SqlBulkCopy).
        if !ctx.is_fixed_type() {
            writer.write_byte_async(16).await?;
        }
        let guid_bytes = uuid.to_bytes_le();
        writer.write_async(&guid_bytes).await?;
        Ok(())
    }

    // Date/Time types
    #[inline]
    async fn serialize_date<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        date: &SqlDate,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        writer.write_byte_async(3).await?; // Length
        let days_bytes = date.get_days().to_le_bytes();
        writer.write_async(&days_bytes[0..3]).await?;
        Ok(())
    }

    #[inline]
    async fn serialize_time<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        time: &crate::datatypes::column_values::SqlTime,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        let scale = ctx.scale.unwrap_or(time.scale);
        let length = Self::time_length_from_scale(scale);
        writer.write_byte_async(length).await?;

        let time_bytes = time.time_nanoseconds.to_le_bytes();
        writer.write_async(&time_bytes[0..length as usize]).await?;
        Ok(())
    }

    #[inline]
    async fn serialize_datetime2<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        dt2: &SqlDateTime2,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        let scale = ctx.scale.unwrap_or(dt2.time.scale);
        let time_length = Self::time_length_from_scale(scale);
        let total_length = time_length + 3; // time + 3 bytes for days
        writer.write_byte_async(total_length).await?;

        let time_bytes = dt2.time.time_nanoseconds.to_le_bytes();
        writer
            .write_async(&time_bytes[0..time_length as usize])
            .await?;

        let days_bytes = dt2.days.to_le_bytes();
        writer.write_async(&days_bytes[0..3]).await?;
        Ok(())
    }

    #[inline]
    async fn serialize_datetimeoffset<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        dto: &SqlDateTimeOffset,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        let scale = ctx.scale.unwrap_or(dto.datetime2.time.scale);
        let time_length = Self::time_length_from_scale(scale);
        let total_length = time_length + 3 + 2; // time + days + offset
        writer.write_byte_async(total_length).await?;

        let time_bytes = dto.datetime2.time.time_nanoseconds.to_le_bytes();
        writer
            .write_async(&time_bytes[0..time_length as usize])
            .await?;

        let days_bytes = dto.datetime2.days.to_le_bytes();
        writer.write_async(&days_bytes[0..3]).await?;

        writer.write_i16_async(dto.offset).await?;
        Ok(())
    }

    #[inline]
    async fn serialize_datetime<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        dt: &SqlDateTime,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // DateTime: 8 bytes (4 days + 4 time)
        writer.write_byte_async(8).await?; // Length for DATETIMEN
        writer.write_i32_async(dt.days).await?;
        writer.write_u32_async(dt.time).await?;
        Ok(())
    }

    #[inline]
    async fn serialize_smalldatetime<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        sdt: &SqlSmallDateTime,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // SmallDateTime: 4 bytes (2 days + 2 minutes)
        writer.write_byte_async(4).await?; // Length for DATETIMEN
        writer.write_u16_async(sdt.days).await?;
        writer.write_u16_async(sdt.time).await?;
        Ok(())
    }

    // Money types
    #[inline]
    async fn serialize_money<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        money: &crate::datatypes::column_values::SqlMoney,
        _ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Money: 8 bytes (MSB, LSB)
        writer.write_byte_async(8).await?; // Length for MONEYN
        writer.write_i32_async(money.msb_part).await?;
        writer.write_i32_async(money.lsb_part).await?;
        Ok(())
    }

    #[inline]
    async fn serialize_smallmoney<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        sm: &crate::datatypes::column_values::SqlSmallMoney,
        _ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // SmallMoney: 4 bytes
        writer.write_byte_async(4).await?; // Length for MONEYN
        writer.write_i32_async(sm.int_val).await?;
        Ok(())
    }

    /// Helper: Serialize PLP (Partially Length-Prefixed) data
    /// Format: [total_length: u64] [chunk_len: u32] [chunk_data] [...] [terminator: 0x00000000]
    #[inline]
    async fn serialize_plp_bytes<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        bytes: &[u8],
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        const CHUNK_SIZE: usize = 8000;

        // Total length
        writer.write_u64_async(bytes.len() as u64).await?;

        // Write chunks
        for chunk in bytes.chunks(CHUNK_SIZE) {
            writer.write_u32_async(chunk.len() as u32).await?;
            writer.write_async(chunk).await?;
        }

        // Terminator
        writer.write_u32_async(PLP_TERMINATOR).await?;

        Ok(())
    }

    /// Calculate time length from scale.
    #[inline(always)]
    fn time_length_from_scale(scale: u8) -> u8 {
        match scale {
            0..=2 => 3,
            3..=4 => 4,
            5..=7 => 5,
            _ => 5,
        }
    }
}
