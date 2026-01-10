// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;
use core::fmt;
use std::{fmt::Debug, io::Error, vec};

use super::{
    sql_string::{SqlString, get_encoding_type},
    sqldatatypes::{TdsDataType, TypeInfoVariant},
};
use crate::datatypes::sqldatatypes::TypeInfo;
use crate::{
    core::TdsResult,
    datatypes::{sql_json::SqlJson, sql_string::EncodingType, sqldatatypes::FixedLengthTypes},
};
use crate::{
    datatypes::column_values::{
        ColumnValues, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney,
        SqlSmallDateTime, SqlSmallMoney, SqlTime, SqlXml,
    },
    io::packet_reader::TdsPacketReader,
};
use crate::{query::metadata::ColumnMetadata, token::tokens::SqlCollation};

// Maximum reasonable allocation size for a single value (100MB)
// This prevents fuzzer-induced capacity overflow panics
#[cfg(fuzzing)]
const MAX_ALLOC_SIZE: usize = 64 * 1024; // 64KB for fuzzing
#[cfg(not(fuzzing))]
const MAX_ALLOC_SIZE: usize = 100 * 1024 * 1024;

// Maximum allocation size for PLP (Partial Length Pointer) types
// SQL Server supports PLP types up to 2GB (i32::MAX is approximately 2.1GB)
#[cfg(fuzzing)]
const MAX_PLP_SIZE: usize = 64 * 1024; // 64KB for fuzzing
#[cfg(not(fuzzing))]
const MAX_PLP_SIZE: usize = i32::MAX as usize;

// Helper function to validate allocation size before allocating
#[inline]
fn validate_alloc_size(size: usize, context: &str) -> TdsResult<()> {
    if size > MAX_ALLOC_SIZE {
        #[cfg(fuzzing)]
        {
            use std::io::Write;
            let _ = writeln!(
                std::io::stderr(),
                "[ALLOC-REJECT] {} requesting {} bytes (max {})",
                context,
                size,
                MAX_ALLOC_SIZE
            );
        }

        return Err(crate::error::Error::ProtocolError(format!(
            "{context}: allocation size {size} exceeds maximum allowed {MAX_ALLOC_SIZE} bytes"
        )));
    }
    #[cfg(fuzzing)]
    {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr(),
            "[ALLOC-OK] {} requesting {} bytes",
            context,
            size
        );
    }
    Ok(())
}

// Macro to create validated Vec allocations
#[cfg(fuzzing)]
macro_rules! safe_vec {
    ($elem:expr; $size:expr, $context:expr) => {{
        let size = $size;
        validate_alloc_size(size, $context)?;
        vec![$elem; size]
    }};
}

#[cfg(not(fuzzing))]
macro_rules! safe_vec {
    ($elem:expr; $size:expr, $context:expr) => {{
        let size = $size;
        validate_alloc_size(size, $context)?;
        vec![$elem; size]
    }};
}

#[async_trait]
pub(crate) trait SqlTypeDecode {
    async fn decode<T>(&self, reader: &mut T, metadata: &ColumnMetadata) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync;
}

impl From<u8> for ColumnValues {
    fn from(value: u8) -> Self {
        ColumnValues::TinyInt(value)
    }
}

impl From<i32> for ColumnValues {
    fn from(value: i32) -> Self {
        ColumnValues::Int(value)
    }
}

#[derive(Debug, Default)]
pub(crate) struct GenericDecoder {
    string_decoder: StringDecoder,
}

impl GenericDecoder {
    const SHORTLEN_MAXVALUE: usize = 65535;
    const SQL_PLP_NULL: usize = 0xffffffffffffffff;
    const SQL_PLP_UNKNOWNLEN: usize = 0xfffffffffffffffe;

    // Reads a SQL_VARIANT type from the TDS stream.
    async fn read_sql_variant<T>(&self, reader: &mut T) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let length = reader.read_uint32().await?;
        let variant_base_type = reader.read_byte().await?;
        let tds_type = TdsDataType::try_from(variant_base_type)?;
        let variant_prop_bytes = reader.read_byte().await?;
        let bytes_for_type_and_properties_byte = 2;

        // Use checked arithmetic to prevent integer underflow
        let data_length = length
            .checked_sub(bytes_for_type_and_properties_byte)
            .and_then(|v| v.checked_sub(variant_prop_bytes as u32))
            .ok_or_else(|| {
                crate::error::Error::ProtocolError(format!(
                    "SQL_VARIANT data length calculation underflow: length={length}, prop_bytes={variant_prop_bytes}"
                ))
            })?;

        let col_value = match variant_prop_bytes {
            0 => {
                self.decode_zero_propbyte_variant(reader, tds_type, data_length)
                    .await?
            }
            1 => {
                // TIMENTYPE, DATETIME2NTYPE, DATETIMEOFFSETNTYPE
                self.decode_one_byte_variant(reader, tds_type, data_length)
                    .await?
            }
            2 => {
                decode_two_propbyte_variant(reader, variant_base_type, tds_type, data_length)
                    .await?
            }
            7 => {
                // BIGVARCHARTYPE, BIGCHARTYPE, NVARCHARTYPE, NCHARTYPE
                decode_seven_propbyte_variant(reader, tds_type, data_length).await?
            }
            _ => {
                return Err(crate::error::Error::ProtocolError(format!(
                    "Unexpected SQL variant properties length: {variant_prop_bytes}. Expected 0, 1, 2, or 7. This indicates malformed or invalid data."
                )));
            }
        };
        Ok(col_value)
    }

    async fn decode_zero_propbyte_variant<T>(
        &self,
        reader: &mut T,
        tds_type: TdsDataType,
        data_length: u32,
    ) -> Result<ColumnValues, crate::error::Error>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let fixed_length_type_result = FixedLengthTypes::try_from(tds_type);

        // The type may be a fixed length type, or it may be a variable length type like Guid/DateN
        match fixed_length_type_result {
            Ok(fixed_length_type) => {
                let type_info = TypeInfo {
                    tds_type,
                    length: data_length as usize,
                    type_info_variant: TypeInfoVariant::FixedLen(fixed_length_type),
                };
                let variant_actual_type_md = ColumnMetadata {
                    user_type: 0,
                    flags: 0,
                    type_info,
                    data_type: tds_type,
                    column_name: "".to_string(),
                    multi_part_name: None,
                };
                self.decode(reader, &variant_actual_type_md).await
            }
            _ => {
                // If the type is not a fixed length type, we should not reach here.
                match tds_type {
                    TdsDataType::Guid => Self::read_guid(reader, data_length as u8).await,
                    TdsDataType::DateN => Self::read_daten(reader, data_length as u8).await,
                    _ => Err(crate::error::Error::ProtocolError(format!(
                        "For 0 byte property, only Guid and DateN are expected, but got: {tds_type:?}"
                    ))),
                }
            }
        }
    }

    async fn decode_one_byte_variant<T>(
        &self,
        reader: &mut T,
        tds_type: TdsDataType,
        data_length: u32,
    ) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let scale = reader.read_byte().await?;
        Ok(match tds_type {
            TdsDataType::TimeN => {
                let time_nanos = self.read_time(reader, data_length as u8, scale).await?;
                ColumnValues::Time(time_nanos)
            }
            TdsDataType::DateTime2N => {
                self.read_datetime2(reader, data_length as u8, scale)
                    .await?
            }
            TdsDataType::DateTimeOffsetN => {
                self.read_datetime_offset(reader, data_length as u8, scale)
                    .await?
            }
            _ => {
                return Err(crate::error::Error::ProtocolError(format!(
                    "Invalid SQL_VARIANT: 1-byte property is only valid for TimeN, DateTime2N, and DateTimeOffsetN types, but got: {tds_type:?}"
                )));
            }
        })
    }

    async fn read_decimal<T>(
        &self,
        reader: &mut T,
        metadata: &ColumnMetadata,
    ) -> TdsResult<Option<DecimalParts>>
    where
        T: TdsPacketReader + Send + Sync,
    {
        // Decimal/numeric data type has 1 byte length.
        let length = reader.read_byte().await?;
        let TypeInfoVariant::VarLenPrecisionScale(_, _, precision, scale) =
            metadata.type_info.type_info_variant
        else {
            return Err(crate::error::Error::ProtocolError(format!(
                "Invalid type info variant for Decimal/Numeric: expected VarLenPrecisionScale, got: {:?}",
                metadata.type_info.type_info_variant
            )));
        };
        GenericDecoder::read_decimal_data(reader, length, precision, scale).await
    }

    async fn read_decimal_data<T>(
        reader: &mut T,
        length: u8,
        precision: u8,
        scale: u8,
    ) -> TdsResult<Option<DecimalParts>>
    where
        T: TdsPacketReader + Send + Sync,
    {
        // If length is 0, then it is NULL.
        if length == 0 {
            return Ok(None);
        }
        let sign = reader.read_byte().await?;
        let is_positive = sign == 1;

        let number_of_int_parts = (length - 1) >> 2;

        // Limit decimal parts allocation for fuzzing
        #[cfg(fuzzing)]
        const MAX_DECIMAL_INT_PARTS: u8 = 10; // Maximum 10 int parts = 40 bytes
        #[cfg(not(fuzzing))]
        const MAX_DECIMAL_INT_PARTS: u8 = 64; // SQL Server max precision is 38, which needs max ~17 int parts

        if number_of_int_parts > MAX_DECIMAL_INT_PARTS {
            return Err(crate::error::Error::ProtocolError(format!(
                "Decimal int parts {number_of_int_parts} exceeds maximum allowed {MAX_DECIMAL_INT_PARTS} (length was {length})"
            )));
        }

        let int_parts_len = number_of_int_parts as usize;
        validate_alloc_size(int_parts_len * 4, "read_decimal int_parts")?;
        let mut int_parts = vec![0i32; int_parts_len];
        for part_index in 0..number_of_int_parts {
            int_parts[part_index as usize] = reader.read_int32().await?;
        }

        Ok(Some(DecimalParts {
            is_positive,
            scale,
            precision,
            int_parts,
        }))
    }

    async fn read_datetime<T>(&self, reader: &mut T) -> TdsResult<SqlDateTime>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let days = reader.read_int32().await?;
        let ticks = reader.read_uint32().await?;

        Ok(SqlDateTime { days, time: ticks })
    }

    async fn read_small_datetime<T>(&self, reader: &mut T) -> TdsResult<SqlSmallDateTime>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let days = reader.read_uint16().await?;
        let minutes = reader.read_uint16().await?;
        Ok(SqlSmallDateTime {
            days,
            time: minutes,
        })
    }

    async fn read_date<T>(reader: &mut T) -> TdsResult<SqlDate>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let days = reader.read_uint24().await?;
        Ok(SqlDate::unchecked_create(days))
    }

    async fn read_time<T>(&self, reader: &mut T, byte_len: u8, scale: u8) -> TdsResult<SqlTime>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let scaled_value = match byte_len {
            3 => reader.read_uint24().await? as u64,
            4 => reader.read_uint32().await? as u64,
            _ => reader.read_uint40().await?,
        };

        // The value from SQL Server is in scaled units based on the scale:
        // Scale 0: seconds (need to multiply by 10^7)
        // Scale 1: tenths of seconds (multiply by 10^6)
        // Scale 2: hundredths (multiply by 10^5)
        // Scale 3: milliseconds (multiply by 10^4)
        // Scale 4: ten-thousandths (multiply by 10^3)
        // Scale 5: hundred-thousandths (multiply by 10^2)
        // Scale 6: microseconds (multiply by 10^1)
        // Scale 7: 100-nanoseconds (multiply by 10^0 = no scaling)
        // We need to convert to 100-nanosecond units for consistency
        let time_nanoseconds = match scale {
            0 => scaled_value * 10_000_000, // Seconds to 100ns
            1 => scaled_value * 1_000_000,  // Tenths to 100ns
            2 => scaled_value * 100_000,    // Hundredths to 100ns
            3 => scaled_value * 10_000,     // Milliseconds to 100ns
            4 => scaled_value * 1_000,      // Ten-thousandths to 100ns
            5 => scaled_value * 100,        // Hundred-thousandths to 100ns
            6 => scaled_value * 10,         // Microseconds to 100ns
            7 => scaled_value,              // Already in 100ns
            _ => scaled_value,              // Fallback
        };

        Ok(SqlTime {
            time_nanoseconds,
            scale,
        })
    }

    async fn read_datetime2<T>(
        &self,
        reader: &mut T,
        byte_len: u8,
        scale: u8,
    ) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let time_byte_len = byte_len.checked_sub(3).ok_or_else(|| {
            crate::error::Error::ProtocolError(format!(
                "Invalid DateTime2 byte length: {byte_len}. Expected at least 3 bytes for date component."
            ))
        })?;
        let time_nanos = self.read_time(reader, time_byte_len, scale).await?;
        let sql_date = Self::read_date(reader).await?;
        let datetime2 = SqlDateTime2 {
            days: sql_date.get_days(),
            time: time_nanos,
        };
        Ok(ColumnValues::DateTime2(datetime2))
    }

    async fn read_datetime_offset<T>(
        &self,
        reader: &mut T,
        byte_len: u8,
        scale: u8,
    ) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let datetime2_byte_len = byte_len.checked_sub(2).ok_or_else(|| {
            crate::error::Error::ProtocolError(format!(
                "Invalid DateTimeOffset byte length: {byte_len}. Expected at least 2 bytes for offset component."
            ))
        })?;
        let datetime2 = self
            .read_datetime2(reader, datetime2_byte_len, scale)
            .await?;
        let datetime2 = match datetime2 {
            ColumnValues::DateTime2(dt2) => dt2,
            _ => {
                return Err(crate::error::Error::ProtocolError(format!(
                    "Internal error: read_datetime2 returned unexpected type: {datetime2:?}"
                )));
            }
        };
        let offset = reader.read_int16().await?;
        let datetime_offset = SqlDateTimeOffset { datetime2, offset };
        Ok(ColumnValues::DateTimeOffset(datetime_offset))
    }

    async fn read_intn<T>(&self, reader: &mut T, byte_len: u8) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let value: ColumnValues = match byte_len {
            1 => ColumnValues::TinyInt(reader.read_byte().await?), // Some(reader.read_byte().await? as i64),
            2 => ColumnValues::SmallInt(reader.read_int16().await?), // Some(reader.read_int16().await? as i64),
            4 => ColumnValues::Int(reader.read_int32().await?),
            8 => ColumnValues::BigInt(reader.read_int64().await?),
            0 => ColumnValues::Null,
            _ => {
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid IntN length",
                )));
            }
        };
        Ok(value)
    }

    async fn read_money4<T>(&self, reader: &mut T) -> TdsResult<SqlSmallMoney>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let small_money_val = reader.read_int32().await?;
        Ok(small_money_val.into())
    }

    // Reads the TDS 8-byte money value. It is represented in TDS as two 4-byte integers (mixed endian).
    // See comments in MoneyParts definition for more details.
    async fn read_money8<T>(&self, reader: &mut T) -> TdsResult<SqlMoney>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let msb = reader.read_int32().await?;
        let lsb = reader.read_int32().await?;
        Ok(SqlMoney {
            lsb_part: lsb,
            msb_part: msb,
        })
    }

    async fn read_daten<T>(reader: &mut T, length: u8) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        if length == 0 {
            Ok(ColumnValues::Null)
        } else {
            // length == 3.
            Ok(ColumnValues::Date(Self::read_date(reader).await?))
        }
    }

    async fn read_guid<T>(reader: &mut T, length: u8) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        if length > 0 {
            // UUID must be exactly 16 bytes
            if length != 16 {
                return Err(crate::error::Error::ProtocolError(format!(
                    "Invalid GUID length: expected 16 bytes, got {length}"
                )));
            }
            let mut bytes = safe_vec![0u8; length as usize, "read_guid"];
            reader.read_bytes(&mut bytes).await?;
            let unique_id = uuid::Uuid::from_slice_le(&bytes).map_err(|e| {
                crate::error::Error::ProtocolError(format!("Failed to parse UUID: {e}"))
            })?;
            Ok(ColumnValues::Uuid(unique_id))
        } else {
            Ok(ColumnValues::Null)
        }
    }

    async fn decode_vector<T>(
        &self,
        reader: &mut T,
        metadata: &ColumnMetadata,
    ) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        use crate::datatypes::sql_vector::SqlVector;
        use crate::datatypes::sqldatatypes::{
            VECTOR_HEADER_SIZE, VECTOR_MAX_SIZE, VectorBaseType, VectorLayoutFormat,
            VectorLayoutVersion,
        };

        // Read length prefix (USHORTLEN format)
        let length_prefix_value = reader.read_uint16().await? as usize;

        // Handle NULL (length = 0xFFFF)
        if length_prefix_value == 0xFFFF {
            return Ok(ColumnValues::Null);
        }

        // Validate length
        if length_prefix_value > VECTOR_MAX_SIZE {
            return Err(crate::error::Error::ProtocolError(format!(
                "Vector length {} exceeds maximum of {} bytes",
                length_prefix_value, VECTOR_MAX_SIZE
            )));
        }

        // Must have at least header
        if length_prefix_value < VECTOR_HEADER_SIZE {
            return Err(crate::error::Error::ProtocolError(format!(
                "Vector length {} is less than minimum header size of {} bytes",
                length_prefix_value, VECTOR_HEADER_SIZE
            )));
        }

        // Read 8-byte header
        let layout_format_byte = reader.read_byte().await?;
        let layout_version_byte = reader.read_byte().await?;
        let dimension_count = reader.read_uint16().await?;
        let base_type_byte = reader.read_byte().await?;
        let _reserved1 = reader.read_byte().await?; // Reserved
        let _reserved2 = reader.read_byte().await?; // Reserved
        let _reserved3 = reader.read_byte().await?; // Reserved

        // Validate header using enum conversions
        let _layout_format = VectorLayoutFormat::try_from(layout_format_byte)?;
        let _layout_version = VectorLayoutVersion::try_from(layout_version_byte)?;

        // Get base type from metadata's TypeInfoVariant scale field
        let base_type_in_metadata = match &metadata.type_info.type_info_variant {
            TypeInfoVariant::VarLenScale(_, scale) => *scale,
            _ => {
                return Err(crate::error::Error::ProtocolError(
                    "Vector metadata missing scale (base type)".to_string(),
                ));
            }
        };

        if base_type_byte != base_type_in_metadata {
            return Err(crate::error::Error::ProtocolError(format!(
                "Vector base type mismatch: metadata has 0x{:02X}, vector header has 0x{:02X}",
                base_type_in_metadata, base_type_byte
            )));
        }

        // Validate base type using enum conversion
        let base_type = VectorBaseType::try_from(base_type_byte)?;

        let length_in_metadata = metadata.type_info.length;
        // Calculate data length based on vector header info
        let element_size = base_type.element_size_bytes();
        let length_from_vector_header =
            VECTOR_HEADER_SIZE + (dimension_count as usize * element_size);
        if length_prefix_value != length_from_vector_header
            || length_prefix_value != length_in_metadata
        {
            return Err(crate::error::Error::ProtocolError(format!(
                "Vector length mismatch: length in prefix {} bytes, length from vector header {} bytes, length in metadata {} bytes, for {} dimensions (element size: {} bytes)",
                length_prefix_value,
                length_from_vector_header,
                length_in_metadata,
                dimension_count,
                element_size
            )));
        }

        // Read raw element bytes (let SqlVector parse based on base_type)
        let element_bytes = length_prefix_value - VECTOR_HEADER_SIZE;
        let mut raw_bytes = vec![0u8; element_bytes];
        reader.read_bytes(&mut raw_bytes).await?;

        // Create SqlVector - try_from_raw validates header, parses bytes by type, and validates dimensions
        let vector = SqlVector::try_from_raw(
            layout_format_byte,
            layout_version_byte,
            base_type_byte,
            raw_bytes,
        )?;

        Ok(ColumnValues::Vector(vector))
    }

    async fn read_plp_bytes<T>(reader: &mut T) -> TdsResult<Option<Vec<u8>>>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let long_len_i64 = reader.read_int64().await?;
        let long_len = long_len_i64 as u64;

        // If the length is SQL_PLP_NULL, it means the value is NULL.
        if long_len as usize == Self::SQL_PLP_NULL {
            Ok(None)
        } else {
            // If the length is SQL_PLP_UNKNOWNLEN, it means the length is unknown and we have to
            // gather all the chunks until we reach the end of the PLP data which is a zero length
            // chunk.
            let mut vector_capacity = if long_len as usize != Self::SQL_PLP_UNKNOWNLEN {
                let capacity = long_len as usize;
                // Check for overflow or excessively large values
                // If long_len_i64 was negative, casting to u64 then usize can produce huge values
                if long_len_i64 < 0 || capacity > MAX_PLP_SIZE {
                    return Err(crate::error::Error::ProtocolError(format!(
                        "PLP length {capacity} (raw i64: {long_len_i64}) exceeds maximum allowed size of {MAX_PLP_SIZE} bytes"
                    )));
                }
                capacity
            } else {
                0
            };
            let mut plp_buffer = vec![0u8; vector_capacity];
            let mut chunk_len = reader.read_uint32().await? as usize;
            let mut offset: usize = 0;
            let mut chunk_count = 0u32;

            #[cfg(fuzzing)]
            const MAX_PLP_CHUNKS: u32 = 1000;
            #[cfg(not(fuzzing))]
            const MAX_PLP_CHUNKS: u32 = 100000;

            #[cfg(fuzzing)]
            const MAX_CHUNK_SIZE: usize = 8 * 1024; // 8KB per chunk for fuzzing
            #[cfg(not(fuzzing))]
            const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024; // 16MB per chunk normally

            while chunk_len > 0 {
                chunk_count += 1;

                #[cfg(fuzzing)]
                {
                    eprintln!(
                        "[ALLOC] read_plp_bytes: chunk #{chunk_count}, chunk_len={chunk_len}, total_capacity={vector_capacity}"
                    );
                }

                if chunk_count > MAX_PLP_CHUNKS {
                    return Err(crate::error::Error::ProtocolError(format!(
                        "Too many PLP chunks: {chunk_count} (max {MAX_PLP_CHUNKS})"
                    )));
                }

                // Limit individual chunk size
                if chunk_len > MAX_CHUNK_SIZE {
                    return Err(crate::error::Error::ProtocolError(format!(
                        "PLP chunk size {chunk_len} exceeds maximum allowed chunk size of {MAX_CHUNK_SIZE} bytes"
                    )));
                }

                if long_len as usize == Self::SQL_PLP_UNKNOWNLEN {
                    // Use checked_add to prevent capacity overflow
                    vector_capacity = vector_capacity.checked_add(chunk_len).ok_or_else(|| {
                        crate::error::Error::ProtocolError(format!(
                            "PLP chunk accumulation would overflow capacity: {vector_capacity} + {chunk_len}"
                        ))
                    })?;
                    // Validate against MAX_PLP_SIZE after accumulation
                    if vector_capacity > MAX_PLP_SIZE {
                        return Err(crate::error::Error::ProtocolError(format!(
                            "PLP accumulated size {vector_capacity} exceeds maximum allowed size of {MAX_PLP_SIZE} bytes (SQL Server limit: 2GB)"
                        )));
                    }
                    plp_buffer.resize(vector_capacity, 0);
                } else {
                    // For known length, validate that chunk fits within the allocated buffer
                    let end_offset = offset.checked_add(chunk_len).ok_or_else(|| {
                        crate::error::Error::ProtocolError(format!(
                            "PLP chunk offset would overflow: {offset} + {chunk_len}"
                        ))
                    })?;
                    if end_offset > plp_buffer.len() {
                        return Err(crate::error::Error::ProtocolError(format!(
                            "PLP chunk exceeds declared length: offset={offset}, chunk_len={chunk_len}, buffer_len={}, declared_len={long_len}",
                            plp_buffer.len()
                        )));
                    }
                }
                let chunk_size_read = reader
                    .read_bytes(&mut plp_buffer[offset..offset + chunk_len])
                    .await?;
                offset += chunk_size_read;
                chunk_len = reader.read_uint32().await? as usize;
            }
            Ok(Some(plp_buffer))
        }
    }
}

#[async_trait]
impl SqlTypeDecode for GenericDecoder {
    async fn decode<T>(&self, reader: &mut T, metadata: &ColumnMetadata) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let result = match metadata.data_type {
            TdsDataType::Int1 => {
                let value = reader.read_byte().await?;
                ColumnValues::from(value)
            }
            TdsDataType::Int2 => {
                let value = reader.read_int16().await?;
                ColumnValues::SmallInt(value)
            }
            TdsDataType::Int4 => {
                let value = reader.read_int32().await?;
                ColumnValues::from(value)
            }
            TdsDataType::Int8 => {
                let value = reader.read_int64().await?;
                ColumnValues::BigInt(value)
            }
            TdsDataType::Flt4 => {
                let value = reader.read_float32().await?;
                ColumnValues::Real(value)
            }
            TdsDataType::Flt8 => {
                let value = reader.read_float64().await?;
                ColumnValues::Float(value)
            }
            TdsDataType::Money4 => ColumnValues::SmallMoney(self.read_money4(reader).await?),
            TdsDataType::Money => ColumnValues::Money(self.read_money8(reader).await?),
            TdsDataType::MoneyN => {
                let byte_len = reader.read_byte().await?;
                match byte_len {
                    4 => ColumnValues::SmallMoney(self.read_money4(reader).await?),
                    8 => ColumnValues::Money(self.read_money8(reader).await?),
                    0 => ColumnValues::Null,
                    _ => {
                        return Err(crate::error::Error::ProtocolError(format!(
                            "Invalid MoneyN length - {byte_len}"
                        )));
                    }
                }
            }
            TdsDataType::DecimalN => {
                let value = self.read_decimal(reader, metadata).await?;
                match value {
                    Some(value) => ColumnValues::Decimal(value),
                    None => ColumnValues::Null,
                }
            }
            TdsDataType::NumericN => {
                let value = self.read_decimal(reader, metadata).await?;
                match value {
                    Some(value) => ColumnValues::Numeric(value),
                    None => ColumnValues::Null,
                }
            }
            TdsDataType::Bit => {
                let value = reader.read_byte().await?;
                ColumnValues::Bit(value == 1)
            }
            TdsDataType::NChar
            | TdsDataType::NVarChar
            | TdsDataType::BigChar
            | TdsDataType::BigVarChar
            | TdsDataType::Char
            | TdsDataType::VarChar
            | TdsDataType::NText
            | TdsDataType::Text => self.string_decoder.decode(reader, metadata).await?,
            TdsDataType::DateTime => {
                let value = self.read_datetime(reader).await?;
                ColumnValues::DateTime(value)
            }
            TdsDataType::IntN => {
                let byte_len = reader.read_byte().await?;
                self.read_intn(reader, byte_len).await?
            }
            TdsDataType::BigBinary => {
                let length = reader.read_uint16().await?;
                if length as usize > MAX_ALLOC_SIZE {
                    return Err(crate::error::Error::ProtocolError(format!(
                        "BigBinary length {length} exceeds maximum allowed size of {MAX_ALLOC_SIZE} bytes"
                    )));
                }
                let mut bytes = vec![0u8; length as usize];
                reader.read_bytes(&mut bytes).await?;
                ColumnValues::Bytes(bytes)
            }
            TdsDataType::BigVarBinary => {
                if metadata.is_plp() {
                    let some_bytes = GenericDecoder::read_plp_bytes(reader).await?;
                    match some_bytes {
                        Some(bytes) => ColumnValues::Bytes(bytes),
                        None => ColumnValues::Null,
                    }
                } else {
                    let length = reader.read_uint16().await?;
                    if length as usize > MAX_ALLOC_SIZE {
                        return Err(crate::error::Error::ProtocolError(format!(
                            "BigVarBinary length {length} exceeds maximum allowed size of {MAX_ALLOC_SIZE} bytes"
                        )));
                    }
                    let mut bytes = vec![0u8; length as usize];
                    reader.read_bytes(&mut bytes).await?;
                    ColumnValues::Bytes(bytes)
                }
            }
            TdsDataType::Xml => {
                assert!(metadata.is_plp());
                let some_bytes = GenericDecoder::read_plp_bytes(reader).await?;
                match some_bytes {
                    Some(bytes) => ColumnValues::Xml(SqlXml { bytes }),
                    None => ColumnValues::Null,
                }
            }
            TdsDataType::Json => {
                assert!(metadata.is_plp());
                let some_bytes = GenericDecoder::read_plp_bytes(reader).await?;
                match some_bytes {
                    Some(bytes) => ColumnValues::Json(SqlJson::new(bytes)),
                    None => ColumnValues::Null,
                }
            }
            TdsDataType::Vector => self.decode_vector(reader, metadata).await?,
            TdsDataType::BitN => {
                let byte_len = reader.read_byte().await?;
                if byte_len > 0 {
                    let value = reader.read_byte().await?;
                    ColumnValues::Bit(value == 1)
                } else {
                    ColumnValues::Null
                }
            }
            TdsDataType::Guid => {
                let length = reader.read_byte().await?;
                Self::read_guid(reader, length).await?
            }
            TdsDataType::FltN => {
                // This is variable length float, hence the length needs to be read first
                let length = reader.read_byte().await?;
                if length == 0 {
                    return Ok(ColumnValues::Null);
                }
                if length == 4 {
                    let value = reader.read_float32().await?;
                    ColumnValues::Real(value)
                } else {
                    let value = reader.read_float64().await?;
                    ColumnValues::Float(value)
                }
            }
            TdsDataType::DateTimeN => {
                let length = reader.read_byte().await?;
                // If length is 0, then it is NULL
                if length == 0 {
                    return Ok(ColumnValues::Null);
                } else if length == 4 {
                    // SmallDateTime
                    let smalldatetime = self.read_small_datetime(reader).await?;
                    return Ok(ColumnValues::SmallDateTime(smalldatetime));
                } else {
                    // DateTime
                    return Ok(ColumnValues::DateTime(self.read_datetime(reader).await?));
                }
            }
            TdsDataType::DateN => {
                let length = reader.read_byte().await?;
                return Self::read_daten(reader, length).await;
            }
            TdsDataType::TimeN => {
                let length = reader.read_byte().await?;
                match length {
                    0 => return Ok(ColumnValues::Null),
                    _ => {
                        return Ok(ColumnValues::Time(
                            self.read_time(reader, length, metadata.get_scale()).await?,
                        ));
                    }
                }
            }
            TdsDataType::DateTime2N => {
                let length = reader.read_byte().await?;
                match length {
                    0 => Ok(ColumnValues::Null),
                    _ => {
                        self.read_datetime2(reader, length, metadata.get_scale())
                            .await
                    }
                }
            }?,
            TdsDataType::DateTimeOffsetN => {
                let length = reader.read_byte().await?;
                match length {
                    0 => Ok(ColumnValues::Null),
                    _ => {
                        self.read_datetime_offset(reader, length, metadata.get_scale())
                            .await
                    }
                }
            }?,
            TdsDataType::Image => {
                let text_ptr_len = reader.read_byte().await? as usize;

                let length = if text_ptr_len > 0 {
                    const TIMESTAMP_BYTE_COUNT: usize = 8;
                    reader.skip_bytes(text_ptr_len).await?;
                    reader.skip_bytes(TIMESTAMP_BYTE_COUNT).await?;
                    reader.read_uint32().await? as usize
                } else {
                    0
                };

                if length == 0 {
                    ColumnValues::Null
                } else {
                    if length > MAX_ALLOC_SIZE {
                        return Err(crate::error::Error::ProtocolError(format!(
                            "Image length {length} exceeds maximum allowed size of {MAX_ALLOC_SIZE} bytes"
                        )));
                    }
                    let mut buffer = vec![0u8; length];
                    reader.read_bytes(&mut buffer).await?;
                    ColumnValues::Bytes(buffer)
                }
            }
            TdsDataType::Udt => {
                assert!(metadata.is_plp());
                let some_bytes = GenericDecoder::read_plp_bytes(reader).await?;
                match some_bytes {
                    Some(bytes) => ColumnValues::Bytes(bytes),
                    None => ColumnValues::Null,
                }
            }
            TdsDataType::SsVariant => self.read_sql_variant(reader).await?,
            TdsDataType::DateTim4 => {
                let daypart = reader.read_uint16().await?;
                let timepart = reader.read_uint16().await?;
                ColumnValues::SmallDateTime(SqlSmallDateTime {
                    days: daypart,
                    time: timepart,
                })
            }
            TdsDataType::Decimal => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "Fixed-length Decimal type".to_string(),
                    context: format!(
                        "Data type {:?} (0x{:02X}) is not implemented. Use DecimalN instead.",
                        metadata.data_type, metadata.data_type as u8
                    ),
                });
            }
            TdsDataType::Numeric => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: "Fixed-length Numeric type".to_string(),
                    context: format!(
                        "Data type {:?} (0x{:02X}) is not implemented. Use NumericN instead.",
                        metadata.data_type, metadata.data_type as u8
                    ),
                });
            }
            _ => {
                return Err(crate::error::Error::UnimplementedFeature {
                    feature: format!("Data type {:?}", metadata.data_type),
                    context: format!(
                        "Data type {:?} (0x{:02X}) is not yet supported in the decoder",
                        metadata.data_type, metadata.data_type as u8
                    ),
                });
            }
        };
        Ok(result)
    }
}

#[derive(Debug, Default)]
struct StringDecoder {
    // TODO: Make this non-optional
    db_collation: Option<SqlCollation>,
}

impl StringDecoder {
    fn new() -> Self {
        StringDecoder { db_collation: None }
    }

    fn is_long_len_type(data_type: TdsDataType) -> bool {
        matches!(data_type, TdsDataType::NText | TdsDataType::Text)
    }
}

#[async_trait]
impl SqlTypeDecode for StringDecoder {
    async fn decode<T>(&self, reader: &mut T, metadata: &ColumnMetadata) -> TdsResult<ColumnValues>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let encoding_type = get_encoding_type(metadata);

        // If Plp Column. (BIGVARCHARTYPE, BIGVARBINARYTYPE, NVARCHARTYPE with md.length == ushort.max)
        if metadata.is_plp() {
            let some_bytes = GenericDecoder::read_plp_bytes(reader).await?;
            match some_bytes {
                Some(bytes) => Ok(ColumnValues::String(SqlString::new(bytes, encoding_type))),
                None => Ok(ColumnValues::Null),
            }
        } else if Self::is_long_len_type(metadata.data_type) {
            // Legacy LOB types (TEXT/NTEXT/IMAGE) reading implementation
            //
            // WIRE FORMAT (from .NET TdsParser.cs:6517-6600):
            // 1. textptr_len (1 byte): Length of text pointer
            //    - 0x00 = NULL value
            //    - 0x10 (16) = Valid pointer (typical)
            // 2. textptr (textptr_len bytes): Text pointer (usually 16 bytes)
            //    - Server-managed pointer, client treats as opaque
            // 3. timestamp (8 bytes): Row timestamp
            //    - Used for optimistic concurrency
            // 4. data_length (4 bytes, uint32): Actual data length in bytes
            //    - For NTEXT: byte count (divide by 2 for char count)
            //    - For TEXT: byte count in the collation's encoding
            // 5. data (data_length bytes): The actual string data
            //    - For NTEXT: UTF-16LE encoded
            //    - For TEXT: encoded per collation (LCID-based)
            //
            // CURRENT IMPLEMENTATION STATUS:
            // Reads textptr_len (1 byte)
            // Skips textptr (16 bytes) and timestamp (8 bytes)
            // Reads data_length (4 bytes, uint32)
            // Allocates buffer and reads data
            // Creates SqlString with appropriate encoding type
            // NULL handling works (textptr_len = 0)
            // LCID-based decoding implemented (see sql_string.rs)
            //
            // FUTURE ENHANCEMENTS:
            // 1. Add chunked reading for very large TEXT/NTEXT values
            //    - SQL Server may send data in chunks for values > ~8KB
            //    - .NET: TdsParserStateObject.TryReadStringWithEncoding() handles this
            //    - Consider streaming API for large values
            //
            // 2. Validate textptr_len values:
            //    - Common values: 0x00 (NULL), 0x10 (16 bytes)
            //    - Handle unexpected lengths gracefully
            //
            // 3. Add tracing/logging for debugging:
            //    - Log textptr_len, data_length for troubleshooting
            //    - Especially useful for large values or encoding issues
            //
            // 4. Performance optimization:
            //    - Consider pre-allocating common buffer sizes
            //    - Reuse buffers for multiple rows
            //
            // REFERENCES:
            // - .NET: TdsParser.cs:6517 (case TdsEnums.SQLTEXT)
            // - .NET: TdsParser.cs:6548 (case TdsEnums.SQLNTEXT)
            // - .NET: TdsParserStateObject.TryReadStringWithEncoding()
            let text_ptr_len = reader.read_byte().await? as usize;

            let length = if text_ptr_len > 0 {
                const TIMESTAMP_BYTE_COUNT: usize = 8;
                reader.skip_bytes(text_ptr_len).await?;
                reader.skip_bytes(TIMESTAMP_BYTE_COUNT).await?;
                reader.read_uint32().await? as usize
            } else {
                // text_ptr_len == 0 means NULL value
                return Ok(ColumnValues::Null);
            };

            // Empty string (length == 0 but textptr_len > 0) is valid - return empty string, not NULL
            if length > MAX_ALLOC_SIZE {
                return Err(crate::error::Error::ProtocolError(format!(
                    "Text data length {length} exceeds maximum allowed size of {MAX_ALLOC_SIZE} bytes"
                )));
            }

            let sql_string = if length == 0 {
                // Create empty SqlString with appropriate encoding
                SqlString::new(Vec::new(), encoding_type)
            } else {
                let mut buffer = vec![0u8; length];
                reader.read_bytes(&mut buffer).await?;
                SqlString::new(buffer, encoding_type)
            };
            Ok(ColumnValues::String(sql_string))
        } else {
            let length = reader.read_uint16().await? as usize;
            if length == 0xFFFF {
                return Ok(ColumnValues::Null);
            } else {
                let mut buffer = vec![0u8; length];
                reader.read_bytes(&mut buffer).await?;

                let sql_string = SqlString::new(buffer, encoding_type);

                Ok(ColumnValues::String(sql_string))
            }
        }
    }
}

/// TDS representation of Decimal and Numeric types.
#[derive(Clone)]
pub struct DecimalParts {
    pub is_positive: bool,
    pub scale: u8,
    pub precision: u8,
    pub int_parts: Vec<i32>,
}

impl fmt::Display for DecimalParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_decimal_string())
    }
}

impl DecimalParts {
    /// Create a DecimalParts from a decimal string representation.
    ///
    /// # Arguments
    /// * `s` - String like "123.45", "-0.01", "99999999.99"
    /// * `precision` - Total number of digits
    /// * `scale` - Number of digits after decimal point
    ///
    /// # Returns
    /// DecimalParts or Error if parsing fails or precision/scale validation fails
    pub fn from_string(s: &str, precision: u8, scale: u8) -> TdsResult<Self> {
        let s = s.trim();
        let is_positive = !s.starts_with('-');
        let s = s.trim_start_matches('-').trim_start_matches('+');

        // Split on decimal point
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 2 {
            return Err(crate::error::Error::TypeConversionError(format!(
                "Invalid decimal string: {}",
                s
            )));
        }

        let integer_part = parts.first().unwrap_or(&"0");
        let fractional_part = parts.get(1).unwrap_or(&"");

        // Validate that integer and fractional parts contain only digits
        if !integer_part.chars().all(|c| c.is_ascii_digit()) {
            return Err(crate::error::Error::TypeConversionError(format!(
                "Failed to parse decimal '{}': invalid digit found in string",
                s
            )));
        }
        if !fractional_part.is_empty() && !fractional_part.chars().all(|c| c.is_ascii_digit()) {
            return Err(crate::error::Error::TypeConversionError(format!(
                "Failed to parse decimal '{}': invalid digit found in string",
                s
            )));
        }

        // Check scale
        if fractional_part.len() > scale as usize {
            return Err(crate::error::Error::TypeConversionError(format!(
                "Decimal scale {} exceeds target scale {}",
                fractional_part.len(),
                scale
            )));
        }

        // Trim leading zeros from integer part for precision check
        let integer_part_trimmed = integer_part.trim_start_matches('0');
        let integer_part_trimmed = if integer_part_trimmed.is_empty() {
            "0"
        } else {
            integer_part_trimmed
        };

        // Check precision: count actual significant digits (integer part + fractional part)
        let actual_precision = integer_part_trimmed.len() + fractional_part.len();
        if actual_precision > precision as usize {
            return Err(crate::error::Error::TypeConversionError(format!(
                "Decimal precision {} exceeds target precision {}",
                actual_precision, precision
            )));
        }

        // Construct the full number string without decimal point, padded to scale
        let mut full_num = integer_part.to_string();
        full_num.push_str(fractional_part);
        // Pad with zeros if needed to match scale
        full_num.push_str(&"0".repeat(scale as usize - fractional_part.len()));

        // Parse as u128
        let value = full_num.parse::<u128>().map_err(|e| {
            crate::error::Error::TypeConversionError(format!(
                "Failed to parse decimal '{}': {}",
                s, e
            ))
        })?;

        // Convert to int_parts (array of i32)
        let mut int_parts = Vec::new();
        let mut remaining = value;
        while remaining > 0 || int_parts.is_empty() {
            int_parts.push((remaining & 0xFFFFFFFF) as i32);
            remaining >>= 32;
        }

        Ok(DecimalParts {
            is_positive,
            scale,
            precision,
            int_parts,
        })
    }

    /// Create a DecimalParts from an i64 value.
    pub fn from_i64(value: i64, precision: u8, scale: u8) -> TdsResult<Self> {
        let is_positive = value >= 0;
        let abs_value = value.unsigned_abs();

        // Scale the value by multiplying by 10^scale
        let scaled_value = abs_value as u128 * 10u128.pow(scale as u32);

        // Convert to int_parts
        let mut int_parts = Vec::new();
        let mut remaining = scaled_value;
        while remaining > 0 || int_parts.is_empty() {
            int_parts.push((remaining & 0xFFFFFFFF) as i32);
            remaining >>= 32;
        }

        Ok(DecimalParts {
            is_positive,
            scale,
            precision,
            int_parts,
        })
    }

    /// Create a DecimalParts from an f64 value.
    pub fn from_f64(value: f64, precision: u8, scale: u8) -> TdsResult<Self> {
        // Convert f64 to string with appropriate precision
        let s = format!("{:.prec$}", value, prec = scale as usize);
        Self::from_string(&s, precision, scale)
    }

    /// Convert DecimalParts to a string representation suitable for Python Decimal.
    /// Returns a string like "123.45", "-0.01", etc.
    fn to_decimal_string(&self) -> String {
        // Convert int_parts to u128
        // int_parts[0] is the least significant, int_parts[n-1] is most significant
        let u128_value = self
            .int_parts
            .iter()
            .enumerate()
            .fold(0u128, |acc, (i, &part)| {
                acc + ((part as u32 as u128) << (i * 32))
            });

        let value_str = u128_value.to_string();

        // Insert decimal point at the correct position
        let result = if self.scale == 0 {
            value_str
        } else {
            let scale_pos = self.scale as usize;
            if value_str.len() <= scale_pos {
                // Need to pad with leading zeros
                format!("0.{}{}", "0".repeat(scale_pos - value_str.len()), value_str)
            } else {
                let split_pos = value_str.len() - scale_pos;
                format!("{}.{}", &value_str[..split_pos], &value_str[split_pos..])
            }
        };

        if self.is_positive {
            result
        } else {
            format!("-{}", result)
        }
    }

    fn to_f64(&self) -> f64 {
        let u128_value = self
            .int_parts
            .iter()
            .enumerate()
            .fold(0u128, |acc, (i, &part)| {
                acc + ((part as u32 as u128) << (i * 32))
            });

        let mut d_ret: f64 = u128_value as f64;

        d_ret /= 10.0_f64.powi(self.scale as i32);

        if self.is_positive { d_ret } else { -d_ret }
    }
}

impl PartialEq for DecimalParts {
    fn eq(&self, other: &Self) -> bool {
        let min_len = self.int_parts.len().min(other.int_parts.len());
        for i in 0..min_len {
            if self.int_parts[i] != other.int_parts[i] {
                return false;
            }
        }
        if self.int_parts.len() > other.int_parts.len() {
            if self.int_parts[min_len..].iter().any(|&x| x != 0) {
                return false;
            }
        } else if other.int_parts.len() > self.int_parts.len()
            && other.int_parts[min_len..].iter().any(|&x| x != 0)
        {
            return false;
        }
        self.is_positive == other.is_positive
            && self.scale == other.scale
            && self.precision == other.precision
    }
}

impl Debug for DecimalParts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Decimal: {}{} Precision {} Scale {} F64 value: {}",
            if self.is_positive { "" } else { "-" },
            self.int_parts
                .iter()
                .map(|part| part.to_string())
                .collect::<Vec<String>>()
                .join(" "),
            self.precision,
            self.scale,
            self.to_f64()
        )
    }
}

async fn decode_two_propbyte_variant<T>(
    reader: &mut T,
    variant_base_type: u8,
    tds_type: TdsDataType,
    data_length: u32,
) -> TdsResult<ColumnValues>
where
    T: TdsPacketReader + Send + Sync,
{
    Ok(match tds_type {
        // BIGVARBINARYTYPE, BIGBINARYTYPE
        TdsDataType::BigVarBinary | TdsDataType::BigBinary => {
            let _max_length: u16 = reader.read_uint16().await?;
            if data_length as usize > MAX_ALLOC_SIZE {
                return Err(crate::error::Error::ProtocolError(format!(
                    "SQL Variant binary data length {data_length} exceeds maximum allowed size of {MAX_ALLOC_SIZE} bytes"
                )));
            }
            let mut buffer = vec![0u8; data_length as usize];
            reader.read_bytes(&mut buffer).await?;
            ColumnValues::Bytes(buffer)
        }
        TdsDataType::NumericN | TdsDataType::DecimalN => {
            let precision = reader.read_byte().await?;
            let scale = reader.read_byte().await?;
            let decimal_parts =
                GenericDecoder::read_decimal_data(reader, data_length as u8, precision, scale)
                    .await?;

            if matches!(tds_type, TdsDataType::NumericN) {
                match decimal_parts {
                    Some(value) => ColumnValues::Numeric(value),
                    None => ColumnValues::Null,
                }
            } else {
                match decimal_parts {
                    Some(value) => ColumnValues::Decimal(value),
                    None => ColumnValues::Null,
                }
            }
        }
        _ => {
            return Err(crate::error::Error::ProtocolError(format!(
                "Unexpected SQL variant base type for len(2) prop bytes: {variant_base_type:#04X}. Expected binary or numeric types."
            )));
        }
    })
}

async fn decode_seven_propbyte_variant<T>(
    reader: &mut T,
    tds_type: TdsDataType,
    data_length: u32,
) -> TdsResult<ColumnValues>
where
    T: TdsPacketReader + Send + Sync,
{
    assert!(matches!(
        tds_type,
        TdsDataType::BigVarChar | TdsDataType::BigChar | TdsDataType::NVarChar | TdsDataType::NChar
    ));
    let mut collation_bytes = vec![0u8; 5];
    reader.read_bytes(&mut collation_bytes).await?;
    let _max_length = reader.read_uint16().await? as usize;
    let collation: SqlCollation = collation_bytes.as_slice().try_into()?;
    if data_length as usize > MAX_ALLOC_SIZE {
        return Err(crate::error::Error::ProtocolError(format!(
            "SQL Variant string data length {data_length} exceeds maximum allowed size of {MAX_ALLOC_SIZE} bytes"
        )));
    }
    let mut buffer = vec![0u8; data_length as usize];
    reader.read_bytes(&mut buffer).await?;
    let encoding = if matches!(tds_type, TdsDataType::NVarChar | TdsDataType::NChar) {
        EncodingType::Utf16
    } else if collation.utf8() {
        EncodingType::Utf8
    } else {
        EncodingType::LcidBased(collation)
    };
    let sql_string = SqlString::new(buffer, encoding);
    Ok(ColumnValues::String(sql_string))
}

#[cfg(test)]
mod test {
    use crate::datatypes::{
        column_values::ColumnValues,
        decoder::{
            DecimalParts, GenericDecoder, MAX_ALLOC_SIZE, StringDecoder, validate_alloc_size,
        },
        sqldatatypes::TdsDataType,
    };

    #[test]
    fn test_f64_conversion() {
        let expected: f64 = 123456.322;

        // Represents 123456.322 as observed over TDS wire.
        let int_parts = vec![-539269688, 2];
        let parts = DecimalParts {
            is_positive: true,
            scale: 5,
            precision: 18,
            int_parts,
        };

        assert_eq!(expected, parts.to_f64());
    }

    #[test]
    fn test_f64_conversion_negative() {
        let expected: f64 = -123456.322;

        // Represents -123456.322 as observed over TDS wire.
        let int_parts = vec![-539269688, 2];
        let parts = DecimalParts {
            is_positive: false,
            scale: 5,
            precision: 18,
            int_parts,
        };

        assert_eq!(expected, parts.to_f64());
    }

    #[test]
    fn test_f64_conversion_zero() {
        let expected: f64 = 0.0;

        let int_parts = vec![0];
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts,
        };

        assert_eq!(expected, parts.to_f64());
    }

    #[test]
    fn test_f64_conversion_large_number() {
        // Test conversion with larger numbers
        let int_parts = vec![100000, 0];
        let parts = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 7,
            int_parts,
        };

        let result = parts.to_f64();
        // With scale of 2, int value 100000 should become 1000.00
        assert!((result - 1000.0).abs() < 0.01);
    }

    #[test]
    fn test_decimal_parts_with_multiple_int_parts() {
        // Test with multiple integer parts to ensure full conversion
        let int_parts = vec![1000000000, 1];
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 19,
            int_parts,
        };

        // Should successfully convert to f64
        let result = parts.to_f64();
        assert!(result > 0.0);
    }

    #[test]
    fn test_u8_to_column_values() {
        let value: u8 = 123;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::TinyInt(v) => assert_eq!(v, 123),
            _ => panic!("Expected TinyInt variant"),
        }
    }

    #[test]
    fn test_i32_to_column_values() {
        let value: i32 = 12345;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::Int(v) => assert_eq!(v, 12345),
            _ => panic!("Expected Int variant"),
        }
    }

    #[test]
    fn test_i32_negative_to_column_values() {
        let value: i32 = -12345;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::Int(v) => assert_eq!(v, -12345),
            _ => panic!("Expected Int variant"),
        }
    }

    #[test]
    fn test_generic_decoder_default() {
        let decoder = GenericDecoder::default();
        // Just verify it can be created
        assert!(std::mem::size_of_val(&decoder) > 0);
    }

    #[test]
    fn test_string_decoder_default() {
        let decoder = StringDecoder::default();
        // Just verify it can be created
        assert!(std::mem::size_of_val(&decoder) > 0);
    }

    #[test]
    fn test_decimal_parts_debug() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![123, 456],
        };
        let debug_str = format!("{parts:?}");
        // Just verify the debug trait works - don't assert on exact format
        assert!(!debug_str.is_empty());
    }

    #[test]
    fn test_generic_decoder_constants() {
        assert_eq!(GenericDecoder::SHORTLEN_MAXVALUE, 65535);
        assert_eq!(GenericDecoder::SQL_PLP_NULL, 0xffffffffffffffff);
        assert_eq!(GenericDecoder::SQL_PLP_UNKNOWNLEN, 0xfffffffffffffffe);
    }

    #[test]
    fn test_decimal_parts_scale_precision() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 5,
            precision: 18,
            int_parts: vec![100000],
        };

        // Test that scale affects the decimal conversion
        let result = parts.to_f64();
        // With scale of 5, int value 100000 should become 1.00000
        assert!((result - 1.0).abs() < 0.00001);
    }

    #[test]
    fn test_decimal_parts_empty_int_parts() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts: vec![],
        };

        let result = parts.to_f64();
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_decimal_parts_single_int_part() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 5,
            int_parts: vec![12345],
        };

        let result = parts.to_f64();
        assert_eq!(result, 12345.0);
    }

    #[test]
    fn test_column_values_from_u8_zero() {
        let value: u8 = 0;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::TinyInt(v) => assert_eq!(v, 0),
            _ => panic!("Expected TinyInt variant"),
        }
    }

    #[test]
    fn test_column_values_from_u8_max() {
        let value: u8 = 255;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::TinyInt(v) => assert_eq!(v, 255),
            _ => panic!("Expected TinyInt variant"),
        }
    }

    #[test]
    fn test_column_values_from_i32_zero() {
        let value: i32 = 0;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::Int(v) => assert_eq!(v, 0),
            _ => panic!("Expected Int variant"),
        }
    }

    #[test]
    fn test_column_values_from_i32_max() {
        let value: i32 = i32::MAX;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::Int(v) => assert_eq!(v, i32::MAX),
            _ => panic!("Expected Int variant"),
        }
    }

    #[test]
    fn test_column_values_from_i32_min() {
        let value: i32 = i32::MIN;
        let col_val: ColumnValues = value.into();
        match col_val {
            ColumnValues::Int(v) => assert_eq!(v, i32::MIN),
            _ => panic!("Expected Int variant"),
        }
    }

    #[test]
    fn test_validate_alloc_size_within_limit() {
        let result = validate_alloc_size(1024, "test_allocation");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_alloc_size_at_limit() {
        let result = validate_alloc_size(MAX_ALLOC_SIZE, "test_at_limit");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_alloc_size_exceeds_limit() {
        let result = validate_alloc_size(MAX_ALLOC_SIZE + 1, "test_exceeds");
        assert!(result.is_err());
        if let Err(e) = result {
            let error_msg = format!("{e:?}");
            assert!(error_msg.contains("exceeds maximum allowed"));
        }
    }

    #[test]
    fn test_validate_alloc_size_zero() {
        let result = validate_alloc_size(0, "test_zero");
        assert!(result.is_ok());
    }

    #[test]
    fn test_decimal_parts_equality_same() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100, 200],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100, 200],
        };
        assert_eq!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_equality_different_sign() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        let parts2 = DecimalParts {
            is_positive: false,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        assert_ne!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_equality_different_scale() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 3,
            precision: 10,
            int_parts: vec![100],
        };
        assert_ne!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_equality_different_precision() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 12,
            int_parts: vec![100],
        };
        assert_ne!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_equality_different_length_with_zeros() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100, 0, 0],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        assert_eq!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_equality_different_length_with_nonzeros() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100, 200],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        assert_ne!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_debug_format_positive() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![12345],
        };
        let debug_str = format!("{parts:?}");
        assert!(debug_str.contains("Decimal:"));
        assert!(debug_str.contains("12345"));
        assert!(debug_str.contains("Precision 10"));
        assert!(debug_str.contains("Scale 2"));
        assert!(!debug_str.starts_with("Decimal: -"));
    }

    #[test]
    fn test_decimal_parts_debug_format_negative() {
        let parts = DecimalParts {
            is_positive: false,
            scale: 3,
            precision: 15,
            int_parts: vec![54321],
        };
        let debug_str = format!("{parts:?}");
        assert!(debug_str.contains("Decimal: -"));
        assert!(debug_str.contains("54321"));
        assert!(debug_str.contains("Precision 15"));
        assert!(debug_str.contains("Scale 3"));
    }

    #[test]
    fn test_decimal_parts_debug_format_multiple_parts() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 20,
            int_parts: vec![100, 200, 300],
        };
        let debug_str = format!("{parts:?}");
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("200"));
        assert!(debug_str.contains("300"));
    }

    #[test]
    fn test_f64_conversion_high_scale() {
        let int_parts = vec![12345];
        let parts = DecimalParts {
            is_positive: true,
            scale: 10,
            precision: 15,
            int_parts,
        };
        let result = parts.to_f64();
        // With scale of 10, 12345 should become 0.0000012345
        assert!((result - 0.0000012345).abs() < 0.0000000001);
    }

    #[test]
    fn test_f64_conversion_single_zero() {
        let parts = DecimalParts {
            is_positive: true,
            scale: 5,
            precision: 10,
            int_parts: vec![0],
        };
        let result = parts.to_f64();
        assert_eq!(result, 0.0);
    }

    #[test]
    fn test_f64_conversion_negative_zero() {
        let parts = DecimalParts {
            is_positive: false,
            scale: 0,
            precision: 1,
            int_parts: vec![0],
        };
        let result = parts.to_f64();
        assert_eq!(result, -0.0);
    }

    #[test]
    fn test_string_decoder_new() {
        let decoder = StringDecoder::new();
        assert!(decoder.db_collation.is_none());
    }

    #[test]
    fn test_string_decoder_is_long_len_type_ntext() {
        assert!(StringDecoder::is_long_len_type(TdsDataType::NText));
    }

    #[test]
    fn test_string_decoder_is_long_len_type_text() {
        assert!(StringDecoder::is_long_len_type(TdsDataType::Text));
    }

    #[test]
    fn test_string_decoder_is_long_len_type_not_long() {
        assert!(!StringDecoder::is_long_len_type(TdsDataType::NVarChar));
        assert!(!StringDecoder::is_long_len_type(TdsDataType::BigVarChar));
        assert!(!StringDecoder::is_long_len_type(TdsDataType::Int4));
    }

    #[test]
    fn test_decimal_parts_f64_conversion_with_many_int_parts() {
        // Test with 3 int parts
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 30,
            int_parts: vec![1, 2, 3],
        };
        let result = parts.to_f64();
        // Just verify it doesn't panic and produces a value
        assert!(result > 0.0);
    }

    #[test]
    fn test_decimal_parts_equality_reversed_order() {
        // Test that order matters for trailing zeros
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![100],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 2,
            precision: 10,
            int_parts: vec![0, 100],
        };
        // These should not be equal as trailing zeros are in different positions
        assert_ne!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_equality_both_empty() {
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts: vec![],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts: vec![],
        };
        assert_eq!(parts1, parts2);
    }

    #[test]
    fn test_validate_alloc_size_mid_range() {
        // Test allocation in the middle range
        let result = validate_alloc_size(MAX_ALLOC_SIZE / 2, "test_mid_range");
        assert!(result.is_ok());
    }

    #[test]
    fn test_decimal_parts_f64_negative_with_scale() {
        // Test negative number with scale
        let parts = DecimalParts {
            is_positive: false,
            scale: 3,
            precision: 10,
            int_parts: vec![123456],
        };
        let result = parts.to_f64();
        assert!((result + 123.456).abs() < 0.001);
    }

    #[test]
    fn test_decimal_parts_equality_one_empty_one_zero() {
        // Test equality between empty vec and vec with zero
        let parts1 = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts: vec![],
        };
        let parts2 = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts: vec![0],
        };
        // Empty should equal to [0]
        assert_eq!(parts1, parts2);
    }

    #[test]
    fn test_decimal_parts_debug_with_zero() {
        // Test Debug formatting with zero value
        let parts = DecimalParts {
            is_positive: true,
            scale: 0,
            precision: 1,
            int_parts: vec![0],
        };
        let debug_str = format!("{parts:?}");
        assert!(debug_str.contains("0"));
        assert!(debug_str.contains("F64 value: 0"));
    }

    // Tests for DecimalParts::from_string
    #[test]
    fn test_from_string_positive_decimal() {
        let result = DecimalParts::from_string("123.45", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert!(parts.is_positive);
        assert_eq!(parts.scale, 2);
        assert_eq!(parts.precision, 10);
        assert_eq!(parts.to_decimal_string(), "123.45");
    }

    #[test]
    fn test_from_string_negative_decimal() {
        let result = DecimalParts::from_string("-123.45", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert!(!parts.is_positive);
        assert_eq!(parts.scale, 2);
        assert_eq!(parts.precision, 10);
        assert_eq!(parts.to_decimal_string(), "-123.45");
    }

    #[test]
    fn test_from_string_integer_no_decimal_point() {
        let result = DecimalParts::from_string("12345", 10, 0);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert!(parts.is_positive);
        assert_eq!(parts.scale, 0);
        assert_eq!(parts.to_decimal_string(), "12345");
    }

    #[test]
    fn test_from_string_with_leading_zeros() {
        let result = DecimalParts::from_string("00123.45", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "123.45");
    }

    #[test]
    fn test_from_string_small_fractional_value() {
        let result = DecimalParts::from_string("0.01", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "0.01");
    }

    #[test]
    fn test_from_string_zero() {
        let result = DecimalParts::from_string("0", 10, 0);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "0");
    }

    #[test]
    fn test_from_string_zero_with_scale() {
        let result = DecimalParts::from_string("0.00", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "0.00");
    }

    #[test]
    fn test_from_string_fractional_padding() {
        // "1.5" with scale 3 should be treated as "1.500"
        let result = DecimalParts::from_string("1.5", 10, 3);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "1.500");
    }

    #[test]
    fn test_from_string_max_precision_38_digits() {
        let value = "12345678901234567890123456789012345678";
        let result = DecimalParts::from_string(value, 38, 0);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), value);
    }

    #[test]
    fn test_from_string_high_scale() {
        let result = DecimalParts::from_string("123.456789", 10, 6);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "123.456789");
    }

    #[test]
    fn test_from_string_leading_zeros_precision_check() {
        // "00001.00" should have precision of 3 (1 significant digit + 2 scale)
        let result = DecimalParts::from_string("00001.00", 5, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "1.00");
    }

    #[test]
    fn test_from_string_with_positive_sign() {
        let result = DecimalParts::from_string("+123.45", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert!(parts.is_positive);
        assert_eq!(parts.to_decimal_string(), "123.45");
    }

    // Error cases
    #[test]
    fn test_from_string_invalid_characters() {
        let result = DecimalParts::from_string("not_a_number", 10, 2);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("invalid digit"));
    }

    #[test]
    fn test_from_string_multiple_decimal_points() {
        let result = DecimalParts::from_string("123.45.67", 10, 2);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("Invalid decimal string"));
    }

    #[test]
    fn test_from_string_scale_exceeded() {
        // Trying to parse "123.456" with scale 2 should fail
        let result = DecimalParts::from_string("123.456", 10, 2);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("scale") && error_msg.contains("exceeds"));
    }

    #[test]
    fn test_from_string_precision_exceeded() {
        // "12345" has 5 significant digits, should fail with precision 4
        let result = DecimalParts::from_string("12345", 4, 0);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("precision") && error_msg.contains("exceeds"));
    }

    #[test]
    fn test_from_string_precision_exceeded_with_decimal() {
        // "123.45" has 5 significant digits total, should fail with precision 4
        let result = DecimalParts::from_string("123.45", 4, 2);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("precision") && error_msg.contains("exceeds"));
    }

    #[test]
    fn test_from_string_invalid_digit_in_integer_part() {
        let result = DecimalParts::from_string("12a34", 10, 0);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("invalid digit"));
    }

    #[test]
    fn test_from_string_invalid_digit_in_fractional_part() {
        let result = DecimalParts::from_string("123.4x5", 10, 2);
        assert!(result.is_err());
        let error_msg = format!("{:?}", result.unwrap_err());
        assert!(error_msg.contains("invalid digit"));
    }

    #[test]
    fn test_from_string_leading_zeros_not_counted_in_precision() {
        // "0000123" should be treated as precision 3, not 7
        let result = DecimalParts::from_string("0000123", 3, 0);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "123");
    }

    #[test]
    fn test_from_string_whitespace_trimmed() {
        let result = DecimalParts::from_string("  123.45  ", 10, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "123.45");
    }

    #[test]
    fn test_from_string_negative_zero() {
        let result = DecimalParts::from_string("-0", 10, 0);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert!(!parts.is_positive);
        assert_eq!(parts.to_decimal_string(), "-0");
    }

    #[test]
    fn test_from_string_only_zeros_with_decimal() {
        let result = DecimalParts::from_string("0.0", 10, 1);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "0.0");
    }

    #[test]
    fn test_from_string_exact_precision_match() {
        // Test when actual precision exactly matches target precision
        let result = DecimalParts::from_string("12345", 5, 0);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "12345");
    }

    #[test]
    fn test_from_string_exact_scale_match() {
        // Test when fractional digits exactly match scale
        let result = DecimalParts::from_string("123.45", 5, 2);
        assert!(result.is_ok());
        let parts = result.unwrap();
        assert_eq!(parts.to_decimal_string(), "123.45");
    }

    // Vector deserialization tests
    mod vector_tests {
        use super::*;
        use crate::datatypes::{
            sql_vector::SqlVector,
            sqldatatypes::{
                VECTOR_MAX_DIMENSIONS, VectorBaseType, VectorLayoutFormat, VectorLayoutVersion,
            },
        };

        #[test]
        fn test_vector_creation_and_validation() {
            // Test that SqlVector::try_from_f32 creates valid vectors
            let dimensions = vec![1.0, 2.0, 3.0];
            let vector = SqlVector::try_from_f32(dimensions.clone()).unwrap();

            // Only check semantic data - TDS header fields are not stored
            assert_eq!(vector.as_f32(), Some(dimensions.as_slice()));
            assert_eq!(vector.dimension_count(), 3);
        }

        #[test]
        fn test_vector_single_dimension() {
            let vector = SqlVector::try_from_f32(vec![42.5]).unwrap();
            assert_eq!(vector.as_f32(), Some(&[42.5][..]));
            assert_eq!(vector.dimension_count(), 1);
        }

        #[test]
        fn test_vector_max_dimensions() {
            let dimensions: Vec<f32> = (0..VECTOR_MAX_DIMENSIONS).map(|i| i as f32).collect();
            let vector = SqlVector::try_from_f32(dimensions).unwrap();
            assert_eq!(vector.dimension_count(), VECTOR_MAX_DIMENSIONS);
        }

        #[test]
        fn test_vector_from_raw_valid() {
            let values = vec![1.0_f32, 2.0, 3.0];
            // Convert f32 values to raw bytes
            let mut raw_bytes = Vec::new();
            for val in &values {
                raw_bytes.extend_from_slice(&val.to_le_bytes());
            }

            let vector = SqlVector::try_from_raw(
                VectorLayoutFormat::V1 as u8,
                VectorLayoutVersion::V1 as u8,
                VectorBaseType::Float32 as u8,
                raw_bytes,
            );

            // try_from_raw validates during construction
            assert!(vector.is_ok());
            let vector = vector.unwrap();
            assert_eq!(vector.as_f32(), Some(values.as_slice()));
        }

        #[test]
        fn test_vector_from_raw_invalid_layout_format() {
            let values = vec![1.0_f32, 2.0];
            let mut raw_bytes = Vec::new();
            for val in &values {
                raw_bytes.extend_from_slice(&val.to_le_bytes());
            }

            let result = SqlVector::try_from_raw(
                0x00, // Invalid format
                VectorLayoutVersion::V1 as u8,
                VectorBaseType::Float32 as u8,
                raw_bytes,
            );

            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("layout format"));
        }

        #[test]
        fn test_vector_from_raw_invalid_layout_version() {
            let values = vec![1.0_f32, 2.0];
            let mut raw_bytes = Vec::new();
            for val in &values {
                raw_bytes.extend_from_slice(&val.to_le_bytes());
            }

            let result = SqlVector::try_from_raw(
                VectorLayoutFormat::V1 as u8,
                0x99, // Invalid version
                VectorBaseType::Float32 as u8,
                raw_bytes,
            );

            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("layout version"));
        }

        #[test]
        fn test_vector_from_raw_invalid_base_type() {
            let values = vec![1.0_f32, 2.0];
            let mut raw_bytes = Vec::new();
            for val in &values {
                raw_bytes.extend_from_slice(&val.to_le_bytes());
            }

            let result = SqlVector::try_from_raw(
                VectorLayoutFormat::V1 as u8,
                VectorLayoutVersion::V1 as u8,
                0x99, // Invalid base type
                raw_bytes,
            );

            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("base type"));
        }

        #[test]
        fn test_vector_empty_dimensions() {
            let result = SqlVector::try_from_f32(vec![]);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("at least one dimension")
            );
        }

        #[test]
        fn test_vector_too_many_dimensions() {
            let dimensions: Vec<f32> = (0..(VECTOR_MAX_DIMENSIONS + 1)).map(|i| i as f32).collect();
            let result = SqlVector::try_from_f32(dimensions);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
        }

        #[test]
        fn test_vector_total_size() {
            let vector = SqlVector::try_from_f32(vec![1.0, 2.0, 3.0]).unwrap();
            assert_eq!(vector.total_size(), 8 + 3 * 4); // 8 byte header + 3 floats * 4 bytes
        }

        #[test]
        fn test_column_values_vector_variant() {
            let vector = SqlVector::try_from_f32(vec![1.0, 2.0, 3.0]).unwrap();
            let col_val = ColumnValues::Vector(vector);

            match col_val {
                ColumnValues::Vector(v) => {
                    assert_eq!(v.dimension_count(), 3);
                    assert_eq!(v.as_f32(), Some(&[1.0, 2.0, 3.0][..]));
                }
                _ => panic!("Expected Vector variant"),
            }
        }
    }
}
