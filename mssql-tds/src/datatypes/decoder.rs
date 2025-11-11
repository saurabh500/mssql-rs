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
    read_write::packet_reader::TdsPacketReader,
};
use crate::{query::metadata::ColumnMetadata, token::tokens::SqlCollation};

// Maximum reasonable allocation size for a single value (100MB)
// This prevents fuzzer-induced capacity overflow panics
const MAX_ALLOC_SIZE: usize = 100 * 1024 * 1024;

// Maximum allocation size for PLP (Partial Length Pointer) types
// SQL Server supports PLP types up to 2GB (i32::MAX is approximately 2.1GB)
const MAX_PLP_SIZE: usize = i32::MAX as usize;

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
                    "Invalid SQL_VARIANT: 1-byte property is only valid for TimeN, DateTime2N, and DateTimeOffsetN types, but got: {:?}",
                    tds_type
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
        let mut int_parts = vec![0i32; number_of_int_parts as usize];
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
        let nanoseconds = match byte_len {
            3 => reader.read_uint24().await? as u64,
            4 => reader.read_uint32().await? as u64,
            _ => reader.read_uint40().await?,
        };
        Ok(SqlTime {
            time_nanoseconds: nanoseconds,
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
                    "Internal error: read_datetime2 returned unexpected type: {:?}",
                    datetime2
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
            let mut bytes = vec![0u8; length as usize];
            reader.read_bytes(&mut bytes).await?;
            let unique_id = uuid::Uuid::from_slice_le(&bytes).map_err(|e| {
                crate::error::Error::ProtocolError(format!("Failed to parse UUID: {e}"))
            })?;
            Ok(ColumnValues::Uuid(unique_id))
        } else {
            Ok(ColumnValues::Null)
        }
    }

    async fn read_plp_bytes<T>(reader: &mut T) -> TdsResult<Option<Vec<u8>>>
    where
        T: TdsPacketReader + Send + Sync,
    {
        let long_len = reader.read_int64().await? as u64;

        // If the length is SQL_PLP_NULL, it means the value is NULL.
        if long_len as usize == Self::SQL_PLP_NULL {
            Ok(None)
        } else {
            // If the length is SQL_PLP_UNKNOWNLEN, it means the length is unknown and we have to
            // gather all the chunks until we reach the end of the PLP data which is a zero length
            // chunk.
            let mut vector_capacity = if long_len as usize != Self::SQL_PLP_UNKNOWNLEN {
                let capacity = long_len as usize;
                // Validate the capacity before allocating
                if capacity > MAX_PLP_SIZE {
                    return Err(crate::error::Error::ProtocolError(format!(
                        "PLP length {capacity} exceeds maximum allowed size of {MAX_PLP_SIZE} bytes (SQL Server limit: 2GB)"
                    )));
                }
                capacity
            } else {
                0
            };
            let mut plp_buffer = vec![0u8; vector_capacity];
            let mut chunk_len = reader.read_uint32().await? as usize;
            let mut offset: usize = 0;
            while chunk_len > 0 {
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
            // If it is a long length type (NText, Text), read the length as uint16.
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
                return Ok(ColumnValues::Null);
            } else {
                let mut buffer = vec![0u8; length];
                reader.read_bytes(&mut buffer).await?;
                let sql_string = SqlString::new(buffer, encoding_type);
                Ok(ColumnValues::String(sql_string))
            }
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

impl DecimalParts {
    fn to_f64(&self) -> f64 {
        let u128_value = self
            .int_parts
            .iter()
            .rev()
            .enumerate()
            .fold(0u128, |acc, (i, &part)| {
                (acc << (i * 32)) + (part as u32 as u128)
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
        decoder::{DecimalParts, GenericDecoder, StringDecoder},
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
}
