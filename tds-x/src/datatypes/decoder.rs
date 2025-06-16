use async_trait::async_trait;
use core::fmt;
use std::{fmt::Debug, io::Error, vec};

use super::{
    sql_string::{get_encoding_type, SqlString},
    sqldatatypes::{TdsDataType, TypeInfoVariant},
};
use crate::datatypes::column_values::{ColumnValues, SqlXml};
use crate::datatypes::sqldatatypes::TypeInfo;
use crate::{
    core::TdsResult,
    datatypes::{sql_json::SqlJson, sql_string::EncodingType, sqldatatypes::FixedLengthTypes},
};
use crate::{
    query::metadata::ColumnMetadata, read_write::packet_reader::PacketReader,
    token::tokens::SqlCollation,
};

#[async_trait]
pub(crate) trait SqlTypeDecode<'a> {
    async fn decode(
        &self,
        reader: &'a mut PacketReader,
        metadata: &ColumnMetadata,
    ) -> TdsResult<ColumnValues>;
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
    async fn read_sql_variant(&self, reader: &mut PacketReader<'_>) -> TdsResult<ColumnValues> {
        let length = reader.read_uint32().await?;
        let variant_base_type = reader.read_byte().await?;
        let tds_type = TdsDataType::try_from(variant_base_type)?;
        let variant_prop_bytes = reader.read_byte().await?;
        let bytes_for_type_and_properties_byte = 2;
        let data_length = length - bytes_for_type_and_properties_byte - (variant_prop_bytes as u32);

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
                unreachable!("Unexpected variant properties length: {}. This shouldn't have happened. Did the server send the wrong payload?", variant_prop_bytes);
            }
        };
        Ok(col_value)
    }

    async fn decode_zero_propbyte_variant(
        &self,
        reader: &mut PacketReader<'_>,
        tds_type: TdsDataType,
        data_length: u32,
    ) -> Result<ColumnValues, crate::error::Error> {
        let fixed_length_type_result = FixedLengthTypes::try_from(tds_type);

        // The type may be a fixed length type, or it may be a variable length type like Guid/DateN
        if let Ok(fixed_length_type) = fixed_length_type_result {
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
        } else {
            // If the type is not a fixed length type, we should not reach here.
            match tds_type {
                TdsDataType::Guid => Self::read_guid(reader, data_length as u8).await,
                TdsDataType::DateN => Self::read_daten(reader, data_length as u8).await,
                _ => {
                    unreachable!(
                        "For 0 byte property, only Guid and DateN are expected, but got: {:?}",
                        tds_type
                    );
                }
            }
        }
    }

    async fn decode_one_byte_variant(
        &self,
        reader: &mut PacketReader<'_>,
        tds_type: TdsDataType,
        data_length: u32,
    ) -> TdsResult<ColumnValues> {
        let _scale = reader.read_byte().await?;
        Ok(match tds_type {
            TdsDataType::TimeN => {
                let time_nanos = self.read_time(reader, data_length as u8).await?;
                ColumnValues::Time(time_nanos)
            }
            TdsDataType::DateTime2N => self.read_datetime2(reader, data_length as u8).await?,
            TdsDataType::DateTimeOffsetN => {
                self.read_datetime_offset(reader, data_length as u8).await?
            }
            _ => {
                unreachable!("For 1 byte property, only TimeN, DateTime2N and DateTimeOffsetN are expected, but got: {:?}", tds_type);
            }
        })
    }

    async fn read_decimal(
        &self,
        reader: &mut PacketReader<'_>,
        metadata: &ColumnMetadata,
    ) -> TdsResult<Option<DecimalParts>> {
        // Decimal/numeric data type has 1 byte length.
        let length = reader.read_byte().await?;
        if let TypeInfoVariant::VarLenPrecisionScale(_, _, precision, scale) =
            metadata.type_info.type_info_variant
        {
            GenericDecoder::read_decimal_data(reader, length, precision, scale).await
        } else {
            unreachable!("Should never get here")
        }
    }

    async fn read_decimal_data(
        reader: &mut PacketReader<'_>,
        length: u8,
        precision: u8,
        scale: u8,
    ) -> TdsResult<Option<DecimalParts>> {
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

    async fn read_datetime(&self, reader: &mut PacketReader<'_>) -> TdsResult<(i32, u32)> {
        let days = reader.read_int32().await?;
        let ticks = reader.read_uint32().await?;

        Ok((days, ticks))
    }

    async fn read_small_datetime(&self, reader: &mut PacketReader<'_>) -> TdsResult<(u16, u16)> {
        let days = reader.read_uint16().await?;
        let minutes = reader.read_uint16().await?;
        Ok((days, minutes))
    }

    async fn read_date(reader: &mut PacketReader<'_>) -> TdsResult<u32> {
        let days = reader.read_uint24().await?;
        Ok(days)
    }

    async fn read_time(&self, reader: &mut PacketReader<'_>, byte_len: u8) -> TdsResult<u64> {
        let nanoseconds = match byte_len {
            3 => reader.read_uint24().await? as u64,
            4 => reader.read_uint32().await? as u64,
            _ => reader.read_uint40().await?,
        };
        Ok(nanoseconds)
    }

    async fn read_datetime2(
        &self,
        reader: &mut PacketReader<'_>,
        byte_len: u8,
    ) -> TdsResult<ColumnValues> {
        let days = Self::read_date(reader).await?;
        let time_nanos = self.read_time(reader, byte_len - 3).await?;

        Ok(ColumnValues::DateTime2 { days, time_nanos })
    }

    async fn read_datetime_offset(
        &self,
        reader: &mut PacketReader<'_>,
        byte_len: u8,
    ) -> TdsResult<ColumnValues> {
        let days = Self::read_date(reader).await?;
        let time_nanos = self.read_time(reader, byte_len - 3).await?;
        let offset = reader.read_int16().await?;

        Ok(ColumnValues::DateTimeOffset {
            days,
            time_nanos,
            offset,
        })
    }

    async fn read_intn(
        &self,
        reader: &mut PacketReader<'_>,
        byte_len: u8,
    ) -> TdsResult<ColumnValues> {
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

    async fn read_money4(&self, reader: &mut PacketReader<'_>) -> TdsResult<MoneyParts> {
        let small_money_val = reader.read_int32().await?;
        Ok(small_money_val.into())
    }

    // Reads the TDS 8-byte money value. It is represented in TDS as two 4-byte integers (mixed endian).
    // See comments in MoneyParts definition for more details.
    async fn read_money8(&self, reader: &mut PacketReader<'_>) -> TdsResult<MoneyParts> {
        let msb = reader.read_int32().await?;
        let lsb = reader.read_int32().await?;
        Ok((lsb, msb).into())
    }

    async fn read_moneyn(
        &self,
        reader: &mut PacketReader<'_>,
        byte_len: u8,
    ) -> TdsResult<Option<MoneyParts>> {
        let value: Option<MoneyParts> = match byte_len {
            4 => Some(self.read_money4(reader).await?),
            8 => Some(self.read_money8(reader).await?),
            0 => None,
            _ => {
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid MoneyN length - {}", byte_len),
                )));
            }
        };
        Ok(value)
    }

    async fn read_daten(reader: &mut PacketReader<'_>, length: u8) -> TdsResult<ColumnValues> {
        if length == 0 {
            Ok(ColumnValues::Null)
        } else {
            // length == 3.
            Ok(ColumnValues::Date(Self::read_date(reader).await?))
        }
    }

    async fn read_guid(reader: &mut PacketReader<'_>, length: u8) -> TdsResult<ColumnValues> {
        if length > 0 {
            let mut bytes = vec![0u8; length as usize];
            reader.read_bytes(&mut bytes).await?;
            let unique_id = uuid::Uuid::from_slice_le(&bytes).unwrap();
            Ok(ColumnValues::Uuid(unique_id))
        } else {
            Ok(ColumnValues::Null)
        }
    }

    async fn read_plp_bytes(reader: &mut PacketReader<'_>) -> TdsResult<Option<Vec<u8>>> {
        let long_len = reader.read_int64().await? as u64;

        // If the length is SQL_PLP_NULL, it means the value is NULL.
        if long_len as usize == Self::SQL_PLP_NULL {
            Ok(None)
        } else {
            // If the length is SQL_PLP_UNKNOWNLEN, it means the length is unknown and we have to
            // gather all the chunks until we reach the end of the PLP data which is a zero length
            // chunk.
            let mut vector_capacity = if long_len as usize != Self::SQL_PLP_UNKNOWNLEN {
                long_len as usize
            } else {
                0
            };
            let mut plp_buffer = vec![0u8; vector_capacity];
            let mut chunk_len = reader.read_uint32().await? as usize;
            let mut offset = 0;
            while chunk_len > 0 {
                if long_len as usize == Self::SQL_PLP_UNKNOWNLEN {
                    vector_capacity += chunk_len;
                    plp_buffer.resize(vector_capacity, 0);
                };
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
impl<'a> SqlTypeDecode<'a> for GenericDecoder {
    async fn decode(
        &self,
        reader: &'a mut PacketReader,
        metadata: &ColumnMetadata,
    ) -> TdsResult<ColumnValues> {
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
            TdsDataType::MoneyN => {
                let byte_len = reader.read_byte().await?;
                let moneyn_value = self.read_moneyn(reader, byte_len).await?;
                match moneyn_value {
                    Some(money_parts) => ColumnValues::MoneyN(money_parts),
                    None => ColumnValues::Null,
                }
            }
            TdsDataType::BigBinary => {
                let length = reader.read_uint16().await?;
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
                    let (days, minutes) = self.read_small_datetime(reader).await?;
                    return Ok(ColumnValues::DateTime((days as i32, minutes as u32)));
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
                    _ => return Ok(ColumnValues::Time(self.read_time(reader, length).await?)),
                }
            }
            TdsDataType::DateTime2N => {
                let length = reader.read_byte().await?;
                match length {
                    0 => Ok(ColumnValues::Null),
                    _ => self.read_datetime2(reader, length).await,
                }
            }?,
            TdsDataType::DateTimeOffsetN => {
                let length = reader.read_byte().await?;
                match length {
                    0 => Ok(ColumnValues::Null),
                    _ => self.read_datetime_offset(reader, length).await,
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
                ColumnValues::SmallDateTime {
                    day: daypart,
                    time: timepart,
                }
            }
            _ => unimplemented!("Data type not implemented: {:?}", metadata.data_type),
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
impl<'a> SqlTypeDecode<'a> for StringDecoder {
    async fn decode(
        &self,
        reader: &'a mut PacketReader,
        metadata: &ColumnMetadata,
    ) -> TdsResult<ColumnValues> {
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

#[derive(PartialEq)]
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

        if self.is_positive {
            d_ret
        } else {
            -d_ret
        }
    }
}

impl Debug for DecimalParts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Decimal: {}{} F64 value: {}",
            if self.is_positive { "" } else { "-" },
            self.int_parts
                .iter()
                .map(|part| part.to_string())
                .collect::<Vec<String>>()
                .join(" "),
            self.to_f64()
        )
    }
}

#[cfg(test)]
mod test {
    use crate::datatypes::decoder::DecimalParts;

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
}

const PRECISION_SMALL_MONEY: u8 = 10;
const PRECISION_MONEY: u8 = 19;

// This struct represents the TDS money & smallmoney data types. In TDS wire-format, smallmoney is represented
// as a 4-byte signed integer, & money is represented as 8 byte (two 4-byte) signed integers
// Quoting from TDS spec:
//   - "smallmoney is represented as a 4-byte signed integer. The TDS value is the smallmoney value
//      multiplied by 10^4."
//   - "money is represented as an 8-byte signed integer. The TDS value is the money value multiplied by
//      10^4. The 8-byte signed integer itself is represented in the following sequence:
//      1. One 4-byte integer that represents the more significant half (MSB)
//      2. One 4-byte integer that represents the less significant half (LSB)"
#[derive(PartialEq)]
pub struct MoneyParts {
    pub int_part_1: i32, // LSB
    pub int_part_2: i32, // MSB - Only populated for Money, 0 for SmallMoney
    scale: u8,
    precision: u8,
}

impl MoneyParts {
    pub fn is_smallmoney(&self) -> bool {
        self.precision == 10
    }
    pub fn is_money(&self) -> bool {
        self.precision == 19
    }
    pub fn get_scale(&self) -> u8 {
        self.scale
    }
    pub fn get_precision(&self) -> u8 {
        self.precision
    }
}

impl Debug for MoneyParts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_smallmoney() {
            write!(
                f,
                "SmallMoney value: {:?}, int_part_1: {:?}, int_part_2: {:?}",
                TdsResult::<f32>::from(self).unwrap(),
                self.int_part_1,
                self.int_part_2
            )
        } else {
            write!(
                f,
                "Money value: {:?}, int_part_1: {:?}, int_part_2: {:?}",
                TdsResult::<f64>::from(self).unwrap(),
                self.int_part_1,
                self.int_part_2
            )
        }
    }
}

impl From<(i32, i32)> for MoneyParts {
    fn from(value: (i32, i32)) -> Self {
        MoneyParts {
            int_part_1: value.0,
            int_part_2: value.1,
            scale: 4,
            precision: PRECISION_MONEY,
        }
    }
}

impl From<i32> for MoneyParts {
    fn from(value: i32) -> Self {
        MoneyParts {
            int_part_1: value,
            int_part_2: 0,
            scale: 4,
            precision: PRECISION_SMALL_MONEY,
        }
    }
}

// This function reassembles the two 4-byte integers (in mixed endian format) into a single 8-byte signed integer.
// The resulting value is the TDS money value, which can be divided by 10^4 to get the actual money value.
// (See comments in MoneyParts definition for more details)
impl From<&MoneyParts> for TdsResult<f64> {
    fn from(value: &MoneyParts) -> Self {
        if !value.is_money() {
            return Err(crate::error::Error::UsageError(
                "get_money_as_f64 called on non-money type".to_string(),
            ));
        }
        let lsb = value.int_part_1;
        let msb = value.int_part_2;
        // -----Example:------
        // While this logic works on both little and big endian machines, this example assumes
        // a little endian machine. Coz big endian case is trivial.
        // 1) Hex representation of an 8-byte int value (MSB to LSB):
        //       - 11 22 33 44 55 66 77 88
        // 2) 8-byte int value stored in LE machine (Low Mem address First (LMF)):
        //       - 88 77 66 55 44 33 22 11
        // 3) This int value stored in TDS wire-format as two 4-byte integers (mixed endian, LMF):
        //       - 44 33 22 11, 88 77 66 55 (MSB = 44 33 22 11, LSB = 88 77 66 55)

        // *** We have (3) in variables msb and lsb. We need to reassemble it into (2) ***
        // - lsb as i64 =
        //       - +ve LSB: 88 77 66 55 00 00 00 00 (LMF)
        //       - -ve LSB: 88 77 66 55 ff ff ff ff (LMF)
        // - (lsb as i64) & 0x00000000FFFFFFFF = lsb_in_i64 = 88 77 66 55 00 00 00 00 (LMF)
        //       - This step is to handle -ve LSB case. We need to convert the ff ff ff ff MSB bytes
        //         to 00 00 00 00. This is done by masking the LSB with 0x00000000FFFFFFFF.
        // - (msb as i64) << 32 = 00 00 00 00 44 33 22 11 (LMF)
        // - (lsb_in_i64) | ((msb as i64) << 32) = 88 77 66 55 44 33 22 11 (LMF)
        let lsb_in_i64 = (lsb as i64) & 0x00000000FFFFFFFF;
        let money_val = lsb_in_i64 | ((msb as i64) << 32);
        // TDS value of money is the value multiplied by 10^4, hence we need to divide while decoding.
        // TODO: (value as f64) can cause precision loss
        Ok((money_val as f64) / 10000.0000)
    }
}

impl From<&MoneyParts> for TdsResult<f32> {
    fn from(value: &MoneyParts) -> Self {
        if !value.is_smallmoney() {
            return Err(crate::error::Error::UsageError(
                "get_smallmoney_as_f32 called on non-smallmoney type".to_string(),
            ));
        }
        // TDS value of money is the value multiplied by 10^4, hence we need to divide while decoding.
        let scaled_value = (value.int_part_1 as f64) / 10000.0000; // f64 so that we don't lose precision
        Ok(scaled_value as f32) // Post division, money value  must fit in f32
                                // TODO: For max (& min) value of smallmoney (214748.3647), the f32 value is 214748.36, which is not accurate. Debug & fix this.
                                //       See test test_money_no_panic. Trying to query these max values from SSMS or ODBC gives correct value.
    }
}

async fn decode_two_propbyte_variant(
    reader: &mut PacketReader<'_>,
    variant_base_type: u8,
    tds_type: TdsDataType,
    data_length: u32,
) -> TdsResult<ColumnValues> {
    Ok(match tds_type {
        // BIGVARBINARYTYPE, BIGBINARYTYPE
        TdsDataType::BigVarBinary | TdsDataType::BigBinary => {
            let _max_length = reader.read_uint16().await?;
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
            unreachable!(
                "Unexpected variant base type for len(2) prop bytes: {:?}",
                variant_base_type
            );
        }
    })
}

async fn decode_seven_propbyte_variant(
    reader: &mut PacketReader<'_>,
    tds_type: TdsDataType,
    data_length: u32,
) -> TdsResult<ColumnValues> {
    assert!(matches!(
        tds_type,
        TdsDataType::BigVarChar | TdsDataType::BigChar | TdsDataType::NVarChar | TdsDataType::NChar
    ));
    let mut collation_bytes = vec![0u8; 5];
    reader.read_bytes(&mut collation_bytes).await?;
    let _max_length = reader.read_uint16().await? as usize;
    let collation = SqlCollation::new(&collation_bytes);
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
