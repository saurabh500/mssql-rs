// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use byteorder::{ByteOrder, LittleEndian};
use uuid::Uuid;

use crate::datatypes::column_values::{
    DEFAULT_VARTIME_SCALE, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney,
    SqlSmallDateTime, SqlSmallMoney, SqlTime, SqlXml,
};
use crate::datatypes::sql_json::SqlJson;
use crate::datatypes::sql_vector::SqlVector;
use crate::{
    core::TdsResult,
    datatypes::{
        decoder::DecimalParts,
        sql_string::SqlString,
        sqldatatypes::{
            FixedLengthTypes, TdsDataType, VECTOR_HEADER_SIZE, VECTOR_MAX_DIMENSIONS,
            VectorBaseType, VectorLayoutFormat, VectorLayoutVersion,
        },
    },
    error::Error,
    io::packet_writer::{PacketWriter, TdsPacketWriter},
    token::tokens::SqlCollation,
};

#[derive(Debug, PartialEq, Clone)]
pub enum SqlType {
    Bit(Option<bool>),
    TinyInt(Option<u8>),
    SmallInt(Option<i16>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Real(Option<f32>),
    Float(Option<f64>),
    Decimal(Option<DecimalParts>),
    Numeric(Option<DecimalParts>),
    Money(Option<SqlMoney>),
    SmallMoney(Option<SqlSmallMoney>),

    Time(Option<SqlTime>),
    DateTime2(Option<SqlDateTime2>),
    DateTimeOffset(Option<SqlDateTimeOffset>),
    SmallDateTime(Option<SqlSmallDateTime>),
    DateTime(Option<SqlDateTime>),
    Date(Option<SqlDate>),

    /// Represents a Varchar with a specifiied length.
    NVarchar(Option<SqlString>, u16),

    /// Represents a Varchar with MAX length.
    NVarcharMax(Option<SqlString>),

    Varchar(Option<SqlString>, u16),
    VarcharMax(Option<SqlString>),

    VarBinary(Option<Vec<u8>>, u16),
    VarBinaryMax(Option<Vec<u8>>),

    Binary(Option<Vec<u8>>, u16),
    Char(Option<SqlString>, u16),
    NChar(Option<SqlString>, u16),

    Text(Option<SqlString>),
    NText(Option<SqlString>),

    Json(Option<SqlJson>),

    Xml(Option<SqlXml>),
    Uuid(Option<Uuid>),

    /// Parameters: (data, dimensions, base_type)
    /// Although SqlVector has dimension & base type information, we also pass it separately so
    /// that we can serialize NULL vector parameters (where SqlVector=None) with correct metadata.
    Vector(Option<SqlVector>, u16, VectorBaseType),
    // To be added in future
    // Variant
    // TVP
}

type NullableTdsType = TdsDataType;

// The maximum length of a variable length type in TDS is 8000 bytes.
pub(crate) const VAR_TDS_MAX_LENGTH: u16 = 8000u16;

// The length of a NULL value in TDS is 65535 bytes for variable length types.
pub(crate) const MAX_U16_LENGTH: u16 = 65535u16;

// The length of a NULL value in TDS is 0 bytes.
pub(crate) const NULL_LENGTH: u8 = 0u8;

// The fixed size for Decimal in TDS is 17 bytes.
pub(crate) const DECIMAL_FIXED_SIZE: u8 = 17;

// The short data length which signifies that the data is being sent as PLP (Partial Length Packet).
pub(crate) const MAX_SHORT_DATA_LENGTH: u16 = 0xFFFF;

pub(crate) const PLP_TERMINATOR_CHUNK_LEN: u32 = 0x00000000;

pub(crate) const PLP_UNKNOWN_LENGTH: u64 = 0xFFFF_FFFF_FFFF_FFFE;

pub(crate) const PLP_NULL: u64 = 0xFFFF_FFFF_FFFF_FFFF;

pub(crate) const NO_XML_SCHEMA: u8 = 0x00;

impl SqlType {
    fn get_nullable_type(&self) -> NullableTdsType {
        match self {
            SqlType::Bit(_)
            | SqlType::TinyInt(_)
            | SqlType::SmallInt(_)
            | SqlType::Int(_)
            | SqlType::BigInt(_) => TdsDataType::IntN,
            SqlType::Real(_) | SqlType::Float(_) => TdsDataType::FltN,
            SqlType::Decimal(_) => TdsDataType::NumericN,
            SqlType::Numeric(_) => TdsDataType::NumericN,
            SqlType::NVarchar(_, _) => TdsDataType::NVarChar,
            SqlType::VarBinary(_items, _size) => TdsDataType::BigVarBinary,
            SqlType::Binary(_items, _) => TdsDataType::BigBinary,
            SqlType::Char(_, _) => TdsDataType::BigChar,
            SqlType::NChar(_, _) => TdsDataType::NChar,
            SqlType::Text(_sql_string) => todo!(),
            SqlType::NText(_sql_string) => todo!(),
            SqlType::Json(_) => TdsDataType::Json,

            SqlType::Time(_) => TdsDataType::TimeN,
            SqlType::DateTime2(_) => TdsDataType::DateTime2N,
            SqlType::DateTimeOffset(_) => TdsDataType::DateTimeOffsetN,
            SqlType::DateTime(_) => TdsDataType::DateTimeN,
            SqlType::Date(_) => TdsDataType::DateN,
            SqlType::SmallDateTime(_) => TdsDataType::DateTimeN,
            SqlType::NVarcharMax(_) => TdsDataType::NVarChar,
            SqlType::Varchar(_, _) => TdsDataType::BigVarChar,
            SqlType::VarcharMax(_) => TdsDataType::BigVarChar,
            SqlType::VarBinaryMax(_) => TdsDataType::BigVarBinary,
            SqlType::Xml(_) => TdsDataType::Xml,
            SqlType::Uuid(_) => TdsDataType::Guid,
            SqlType::Money(_) => TdsDataType::MoneyN,
            SqlType::SmallMoney(_) => TdsDataType::MoneyN,
            SqlType::Vector(_, _, _) => TdsDataType::Vector,
        }
    }

    fn get_fixed_length_size(&self) -> usize {
        let fixed_length_type = FixedLengthTypes::try_from(self);
        assert!(
            fixed_length_type.is_ok(),
            "SqlType is not a fixed length type."
        );
        fixed_length_type.unwrap().get_len()
    }

    pub(crate) async fn serialize(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
    ) -> TdsResult<()> {
        match &self {
            SqlType::Bit(value) => self.serialize_bit(packet_writer, value).await?,
            SqlType::TinyInt(value) => self.serialize_tinyint(packet_writer, value).await?,
            SqlType::SmallInt(value) => self.serialize_smallint(packet_writer, value).await?,
            SqlType::Int(value) => self.serialize_int(packet_writer, value).await?,
            SqlType::BigInt(value) => self.serialize_bigint(packet_writer, value).await?,
            SqlType::Real(value) => self.serialize_f32(packet_writer, value).await?,
            SqlType::Float(value) => self.serialize_f64(packet_writer, value).await?,
            SqlType::Decimal(decimal_parts) => {
                self.serialize_decimalparts(packet_writer, decimal_parts)
                    .await?
            }
            SqlType::Numeric(decimal_parts) => {
                self.serialize_decimalparts(packet_writer, decimal_parts)
                    .await?
            }
            SqlType::Binary(_items, param_length) => {
                self.serialize_binary(packet_writer, _items, *param_length)
                    .await?;
            }
            SqlType::Char(_sql_string, _) => todo!(),
            SqlType::NChar(_sql_string, _) => todo!(),
            SqlType::Text(_sql_string) => todo!(),
            SqlType::NText(_sql_string) => todo!(),
            SqlType::Json(json) => self.serialize_json(packet_writer, json).await?,
            SqlType::Money(money) => self.serialize_money(packet_writer, money).await?,
            SqlType::SmallMoney(smallmoney) => {
                self.serialize_smallmoney(packet_writer, smallmoney).await?
            }
            SqlType::Time(time) => self.serialize_time(packet_writer, time).await?,
            SqlType::DateTime2(datetime2) => {
                self.serialize_datetime2(packet_writer, datetime2).await?
            }
            SqlType::DateTimeOffset(datetimeoffset) => {
                self.serialize_datetimeoffset(packet_writer, datetimeoffset)
                    .await?
            }
            SqlType::SmallDateTime(smalldatetime) => {
                self.serialize_smalldatetime(packet_writer, smalldatetime)
                    .await?
            }
            SqlType::DateTime(datetime) => self.serialize_datetime(packet_writer, datetime).await?,
            SqlType::Date(sqldate) => self.serialize_date(packet_writer, sqldate).await?,
            SqlType::NVarchar(sql_string, param_length) => {
                self.serialize_nvarchar(packet_writer, db_collation, sql_string, *param_length)
                    .await?
            }
            SqlType::NVarcharMax(sql_string) => {
                self.serialize_nvarchar(packet_writer, db_collation, sql_string, MAX_U16_LENGTH)
                    .await?
            }
            SqlType::Varchar(sql_string, param_len) => {
                self.serialize_string(packet_writer, db_collation, sql_string, *param_len)
                    .await?
            }
            SqlType::VarcharMax(sql_string) => {
                self.serialize_string(packet_writer, db_collation, sql_string, MAX_U16_LENGTH)
                    .await?
            }
            SqlType::VarBinary(binary_data, param_len) => {
                self.serialize_binary(packet_writer, binary_data, *param_len)
                    .await?
            }
            SqlType::VarBinaryMax(binary_data) => {
                self.serialize_binary(packet_writer, binary_data, MAX_U16_LENGTH)
                    .await?
            }
            SqlType::Xml(sql_xml) => self.serialize_xml(packet_writer, sql_xml).await?,
            SqlType::Uuid(uuid) => self.serialize_uuid(packet_writer, uuid).await?,
            SqlType::Vector(sql_vector, dimensions, base_type) => {
                self.serialize_vector(packet_writer, sql_vector, *dimensions, *base_type)
                    .await?
            }
        }
        Ok(())
    }

    async fn serialize_bit(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<bool>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        let type_size = self.get_fixed_length_size();
        // Write the nullable type and size.
        packet_writer.write_byte_async(nullable_type as u8).await?;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(type_size as u8).await?;
        match value {
            Some(v) => {
                let data_size = FixedLengthTypes::try_from(self).unwrap().get_len();
                // Send the actual data size.
                packet_writer.write_byte_async(data_size as u8).await?;
                // Send the actual data.
                let byte_value = if *v { 1 } else { 0 };
                packet_writer.write_byte_async(byte_value).await?;
            }
            None => packet_writer.write_byte_async(NULL_LENGTH).await?,
        };
        Ok(())
    }

    async fn serialize_tinyint(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<u8>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        let type_size = self.get_fixed_length_size();

        packet_writer.write_byte_async(nullable_type as u8).await?;
        packet_writer.write_byte_async(type_size as u8).await?;

        match value {
            Some(v) => {
                let data_size = FixedLengthTypes::try_from(self).unwrap().get_len();
                packet_writer.write_byte_async(data_size as u8).await?;
                packet_writer.write_byte_async(*v).await?;
            }
            None => packet_writer.write_byte_async(NULL_LENGTH).await?,
        };
        Ok(())
    }

    async fn serialize_smallint(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<i16>,
    ) -> TdsResult<()> {
        let type_size = self.get_fixed_length_size();
        let nullable_type: NullableTdsType = self.get_nullable_type();
        // Write the nullable type and size.
        packet_writer.write_byte_async(nullable_type as u8).await?;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(type_size as u8).await?;
        match value {
            Some(v) => {
                let data_size = FixedLengthTypes::try_from(self).unwrap().get_len();
                // Send the actual data size.
                packet_writer.write_byte_async(data_size as u8).await?;
                // Send the actual data.
                packet_writer.write_i16_async(*v).await?;
            }
            None => packet_writer.write_byte_async(NULL_LENGTH).await?,
        };
        Ok(())
    }

    async fn serialize_int(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<i32>,
    ) -> TdsResult<()> {
        let type_size = self.get_fixed_length_size();
        let nullable_type: NullableTdsType = self.get_nullable_type();
        // Write the nullable type and size.
        packet_writer.write_byte_async(nullable_type as u8).await?;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(type_size as u8).await?;
        match value {
            Some(v) => {
                let data_size = FixedLengthTypes::try_from(self).unwrap().get_len();
                // Send the actual data size.
                packet_writer.write_byte_async(data_size as u8).await?;
                // Send the actual data.
                packet_writer.write_i32_async(*v).await?;
            }
            None => packet_writer.write_byte_async(NULL_LENGTH).await?,
        };
        Ok(())
    }

    async fn serialize_bigint(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<i64>,
    ) -> TdsResult<()> {
        let type_size = self.get_fixed_length_size();
        let nullable_type: NullableTdsType = self.get_nullable_type();
        // Write the nullable type and size.
        packet_writer.write_byte_async(nullable_type as u8).await?;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(type_size as u8).await?;
        match value {
            Some(v) => {
                let data_size = FixedLengthTypes::try_from(self).unwrap().get_len();
                // Send the actual data size.
                packet_writer.write_byte_async(data_size as u8).await?;
                // Send the actual data.
                packet_writer.write_i64_async(*v).await?;
            }
            None => packet_writer.write_byte_async(0x00).await?,
        };
        Ok(())
    }

    async fn serialize_decimalparts(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<DecimalParts>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        let type_size = DECIMAL_FIXED_SIZE;

        packet_writer.write_byte_async(nullable_type as u8).await?;
        packet_writer.write_byte_async(type_size).await?;

        match value {
            Some(v) => {
                packet_writer.write_byte_async(v.precision).await?;
                packet_writer.write_byte_async(v.scale).await?;

                // For TDS 7.0 and above, there are always 17 bytes of data
                // This matches .NET SqlClient behavior
                packet_writer.write_byte_async(DECIMAL_FIXED_SIZE).await?;

                if v.is_positive {
                    packet_writer.write_byte_async(0x01).await?;
                } else {
                    packet_writer.write_byte_async(0x00).await?;
                }

                // Always write 4 i32 values (16 bytes) regardless of precision
                // Pad with zeros if fewer int_parts are needed
                for i in 0..4 {
                    let part = v.int_parts.get(i).copied().unwrap_or(0);
                    packet_writer.write_i32_async(part).await?;
                }
            }
            None => {
                packet_writer.write_byte_async(1).await?;
                packet_writer.write_byte_async(0).await?;
                packet_writer.write_byte_async(NULL_LENGTH).await?;
            }
        };
        Ok(())
    }

    async fn serialize_f32(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<f32>,
    ) -> TdsResult<()> {
        let type_size = self.get_fixed_length_size();
        let data_size = value.map_or(0, |_| FixedLengthTypes::try_from(self).unwrap().get_len());
        let nullable_type: NullableTdsType = self.get_nullable_type();
        // Write the nullable type and size.
        packet_writer.write_byte_async(nullable_type as u8).await?;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(type_size as u8).await?;
        match value {
            Some(v) => {
                // Send the actual data size.
                packet_writer.write_byte_async(data_size as u8).await?;
                // Send the actual data.
                let mut buf = [0u8; 4];
                LittleEndian::write_f32(&mut buf, *v);
                packet_writer.write_async(&buf).await?;
            }
            None => packet_writer.write_byte_async(0x00).await?,
        };
        Ok(())
    }

    async fn serialize_f64(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        value: &Option<f64>,
    ) -> TdsResult<()> {
        let type_size = self.get_fixed_length_size();
        let data_size = value.map_or(0, |_| FixedLengthTypes::try_from(self).unwrap().get_len());
        let nullable_type: NullableTdsType = self.get_nullable_type();
        // Write the nullable type and size.
        packet_writer.write_byte_async(nullable_type as u8).await?;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(type_size as u8).await?;
        match value {
            Some(v) => {
                // Send the actual data size.
                packet_writer.write_byte_async(data_size as u8).await?;
                // Send the actual data.
                let mut buf = [0u8; 8];
                LittleEndian::write_f64(&mut buf, *v);
                packet_writer.write_async(&buf).await?;
            }
            None => packet_writer.write_byte_async(0x00).await?,
        };
        Ok(())
    }

    // Serializes the string payload.
    // The param length is sent as the metadata. If param length is greater than 8000, it is clamped to 65535.
    // If the string is None, it sends a NULL value.
    // If the bytes in string are less than or equal to 8000, it sends the bytes directly.
    // If the bytes in string are greater than 8000, it sends the bytes as a PLP (Partial Length Packet).
    async fn serialize_string(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
        sql_string: &Option<SqlString>,
        param_len: u16,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();

        let max_size = match nullable_type {
            NullableTdsType::NVarChar => 4000,
            NullableTdsType::BigVarChar => 8000,
            _ => {
                return Err(Error::ImplementationError(
                    "Incorrect invocation of serialize_string for non-string type".to_owned(),
                ));
            } // For other types, we use the max size of 65535.
        };
        // Clamp param_len to 65535 if > maxSize
        let param_len = if param_len > max_size {
            MAX_U16_LENGTH
        } else {
            param_len
        };

        let optional_string = match sql_string {
            Some(string) => Some(string),
            None => None,
        };

        packet_writer.write_byte_async(nullable_type as u8).await?;

        packet_writer.write_u16_async(param_len).await?;
        packet_writer.write_u32_async(db_collation.info).await?;
        packet_writer.write_byte_async(db_collation.sort_id).await?;

        match optional_string {
            Some(string) => {
                let should_send_as_plp = (string.bytes.len() > VAR_TDS_MAX_LENGTH as usize)
                    || (param_len == MAX_U16_LENGTH);
                if !should_send_as_plp {
                    // Write the length for the metadata.
                    // Write the length of the actual data.
                    packet_writer
                        .write_i16_async(string.bytes.len() as i16)
                        .await?;
                    // Write the data.
                    packet_writer.write_async(&string.bytes).await?;
                } else {
                    // Write the PLP length.
                    packet_writer
                        .write_u64_async(string.bytes.len() as u64)
                        .await?;

                    // Write the data chunk length, which is the same as PLP length.
                    packet_writer
                        .write_u32_async(string.bytes.len() as u32)
                        .await?;
                    packet_writer.write_async(&string.bytes).await?;

                    // Write a zero-length PLP chunk terminator to signal the end of the PLP stream.
                    packet_writer
                        .write_u32_async(PLP_TERMINATOR_CHUNK_LEN)
                        .await?;
                }
            }
            None => {
                match param_len {
                    // For max length, we send a PLP NULL, for all other values, we send a NULL length which is u16::MAX.
                    u16::MAX => packet_writer.write_u64_async(PLP_NULL).await?,
                    _ => packet_writer.write_u16_async(u16::MAX).await?,
                };
            }
        }
        Ok(())
    }

    // Serializes the string payload.
    // The param length is sent as the metadata. If param length is greater than 8000, it is clamped to 65535.
    // If the string is None, it sends a NULL value.
    // If the bytes in string are less than or equal to 8000, it sends the bytes directly.
    // If the bytes in string are greater than 8000, it sends the bytes as a PLP (Partial Length Packet).
    async fn serialize_nvarchar(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
        sql_string: &Option<SqlString>,
        param_len: u16,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();

        // MAX size for Nvarchar parameter is 4000 characters.
        let max_size = 4000;
        // Clamp param_len to 65535 if > maxSize
        let param_len = if param_len > max_size {
            MAX_U16_LENGTH
        } else {
            // NVarchar length takes 2 bytes per character, so we need to multiply by 2.
            param_len * 2
        };

        let optional_string = match sql_string {
            Some(string) => Some(string),
            None => None,
        };

        packet_writer.write_byte_async(nullable_type as u8).await?;

        packet_writer.write_u16_async(param_len).await?;
        packet_writer.write_u32_async(db_collation.info).await?;
        packet_writer.write_byte_async(db_collation.sort_id).await?;

        match optional_string {
            Some(string) => {
                let should_send_as_plp = (string.bytes.len() > VAR_TDS_MAX_LENGTH as usize)
                    || (param_len == MAX_U16_LENGTH);
                if !should_send_as_plp {
                    // Write the length for the metadata.
                    // Write the length of the actual data.
                    packet_writer
                        .write_i16_async(string.bytes.len() as i16)
                        .await?;
                    // Write the data.
                    packet_writer.write_async(&string.bytes).await?;
                } else {
                    // Write the PLP length.
                    packet_writer
                        .write_u64_async(string.bytes.len() as u64)
                        .await?;

                    // Write the data chunk length, which is the same as PLP length.
                    packet_writer
                        .write_u32_async(string.bytes.len() as u32)
                        .await?;
                    packet_writer.write_async(&string.bytes).await?;

                    // Write a zero-length PLP chunk terminator to signal the end of the PLP stream.
                    packet_writer
                        .write_u32_async(PLP_TERMINATOR_CHUNK_LEN)
                        .await?;
                }
            }
            None => {
                match param_len {
                    // For max length, we send a PLP NULL, for all other values, we send a NULL length which is u16::MAX.
                    u16::MAX => packet_writer.write_u64_async(PLP_NULL).await?,
                    _ => packet_writer.write_u16_async(u16::MAX).await?,
                };
            }
        }
        Ok(())
    }

    async fn serialize_binary(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        binary_data: &Option<Vec<u8>>,
        param_len: u16,
    ) -> TdsResult<()> {
        // TODO: Should we validate the length here ?
        // Clamp param_len to 65535 if > 8000
        let param_len = if param_len > VAR_TDS_MAX_LENGTH {
            u16::MAX
        } else {
            param_len
        };
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        packet_writer.write_u16_async(param_len).await?;

        let optional_binary = match binary_data {
            Some(binary) => Some(binary),
            None => None,
        };
        match optional_binary {
            Some(data) => {
                let should_send_as_plp =
                    data.len() > VAR_TDS_MAX_LENGTH as usize || param_len == u16::MAX;
                if !should_send_as_plp {
                    // Write the length of the actual data.
                    packet_writer.write_i16_async(data.len() as i16).await?;
                    // Write the data.
                    packet_writer.write_async(data).await?;
                } else {
                    // Write the PLP length.
                    packet_writer.write_u64_async(data.len() as u64).await?;

                    // Write the data chunk length, which is the same as PLP length.
                    packet_writer.write_u32_async(data.len() as u32).await?;
                    packet_writer.write_async(data).await?;

                    // Write a zero-length PLP chunk terminator to signal the end of the PLP stream.
                    packet_writer.write_u32_async(0).await?;
                }
            }
            None => {
                match param_len {
                    // For max length, we send a PLP NULL, for all other values, we send a NULL length which is u16::MAX.
                    u16::MAX => packet_writer.write_u64_async(PLP_NULL).await?,
                    _ => packet_writer.write_u16_async(u16::MAX).await?,
                };
            }
        }
        Ok(())
    }

    async fn serialize_xml(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        xml: &Option<SqlXml>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let optional_sqlxml = match xml {
            Some(binary) => Some(binary),
            None => None,
        };
        // No Schema.
        packet_writer.write_byte_async(0x00).await?;

        match optional_sqlxml {
            Some(sqlxml) => {
                let data = &sqlxml.bytes;

                // Write unknown length for PLP.
                packet_writer.write_u64_async(PLP_UNKNOWN_LENGTH).await?;

                let data_len = match sqlxml.has_bom() {
                    true => data.len(), // BOM is 2 bytes.
                    false => data.len() + 2,
                };
                // Write the data chunk length, which is the same as PLP length.
                packet_writer.write_u32_async(data_len as u32).await?;

                if !sqlxml.has_bom() {
                    // Write BOM if not present.
                    packet_writer.write_byte_async(0xFF).await?;
                    packet_writer.write_byte_async(0xFE).await?;
                }

                packet_writer.write_async(data).await?;

                // Write a zero-length PLP chunk terminator to signal the end of the PLP stream.
                packet_writer.write_u32_async(0).await?;
            }
            None => {
                packet_writer.write_u64_async(PLP_NULL).await?;
            }
        }
        Ok(())
    }

    async fn serialize_json(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        json: &Option<SqlJson>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let optional_sqljson = match json {
            Some(binary) => Some(binary),
            None => None,
        };

        match optional_sqljson {
            Some(sqljson) => {
                let data = &sqljson.bytes;

                // Write unknown length for PLP.
                packet_writer.write_u64_async(PLP_UNKNOWN_LENGTH).await?;

                let data_len = data.len();

                // Write the data chunk length, which is the same as PLP length.
                packet_writer.write_u32_async(data_len as u32).await?;

                packet_writer.write_async(data).await?;

                // Write a zero-length PLP chunk terminator to signal the end of the PLP stream.
                packet_writer.write_u32_async(0).await?;
            }
            None => {
                packet_writer.write_u64_async(PLP_NULL).await?;
            }
        }
        Ok(())
    }

    async fn serialize_uuid(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        uuid: &Option<Uuid>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let byte_length = 16u8;
        // Write the type size. For Guid there is no other variant. Hence 16 is always sent
        packet_writer.write_byte_async(byte_length).await?;
        match uuid {
            Some(u) => {
                // Write the data length.
                packet_writer.write_byte_async(byte_length).await?;

                // Send the actual data size.
                let guid_bytes = u.to_bytes_le();
                packet_writer.write_async(&guid_bytes).await?;
            }
            None => packet_writer.write_byte_async(NULL_LENGTH).await?,
        };
        Ok(())
    }

    async fn serialize_vector(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        sql_vector: &Option<SqlVector>,
        dimensions: u16,
        declared_base_type: VectorBaseType,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();

        // Write TDS type byte (0xF5)
        packet_writer.write_byte_async(nullable_type as u8).await?;

        // Validate dimensions are within server limits
        if dimensions > VECTOR_MAX_DIMENSIONS {
            return Err(Error::UsageError(format!(
                "Vector dimensions {} exceeds maximum supported dimensions {}",
                dimensions, VECTOR_MAX_DIMENSIONS
            )));
        }

        // Validate that the vector's base type matches the declared type
        if let Some(vector) = sql_vector {
            let actual_base_type = vector.base_type();
            if actual_base_type != declared_base_type {
                return Err(Error::TypeConversionError(format!(
                    "Vector base type mismatch: declared {:?}, but vector has {:?}",
                    declared_base_type, actual_base_type
                )));
            }

            // Validate that the vector's dimensions match the declared dimensions
            let actual_dimensions = vector.dimension_count();
            if actual_dimensions != dimensions {
                return Err(Error::TypeConversionError(format!(
                    "Vector dimension mismatch: declared {}, but vector has {}",
                    dimensions, actual_dimensions
                )));
            }
        }

        // Calculate exact size from dimensions: header (8 bytes) + dimensions * element_size
        let element_size = declared_base_type.element_size_bytes() as u16;
        let exact_size = (VECTOR_HEADER_SIZE as u16) + (dimensions * element_size);

        // Write USHORTLEN (u16) for exact length (TypeInfo)
        packet_writer.write_u16_async(exact_size).await?;

        // Write SCALE byte (base type)
        packet_writer
            .write_byte_async(declared_base_type as u8)
            .await?;

        match sql_vector {
            Some(vector) => {
                let dimension_count = vector.dimension_count();
                let element_size = vector.base_type().element_size_bytes();
                let data_length =
                    (VECTOR_HEADER_SIZE + (dimension_count as usize * element_size)) as u16;

                // Write length prefix (u16)
                packet_writer.write_u16_async(data_length).await?;

                // Write 8-byte header
                Self::encode_vector_header(packet_writer, dimension_count, vector.base_type())
                    .await?;

                // Write vector values based on base type
                match vector.base_type() {
                    VectorBaseType::Float32 => {
                        let values = vector.as_f32().ok_or_else(|| {
                            Error::TypeConversionError("Vector is not Float32 type".into())
                        })?;
                        for &value in values {
                            let bytes = value.to_le_bytes();
                            packet_writer.write_async(&bytes).await?;
                        }
                    }
                }
            }
            None => {
                // Write 0xFFFF for NULL
                packet_writer.write_u16_async(u16::MAX).await?;
            }
        }

        Ok(())
    }

    /// Encodes the 8-byte vector header according to TDS protocol specification.
    ///
    /// The header format is:
    /// - Byte 0: Layout format (0xA9 for V1)
    /// - Byte 1: Layout version (0x01 for V1)
    /// - Bytes 2-3: Dimension count (u16, little-endian)
    /// - Byte 4: Base type (0x00 for Float32)
    /// - Bytes 5-7: Reserved (0x00)
    async fn encode_vector_header(
        packet_writer: &mut PacketWriter<'_>,
        dimension_count: u16,
        base_type: VectorBaseType,
    ) -> TdsResult<()> {
        packet_writer
            .write_byte_async(VectorLayoutFormat::V1 as u8)
            .await?;
        packet_writer
            .write_byte_async(VectorLayoutVersion::V1 as u8)
            .await?;
        packet_writer.write_u16_async(dimension_count).await?;
        packet_writer.write_byte_async(base_type as u8).await?;
        packet_writer.write_byte_async(0x00).await?; // reserved
        packet_writer.write_byte_async(0x00).await?; // reserved
        packet_writer.write_byte_async(0x00).await?; // reserved
        Ok(())
    }

    async fn serialize_datetime2(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        datetime2: &Option<SqlDateTime2>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;

        match datetime2 {
            Some(datetime2val) => {
                let t = &datetime2val.time;

                let scale = t.get_scale();

                let byte_count_for_date = 3; // Datetime2 has 3 bytes for the date.

                let byte_count_for_time = get_scale_based_length(t)?;

                let length = byte_count_for_time + byte_count_for_date;

                packet_writer.write_byte_async(scale).await?;

                packet_writer.write_byte_async(length).await?;

                // Write the time in nanoseconds.
                packet_writer
                    .write_partial_u64_async(t.time_nanoseconds, byte_count_for_time)
                    .await?;

                // Write the day count.
                packet_writer
                    .write_partial_u64_async(datetime2val.days as u64, byte_count_for_date)
                    .await?;
            }
            None => {
                write_default_scale_and_null(packet_writer).await?;
            }
        }
        Ok(())
    }

    async fn serialize_datetimeoffset(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        datetimeoffset: &Option<SqlDateTimeOffset>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;

        match datetimeoffset {
            Some(datetimeoffset) => {
                let t = &datetimeoffset.datetime2.time;
                let scale = t.get_scale();

                let byte_count_for_offset = 2; // Datetimeoffset has 2 bytes for the offset.

                let byte_count_for_date = 3; // Datetime2 has 3 bytes for the date.

                let byte_count_for_time = get_scale_based_length(t)?;

                let length = byte_count_for_time + byte_count_for_date + byte_count_for_offset;

                packet_writer.write_byte_async(scale).await?;

                packet_writer.write_byte_async(length).await?;

                // Write the time in nanoseconds.
                packet_writer
                    .write_partial_u64_async(t.time_nanoseconds, byte_count_for_time)
                    .await?;
                // Write the day count.
                packet_writer
                    .write_partial_u64_async(
                        datetimeoffset.datetime2.days as u64,
                        byte_count_for_date,
                    )
                    .await?;

                // Write the offset:  2-byte signed
                // integer that represents the time zone offset as the number of minutes from UTC. The time zone offset
                // MUST be between -840 and 840.
                packet_writer.write_i16_async(datetimeoffset.offset).await?;
            }
            None => {
                write_default_scale_and_null(packet_writer).await?;
            }
        }
        Ok(())
    }

    async fn serialize_time(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        time: &Option<SqlTime>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;

        match time {
            Some(t) => {
                let scale = t.get_scale();

                let scale_based_byte_length = get_scale_based_length(t)?;

                packet_writer.write_byte_async(scale).await?;

                packet_writer
                    .write_byte_async(scale_based_byte_length)
                    .await?;

                // Write the time in nanoseconds.
                packet_writer
                    .write_partial_u64_async(t.time_nanoseconds, scale_based_byte_length)
                    .await?;
            }
            None => {
                write_default_scale_and_null(packet_writer).await?;
            }
        }
        Ok(())
    }

    async fn serialize_datetime(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        date: &Option<SqlDateTime>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let byte_len = 8; // DateTime is 8 bytes for non-null datetime.
        // Write the length of datatype
        packet_writer.write_byte_async(byte_len).await?;
        match date {
            Some(datetime) => {
                // Write the length of the data.
                packet_writer.write_byte_async(byte_len).await?;
                packet_writer.write_i32_async(datetime.days).await?;
                packet_writer.write_u32_async(datetime.time).await?;
            }
            None => {
                // Write 0 length to signify that the data is NULL.
                packet_writer.write_byte_async(NULL_LENGTH).await?;
            }
        }
        Ok(())
    }

    async fn serialize_date(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        date: &Option<SqlDate>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        match date {
            Some(d) => {
                // Write the length of the dateN byte len.
                let byte_len = 3; // Date is always 3 byes for non-null dates.
                packet_writer.write_byte_async(byte_len).await?;
                // Write the date.
                packet_writer
                    .write_partial_u64_async(d.get_days() as u64, 3)
                    .await?;
            }
            None => {
                // Write 0 length to signify that the data is NULL.
                packet_writer.write_byte_async(NULL_LENGTH).await?;
            }
        }
        Ok(())
    }

    async fn serialize_smalldatetime(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        smalldatetime: &Option<SqlSmallDateTime>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let len = size_of::<u32>() as u8;
        // Write the length of the date.
        packet_writer.write_byte_async(len).await?;

        match smalldatetime {
            Some(d) => {
                // Write the length of the data.
                packet_writer.write_byte_async(len).await?;
                // Write the date.
                packet_writer.write_u16_async(d.days).await?;
                packet_writer.write_u16_async(d.time).await?;
            }
            None => {
                // Write 0 length to signify that the data is NULL.
                packet_writer.write_byte_async(NULL_LENGTH).await?;
            }
        }
        Ok(())
    }

    async fn serialize_smallmoney(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        smallmoney: &Option<SqlSmallMoney>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let length_in_bytes = 4u8;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(length_in_bytes).await?;

        match smallmoney {
            Some(smallmoney) => {
                // The length of the datatype.
                packet_writer.write_byte_async(length_in_bytes).await?;

                packet_writer.write_i32_async(smallmoney.int_val).await?;
            }
            None => {
                packet_writer.write_byte_async(NULL_LENGTH).await?;
            }
        }
        Ok(())
    }

    async fn serialize_money(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        time: &Option<SqlMoney>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;

        let length_in_bytes = 8u8;
        // Write the type size to indicate the kind of int being sent.
        packet_writer.write_byte_async(length_in_bytes).await?;

        match time {
            Some(moneyparts) => {
                // The length of the datatype.
                packet_writer.write_byte_async(length_in_bytes).await?;

                packet_writer.write_i32_async(moneyparts.msb_part).await?;
                packet_writer.write_i32_async(moneyparts.lsb_part).await?;
            }
            None => {
                packet_writer.write_byte_async(NULL_LENGTH).await?;
            }
        }
        Ok(())
    }
}

/// Calculate the byte length for time-based types based on scale value.
///
/// This mapping is defined in the TDS protocol documentation:
/// - Scale 0-2: 3 bytes
/// - Scale 3-4: 4 bytes  
/// - Scale 5-7: 5 bytes
pub(crate) fn get_time_length_from_scale(scale: u8) -> TdsResult<u8> {
    match scale {
        0..=2 => Ok(0x03),
        3 | 4 => Ok(0x04),
        5..=7 => Ok(0x05),
        _ => Err(Error::UsageError(format!(
            "Invalid scale for Time type: {scale}"
        ))),
    }
}

// We are taking the map from the protocol documentation that defines the scale.
// However the scale essentially defines the precision of the time and
// the length of bytes can be computed from the scale. But since
// this is documented, we will use this map.
fn get_scale_based_length(time: &SqlTime) -> TdsResult<u8> {
    get_time_length_from_scale(time.scale)
}

async fn write_default_scale_and_null(packet_writer: &mut PacketWriter<'_>) -> TdsResult<()> {
    // Since we dont have a scale, we will send out the default scale.
    // This doesn't matter, if the following data is NULL.
    packet_writer
        .write_byte_async(DEFAULT_VARTIME_SCALE)
        .await?;
    // Write 0 length to signify that the data is NULL.
    packet_writer.write_byte_async(NULL_LENGTH).await?;
    Ok(())
}

impl TryFrom<&SqlType> for FixedLengthTypes {
    type Error = Error;

    fn try_from(value: &SqlType) -> TdsResult<FixedLengthTypes> {
        match value {
            SqlType::Bit(_) => Ok(FixedLengthTypes::Int1),
            SqlType::TinyInt(_) => Ok(FixedLengthTypes::Int1),
            SqlType::SmallInt(_) => Ok(FixedLengthTypes::Int2),
            SqlType::Int(_) => Ok(FixedLengthTypes::Int4),
            SqlType::BigInt(_) => Ok(FixedLengthTypes::Int8),
            SqlType::Real(_) => Ok(FixedLengthTypes::Flt4),
            SqlType::Float(_) => Ok(FixedLengthTypes::Flt8),
            _ => Err(Error::UsageError(
                "SqlType is not a fixed length type.".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod datetime_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            column_values::{
                DEFAULT_VARTIME_SCALE, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset,
                SqlSmallDateTime, SqlTime,
            },
            sqldatatypes::TdsDataType,
            sqltypes::{NULL_LENGTH, SqlType, get_scale_based_length},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_date() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let days = 5u32;
        let sqldate = SqlDate::unchecked_create(days);
        let dateval = Some(sqldate.clone());
        let sqldate = SqlType::Date(dateval.clone());

        // Test
        sqldate
            .serialize_date(&mut packet_writer, &dateval)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let byte_len = 3;

        let payload = mock_reader_writer.get_written_data();
        let test_bytes = get_partial_bytes(days as u64, 3);
        let mut written_bytes = vec![0u8; byte_len as usize];
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate data length
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, test_bytes);
    }

    #[tokio::test]
    async fn test_write_null_date() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let sqldate = SqlType::Date(None);

        // Test
        sqldate
            .serialize_date(&mut packet_writer, &None)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // Validate byte length
    }

    #[tokio::test]
    async fn test_write_datetime() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let days = 5i32;
        let time = 1_234_567u32;
        let sqldatetime = SqlDateTime {
            days,
            time, // Example time in nanoseconds
        };
        let datetimeval = Some(sqldatetime.clone());
        let sqldatetime = SqlType::DateTime(datetimeval.clone());

        // Test
        sqldatetime
            .serialize_datetime(&mut packet_writer, &datetimeval)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let byte_len = 8;
        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTimeN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate byte length
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate data length
        assert_eq!(test_cursor.get_i32_le(), days);
        assert_eq!(test_cursor.get_u32_le(), time);
    }

    #[tokio::test]
    async fn test_write_null_datetime() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let sqldatetime = SqlType::DateTime(None);

        // Test
        sqldatetime
            .serialize_datetime(&mut packet_writer, &None)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTimeN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 8); // Validate type length
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // Validate byte length
    }

    #[tokio::test]
    async fn test_write_smalldatetime() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let days = 5u16;
        let time = 34_567u16;
        let smallsqldatetime = SqlSmallDateTime {
            days,
            time, // Example time in nanoseconds
        };
        let smalldatetimeval = Some(smallsqldatetime.clone());
        let sqldatetime = SqlType::SmallDateTime(smalldatetimeval.clone());

        // Test
        sqldatetime
            .serialize_smalldatetime(&mut packet_writer, &smalldatetimeval)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let byte_len = 4;
        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTimeN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate byte length
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate data length

        assert_eq!(test_cursor.get_u16_le(), days);
        assert_eq!(test_cursor.get_u16_le(), time);
    }

    #[tokio::test]
    async fn test_write_null_smalldatetime() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let sqldatetime = SqlType::SmallDateTime(None);

        // Test
        sqldatetime
            .serialize_smalldatetime(&mut packet_writer, &None)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let byte_len = 4;
        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTimeN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate type length

        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // Validate byte length
    }

    #[tokio::test]
    async fn test_write_datetime2() {
        let nanoseconds = 4_300_001;
        let time = SqlTime {
            time_nanoseconds: nanoseconds,
            scale: 5,
        };
        let datetime2 = SqlDateTime2 {
            time: time.clone(),
            days: 1_000,
        };
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            crate::message::messages::PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let datetime2_val = Some(datetime2.clone());
        let sqltype_datetime2 = SqlType::DateTime2(datetime2_val.clone());

        // Test
        sqltype_datetime2
            .serialize_datetime2(&mut packet_writer, &datetime2_val)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();
        let byte_len = get_scale_based_length(&time).unwrap() + 3;
        let mut written_bytes = vec![0u8; byte_len as usize];
        let test_time_bytes = get_partial_bytes(time.time_nanoseconds, byte_len - 3);
        let test_days_bytes = get_partial_bytes(datetime2.days as u64, 3);
        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTime2N as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), time.scale); // Validate scale
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate byte length
        test_cursor.copy_to_slice(&mut written_bytes);
        // Validate time portions
        assert_eq!(written_bytes[0..((byte_len - 3) as usize)], test_time_bytes);
        // Validate time portions
        assert_eq!(written_bytes[((byte_len - 3) as usize)..], test_days_bytes);
    }

    #[tokio::test]
    async fn test_write_null_datetime2() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            crate::message::messages::PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let sqltype_datetime2 = SqlType::DateTime2(None);

        // Test
        sqltype_datetime2
            .serialize_datetime2(&mut packet_writer, &None)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTime2N as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), DEFAULT_VARTIME_SCALE); // Validate scale
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // Validate byte length
    }

    #[tokio::test]
    async fn test_write_datetimeoffset() {
        let nanoseconds = 4_300_001;
        let time = SqlTime {
            time_nanoseconds: nanoseconds,
            scale: 5,
        };
        let datetime2 = SqlDateTime2 {
            time: time.clone(),
            days: 1_000,
        };
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            crate::message::messages::PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let datetimeoffset = SqlDateTimeOffset {
            datetime2: datetime2.clone(),
            offset: 120, // Example offset in minutes
        };
        let datetimeoffset_val = Some(datetimeoffset.clone());
        let sqltype_datetimeoffset = SqlType::DateTimeOffset(datetimeoffset_val.clone());

        // Test
        sqltype_datetimeoffset
            .serialize_datetimeoffset(&mut packet_writer, &datetimeoffset_val)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();
        let byte_len = get_scale_based_length(&time).unwrap() + 3 + 2;
        let mut written_bytes = vec![0u8; (byte_len - 2) as usize];
        let test_time_bytes = get_partial_bytes(time.time_nanoseconds, byte_len - 5);
        let test_days_bytes = get_partial_bytes(datetime2.days as u64, 3);
        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTimeOffsetN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), time.scale); // Validate scale
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate byte length
        test_cursor.copy_to_slice(&mut written_bytes);
        // Validate time portions
        assert_eq!(written_bytes[0..((byte_len - 5) as usize)], test_time_bytes);
        // Validate time portions
        assert_eq!(written_bytes[((byte_len - 5) as usize)..], test_days_bytes);
        // Validate offset
        assert_eq!(test_cursor.get_i16_le(), datetimeoffset.offset); // Validate offset
    }

    #[tokio::test]
    async fn test_write_null_datetimeoffset() {
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            crate::message::messages::PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let sqltype_datetimeoffset = SqlType::DateTimeOffset(None);

        // Test
        sqltype_datetimeoffset
            .serialize_datetimeoffset(&mut packet_writer, &None)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::DateTimeOffsetN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), DEFAULT_VARTIME_SCALE); // Validate scale
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // Validate byte length
    }

    #[tokio::test]
    async fn test_write_time() {
        let nanoseconds = 1_000_001;
        let time = SqlTime {
            time_nanoseconds: nanoseconds,
            scale: 1,
        };
        let mut mock_reader_writer =
            crate::io::packet_reader::tests::MockNetworkReaderWriter::default();
        let mut packet_writer = crate::io::packet_writer::PacketWriter::new(
            crate::message::messages::PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let time_val = Some(time.clone());
        let sqltype_time = SqlType::Time(time_val.clone());

        // Test
        sqltype_time
            .serialize_time(&mut packet_writer, &time_val)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();
        let byte_len = get_scale_based_length(&time).unwrap();
        let mut written_bytes = vec![0u8; byte_len as usize];
        let test_bytes = get_partial_bytes(time.time_nanoseconds, byte_len);

        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::TimeN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), time.scale); // Validate scale
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate byte length
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, test_bytes); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_time() {
        let mut mock_reader_writer =
            crate::io::packet_reader::tests::MockNetworkReaderWriter::default();
        let mut packet_writer = crate::io::packet_writer::PacketWriter::new(
            crate::message::messages::PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let sqltype_time = SqlType::Time(None);

        // Test
        sqltype_time
            .serialize_time(&mut packet_writer, &None)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();

        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::TimeN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), DEFAULT_VARTIME_SCALE); // Validate scale
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // Validate byte length
    }

    fn get_partial_bytes(value: u64, length: u8) -> Vec<u8> {
        let bytes = value.to_le_bytes();
        bytes[..length as usize].to_vec()
    }

    #[test]
    fn test_scale_based_length() {
        for scale in 1..=7 {
            let time = SqlTime {
                time_nanoseconds: 0,
                scale,
            };
            let length = get_scale_based_length(&time).unwrap();
            match scale {
                1 | 2 => assert_eq!(length, 0x03),
                3 | 4 => assert_eq!(length, 0x04),
                5..=7 => assert_eq!(length, 0x05),
                _ => unreachable!("Scale is guaranteed to be 1-7 by loop bounds"),
            }
        }

        // Test an invalid scale
        let time = SqlTime {
            time_nanoseconds: 0,
            scale: 8, // Invalid scale
        };
        let result = get_scale_based_length(&time);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod binary_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::TdsDataType,
            sqltypes::{PLP_TERMINATOR_CHUNK_LEN, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_small_binary() {
        let payload: Vec<u8> = (0..10).collect();
        // Doesn't matter for serialization, but we need a length.
        let byte_len = payload.len() as u32;
        let val = Some(payload.clone());
        let bit = SqlType::VarBinary(val, byte_len as u16);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let copied_bytes = payload.clone();
        let val = Some(payload);
        bit.serialize_binary(&mut packet_writer, &val, byte_len as u16)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::BigVarBinary as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u16_le(), byte_len as u16);

        assert_eq!(test_cursor.get_i16_le(), byte_len as i16);
        let mut written_bytes = vec![0u8; byte_len as usize];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_large_binary() {
        let payload: Vec<u8> = vec![0xAB; 9000];
        // Doesn't matter for serialization, but we need a length.
        let byte_len = payload.len() as u32;
        let val = Some(payload.clone());
        let bit = SqlType::VarBinary(val, byte_len as u16);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let copied_bytes = payload.clone();
        let val = Some(payload);
        bit.serialize_binary(&mut packet_writer, &val, byte_len as u16)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let written_payload = mock_reader_writer.get_written_data();
        let payload_chunks: Vec<&[u8]> = written_payload.chunks(4096).collect();

        let mut reassembled_packet: Vec<u8> = Vec::new();
        for chunk in payload_chunks.iter() {
            if chunk.len() > 8 {
                reassembled_packet.extend_from_slice(&chunk[8..]);
            }
        }

        let mut test_cursor = Cursor::new(reassembled_packet);

        assert_eq!(test_cursor.get_u8(), TdsDataType::BigVarBinary as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u16_le(), u16::MAX);

        // The PLP length is the total length of the data.
        assert_eq!(test_cursor.get_u64_le(), byte_len as u64);

        // Chunk length.
        assert_eq!(test_cursor.get_u32_le(), byte_len);
        let mut written_bytes = vec![0u8; byte_len as usize];
        test_cursor.copy_to_slice(&mut written_bytes);

        // Verify the string bytes.
        assert_eq!(written_bytes, copied_bytes); // size for Some Data

        // The data is followed by a PLP terminator chunk.
        assert_eq!(test_cursor.get_u32_le(), PLP_TERMINATOR_CHUNK_LEN); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_binary() {
        let bit = SqlType::VarBinary(None, 100);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let val = None;

        bit.serialize_binary(&mut packet_writer, &val, 100)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::BigVarBinary as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u16_le(), 100); // Size of type. 2 bytes when NULL.
        assert_eq!(test_cursor.get_u16_le(), u16::MAX);
    }
}

#[cfg(test)]
mod nvarchar_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sql_string::SqlString,
            sqldatatypes::TdsDataType,
            sqltypes::{PLP_NULL, PLP_TERMINATOR_CHUNK_LEN, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
        token::tokens::SqlCollation,
    };

    #[tokio::test]
    async fn test_write_small_nvarchar() {
        let payload = "Something to write";
        let sql_string = SqlString::from_utf8_string(payload.to_string());
        // Doesn't matter for serialization, but we need a length.
        let len = sql_string.bytes.len() / 2;
        let byte_len = sql_string.bytes.len() as u32;
        let val = Some(sql_string);
        let bit = SqlType::NVarchar(val, len as u16);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let collation = SqlCollation::default();

        let sql_string = SqlString::from_utf8_string(payload.to_string());
        let copied_bytes = sql_string.bytes.clone();
        let val = Some(sql_string);
        bit.serialize_string(&mut packet_writer, &collation, &val, len as u16)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NVarChar as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u16_le(), len as u16);
        let _ignore_collation_info = test_cursor.get_u32();
        let _ignore_collation_sortid = test_cursor.get_u8();
        assert_eq!(test_cursor.get_u16_le(), byte_len as u16);
        let mut written_bytes = vec![0u8; byte_len as usize];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_large_nvarchar() {
        let payload = (0..8005).map(|_| 'a').collect::<String>();
        let sql_string = SqlString::from_utf8_string(payload.to_string());
        // Doesn't matter for serialization, but we need a length.
        let len = sql_string.bytes.len() / 2;
        let byte_len = sql_string.bytes.len() as u32;
        let val = Some(sql_string);
        let bit = SqlType::NVarchar(val, len as u16);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let collation = SqlCollation::default();

        let sql_string = SqlString::from_utf8_string(payload.to_string());
        let copied_bytes = sql_string.bytes.clone();
        let val = Some(sql_string);
        bit.serialize_string(&mut packet_writer, &collation, &val, len as u16)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let written_payload = mock_reader_writer.get_written_data();
        let payload_chunks: Vec<&[u8]> = written_payload.chunks(4096).collect();

        let mut reassembled_packet: Vec<u8> = Vec::new();
        for chunk in payload_chunks.iter() {
            if chunk.len() > 8 {
                reassembled_packet.extend_from_slice(&chunk[8..]);
            }
        }

        let mut test_cursor = Cursor::new(reassembled_packet);

        assert_eq!(test_cursor.get_u8(), TdsDataType::NVarChar as u8); // Valdate tds type

        // Metadata length > 8000 is changed to u16::MAX.
        assert_eq!(test_cursor.get_u16_le(), u16::MAX);
        let _ignore_collation_info = test_cursor.get_u32();
        let _ignore_collation_sortid = test_cursor.get_u8();

        // The PLP length is the total length of the data.
        assert_eq!(test_cursor.get_u64_le(), byte_len as u64);

        // Chunk length.
        assert_eq!(test_cursor.get_u32_le(), byte_len);
        let mut written_bytes = vec![0u8; byte_len as usize];
        test_cursor.copy_to_slice(&mut written_bytes);

        // Verify the string bytes.
        assert_eq!(written_bytes, copied_bytes); // size for Some Data

        // The data is followed by a PLP terminator chunk.
        assert_eq!(test_cursor.get_u32_le(), PLP_TERMINATOR_CHUNK_LEN); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_nvarchar() {
        let bit = SqlType::NVarchar(None, 100);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let collation = SqlCollation::default();
        let size = u16::MAX;
        let val = None;
        bit.serialize_string(&mut packet_writer, &collation, &val, size)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NVarChar as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u16_le(), u16::MAX); // Size of type. 2 bytes when NULL.
        let _ignore_collation_info = test_cursor.get_u32();
        let _ignore_collation_sortid = test_cursor.get_u8();
        assert_eq!(test_cursor.get_u64_le(), PLP_NULL);
    }

    #[tokio::test]
    async fn test_write_null_nvarchar_non_plp_via_serialize_nvarchar() {
        // Use a non-MAX declared length to trigger non-PLP NULL path
        let declared_len_chars: u16 = 100; // characters
        let bit = SqlType::NVarchar(None, declared_len_chars);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        let collation = SqlCollation::default();

        // Call serialize_nvarchar directly to exercise the NVARCHAR-specific path
        bit.serialize_nvarchar(&mut packet_writer, &collation, &None, declared_len_chars)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NVarChar as u8); // TDS type
        // NVARCHAR metadata length is bytes, so chars*2
        assert_eq!(test_cursor.get_u16_le(), declared_len_chars * 2);
        let _ignore_collation_info = test_cursor.get_u32();
        let _ignore_collation_sortid = test_cursor.get_u8();
        // Non-PLP NULL must write u16::MAX as the data length
        assert_eq!(test_cursor.get_u16_le(), u16::MAX);
    }
}

#[cfg(test)]
mod bigint_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::{FixedLengthTypes, TdsDataType},
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_positive_bigint() {
        let bigint = 1234456778123123123;
        let val = Some(bigint);
        let bit = SqlType::BigInt(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_bigint(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int8.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int8.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_i64_le(), bigint); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_bigint() {
        let bit = SqlType::BigInt(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_bigint(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int8.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size of the data
    }
}

#[cfg(test)]
mod f32_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::{FixedLengthTypes, TdsDataType},
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_f32() {
        let f32 = 1234.56f32;
        let val = Some(f32);
        let bit = SqlType::Real(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_f32(&mut packet_writer, &val).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::FltN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Flt4.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Flt4.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_f32_le(), f32); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_f32() {
        let bit = SqlType::Real(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_f32(&mut packet_writer, &None).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::FltN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Flt4.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size of the data
    }
}

#[cfg(test)]
mod f64_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::{FixedLengthTypes, TdsDataType},
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_f64() {
        let f64 = 1234.56f64;
        let val = Some(f64);
        let bit = SqlType::Float(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_f64(&mut packet_writer, &val).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::FltN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Flt8.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Flt8.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_f64_le(), f64); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_f32() {
        let bit = SqlType::Float(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_f32(&mut packet_writer, &None).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::FltN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Flt8.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size of the data
    }
}

#[cfg(test)]
mod decimalparts_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::TdsDataType,
            sqltypes::{DECIMAL_FIXED_SIZE, NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_positive_decimal() {
        let int_parts = vec![123456789, 1234, 123];
        let scale = 2;
        let precision = 10;
        let decimal_parts = crate::datatypes::decoder::DecimalParts {
            precision,
            scale,
            is_positive: true,
            int_parts: int_parts.clone(),
        };
        let val = Some(decimal_parts);
        let decimal_parts2 = crate::datatypes::decoder::DecimalParts {
            precision,
            scale,
            is_positive: true,
            int_parts: int_parts.clone(),
        };
        let bit = SqlType::Decimal(Some(decimal_parts2));

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_decimalparts(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NumericN as u8); // Validate tds type
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // type length data
        assert_eq!(test_cursor.get_u8(), precision); // precision
        assert_eq!(test_cursor.get_u8(), scale); // scale
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // size of the data (always 17 for TDS 7.0+)
        assert_eq!(test_cursor.get_u8(), 0x01); // Positive value
        let mut parts: Vec<i32> = Vec::new();
        for _ in 0..3 {
            parts.push(test_cursor.get_i32_le());
        }
        assert_eq!(int_parts.clone(), parts); // verify written data
    }

    #[tokio::test]
    async fn test_write_negative_decimal() {
        let int_parts = vec![123456789, 1234, 123];
        let scale = 2;
        let precision = 10;
        let decimal_parts = crate::datatypes::decoder::DecimalParts {
            precision,
            scale,
            is_positive: false,
            int_parts: int_parts.clone(),
        };
        let val = Some(decimal_parts);
        let decimal_parts2 = crate::datatypes::decoder::DecimalParts {
            precision,
            scale,
            is_positive: false,
            int_parts: int_parts.clone(),
        };
        let bit = SqlType::Decimal(Some(decimal_parts2));

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_decimalparts(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NumericN as u8); // Validate tds type
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // type length data
        assert_eq!(test_cursor.get_u8(), precision); // precision
        assert_eq!(test_cursor.get_u8(), scale); // scale
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // size of the data (always 17 for TDS 7.0+)
        assert_eq!(test_cursor.get_u8(), 0x00); // Negative value
        let mut parts: Vec<i32> = Vec::new();
        for _ in 0..3 {
            parts.push(test_cursor.get_i32_le());
        }
        assert_eq!(int_parts.clone(), parts); // verify written data
    }

    #[tokio::test]
    async fn test_write_null_decimal() {
        let bit = SqlType::Decimal(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_decimalparts(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NumericN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // type length data
        assert_eq!(test_cursor.get_u8(), 1); // type length data
        assert_eq!(test_cursor.get_u8(), 0); // type length data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // length data
    }
}

#[cfg(test)]
mod int_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::{FixedLengthTypes, TdsDataType},
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_positive_int() {
        let numval = 1234;
        let val = Some(numval);
        let bit = SqlType::Int(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_int(&mut packet_writer, &val).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int4.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int4.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_i32_le(), numval); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_negative_int() {
        let intval = -1234i32;
        let val = Some(intval);
        let data = SqlType::Int(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        data.serialize_int(&mut packet_writer, &val).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int4.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int4.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_i32_le(), intval); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_int() {
        let val = None;
        let bit = SqlType::Int(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_int(&mut packet_writer, &val).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int4.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
    }
}

#[cfg(test)]
mod smallint_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::{FixedLengthTypes, TdsDataType},
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_positive_smallint() {
        let numval = 1234;
        let val = Some(numval);
        let bit = SqlType::SmallInt(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_smallint(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int2.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int2.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_i16_le(), numval); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_negative_smallint() {
        let negval = -1234;
        let val = Some(negval);
        let bit = SqlType::SmallInt(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_smallint(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int2.get_len() as u8); // type length data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int2.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_i16_le(), negval); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_null_smallint() {
        let val = None;
        let bit = SqlType::SmallInt(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_smallint(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int2.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
    }
}

#[cfg(test)]
mod tinyint_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{io::packet_reader::tests::MockNetworkReaderWriter, message::messages::PacketType};

    use super::*;

    #[tokio::test]
    async fn test_write_some_bit_positive() {
        let byte = 5;
        let val = Some(byte);
        let bit = SqlType::TinyInt(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_tinyint(&mut packet_writer, &val)
            .await
            .unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int1.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int1.get_len() as u8); // size for Some Data
        assert_eq!(test_cursor.get_u8(), byte); // Data
    }

    #[tokio::test]
    async fn test_write_null_tinyint() {
        let val = None;
        let bit = SqlType::TinyInt(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_tinyint(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Int1.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
    }
}

#[cfg(test)]
mod bit_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{io::packet_reader::tests::MockNetworkReaderWriter, message::messages::PacketType};

    use super::*;

    #[tokio::test]
    async fn test_write_some_bit_true() {
        let val = Some(true);
        let bit = SqlType::Bit(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_bit(&mut packet_writer, &val).await.unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Bit.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Bit.get_len() as u8); // size for Some Data
        assert_eq!(test_cursor.get_u8(), 1); // Data
    }

    #[tokio::test]
    async fn test_write_some_bit_false() {
        let val = Some(false);
        let bit = SqlType::Bit(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_bit(&mut packet_writer, &val).await.unwrap();

        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Bit.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Bit.get_len() as u8); // size for Some Data
        assert_eq!(test_cursor.get_u8(), 0); // Data
    }

    #[tokio::test]
    async fn test_write_null_bit() {
        let val = None;
        let bit = SqlType::Bit(val);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        bit.serialize_bit(&mut packet_writer, &val).await.unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::IntN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), FixedLengthTypes::Bit.get_len() as u8); // size of the data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
    }
}

#[cfg(test)]
mod xml_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            column_values::SqlXml,
            sqldatatypes::TdsDataType,
            sqltypes::{NO_XML_SCHEMA, PLP_NULL, PLP_UNKNOWN_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_nobom_xml() {
        let xml = "<root><child>Test</child></root>";
        let sqlxml: SqlXml = xml.to_string().into();

        let mut copied_bytes = Vec::new();
        copied_bytes.push(0xFF);
        copied_bytes.push(0xFE);
        copied_bytes.extend_from_slice(sqlxml.bytes.as_slice());

        let byte_len_with_bom = sqlxml.bytes.len() + 2;

        let val = Some(sqlxml);
        let sqltypexml = SqlType::Xml(val.clone());

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypexml
            .serialize_xml(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Xml as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), NO_XML_SCHEMA);
        assert_eq!(test_cursor.get_u64_le(), PLP_UNKNOWN_LENGTH);
        assert_eq!(test_cursor.get_u32_le(), byte_len_with_bom as u32); // Chunk len
        let mut written_bytes = vec![0u8; byte_len_with_bom];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes);
    }

    #[tokio::test]
    async fn test_write_withbom_xml() {
        let xml = "<root><child>Test</child></root>";
        let sqlxml: SqlXml = xml.to_string().into();

        let mut copied_bytes = Vec::new();
        copied_bytes.push(0xFF);
        copied_bytes.push(0xFE);
        copied_bytes.extend_from_slice(sqlxml.bytes.as_slice());

        let sqlxml = SqlXml {
            bytes: copied_bytes.clone(),
        };
        let byte_len_with_bom = sqlxml.bytes.len();

        let val = Some(sqlxml);
        let sqltypexml = SqlType::Xml(val.clone());

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypexml
            .serialize_xml(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let written_payload = mock_reader_writer.get_written_data();
        let payload_chunks: Vec<&[u8]> = written_payload.chunks(4096).collect();

        let mut reassembled_packet: Vec<u8> = Vec::new();
        for chunk in payload_chunks.iter() {
            if chunk.len() > 8 {
                reassembled_packet.extend_from_slice(&chunk[8..]);
            }
        }

        let mut test_cursor = Cursor::new(reassembled_packet);

        assert_eq!(test_cursor.get_u8(), TdsDataType::Xml as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), NO_XML_SCHEMA);
        assert_eq!(test_cursor.get_u64_le(), PLP_UNKNOWN_LENGTH);
        assert_eq!(test_cursor.get_u32_le(), byte_len_with_bom as u32); // Chunk len
        let mut written_bytes = vec![0u8; byte_len_with_bom];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes);
    }

    #[tokio::test]
    async fn test_write_null_xml() {
        let sqltypexml = SqlType::Xml(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypexml
            .serialize_xml(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Xml as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), NO_XML_SCHEMA);
        assert_eq!(test_cursor.get_u64_le(), PLP_NULL);
    }
}

#[cfg(test)]
mod uuid_tests {
    use crate::{
        datatypes::{
            sqldatatypes::TdsDataType,
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };
    use bytes::Buf;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_write_uuid() {
        let generated_uuid = uuid::Uuid::new_v4();
        let uuid = SqlType::Uuid(Some(generated_uuid));
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        uuid.serialize_uuid(&mut packet_writer, &Some(generated_uuid))
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();
        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Guid as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 16); // size of the type
        assert_eq!(test_cursor.get_u8(), 16); // size for data.
        let mut written_bytes = vec![0u8; 16];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, generated_uuid.to_bytes_le()); // Validate the written UUID bytes
        assert!(!test_cursor.has_remaining()); // Ensure that the cursor has no remaining data
    }

    #[tokio::test]
    async fn test_write_null_uuid() {
        let uuid = SqlType::Uuid(None);
        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );
        uuid.serialize_uuid(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();
        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Guid as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 16); // size of the type
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
        assert!(!test_cursor.has_remaining()); // Ensure that the cursor has no remaining data
    }
}

#[cfg(test)]
mod vector_tests {
    use crate::{
        datatypes::{
            sql_vector::SqlVector,
            sqldatatypes::{
                TdsDataType, VECTOR_HEADER_SIZE, VectorBaseType, VectorLayoutFormat,
                VectorLayoutVersion,
            },
            sqltypes::SqlType,
        },
        error::Error,
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };
    use bytes::Buf;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_write_vector_3d() {
        let values = vec![1.0f32, 2.0f32, 3.0f32];
        let vector = SqlVector::try_from_f32(values.clone()).unwrap();
        let sql_type = SqlType::Vector(Some(vector.clone()), 3, VectorBaseType::Float32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sql_type
            .serialize_vector(
                &mut packet_writer,
                &Some(vector),
                3,
                VectorBaseType::Float32,
            )
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut cursor = Cursor::new(payload);
        cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);

        // Validate TDS type byte (0xF5)
        assert_eq!(cursor.get_u8(), TdsDataType::Vector as u8);

        // Validate USHORTLEN (max size)
        let max_size = (VECTOR_HEADER_SIZE as u16) + (3 * 4); // 3 dimensions * 4 bytes
        assert_eq!(cursor.get_u16_le(), max_size);

        // Validate SCALE (base type: 0x00 for Float32)
        assert_eq!(cursor.get_u8(), VectorBaseType::Float32 as u8);

        // Validate length prefix
        let data_length = (VECTOR_HEADER_SIZE as u16) + (3 * 4);
        assert_eq!(cursor.get_u16_le(), data_length);

        // Validate 8-byte header
        assert_eq!(cursor.get_u8(), VectorLayoutFormat::V1 as u8); // layout_format
        assert_eq!(cursor.get_u8(), VectorLayoutVersion::V1 as u8); // layout_version
        assert_eq!(cursor.get_u16_le(), 3); // dimension_count
        assert_eq!(cursor.get_u8(), VectorBaseType::Float32 as u8); // base_type
        assert_eq!(cursor.get_u8(), 0x00); // reserved
        assert_eq!(cursor.get_u8(), 0x00); // reserved
        assert_eq!(cursor.get_u8(), 0x00); // reserved

        // Validate element values (little-endian floats)
        assert_eq!(cursor.get_f32_le(), 1.0f32);
        assert_eq!(cursor.get_f32_le(), 2.0f32);
        assert_eq!(cursor.get_f32_le(), 3.0f32);

        assert!(!cursor.has_remaining());
    }

    #[tokio::test]
    async fn test_write_null_vector() {
        let sql_type = SqlType::Vector(None, 10, VectorBaseType::Float32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sql_type
            .serialize_vector(&mut packet_writer, &None, 10, VectorBaseType::Float32)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut cursor = Cursor::new(payload);
        cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);

        // Validate TDS type byte
        assert_eq!(cursor.get_u8(), TdsDataType::Vector as u8);

        // Validate USHORTLEN
        let max_size = (VECTOR_HEADER_SIZE as u16) + (10 * 4);
        assert_eq!(cursor.get_u16_le(), max_size);

        // Validate SCALE
        assert_eq!(cursor.get_u8(), VectorBaseType::Float32 as u8);

        // Validate NULL length (0xFFFF)
        assert_eq!(cursor.get_u16_le(), 0xFFFF);

        assert!(!cursor.has_remaining());
    }

    #[tokio::test]
    async fn test_write_single_dimension_vector() {
        let values = vec![42.5f32];
        let vector = SqlVector::try_from_f32(values).unwrap();
        let sql_type = SqlType::Vector(Some(vector.clone()), 1, VectorBaseType::Float32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sql_type
            .serialize_vector(
                &mut packet_writer,
                &Some(vector),
                1,
                VectorBaseType::Float32,
            )
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut cursor = Cursor::new(payload);
        cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);

        assert_eq!(cursor.get_u8(), TdsDataType::Vector as u8);
        let max_size = (VECTOR_HEADER_SIZE as u16) + 4;
        assert_eq!(cursor.get_u16_le(), max_size);
        assert_eq!(cursor.get_u8(), VectorBaseType::Float32 as u8);

        let data_length = (VECTOR_HEADER_SIZE as u16) + 4;
        assert_eq!(cursor.get_u16_le(), data_length);

        // Skip header validation (already tested above)
        cursor.advance(8);

        assert_eq!(cursor.get_f32_le(), 42.5f32);
        assert!(!cursor.has_remaining());
    }

    #[tokio::test]
    async fn test_write_large_dimension_vector() {
        // Test with 100 dimensions
        let values: Vec<f32> = (0..100).map(|i| i as f32 * 0.5).collect();
        let vector = SqlVector::try_from_f32(values.clone()).unwrap();
        let sql_type = SqlType::Vector(Some(vector.clone()), 100, VectorBaseType::Float32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sql_type
            .serialize_vector(
                &mut packet_writer,
                &Some(vector),
                100,
                VectorBaseType::Float32,
            )
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut cursor = Cursor::new(payload);
        cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);

        assert_eq!(cursor.get_u8(), TdsDataType::Vector as u8);
        let max_size = (VECTOR_HEADER_SIZE as u16) + (100 * 4);
        assert_eq!(cursor.get_u16_le(), max_size);
        assert_eq!(cursor.get_u8(), VectorBaseType::Float32 as u8);

        let data_length = (VECTOR_HEADER_SIZE as u16) + (100 * 4);
        assert_eq!(cursor.get_u16_le(), data_length);

        // Validate header
        assert_eq!(cursor.get_u8(), VectorLayoutFormat::V1 as u8);
        assert_eq!(cursor.get_u8(), VectorLayoutVersion::V1 as u8);
        assert_eq!(cursor.get_u16_le(), 100);
        cursor.advance(4); // Skip base_type + reserved bytes

        // Validate all dimension values
        for i in 0..100 {
            assert_eq!(cursor.get_f32_le(), i as f32 * 0.5);
        }

        assert!(!cursor.has_remaining());
    }

    #[tokio::test]
    async fn test_vector_dimensions_exceeds_max() {
        // Test with dimensions exceeding VECTOR_MAX_DIMENSIONS (1998)
        let values: Vec<f32> = vec![1.0, 2.0, 3.0];
        let vector = SqlVector::try_from_f32(values).unwrap();
        let sql_type = SqlType::Vector(Some(vector.clone()), 2000, VectorBaseType::Float32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let result = sql_type
            .serialize_vector(
                &mut packet_writer,
                &Some(vector),
                2000,
                VectorBaseType::Float32,
            )
            .await;

        assert!(result.is_err());
        match result {
            Err(Error::UsageError(msg)) => {
                assert!(msg.contains("Vector dimensions 2000 exceeds maximum"));
                assert!(msg.contains("1998"));
            }
            _ => panic!("Expected UsageError for exceeding max dimensions"),
        }
    }

    #[tokio::test]
    async fn test_vector_dimension_mismatch() {
        // Test with declared dimensions not matching actual vector dimensions
        let values: Vec<f32> = vec![1.0, 2.0, 3.0];
        let vector = SqlVector::try_from_f32(values).unwrap();
        // Declare 5 dimensions but vector only has 3
        let sql_type = SqlType::Vector(Some(vector.clone()), 5, VectorBaseType::Float32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();
        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let result = sql_type
            .serialize_vector(
                &mut packet_writer,
                &Some(vector),
                5,
                VectorBaseType::Float32,
            )
            .await;

        assert!(result.is_err());
        match result {
            Err(Error::TypeConversionError(msg)) => {
                assert!(msg.contains("Vector dimension mismatch"));
                assert!(msg.contains("declared 5"));
                assert!(msg.contains("but vector has 3"));
            }
            _ => panic!("Expected TypeConversionError for dimension mismatch"),
        }
    }

    #[tokio::test]
    async fn test_get_nullable_type() {
        let sql_type = SqlType::Vector(None, 10, VectorBaseType::Float32);
        let nullable_type = sql_type.get_nullable_type();
        assert_eq!(nullable_type, TdsDataType::Vector);
    }
}

#[cfg(test)]
mod money_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::TdsDataType,
            sqltypes::{NULL_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_money() {
        let (moneyvallsb, moneyvalmsb) = (12345i32, 123i32);
        let moneyparts = Some((moneyvallsb, moneyvalmsb).into());
        let money = SqlType::Money(moneyparts.clone());

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        money
            .serialize_money(&mut packet_writer, &moneyparts)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::MoneyN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 8); // size of the type
        assert_eq!(test_cursor.get_u8(), 8); // size for Some Data
        assert_eq!(test_cursor.get_i32_le(), moneyvalmsb);
        assert_eq!(test_cursor.get_i32_le(), moneyvallsb);
        assert!(!test_cursor.has_remaining()); // Ensure that the cursor has no remaining data
    }

    #[tokio::test]
    async fn test_write_null_money() {
        let money = SqlType::Money(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        money
            .serialize_money(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::MoneyN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 8); // size of the data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
        assert!(!test_cursor.has_remaining()); // Ensure that the cursor has no remaining data
    }

    #[tokio::test]
    async fn test_write_smallmoney() {
        let moneyval = 12345i32;
        let smallmoney = Some(moneyval.into());
        let money = SqlType::SmallMoney(smallmoney.clone());

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        money
            .serialize_smallmoney(&mut packet_writer, &smallmoney)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::MoneyN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 4); // size of the type
        assert_eq!(test_cursor.get_u8(), 4); // size for Some Data
        assert_eq!(test_cursor.get_i32_le(), moneyval);
        assert!(!test_cursor.has_remaining()); // Ensure that the cursor has no remaining data
    }

    #[tokio::test]
    async fn test_write_null_smallmoney() {
        let money = SqlType::SmallMoney(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        money
            .serialize_smallmoney(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::MoneyN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), 4); // size of the data
        assert_eq!(test_cursor.get_u8(), NULL_LENGTH); // size for Some Data
        assert!(!test_cursor.has_remaining()); // Ensure that the cursor has no remaining data
    }
}

#[cfg(test)]
mod json_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sql_json::SqlJson,
            sqldatatypes::TdsDataType,
            sqltypes::{PLP_NULL, PLP_UNKNOWN_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_json() {
        let json_str = "[\"abc\",\"ghi\",\"def\"]";
        let sqljson: SqlJson = json_str.to_string().into();

        let mut copied_bytes = Vec::new();

        copied_bytes.extend_from_slice(sqljson.bytes.as_slice());

        let byte_len = sqljson.bytes.len();

        let val = Some(sqljson);
        let sqltypejson = SqlType::Json(val.clone());

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypejson
            .serialize_json(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Json as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u64_le(), PLP_UNKNOWN_LENGTH);
        assert_eq!(test_cursor.get_u32_le(), byte_len as u32); // Chunk len
        let mut written_bytes = vec![0u8; byte_len];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes);
    }

    #[tokio::test]
    async fn test_write_null_json() {
        let sqltypejson = SqlType::Json(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypejson
            .serialize_json(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Json as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u64_le(), PLP_NULL);
    }
}
