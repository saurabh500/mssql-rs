use byteorder::{ByteOrder, LittleEndian};
use uuid::Uuid;

use crate::datatypes::column_values::{
    DateTime2, DateTimeOffset, SqlDate, SqlDateTime, SqlSmallDateTime, SqlXml, Time,
    DEFAULT_VARTIME_SCALE,
};
use crate::{
    core::TdsResult,
    datatypes::{
        decoder::{DecimalParts, MoneyParts},
        sql_string::SqlString,
        sqldatatypes::{FixedLengthTypes, TdsDataType},
    },
    error::Error,
    read_write::packet_writer::PacketWriter,
    token::tokens::SqlCollation,
};

#[derive(Debug, PartialEq)]
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
    Money(Option<MoneyParts>),
    SmallMoney(Option<MoneyParts>),

    Time(Option<Time>),
    DateTime2(Option<DateTime2>),
    DateTimeOffset(Option<DateTimeOffset>),
    SmallDateTime(Option<SqlSmallDateTime>),
    DateTime(Option<SqlDateTime>),
    Date(Option<SqlDate>),

    /// Represents a Varchar with a specifiied length.
    NVarchar(Option<SqlString>, u32),

    /// Represents a Varchar with MAX length.
    NVarcharMax(Option<SqlString>),

    Varchar(Option<SqlString>, u32),
    VarcharMax(Option<SqlString>),

    VarBinary(Option<Vec<u8>>, u32),
    VarBinaryMax(Option<Vec<u8>>),

    Binary(Option<Vec<u8>>, u32),
    Char(Option<SqlString>, u32),
    NChar(Option<SqlString>, u32),

    Text(Option<SqlString>),
    NText(Option<SqlString>),

    Json(Option<String>),

    Xml(Option<SqlXml>),
    Uuid(Option<Uuid>),
    // To be added in future
    // Variant
    // TVP
}

type NullableTdsType = TdsDataType;

// The maximum length of a variable length type in TDS is 8000 bytes.
pub(crate) const VAR_TDS_MAX_LENGTH: u16 = 8000u16;

// The length of a NULL value in TDS is 65535 bytes for variable length types.
pub(crate) const VAR_NULL_LENGTH: u16 = 65535u16;

// The length of a NULL value in TDS is 0 bytes.
pub(crate) const NULL_LENGTH: u8 = 0u8;

// The fixed size for Decimal in TDS is 17 bytes.
pub(crate) const DECIMAL_FIXED_SIZE: u8 = 17;

// The short data length which signifies that the data is being sent as PLP (Partial Length Packet).
pub(crate) const MAX_SHORT_DATA_LENGTH: u16 = 0xFFFF;

pub(crate) const PLP_TERMINATOR_CHUNK_LEN: u32 = 0x00000000;

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
            SqlType::Json(_) => todo!(),
            SqlType::Money(_) => todo!(),
            SqlType::SmallMoney(_) => todo!(),
            SqlType::Time(_) => TdsDataType::TimeN,
            SqlType::DateTime2(_) => TdsDataType::DateTime2N,
            SqlType::DateTimeOffset(_) => TdsDataType::DateTimeOffsetN,
            SqlType::DateTime(_) => TdsDataType::DateTimeN,
            SqlType::Date(_) => TdsDataType::DateN,
            SqlType::SmallDateTime(_) => TdsDataType::DateTimeN,
            SqlType::NVarcharMax(_) => TdsDataType::NVarChar,
            SqlType::Varchar(_, _) => TdsDataType::BigVarChar,
            SqlType::VarcharMax(_) => TdsDataType::BigVarChar,
            SqlType::VarBinaryMax(_) => todo!(),
            SqlType::Xml(_) => todo!(),
            SqlType::Uuid(_) => TdsDataType::Guid,
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

            SqlType::Binary(_items, _) => todo!(),
            SqlType::Char(_sql_string, _) => todo!(),
            SqlType::NChar(_sql_string, _) => todo!(),
            SqlType::Text(_sql_string) => todo!(),
            SqlType::NText(_sql_string) => todo!(),
            SqlType::Json(_) => todo!(),
            SqlType::Money(_) => todo!(),
            SqlType::SmallMoney(_) => todo!(),
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
            SqlType::NVarchar(sql_string, _) => {
                self.serialize_nvarchar(packet_writer, db_collation, sql_string)
                    .await?
            }
            SqlType::NVarcharMax(sql_string) => {
                self.serialize_nvarchar(packet_writer, db_collation, sql_string)
                    .await?
            }
            SqlType::Varchar(sql_string, _) => {
                self.serialize_nvarchar(packet_writer, db_collation, sql_string)
                    .await?
            }
            SqlType::VarcharMax(sql_string) => {
                self.serialize_nvarchar(packet_writer, db_collation, sql_string)
                    .await?
            }
            SqlType::VarBinary(binary_data, _) => {
                self.serialize_binary(packet_writer, binary_data).await?
            }
            SqlType::VarBinaryMax(binary_data) => {
                self.serialize_binary(packet_writer, binary_data).await?
            }
            SqlType::Xml(_sql_xml) => todo!(),
            SqlType::Uuid(uuid) => self.serialize_uuid(packet_writer, uuid).await?,
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
                // Send the actual data size.
                packet_writer.write_byte_async(DECIMAL_FIXED_SIZE).await?;
                if v.is_positive {
                    packet_writer.write_byte_async(0x01).await?;
                } else {
                    packet_writer.write_byte_async(0x00).await?;
                }

                // Write up to 3 int_parts, pad with zeros if fewer, ignore extras.
                // Always write 4 i32 values.
                for i in 0..3 {
                    let part = v.int_parts.get(i).copied().unwrap_or(0);
                    packet_writer.write_i32_async(part).await?;
                }
                // The fourth part is always 0 as per your requirement.
                packet_writer.write_i32_async(0).await?;
            }
            None => packet_writer.write_byte_async(NULL_LENGTH).await?,
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

    async fn serialize_nvarchar(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
        sql_string: &Option<SqlString>,
    ) -> TdsResult<()> {
        let optional_string = match sql_string {
            Some(string) => Some(string),
            None => None,
        };
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;

        match optional_string {
            Some(string) => {
                let should_send_as_plp = string.bytes.len() > VAR_TDS_MAX_LENGTH as usize;
                if !should_send_as_plp {
                    // Write the length for the metadata.
                    packet_writer
                        .write_i16_async(string.bytes.len() as i16)
                        .await?;
                    packet_writer.write_u32_async(db_collation.info).await?;
                    packet_writer.write_byte_async(db_collation.sort_id).await?;
                    // Write the length of the actual data.
                    packet_writer
                        .write_i16_async(string.bytes.len() as i16)
                        .await?;
                    // Write the data.
                    packet_writer.write_async(&string.bytes).await?;
                } else {
                    // Write FFFF indicating that data is being sent as PLP.
                    packet_writer.write_u16_async(MAX_SHORT_DATA_LENGTH).await?;
                    packet_writer.write_u32_async(db_collation.info).await?;
                    packet_writer.write_byte_async(db_collation.sort_id).await?;

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
                // Write 2 len to signify that the actual data length is going to be 2 bytes.
                packet_writer.write_i16_async(2).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
                // Write 0 data length to signify that the data is NULL.
                packet_writer.write_u16_async(VAR_NULL_LENGTH).await?;
            }
        }
        Ok(())
    }

    async fn serialize_binary(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        binary_data: &Option<Vec<u8>>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let optional_binary = match binary_data {
            Some(binary) => Some(binary),
            None => None,
        };
        match optional_binary {
            Some(data) => {
                let should_send_as_plp = data.len() > VAR_TDS_MAX_LENGTH as usize;
                if !should_send_as_plp {
                    // Write the length for the metadata.
                    packet_writer.write_i16_async(data.len() as i16).await?;

                    // Write the length of the actual data.
                    packet_writer.write_i16_async(data.len() as i16).await?;
                    // Write the data.
                    packet_writer.write_async(data).await?;
                } else {
                    // Write FFFF indicating that data is being sent as PLP.
                    packet_writer.write_u16_async(MAX_SHORT_DATA_LENGTH).await?;

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
                // Write 2 len to signify that the actual data length is going to be 2 bytes.
                packet_writer.write_i16_async(2).await?;
                // Write 0 data length to signify that the data is NULL.
                packet_writer.write_u16_async(VAR_NULL_LENGTH).await?;
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

    async fn serialize_datetime2(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        datetime2: &Option<DateTime2>,
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

                let scale_adjusted_time = get_scale_adjusted_time(t)?;

                packet_writer.write_byte_async(scale).await?;

                packet_writer.write_byte_async(length).await?;

                // Write the time in nanoseconds.
                packet_writer
                    .write_partial_u64_async(scale_adjusted_time, byte_count_for_time)
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
        datetimeoffset: &Option<DateTimeOffset>,
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

                let scale_adjusted_time = get_scale_adjusted_time(t)?;

                packet_writer.write_byte_async(scale).await?;

                packet_writer.write_byte_async(length).await?;

                // Write the time in nanoseconds.
                packet_writer
                    .write_partial_u64_async(scale_adjusted_time, byte_count_for_time)
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
        time: &Option<Time>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;

        match time {
            Some(t) => {
                let scale = t.get_scale();

                let scale_based_byte_length = get_scale_based_length(t)?;

                let scale_adjusted_time = get_scale_adjusted_time(t)?;

                packet_writer.write_byte_async(scale).await?;

                packet_writer
                    .write_byte_async(scale_based_byte_length)
                    .await?;

                // Write the time in nanoseconds.
                packet_writer
                    .write_partial_u64_async(scale_adjusted_time, scale_based_byte_length)
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
        let byte_len = 3; // Date is always 3 byes for non-null dates.
                          // Write the length of the dateN byte len.
        packet_writer.write_byte_async(byte_len).await?;
        match date {
            Some(d) => {
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
}

// We are taking the map from the protocol documentation that defines the scale.
// However the scale essentially defines the precision of the time and
// the length of bytes can be computed from the scale. But since
// this is documented, we will use this map.
fn get_scale_based_length(time: &Time) -> TdsResult<u8> {
    let scale_based_byte_length: u8 = match time.scale {
        1 | 2 => 0x03,
        3 | 4 => 0x04,
        5..=7 => 0x05,
        _ => {
            return Err(Error::UsageError(
                format!("Invalid scale for Time type. {}", time.scale).to_string(),
            ))
        }
    };
    Ok(scale_based_byte_length)
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

fn get_scale_adjusted_time(t: &Time) -> TdsResult<u64> {
    let scale = t.get_scale();
    let divider = match scale {
        1 => 1_000_001,
        2 => 100_000,
        3 => 10_000,
        4 => 1_000,
        5 => 100,
        6 => 10,
        7 => 1,
        _ => {
            return Err(Error::UsageError(
                "Invalid scale for Time type.".to_string(),
            ))
        }
    };
    let scale_adjusted_time = t.time_nanoseconds / divider;
    Ok(scale_adjusted_time)
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
                DateTime2, DateTimeOffset, SqlDate, SqlDateTime, SqlSmallDateTime, Time,
                DEFAULT_VARTIME_SCALE,
            },
            sqldatatypes::TdsDataType,
            sqltypes::{get_scale_adjusted_time, get_scale_based_length, SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
        assert_eq!(test_cursor.get_u8(), byte_len); // Validate type length
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
        assert_eq!(test_cursor.get_u8(), 3); // Validate type length
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
        let time = Time {
            time_nanoseconds: nanoseconds,
            scale: 5,
        };
        let datetime2 = DateTime2 {
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
        let test_time_bytes =
            get_partial_bytes(get_scale_adjusted_time(&time).unwrap(), byte_len - 3);
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
        let time = Time {
            time_nanoseconds: nanoseconds,
            scale: 5,
        };
        let datetime2 = DateTime2 {
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
        let datetimeoffset = DateTimeOffset {
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
        let test_time_bytes =
            get_partial_bytes(get_scale_adjusted_time(&time).unwrap(), byte_len - 5);
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
        let time = Time {
            time_nanoseconds: nanoseconds,
            scale: 1,
        };
        let mut mock_reader_writer =
            crate::read_write::packet_reader::tests::MockNetworkReaderWriter::default();
        let mut packet_writer = crate::read_write::packet_writer::PacketWriter::new(
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
        let test_bytes = get_partial_bytes(get_scale_adjusted_time(&time).unwrap(), byte_len);

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
            crate::read_write::packet_reader::tests::MockNetworkReaderWriter::default();
        let mut packet_writer = crate::read_write::packet_writer::PacketWriter::new(
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
            let time = Time {
                time_nanoseconds: 0,
                scale,
            };
            let length = get_scale_based_length(&time).unwrap();
            match scale {
                1 | 2 => assert_eq!(length, 0x03),
                3 | 4 => assert_eq!(length, 0x04),
                5..=7 => assert_eq!(length, 0x05),
                _ => panic!("Invalid scale: {}", scale),
            }
        }

        // Test an invalid scale
        let time = Time {
            time_nanoseconds: 0,
            scale: 8, // Invalid scale
        };
        let result = get_scale_based_length(&time);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_scale_adjusted_time() {
        let time = Time {
            time_nanoseconds: 1_000_001, // 100 nanoseconds
            scale: 1,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 1);

        let time = Time {
            time_nanoseconds: 1_000_001,
            scale: 2,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 10);
        let time = Time {
            time_nanoseconds: 1_000_001,
            scale: 3,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 1_00);
        let time = Time {
            time_nanoseconds: 1_000_001,
            scale: 4,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 1_000); // 100 microseconds
        let time = Time {
            time_nanoseconds: 1_000_001, // 1 millisecond
            scale: 5,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 10_000); // 1 millisecond
        let time = Time {
            time_nanoseconds: 1_000_001, // 10 milliseconds
            scale: 6,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 100_000);

        let time = Time {
            time_nanoseconds: 1_000_001,
            scale: 7,
        };
        let adjusted_time = get_scale_adjusted_time(&time).unwrap();
        assert_eq!(adjusted_time, 1_000_001); // 100 milliseconds
    }
}

#[cfg(test)]
mod binary_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::TdsDataType,
            sqltypes::{SqlType, MAX_SHORT_DATA_LENGTH, PLP_TERMINATOR_CHUNK_LEN, VAR_NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
    };

    #[tokio::test]
    async fn test_write_small_binary() {
        let payload: Vec<u8> = (0..10).collect();
        // Doesn't matter for serialization, but we need a length.
        let len = payload.len() / 2;
        let byte_len = payload.len() as u32;
        let val = Some(payload.clone());
        let bit = SqlType::VarBinary(val, len as u32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let copied_bytes = payload.clone();
        let val = Some(payload);
        bit.serialize_binary(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::BigVarBinary as u8); // Valdate tds type
        assert_eq!(test_cursor.get_i16_le(), byte_len as i16);

        assert_eq!(test_cursor.get_i16_le(), byte_len as i16);
        let mut written_bytes = vec![0u8; byte_len as usize];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes); // size for Some Data
    }

    #[tokio::test]
    async fn test_write_large_binary() {
        let payload: Vec<u8> = vec![0xAB; 9000];
        // Doesn't matter for serialization, but we need a length.
        let len = payload.len() / 2;
        let byte_len = payload.len() as u32;
        let val = Some(payload.clone());
        let bit = SqlType::VarBinary(val, len as u32);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        let copied_bytes = payload.clone();
        let val = Some(payload);
        bit.serialize_binary(&mut packet_writer, &val)
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
        assert_eq!(test_cursor.get_u16_le(), MAX_SHORT_DATA_LENGTH);

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

        bit.serialize_binary(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::BigVarBinary as u8); // Valdate tds type
        assert_eq!(test_cursor.get_i16_le(), 2i16); // Size of type. 2 bytes when NULL.
        assert_eq!(test_cursor.get_i16_le(), VAR_NULL_LENGTH as i16);
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
            sqltypes::{SqlType, MAX_SHORT_DATA_LENGTH, PLP_TERMINATOR_CHUNK_LEN, VAR_NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
        let bit = SqlType::NVarchar(val, len as u32);

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
        bit.serialize_nvarchar(&mut packet_writer, &collation, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NVarChar as u8); // Valdate tds type
        assert_eq!(test_cursor.get_i16_le(), byte_len as i16);
        let _ignore_collation_info = test_cursor.get_u32();
        let _ignore_collation_sortid = test_cursor.get_u8();
        assert_eq!(test_cursor.get_i16_le(), byte_len as i16);
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
        let bit = SqlType::NVarchar(val, len as u32);

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
        bit.serialize_nvarchar(&mut packet_writer, &collation, &val)
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
        assert_eq!(test_cursor.get_u16_le(), MAX_SHORT_DATA_LENGTH);
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

        let val = None;
        bit.serialize_nvarchar(&mut packet_writer, &collation, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::NVarChar as u8); // Valdate tds type
        assert_eq!(test_cursor.get_i16_le(), 2i16); // Size of type. 2 bytes when NULL.
        let _ignore_collation_info = test_cursor.get_u32();
        let _ignore_collation_sortid = test_cursor.get_u8();
        assert_eq!(test_cursor.get_i16_le(), VAR_NULL_LENGTH as i16);
    }
}

#[cfg(test)]
mod bigint_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sqldatatypes::{FixedLengthTypes, TdsDataType},
            sqltypes::{SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
            sqltypes::{SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
            sqltypes::{SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
            sqltypes::{SqlType, DECIMAL_FIXED_SIZE, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
        assert_eq!(test_cursor.get_u8(), TdsDataType::NumericN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // type length data
        assert_eq!(test_cursor.get_u8(), precision); // type length data
        assert_eq!(test_cursor.get_u8(), scale); // type length data
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // size of the data
        assert_eq!(test_cursor.get_u8(), 0x01); // Positive value
        let mut parts: Vec<i32> = Vec::new();
        for _ in 0..3 {
            parts.push(test_cursor.get_i32_le());
        }
        assert_eq!(int_parts.clone(), parts); // size for Some Data
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
        assert_eq!(test_cursor.get_u8(), TdsDataType::NumericN as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // type length data
        assert_eq!(test_cursor.get_u8(), precision); // type length data
        assert_eq!(test_cursor.get_u8(), scale); // type length data
        assert_eq!(test_cursor.get_u8(), DECIMAL_FIXED_SIZE); // size of the data
        assert_eq!(test_cursor.get_u8(), 0x00); // Negative value
        let mut parts: Vec<i32> = Vec::new();
        for _ in 0..3 {
            parts.push(test_cursor.get_i32_le());
        }
        assert_eq!(int_parts.clone(), parts); // size for Some Data
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
            sqltypes::{SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
            sqltypes::{SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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

    use crate::{
        message::messages::PacketType, read_write::packet_reader::tests::MockNetworkReaderWriter,
    };

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

    use crate::{
        message::messages::PacketType, read_write::packet_reader::tests::MockNetworkReaderWriter,
    };

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
mod uuid_tests {
    use crate::{
        datatypes::{
            sqldatatypes::TdsDataType,
            sqltypes::{SqlType, NULL_LENGTH},
        },
        message::messages::PacketType,
        read_write::{packet_reader::tests::MockNetworkReaderWriter, packet_writer::PacketWriter},
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
