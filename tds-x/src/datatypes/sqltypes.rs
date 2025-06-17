use byteorder::{ByteOrder, LittleEndian};
use uuid::Uuid;

use crate::datatypes::column_values::SqlXml;
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

    // TODO: Will be migrated to the time struct in future
    Time(u64),
    DateTime2 {
        days: u32,
        time_nanos: u64,
    },
    DateTimeOffset {
        days: u32,
        time_nanos: u64,
        offset: i16,
    },
    SmallDateTime {
        day: u16,
        time: u16,
    },

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
            SqlType::Time(_) => todo!(),
            SqlType::DateTime2 {
                days: _,
                time_nanos: _,
            } => todo!(),
            SqlType::DateTimeOffset {
                days: _,
                time_nanos: _,
                offset: _,
            } => todo!(),
            SqlType::SmallDateTime { day: _, time: _ } => todo!(),
            SqlType::NVarcharMax(_) => TdsDataType::NVarChar,
            SqlType::Varchar(_, _) => TdsDataType::BigVarChar,
            SqlType::VarcharMax(_) => TdsDataType::BigVarChar,
            SqlType::VarBinaryMax(_) => todo!(),
            SqlType::Xml(_) => todo!(),
            SqlType::Uuid(_) => todo!(),
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
            SqlType::Time(_) => todo!(),
            SqlType::DateTime2 {
                days: _,
                time_nanos: _,
            } => todo!(),
            SqlType::DateTimeOffset {
                days: _,
                time_nanos: _,
                offset: _,
            } => todo!(),
            SqlType::SmallDateTime { day: _, time: _ } => todo!(),
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
            SqlType::Uuid(_uuid) => todo!(),
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
