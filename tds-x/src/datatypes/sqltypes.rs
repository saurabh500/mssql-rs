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

// The length of a NULL value in TDS is 0 bytes.
pub(crate) const NULL_LENGTH: u8 = 0u8;

// The fixed size for Decimal in TDS is 17 bytes.
pub(crate) const DECIMAL_FIXED_SIZE: u8 = 17;

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
            SqlType::VarBinary(_items, _size) => todo!(),
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
            SqlType::NVarcharMax(_) => todo!(),
            SqlType::Varchar(_, _) => todo!(),
            SqlType::VarcharMax(_) => todo!(),
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
        _db_collation: &SqlCollation,
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
            SqlType::NVarchar(_, _) => todo!(),
            SqlType::VarBinary(_items, _) => todo!(),
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
            SqlType::NVarcharMax(_sql_string) => todo!(),
            SqlType::Varchar(_sql_string, _) => todo!(),
            SqlType::VarcharMax(_sql_string) => todo!(),
            SqlType::VarBinaryMax(_items) => todo!(),
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
