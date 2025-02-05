use std::io::Error;

use async_trait::async_trait;
use uuid::Uuid;

use crate::{
    query::metadata::ColumnMetadata, read_write::packet_reader::PacketReader,
    token::tokens::SqlCollation,
};

use super::sqldatatypes::TdsDataType;

#[async_trait]
pub(crate) trait SqlTypeDecode<'a> {
    async fn decode(
        &self,
        reader: &'a mut PacketReader,
        metadata: &ColumnMetadata,
    ) -> Result<ColumnValues, Error>;
}

#[derive(Debug)]
pub enum ColumnValues {
    TinyInt(u8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(f32),
    Float(f64),
    Decimal(Vec<u8>), // Todo Decimal needs to be migrated to a strong type.
    Numeric(Vec<u8>), // Todo Numeric needs to be migrated to a strong type.
    Bit(Option<bool>),
    String(Option<String>),
    DateTime((i32, u32)),
    IntN(Option<i64>),
    Bytes(Vec<u8>),
    Null,
    Uuid(Option<Uuid>),
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
    async fn read_decimal(
        &self,
        reader: &mut PacketReader<'_>,
        metadata: &ColumnMetadata,
    ) -> Result<Vec<u8>, Error> {
        // Todo Decimal needs to be migrated to a strong type.
        let _precision = reader.read_byte().await?;
        let _scale = reader.read_byte().await?;

        let mut bytes = vec![0u8; metadata.length];
        reader.read_bytes(&mut bytes).await?;
        Ok(bytes)
    }

    async fn read_datetime(&self, reader: &mut PacketReader<'_>) -> Result<(i32, u32), Error> {
        let days = reader.read_int32().await?;
        let ticks = reader.read_uint32().await?;

        Ok((days, ticks))
    }

    async fn read_small_datetime(
        &self,
        reader: &mut PacketReader<'_>,
    ) -> Result<(i16, u16), Error> {
        let days = reader.read_int16().await?;
        let minutes = reader.read_uint16().await?;
        Ok((days, minutes))
    }

    async fn read_date(&self, reader: &mut PacketReader<'_>) -> Result<i32, Error> {
        let days = reader.read_int32().await?;
        Ok(days)
    }

    async fn read_intn(
        &self,
        reader: &mut PacketReader<'_>,
        byte_len: u8,
    ) -> Result<Option<i64>, Error> {
        let value: Option<i64> = match byte_len {
            1 => Some(reader.read_byte().await? as i64),
            2 => Some(reader.read_int16().await? as i64),
            4 => Some(reader.read_int32().await? as i64),
            8 => Some(reader.read_int64().await?),
            0 => None,
            _ => {
                return Err(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid IntN length",
                ));
            }
        };
        Ok(value)
    }
}

#[async_trait]
impl<'a> SqlTypeDecode<'a> for GenericDecoder {
    async fn decode(
        &self,
        reader: &'a mut PacketReader,
        metadata: &ColumnMetadata,
    ) -> Result<ColumnValues, Error> {
        let result = match metadata.data_type {
            TdsDataType::TinyInt => {
                let value = reader.read_byte().await?;
                ColumnValues::from(value)
            }
            TdsDataType::SmallInt => {
                let value = reader.read_int16().await?;
                ColumnValues::SmallInt(value)
            }
            TdsDataType::Int => {
                let value = reader.read_int32().await?;
                ColumnValues::from(value)
            }
            TdsDataType::BigInt => {
                let value = reader.read_int64().await?;
                ColumnValues::BigInt(value)
            }
            TdsDataType::Real => {
                let value = reader.read_float32().await?;
                ColumnValues::Real(value)
            }
            TdsDataType::Float => {
                let value = reader.read_float64().await?;
                ColumnValues::Float(value)
            }
            TdsDataType::Decimal => {
                let value = self.read_decimal(reader, metadata).await?;
                ColumnValues::Decimal(value)
            }
            TdsDataType::Numeric => {
                let value = self.read_decimal(reader, metadata).await?;
                ColumnValues::Numeric(value)
            }
            TdsDataType::Bit => {
                let value = reader.read_byte().await?;
                ColumnValues::Bit(Some(value == 1))
            }
            TdsDataType::NChar
            | TdsDataType::NVarChar
            | TdsDataType::BigChar
            | TdsDataType::VarChar => self.string_decoder.decode(reader, metadata).await?,
            TdsDataType::DateTime => {
                let value = self.read_datetime(reader).await?;
                ColumnValues::DateTime(value)
            }
            TdsDataType::IntN => {
                let byte_len = reader.read_byte().await?;
                let intn_value = self.read_intn(reader, byte_len).await?;
                ColumnValues::IntN(intn_value)
            }
            TdsDataType::Binary | TdsDataType::VarBinary => {
                let length = reader.read_uint16().await?;
                let mut bytes = vec![0u8; length as usize];
                reader.read_bytes(&mut bytes).await?;
                ColumnValues::Bytes(bytes)
            }
            TdsDataType::BitN => {
                let byte_len = reader.read_byte().await?;
                if byte_len > 0 {
                    let value = reader.read_byte().await?;
                    ColumnValues::Bit(Some(value == 1))
                } else {
                    ColumnValues::Bit(None)
                }
            }
            TdsDataType::UniqueIdentifier => {
                let length = reader.read_byte().await?;
                if length > 0 {
                    let mut bytes = vec![0u8; length as usize];
                    reader.read_bytes(&mut bytes).await?;
                    let unique_id = uuid::Uuid::from_slice(&bytes).unwrap();
                    ColumnValues::Uuid(Some(unique_id))
                } else {
                    ColumnValues::Uuid(None)
                }
            }
            _ => {
                unimplemented!("Data type not implemented: {:?}", metadata.data_type);
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
    const SHORTLEN_MAXVALUE: usize = 65535;
    const SQL_PLP_NULL: usize = 0xffffffffffffffff;
    const SQL_PLP_UNKNOWNLEN: usize = 0xfffffffffffffffe;
    fn new() -> Self {
        StringDecoder { db_collation: None }
    }
}

#[async_trait]
impl<'a> SqlTypeDecode<'a> for StringDecoder {
    async fn decode(
        &self,
        reader: &'a mut PacketReader,
        metadata: &ColumnMetadata,
    ) -> Result<ColumnValues, Error> {
        // If Plp Column.
        if metadata.length == Self::SHORTLEN_MAXVALUE {
            let long_len = reader.read_int64().await? as u64;

            if long_len as usize == Self::SQL_PLP_NULL {
                return Ok(ColumnValues::String(None));
            } else {
                let mut plp_buffer = vec![0u8; long_len as usize];
                if long_len as usize == Self::SQL_PLP_UNKNOWNLEN {
                    // Read the length of the data.
                    unimplemented!("Unknown length not implemented");
                }
                let mut chunk_len = reader.read_uint32().await? as usize;
                let mut offset = 0;
                while chunk_len > 0 {
                    let chunk_size_read = reader
                        .read_bytes(&mut plp_buffer[offset..offset + chunk_len])
                        .await?;
                    offset += chunk_size_read;
                    chunk_len = reader.read_uint32().await? as usize;
                }
                let value = String::from_utf8(plp_buffer)
                    .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(ColumnValues::String(Some(value)))
            }
        } else {
            let length = reader.read_uint16().await? as usize;
            if length == 0xFFFF {
                return Ok(ColumnValues::String(None));
            } else {
                let mut buffer = vec![0u8; length];
                reader.read_bytes(&mut buffer).await?;
                let value = String::from_utf8(buffer)
                    .map_err(|e| Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(ColumnValues::String(Some(value)))
            }
        }
        // Ok(ColumnValues::String(Some(value)))
    }
}
