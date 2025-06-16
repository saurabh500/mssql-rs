use std::collections::HashSet;

use async_trait::async_trait;

use super::sqldatatypes::{FixedLengthTypes, TdsDataType};
use crate::datatypes::column_values::ColumnValues;
use crate::{
    core::TdsResult, read_write::packet_writer::PacketWriter, token::tokens::SqlCollation,
};

#[async_trait]
pub(crate) trait Encoder {
    async fn encode(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        tds_type: TdsDataType,
        value: &ColumnValues,
        collation: &SqlCollation,
    ) -> TdsResult<()>;

    fn get_supported_datatypes(&self) -> &HashSet<TdsDataType>;

    fn get_string_name(&self, tds_type: &TdsDataType) -> &str;
}

pub struct GenericEncode {
    pub(crate) data_types: HashSet<TdsDataType>,
}

impl GenericEncode {
    pub fn new() -> Self {
        let mut data_types = HashSet::new();
        data_types.insert(TdsDataType::IntN);
        data_types.insert(TdsDataType::Int1);
        data_types.insert(TdsDataType::Int2);
        data_types.insert(TdsDataType::Int4);
        data_types.insert(TdsDataType::Int8);
        data_types.insert(TdsDataType::NVarChar);
        Self { data_types }
    }
}

#[async_trait]
impl Encoder for GenericEncode {
    async fn encode(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        tds_type: TdsDataType,
        value: &ColumnValues,
        collation: &SqlCollation,
    ) -> TdsResult<()> {
        // We may have received a nullable type to be sent. In this case, rely on figuring out the right length from the column values.
        match tds_type {
            TdsDataType::IntN => {
                let size = match value {
                    ColumnValues::Int(_) => FixedLengthTypes::Int4.get_len(),
                    ColumnValues::BigInt(_) => FixedLengthTypes::Int8.get_len(),
                    ColumnValues::SmallInt(_) => FixedLengthTypes::Int2.get_len(),
                    ColumnValues::TinyInt(_) => FixedLengthTypes::Int1.get_len(),
                    ColumnValues::Null => {
                        // Write 0 len to signify a null value.
                        0
                    }
                    _ => {
                        return Err(crate::error::Error::UsageError(
                            "Expected an integer value for IntN type".to_string(),
                        ));
                    }
                };

                packet_writer.write_byte_async(tds_type as u8).await?;
                packet_writer.write_byte_async(size as u8).await?;

                if let ColumnValues::Null = value {
                    // Write 0 len to signify a null value.
                    packet_writer.write_byte_async(0x00).await?;
                } else {
                    packet_writer.write_byte_async(size as u8).await?;

                    if let ColumnValues::Int(value) = value {
                        packet_writer.write_i32_async(*value).await?;
                    } else {
                        return Err(crate::error::Error::UsageError(
                            "Expected an integer value for IntN type".to_string(),
                        ));
                    }
                }
            }
            TdsDataType::Int4 => {
                // TODO: We need to re-think NULLS and leverage only the nullable types for sending the data.
                // This is an interesting case, where the OUT params will not have a value, and we have to send a null instead.
                // However from NULL type or the NULLABLE type, we will not be able to figure out the right length.
                packet_writer
                    .write_byte_async(TdsDataType::IntN as u8)
                    .await?;
                packet_writer.write_byte_async(4).await?;

                if let ColumnValues::Null = value {
                    // Write 0 len to signify a null value.
                    packet_writer.write_byte_async(0x00).await?;
                } else {
                    packet_writer.write_byte_async(4).await?;

                    if let ColumnValues::Int(value) = value {
                        packet_writer.write_i32_async(*value).await?;
                    } else {
                        return Err(crate::error::Error::UsageError(
                            "Expected an integer value for IntN type".to_string(),
                        ));
                    }
                }
            }
            TdsDataType::NVarChar => {
                packet_writer.write_byte_async(tds_type as u8).await?;
                let optional_string = match value {
                    ColumnValues::String(value) => Some(value),
                    ColumnValues::Null => {
                        // Write 0 len to signify a null value.
                        None
                    }
                    _ => {
                        return Err(crate::error::Error::UsageError(
                            "Expected a string value for NVarChar type".to_string(),
                        ));
                    }
                };

                match optional_string {
                    Some(string) => {
                        packet_writer
                            .write_i16_async(string.bytes.len() as i16)
                            .await?;
                        packet_writer.write_u32_async(collation.info).await?;
                        packet_writer.write_byte_async(collation.sort_id).await?;
                        packet_writer
                            .write_i16_async(string.bytes.len() as i16)
                            .await?;
                        packet_writer.write_async(&string.bytes).await?;
                    }
                    None => {
                        // Write 0 len to signify a null value.
                        packet_writer.write_i16_async(0).await?;
                    }
                }
                // The UTF-8 value of the string is being persisted.
                // TODO: Convert to UTF-16 or better, write the byte array as is.
            }
            _ => {
                // Handle other data types here
                // For now, we will just panic if the type is not supported
                unimplemented!("Unsupported data type: {:?}", tds_type);
            }
        }
        Ok(())
    }

    fn get_supported_datatypes(&self) -> &HashSet<TdsDataType> {
        &self.data_types
    }

    fn get_string_name(&self, tds_type: &TdsDataType) -> &str {
        match tds_type {
            TdsDataType::Int1 => meta_type_name::TINYINT,
            TdsDataType::Int8 => meta_type_name::BIGINT,
            TdsDataType::Flt8 => meta_type_name::FLOAT,

            _ => panic!("Unsupported data type"),
        }
    }
}

pub mod meta_type_name {
    pub const BIGINT: &str = "bigint";
    pub const BINARY: &str = "binary";
    pub const BIT: &str = "bit";
    pub const CHAR: &str = "char";
    pub const DATETIME: &str = "datetime";
    pub const DECIMAL: &str = "decimal";
    pub const FLOAT: &str = "float";
    pub const IMAGE: &str = "image";
    pub const INT: &str = "int";
    pub const MONEY: &str = "money";
    pub const NCHAR: &str = "nchar";
    pub const NTEXT: &str = "ntext";
    pub const NVARCHAR: &str = "nvarchar";
    pub const REAL: &str = "real";
    pub const ROWGUID: &str = "uniqueidentifier";
    pub const SMALLDATETIME: &str = "smalldatetime";
    pub const SMALLINT: &str = "smallint";
    pub const SMALLMONEY: &str = "smallmoney";
    pub const TEXT: &str = "text";
    pub const TIMESTAMP: &str = "timestamp";
    pub const TINYINT: &str = "tinyint";
    pub const UDT: &str = "udt";
    pub const VARBINARY: &str = "varbinary";
    pub const VARCHAR: &str = "varchar";
    pub const VARIANT: &str = "sql_variant";
    pub const XML: &str = "xml";
    pub const TABLE: &str = "table";
    pub const DATE: &str = "date";
    pub const TIME: &str = "time";
    pub const DATETIME2: &str = "datetime2";
    pub const DATETIMEOFFSET: &str = "datetimeoffset";
    pub const JSON: &str = "json";
}
