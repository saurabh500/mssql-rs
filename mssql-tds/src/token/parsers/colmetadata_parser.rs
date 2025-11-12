// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io::Error;

use async_trait::async_trait;

use super::super::tokens::Tokens;
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    datatypes::sqldatatypes::{TdsDataType, read_type_info},
    io::token_stream::ParserContext,
    query::metadata::{ColumnMetadata, MultiPartName},
    token::tokens::ColMetadataToken,
};

#[derive(Default)]
pub(crate) struct ColMetadataTokenParser {
    // Do we want to create a new parser for every connection, or should
    // this value be passed as a context to the parser? Likely SessionSettings?
    pub is_column_encryption_supported: bool,
}

impl ColMetadataTokenParser {
    pub fn new(is_column_encryption_supported: bool) -> Self {
        Self {
            is_column_encryption_supported,
        }
    }

    pub fn is_column_encryption_supported(&self) -> bool {
        self.is_column_encryption_supported
    }
}

#[async_trait]
impl<T> TokenParser<T> for ColMetadataTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        // Allocate a heap pointer so that we can reference the reader
        // by passing it around into other methods.
        let col_count = reader.read_uint16().await?;

        if self.is_column_encryption_supported {
            return Err(crate::error::Error::UnimplementedFeature {
                feature: "Column Encryption".to_string(),
                context: "Column encryption metadata parsing not yet supported".to_string(),
            });
        }

        // Handle the special case where no metadata is sent
        if col_count == 0xFFFF {
            return Ok(Tokens::from(ColMetadataToken::default()));
        }

        let mut column_metadata: Vec<ColumnMetadata> = Vec::with_capacity(col_count as usize);
        for _ in 0..col_count {
            let user_type = reader.read_uint32().await?;

            let flags = reader.read_uint16().await?;

            let raw_data_type = reader.read_byte().await?;
            let some_data_type = TdsDataType::try_from(raw_data_type);
            if some_data_type.is_err() {
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid data type: {raw_data_type}"),
                )));
            }
            let data_type = some_data_type?;
            let type_info = read_type_info(reader, data_type).await?;

            // Parse Table name
            // TDS Doc snippet
            // The fully qualified base table name for this column.
            // It contains the table name length and table name.
            // This exists only for text, ntext, and image columns. It specifies the number of parts that are returned and then repeats PartName once for each NumParts.
            let multi_part_name = match data_type {
                TdsDataType::Text | TdsDataType::NText | TdsDataType::Image => {
                    let mut part_count = reader.read_byte().await?;
                    if part_count == 0 {
                        None
                    } else {
                        let mut mpt = MultiPartName::default();
                        while part_count > 0 {
                            let part_name = reader.read_varchar_u16_length().await?;
                            if part_count == 4 {
                                mpt.server_name = part_name;
                            } else if part_count == 3 {
                                mpt.catalog_name = part_name;
                            } else if part_count == 2 {
                                mpt.schema_name = part_name;
                            } else if part_count == 1 {
                                mpt.table_name = part_name.unwrap_or_default();
                            }
                            part_count -= 1;
                        }
                        Some(mpt)
                    }
                }
                _ => None,
            };

            let col_name = reader.read_varchar_u8_length().await?;

            let col_metadata = ColumnMetadata {
                user_type,
                flags,
                data_type,
                type_info,
                column_name: col_name,
                multi_part_name,
            };
            if col_metadata.is_encrypted() {
                return Err(crate::error::Error::ProtocolError(
                    "Column encryption is not yet supported".to_string(),
                ));
            }

            column_metadata.push(col_metadata);
        }
        let metadata = ColMetadataToken {
            column_count: col_count,
            columns: column_metadata,
        };
        Ok(Tokens::from(metadata))
    }
}
