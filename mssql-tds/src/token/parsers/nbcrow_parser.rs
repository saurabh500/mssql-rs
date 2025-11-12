// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{io::Error, vec};

use async_trait::async_trait;
use tracing::trace;

use super::super::tokenitems::ReturnValueStatus;
use super::super::tokens::{ReturnValueToken, RowToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    datatypes::{
        column_values::ColumnValues,
        decoder::SqlTypeDecode,
        sqldatatypes::{TdsDataType, read_type_info},
    },
    io::token_stream::ParserContext,
    query::metadata::ColumnMetadata,
};

pub(crate) struct NbcRowTokenParser<T>
where
    T: SqlTypeDecode,
{
    // fields omitted
    decoder: T,
}

impl<T: SqlTypeDecode + Default> Default for NbcRowTokenParser<T> {
    fn default() -> Self {
        Self {
            decoder: T::default(),
        }
    }
}

fn is_null_value_in_column(null_bitmap: &[u8], index: usize) -> bool {
    let byte_index: usize = index / 8;
    let bit_index = index % 8;
    (null_bitmap[byte_index] & (1 << bit_index)) != 0
}

#[derive(Debug)]
pub(crate) struct ReturnValueTokenParser<T>
where
    T: SqlTypeDecode,
{
    decoder: T,
}

impl<T: SqlTypeDecode + Default> Default for ReturnValueTokenParser<T> {
    fn default() -> Self {
        Self {
            decoder: T::default(),
        }
    }
}

#[async_trait]
impl<T: SqlTypeDecode + Sync, P: TdsPacketReader + Send + Sync> TokenParser<P>
    for ReturnValueTokenParser<T>
{
    async fn parse(&self, reader: &mut P, _context: &ParserContext) -> TdsResult<Tokens> {
        let param_ordinal = reader.read_uint16().await?;
        let param_name_length = reader.read_byte().await?;
        let byte_length = (param_name_length as usize).checked_mul(2).ok_or_else(|| {
            crate::error::Error::ProtocolError(format!(
                "Parameter name length overflow: {param_name_length}"
            ))
        })?;
        let param_name = reader.read_unicode_with_byte_length(byte_length).await?;
        let status_byte = reader.read_byte().await?;
        let status = ReturnValueStatus::from(status_byte);
        let user_type = reader.read_uint32().await?;
        let flags = reader.read_uint16().await?;
        let tds_type = reader.read_byte().await?;
        let type_info = read_type_info(reader, TdsDataType::try_from(tds_type)?).await?;

        // TODO: Crypto metadata
        let column_metadata = ColumnMetadata {
            user_type,
            flags,
            data_type: TdsDataType::try_from(tds_type)?,
            type_info,
            column_name: param_name.clone(),
            multi_part_name: None,
        };
        let value = self.decoder.decode(reader, &column_metadata).await?;

        Ok(Tokens::from(ReturnValueToken {
            param_ordinal,
            param_name,
            value,
            column_metadata: Box::new(column_metadata),
            status,
        }))
    }
}

#[async_trait]
impl<T: SqlTypeDecode + Sync, P: TdsPacketReader + Send + Sync> TokenParser<P>
    for NbcRowTokenParser<T>
{
    async fn parse(&self, reader: &mut P, context: &ParserContext) -> TdsResult<Tokens> {
        let column_metadata_token = match context {
            ParserContext::ColumnMetadata(metadata) => {
                trace!("Metadata during Row Parsing: {:?}", metadata);
                metadata
            }
            _ => {
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected ColumnMetadata in context",
                )));
            }
        };

        let all_metadata = &column_metadata_token.columns;
        let mut all_values: Vec<ColumnValues> =
            Vec::with_capacity(column_metadata_token.column_count as usize);
        let col_count = all_metadata.len();

        let bitmap_length = col_count.div_ceil(8);
        let mut bitmap: Vec<u8> = vec![0; bitmap_length as usize];
        reader.read_bytes(bitmap.as_mut_slice()).await?;
        // let mut index = 0;

        for (index, metadata) in all_metadata.iter().enumerate() {
            trace!("Metadata: {:?}", metadata);
            let is_null = is_null_value_in_column(&bitmap, index);

            if is_null {
                all_values.push(ColumnValues::Null);
            } else {
                let column_value = self.decoder.decode(reader, metadata).await?;
                all_values.push(column_value);
            }
        }
        Ok(Tokens::from(RowToken::new(all_values)))
    }
}
