// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{io::Error, vec};

use async_trait::async_trait;
use tracing::trace;

use super::super::tokens::{RowToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    datatypes::{column_values::ColumnValues, decoder::SqlTypeDecode},
    io::token_stream::ParserContext,
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
