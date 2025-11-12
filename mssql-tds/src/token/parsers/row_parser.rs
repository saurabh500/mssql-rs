// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::io::Error;

use async_trait::async_trait;
use tracing::trace;

use super::super::tokens::{RowToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    datatypes::{column_values::ColumnValues, decoder::SqlTypeDecode},
    io::token_stream::ParserContext,
};

pub(crate) struct RowTokenParser<T: SqlTypeDecode> {
    // fields omitted
    decoder: T,
}

impl<T: SqlTypeDecode + Default> Default for RowTokenParser<T> {
    fn default() -> Self {
        Self {
            decoder: T::default(),
        }
    }
}

#[async_trait]
impl<D: SqlTypeDecode + Default + Send + Sync, P: TdsPacketReader + Send + Sync> TokenParser<P>
    for RowTokenParser<D>
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
        for metadata in all_metadata {
            trace!("Metadata: {:?}", metadata);
            let column_value = self.decoder.decode(reader, metadata).await?;

            all_values.push(column_value);
        }
        Ok(Tokens::from(RowToken::new(all_values)))
    }
}
