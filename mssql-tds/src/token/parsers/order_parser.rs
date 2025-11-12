// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::vec;

use async_trait::async_trait;

use super::common::TokenParser;
use super::super::tokens::Tokens;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    io::token_stream::ParserContext,
    token::tokens::OrderToken,
};

#[derive(Default)]
pub(crate) struct OrderTokenParser {}

#[async_trait]
impl<T> TokenParser<T> for OrderTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let length = reader.read_uint16().await?;

        let col_count = length / 2;
        let mut columns = vec![];
        for _ in 0..col_count {
            columns.push(reader.read_uint16().await?);
        }
        Ok(Tokens::from(OrderToken {
            order_columns: columns,
        }))
    }
}
