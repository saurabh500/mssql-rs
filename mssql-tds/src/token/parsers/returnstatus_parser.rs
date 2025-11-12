// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.


use async_trait::async_trait;

use super::common::TokenParser;
use super::super::tokens::{
    ReturnStatusToken, Tokens,
};
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::io::token_stream::ParserContext;

#[derive(Default)]
pub(crate) struct ReturnStatusTokenParser {}

#[async_trait]
impl<T> TokenParser<T> for ReturnStatusTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let value = reader.read_int32().await?;

        Ok(Tokens::from(ReturnStatusToken { value }))
    }
}
