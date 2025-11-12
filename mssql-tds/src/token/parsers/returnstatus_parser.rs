// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;

use super::super::tokens::{ReturnStatusToken, Tokens};
use super::common::TokenParser;
use crate::io::token_stream::ParserContext;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};

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
