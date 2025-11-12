// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;
use tracing::event;

use super::super::tokens::Tokens;
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{io::token_stream::ParserContext, token::tokens::InfoToken};

#[derive(Default)]
pub(crate) struct InfoTokenParser {
    // fields omitted
}

#[async_trait]
impl<T> TokenParser<T> for InfoTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let _length = reader.read_uint16().await?;
        let number = reader.read_uint32().await?;
        let state = reader.read_byte().await?;
        let severity = reader.read_byte().await?;
        let message = reader.read_varchar_u16_length().await?;
        let server_name = reader.read_varchar_u8_length().await?;
        let proc_name = reader.read_varchar_u8_length().await?;
        let line_number = reader.read_uint32().await?;

        event!(tracing::Level::INFO, "Info message: {:?}", message);

        Ok(Tokens::from(InfoToken {
            number,
            state,
            severity,
            message: message.unwrap_or_default(),
            server_name,
            proc_name,
            line_number,
        }))
    }
}
