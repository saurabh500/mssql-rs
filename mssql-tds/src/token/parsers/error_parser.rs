// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;
use tracing::error;

use super::super::tokens::{ErrorToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{io::token_stream::ParserContext, token::tokens::TokenType};

#[derive(Default)]
pub(crate) struct ErrorTokenParser {
    // fields omitted
}

#[async_trait]
impl<T> TokenParser<T> for ErrorTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        error!(
            "Parsing Error token with type: 0x{:02X}",
            TokenType::Error as u8
        );
        let _ = reader.read_uint16().await?;
        let number = reader.read_uint32().await?;
        let state = reader.read_byte().await?;
        let severity = reader.read_byte().await?;

        let message = reader.read_varchar_u16_length().await?.unwrap_or_default();
        error!("Error message: {:?}", message);
        let server_name = reader.read_varchar_u8_length().await?;
        let proc_name = reader.read_varchar_u8_length().await?;

        let line_number = reader.read_uint32().await?;

        Ok(Tokens::from(ErrorToken {
            number,
            state,
            severity,
            message,
            server_name,
            proc_name,
            line_number,
        }))
    }
}
