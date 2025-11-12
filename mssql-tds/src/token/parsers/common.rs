// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Common types and traits for token parsers.

use crate::{
    core::TdsResult, io::packet_reader::TdsPacketReader, io::token_stream::ParserContext,
    token::tokens::Tokens,
};
use async_trait::async_trait;

/// Maximum allowed size for Feature Extension acknowledgment data.
pub(crate) const MAX_ALLOWED_FE_DATA_IN_BYTES: usize = 1024;

/// Trait for parsing TDS tokens from a packet stream.
#[async_trait]
#[cfg(not(fuzzing))]
pub(crate) trait TokenParser<T>
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, context: &ParserContext) -> TdsResult<Tokens>;
}

#[async_trait]
#[cfg(fuzzing)]
pub trait TokenParser<T>
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, context: &ParserContext) -> TdsResult<Tokens>;
}
