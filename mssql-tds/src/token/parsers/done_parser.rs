// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;

use super::super::tokens::{DoneToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    io::token_stream::ParserContext,
    token::tokens::{CurrentCommand, DoneStatus},
};

#[derive(Default)]
pub(crate) struct DoneTokenParser {
    // fields omitted
}

#[cfg(fuzzing)]
pub struct DoneTokenParser {
    // fields omitted
}

#[async_trait]
impl<T> TokenParser<T> for DoneTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command =
            CurrentCommand::try_from(current_command_value).unwrap_or(CurrentCommand::None);
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::Done(DoneToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct DoneInProcTokenParser {
    // fields omitted
}

#[async_trait]
impl<T> TokenParser<T> for DoneInProcTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command =
            CurrentCommand::try_from(current_command_value).unwrap_or(CurrentCommand::None);
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::DoneInProc(DoneToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

#[derive(Debug, Default)]
pub(crate) struct DoneProcTokenParser {
    // fields omitted
}

#[async_trait]
impl<T> TokenParser<T> for DoneProcTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);
        let current_command_value = reader.read_uint16().await?;
        let current_command =
            CurrentCommand::try_from(current_command_value).unwrap_or(CurrentCommand::None);
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::DoneProc(DoneToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}
