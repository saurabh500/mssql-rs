// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! # DONE Token Parser
//!
//! Parses DONE, DONEPROC, and DONEINPROC tokens which indicate completion
//! of SQL statements, stored procedures, and nested procedures respectively.
//!
//! ## Token Byte Layout
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      DONE Token (12 bytes)                      │
//! ├──────────┬──────────┬──────────────────┬──────────────────────┤
//! │  Status  │ CurCmd   │                  │      Row Count       │
//! │ (2 bytes)│ (2 bytes)│    Reserved      │     (8 bytes)        │
//! │ UINT16   │ UINT16   │    (unused)      │      UINT64          │
//! └──────────┴──────────┴──────────────────┴──────────────────────┘
//!     0-1        2-3                            4-11
//!
//! Status flags (bitmask):
//!   0x00 = DONE_FINAL   - Final DONE in response
//!   0x01 = DONE_MORE    - More results coming
//!   0x02 = DONE_ERROR   - Error occurred
//!   0x10 = DONE_COUNT   - Row count is valid
//!   0x20 = DONE_ATTN    - Attention acknowledgment
//!   0x100= DONE_SRVERROR- Server error occurred
//!
//! CurCmd values:
//!   0x00 = None
//!   0xC1 = SELECT
//!   0xC2 = INSERT
//!   0xC3 = DELETE
//!   0xC4 = UPDATE
//!   ... (see CurrentCommand enum)
//! ```
//!
//! ## Token Variants
//!
//! - **DONE (0xFD)**: Indicates completion of a SQL statement
//! - **DONEPROC (0xFE)**: Indicates completion of a stored procedure
//! - **DONEINPROC (0xFF)**: Indicates completion of a statement within a procedure
//!
//! ## Example
//!
//! ```text
//! // After executing "SELECT * FROM users"
//! // Server sends:
//! //   - ColMetadata token (column definitions)
//! //   - Row tokens (data rows)
//! //   - DONE token (Status=0x10, CurCmd=0xC1, RowCount=5)
//! ```

use async_trait::async_trait;

use super::super::tokens::{DoneToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    io::token_stream::ParserContext,
    token::tokens::{CurrentCommand, DoneStatus},
};

/// Parser for DONE token (0xFD) - signals completion of a SQL statement
/// Parser for DONE token (0xFD) - signals completion of a SQL statement
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
        // Read status flags (2 bytes) - indicates completion state
        let status = reader.read_uint16().await?;
        let done_status = DoneStatus::from(status);

        // Read current command type (2 bytes) - what operation completed
        let current_command_value = reader.read_uint16().await?;
        let current_command =
            CurrentCommand::try_from(current_command_value).unwrap_or(CurrentCommand::None);

        // Read row count (8 bytes) - number of rows affected
        // Valid only if DONE_COUNT flag (0x10) is set in status
        let row_count = reader.read_uint64().await?;

        Ok(Tokens::Done(DoneToken {
            status: done_status,
            cur_cmd: current_command,
            row_count,
        }))
    }
}

/// Parser for DONEINPROC token (0xFF) - signals completion within a stored procedure
/// Parser for DONEINPROC token (0xFF) - signals completion within a stored procedure
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
        // Same structure as DONE token, different semantic meaning
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

/// Parser for DONEPROC token (0xFE) - signals completion of a stored procedure
/// Parser for DONEPROC token (0xFE) - signals completion of a stored procedure
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
        // Same structure as DONE token, indicates stored procedure completion
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
