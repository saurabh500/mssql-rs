// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! # ERROR Token Parser
//!
//! Parses ERROR tokens (0xAA) which report SQL Server errors to the client.
//! These tokens are sent when a statement execution fails or encounters an error.
//!
//! ## Token Byte Layout
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │                      ERROR Token (variable length)                   │
//! ├─────────┬──────────┬───────┬──────────┬──────────────┬──────────────┤
//! │ Length  │  Number  │ State │ Severity │   Message    │ Server Name  │
//! │(2 bytes)│ (4 bytes)│(1 byte│ (1 byte) │ (2+N chars)  │ (1+N chars)  │
//! │ UINT16  │  UINT32  │  BYTE │   BYTE   │US_VARCHAR(2) │B_VARCHAR(1)  │
//! └─────────┴──────────┴───────┴──────────┴──────────────┴──────────────┘
//!     0-1       2-5        6        7        8-9+N       10+N-11+M
//!
//! ┌──────────────┬──────────────┐
//! │   Proc Name  │ Line Number  │
//! │ (1+N chars)  │  (4 bytes)   │
//! │ B_VARCHAR(1) │   UINT32     │
//! └──────────────┴──────────────┘
//!   12+M-13+P      14+P-17+P
//!
//! Fields:
//!   Length    - Total token length excluding this field
//!   Number    - SQL Server error number (e.g., 208 = object not found)
//!   State     - State code (internal error state machine position)
//!   Severity  - Error severity (0-25, where >16 requires immediate attention)
//!                 11-16: User errors (can be corrected by user)
//!                 17-19: Software/hardware errors
//!                 20-25: Fatal errors (connection terminated)
//!   Message   - Human-readable error message (UTF-16 LE)
//!   ServerName- Name of SQL Server instance (UTF-16 LE)
//!   ProcName  - Stored procedure name if error occurred in a proc (UTF-16 LE)
//!   LineNumber- Line number in batch/proc where error occurred
//! ```
//!
//! ## Common Error Numbers
//!
//! - **208**: Invalid object name (table/view doesn't exist)
//! - **515**: Cannot insert NULL into non-nullable column
//! - **547**: Foreign key constraint violation
//! - **2601**: Duplicate key violation
//! - **18456**: Login failed
//!
//! ## Example Usage
//!
//! ```ignore
//! // After executing "SELECT * FROM NonExistentTable"
//! // Server sends ERROR token:
//! //   Number   = 208
//! //   State    = 1
//! //   Severity = 16 (user error)
//! //   Message  = "Invalid object name 'NonExistentTable'."
//! //   LineNumber = 1
//! ```

use async_trait::async_trait;
use tracing::error;

use super::super::tokens::{ErrorToken, Tokens};
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{io::token_stream::ParserContext, token::tokens::TokenType};

/// Parser for ERROR token (0xAA) - reports SQL Server errors
/// Parser for ERROR token (0xAA) - reports SQL Server errors
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
        
        // Read token length (2 bytes) - total length excluding this field
        let _ = reader.read_uint16().await?;
        
        // Read error number (4 bytes) - SQL Server error code
        let number = reader.read_uint32().await?;
        
        // Read state (1 byte) - internal state code
        let state = reader.read_byte().await?;
        
        // Read severity (1 byte) - error severity level (0-25)
        let severity = reader.read_byte().await?;

        // Read error message (US_VARCHAR with 2-byte length prefix)
        // Message is in UTF-16 LE format
        let message = reader.read_varchar_u16_length().await?.unwrap_or_default();
        error!("Error message: {:?}", message);
        
        // Read server name (B_VARCHAR with 1-byte length prefix)
        let server_name = reader.read_varchar_u8_length().await?;
        
        // Read procedure name (B_VARCHAR with 1-byte length prefix)
        // Empty if error not in a stored procedure
        let proc_name = reader.read_varchar_u8_length().await?;

        // Read line number (4 bytes) - line where error occurred
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
