// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! # COLMETADATA Token Parser
//!
//! Parses COLMETADATA tokens (0x81) which describe the structure of result set columns.
//! This token appears before any ROW tokens and defines the schema of the data that follows.
//!
//! ## Token Byte Layout
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                  COLMETADATA Token (variable length)                    │
//! ├────────────┬────────────────────────────────────────────────────────────┤
//! │ ColCount   │              Column Definitions (repeated)                 │
//! │ (2 bytes)  │                  (ColCount times)                          │
//! │  UINT16    │                                                            │
//! └────────────┴────────────────────────────────────────────────────────────┘
//!     0-1                        2 ... N
//!
//! Special case: ColCount = 0xFFFF means "no metadata" (e.g., for INSERT/UPDATE)
//!
//! Per-Column Structure:
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │                      Column Definition                                 │
//! ├───────────┬───────┬──────────┬─────────────┬──────────────┬────────────┤
//! │ UserType  │ Flags │ DataType │  TypeInfo   │   TableName  │ ColumnName │
//! │ (4 bytes) │(2 byte│ (1 byte) │  (variable) │  (optional)  │  (variable)│
//! │  UINT32   │ UINT16│   BYTE   │             │ B_VARCHAR(1) │B_VARCHAR(1)│
//! └───────────┴───────┴──────────┴─────────────┴──────────────┴────────────┘
//!     0-3       4-5       6         7 ... M      M+1 ... P     P+1 ... Q
//!
//! Flags (bitmask):
//!   0x01 = Nullable
//!   0x08 = Identity column
//!   0x10 = Computed column
//!   0x20 = Fixed length CLR type
//!   0x40 = Hidden (e.g., FOR BROWSE)
//!   0x80 = Key column (used in cursor operations)
//!   0x100= Nullable unknown
//!   0x200= Column is encrypted (Always Encrypted feature)
//!
//! TypeInfo varies by DataType:
//!   - INT/BIGINT:    No additional info
//!   - VARCHAR(n):    MaxLength(2 bytes) + Collation(5 bytes)
//!   - DECIMAL(p,s):  MaxLength(1 byte) + Precision(1) + Scale(1)
//!   - (See read_type_info for full details)
//!
//! TableName appears only for TEXT/NTEXT/IMAGE types:
//!   - Multi-part table name (server.database.schema.table)
//!   - Specifies source table for LOB columns
//! ```
//!
//! ## Example
//!
//! ```text
//! // For query: SELECT Id, Name, Age FROM Users
//! // COLMETADATA token contains:
//! //   ColCount = 3
//! //   Column 1: UserType=0, Flags=0x00 (not nullable), DataType=INT, Name="Id"
//! //   Column 2: UserType=0, Flags=0x09 (nullable, updatable), DataType=NVARCHAR(50), Name="Name"
//! //   Column 3: UserType=0, Flags=0x01 (nullable), DataType=INT, Name="Age"
//! ```
//!
//! ## Token Flow
//!
//! ```text
//! Query: SELECT * FROM Users
//!
//! Server response:
//!   1. COLMETADATA ← Defines columns (this parser)
//!   2. ROW         ← First data row
//!   3. ROW         ← Second data row
//!   4. ...
//!   5. DONE        ← End of result set
//! ```

use std::io::Error;

use async_trait::async_trait;

use super::super::tokens::Tokens;
use super::common::TokenParser;
use crate::{core::TdsResult, io::packet_reader::TdsPacketReader};
use crate::{
    datatypes::sqldatatypes::{TdsDataType, read_type_info},
    io::token_stream::ParserContext,
    query::metadata::{ColumnMetadata, MultiPartName},
    token::tokens::ColMetadataToken,
};

/// Parser for COLMETADATA token (0x81) - defines result set column schema
#[derive(Default)]
pub(crate) struct ColMetadataTokenParser {
    // Do we want to create a new parser for every connection, or should
    // this value be passed as a context to the parser? Likely SessionSettings?
    pub is_column_encryption_supported: bool,
}

impl ColMetadataTokenParser {
    pub fn new(is_column_encryption_supported: bool) -> Self {
        Self {
            is_column_encryption_supported,
        }
    }

    pub fn is_column_encryption_supported(&self) -> bool {
        self.is_column_encryption_supported
    }
}

#[async_trait]
impl<T> TokenParser<T> for ColMetadataTokenParser
where
    T: TdsPacketReader + Send + Sync,
{
    async fn parse(&self, reader: &mut T, _context: &ParserContext) -> TdsResult<Tokens> {
        // Read column count (2 bytes)
        // Special value 0xFFFF indicates no metadata (used for INSERT/UPDATE/DELETE)
        let col_count = reader.read_uint16().await?;

        if self.is_column_encryption_supported {
            return Err(crate::error::Error::UnimplementedFeature {
                feature: "Column Encryption".to_string(),
                context: "Column encryption metadata parsing not yet supported".to_string(),
            });
        }

        // Handle the special case where no metadata is sent (0xFFFF)
        // This occurs for non-query statements like INSERT, UPDATE, DELETE
        if col_count == 0xFFFF {
            return Ok(Tokens::from(ColMetadataToken::default()));
        }

        // Pre-allocate vector for column metadata
        let mut column_metadata: Vec<ColumnMetadata> = Vec::with_capacity(col_count as usize);
        
        // Parse each column definition
        for _ in 0..col_count {
            // User-defined type identifier (4 bytes)
            // 0 for built-in types, > 0 for UDTs
            let user_type = reader.read_uint32().await?;

            // Column flags (2 bytes) - see bitmask definition above
            let flags = reader.read_uint16().await?;

            // Data type byte (1 byte) - TDS type identifier
            let raw_data_type = reader.read_byte().await?;
            let some_data_type = TdsDataType::try_from(raw_data_type);
            if some_data_type.is_err() {
                return Err(crate::error::Error::from(Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid data type: {raw_data_type}"),
                )));
            }
            let data_type = some_data_type?;
            
            // Type-specific information (variable length)
            // Structure depends on data_type (see sqldatatypes.rs)
            // Type-specific information (variable length)
            // Structure depends on data_type (see sqldatatypes.rs)
            let type_info = read_type_info(reader, data_type).await?;

            // Parse Table name (optional - only for TEXT/NTEXT/IMAGE types)
            // TDS Spec: "The fully qualified base table name for this column.
            // It contains the table name length and table name.
            // This exists only for text, ntext, and image columns."
            // Format: NumParts(1 byte) followed by PartName repeated NumParts times
            // Parts are sent in order: Server, Catalog(DB), Schema, Table
            let multi_part_name = match data_type {
                TdsDataType::Text | TdsDataType::NText | TdsDataType::Image => {
                    let mut part_count = reader.read_byte().await?;
                    if part_count == 0 {
                        None
                    } else {
                        let mut mpt = MultiPartName::default();
                        while part_count > 0 {
                            let part_name = reader.read_varchar_u16_length().await?;
                            // Parse multi-part name in reverse order
                            // 4 = server, 3 = catalog, 2 = schema, 1 = table
                            if part_count == 4 {
                                mpt.server_name = part_name;
                            } else if part_count == 3 {
                                mpt.catalog_name = part_name;
                            } else if part_count == 2 {
                                mpt.schema_name = part_name;
                            } else if part_count == 1 {
                                mpt.table_name = part_name.unwrap_or_default();
                            }
                            part_count -= 1;
                        }
                        Some(mpt)
                    }
                }
                _ => None,
            };

            // Read column name (B_VARCHAR with 1-byte length prefix)
            let col_name = reader.read_varchar_u8_length().await?;

            // Construct column metadata
            let col_metadata = ColumnMetadata {
                user_type,
                flags,
                data_type,
                type_info,
                column_name: col_name,
                multi_part_name,
            };
            
            // Check for Always Encrypted columns (not yet supported)
            if col_metadata.is_encrypted() {
                return Err(crate::error::Error::ProtocolError(
                    "Column encryption is not yet supported".to_string(),
                ));
            }

            column_metadata.push(col_metadata);
        }
        
        // Construct the complete metadata token
        let metadata = ColMetadataToken {
            column_count: col_count,
            columns: column_metadata,
        };
        Ok(Tokens::from(metadata))
    }
}
