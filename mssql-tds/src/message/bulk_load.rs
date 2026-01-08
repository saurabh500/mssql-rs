// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Bulk load message implementation for SQL Server bulk copy protocol.
//!
//! This module implements the TDS bulk load protocol for high-performance data insertion.
//! It follows the .NET SqlBulkCopy implementation pattern from TdsParser.WriteBulkCopyMetaData
//! and WriteBulkCopyValue methods.

use crate::connection::bulk_copy::{BulkCopyOptions, BulkLoadRow};
use crate::core::TdsResult;
use crate::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
use crate::datatypes::column_values::ColumnValues;
use crate::datatypes::sqldatatypes::TdsDataType;
use crate::datatypes::tds_value_serializer::{TdsTypeContext, TdsValueSerializer};
use crate::error::Error;
use crate::io::packet_writer::{PacketWriter, TdsPacketWriter};
use crate::token::tokens::SqlCollation;
use tracing::{debug, trace};

// TDS Token types
const TOKEN_COLMETADATA: u8 = 0x81;
const TOKEN_ROW: u8 = 0xD1;
const TOKEN_NBCROW: u8 = 0xD2; // Null Bitmap Compressed Row
const TOKEN_DONE: u8 = 0xFD;

// NULL markers for different type classes
const FIXEDNULL: u8 = 0x00;
const VARNULL: u16 = 0xFFFF;
// PLP constants imported from tds_value_serializer

/// Streaming bulk load writer for transmitting bulk copy data row-by-row.
///
/// This writer enables streaming bulk copy without accumulating rows in memory.
/// It follows the .NET SqlBulkCopy streaming pattern where rows are written
/// directly to the TDS protocol stream as they are read from the source.
///
/// # Usage Flow
///
/// 1. Create writer with `new()`
/// 2. Call `begin()` to write COLMETADATA token
/// 3. Call `write_row()` for each row (streamed, not buffered)
/// 4. Call `end()` to write DONE token and finalize
pub struct StreamingBulkLoadWriter<'a> {
    /// Packet writer for TDS protocol
    packet_writer: &'a mut PacketWriter<'a>,

    /// Destination table name (for error messages)
    table_name: String,

    /// Column metadata
    column_metadata: Vec<BulkCopyColumnMetadata>,

    /// Bulk copy options
    options: BulkCopyOptions,

    /// Connection's default collation (used when column metadata doesn't specify collation)
    default_collation: SqlCollation,

    /// Whether metadata has been written
    metadata_written: bool,

    /// Number of rows written so far
    rows_written: u64,

    /// Pre-created type contexts for each column (initialized during begin())
    /// This avoids allocating contexts per column per row
    column_contexts: Vec<TdsTypeContext>,
}

impl<'a> StreamingBulkLoadWriter<'a> {
    /// Create a new streaming bulk load writer.
    ///
    /// # Arguments
    ///
    /// * `packet_writer` - TDS packet writer
    /// * `table_name` - Destination table name                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
    /// * `column_metadata` - Column metadata for the bulk load
    /// * `options` - Bulk copy options
    /// * `default_collation` - Connection's default collation (used when column metadata doesn't specify collation)
    pub fn new(
        packet_writer: &'a mut PacketWriter<'a>,
        table_name: String,
        column_metadata: Vec<BulkCopyColumnMetadata>,
        options: BulkCopyOptions,
        default_collation: SqlCollation,
    ) -> Self {
        Self {
            packet_writer,
            table_name,
            column_metadata,
            options,
            default_collation,
            metadata_written: false,
            rows_written: 0,
            column_contexts: Vec::new(), // Will be populated in begin()
        }
    }

    /// Begin streaming - write COLMETADATA token.
    ///
    /// This must be called before any rows can be written.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Metadata has already been written
    /// - Network errors occur during transmission
    pub async fn begin(&mut self) -> TdsResult<()> {
        println!(
            "DEBUG bulk_load: begin() called - {} columns",
            self.column_metadata.len()
        );

        if self.metadata_written {
            return Err(Error::ProtocolError(
                "Metadata already written - cannot call begin() twice".to_string(),
            ));
        }

        // Pre-create type contexts for all columns (one-time allocation)
        // This avoids creating contexts per column per row
        self.column_contexts.clear();
        self.column_contexts.reserve(self.column_metadata.len());

        println!(
            "DEBUG bulk_load: Processing {} columns for contexts",
            self.column_metadata.len()
        );
        for (i, col_meta) in self.column_metadata.iter().enumerate() {
            println!(
                "  Column {}: name='{}', tds_type=0x{:02X}, sql_type={:?}",
                i, col_meta.column_name, col_meta.tds_type, col_meta.sql_type
            );
            let ctx = TdsTypeContext {
                tds_type: col_meta.tds_type,
                max_size: col_meta.length as u8,
                is_plp: col_meta.length_type.is_plp(),
                is_fixed_length: col_meta.length_type.is_fixed(),
                precision: if col_meta.precision > 0 {
                    Some(col_meta.precision)
                } else {
                    None
                },
                scale: if col_meta.scale > 0 {
                    Some(col_meta.scale)
                } else {
                    None
                },
                is_nullable: col_meta.is_nullable,
            };
            self.column_contexts.push(ctx);
        }

        // Write COLMETADATA token and column descriptors
        // This is the same logic as BulkLoadMessage::write_metadata
        self.write_metadata_internal().await?;
        self.metadata_written = true;

        trace!(
            "StreamingBulkLoadWriter: Metadata written for {} columns",
            self.column_metadata.len()
        );

        Ok(())
    }

    /// Write a single row.
    ///
    /// Rows are written immediately to the TDS stream - no buffering occurs.
    ///
    /// # Arguments
    ///
    /// * `row` - Column values for this row
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `begin()` has not been called yet
    /// - Row has wrong number of columns
    /// - Network errors occur during transmission
    /// - Type conversion errors occur
    pub async fn write_row(&mut self, row: &[ColumnValues]) -> TdsResult<()> {
        if !self.metadata_written {
            return Err(Error::ProtocolError(
                "Must call begin() before write_row()".to_string(),
            ));
        }

        // Validate row length
        if row.len() != self.column_metadata.len() {
            return Err(Error::ProtocolError(format!(
                "Row column count ({}) does not match metadata count ({})",
                row.len(),
                self.column_metadata.len()
            )));
        }

        // Write ROW token
        self.packet_writer.write_byte_async(TOKEN_ROW).await?;

        // Write each column value using pre-created contexts (zero allocations per row)
        for (i, value) in row.iter().enumerate() {
            let ctx = &self.column_contexts[i];
            TdsValueSerializer::serialize_value(self.packet_writer, value, ctx).await?;
        }

        self.rows_written += 1;

        trace!(
            "StreamingBulkLoadWriter: Row {} written ({} columns)",
            self.rows_written,
            row.len()
        );

        Ok(())
    }

    /// Write a single column value directly (for zero-copy bulk load).
    ///
    /// This is used by the `BulkLoadRow` trait to write columns one at a time
    /// without allocating a Vec<ColumnValues>.
    ///
    /// # Arguments
    ///
    /// * `column_index` - The index of the column being written
    /// * `value` - Column value to write
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Column index is out of bounds
    /// - Network errors occur during transmission
    /// - Type conversion errors occur
    pub async fn write_column_value(
        &mut self,
        column_index: usize,
        value: &ColumnValues,
    ) -> TdsResult<()> {
        // Get the context for the specified column
        let ctx = self.column_contexts.get(column_index).ok_or_else(|| {
            Error::UsageError(format!(
                "Column index {} out of bounds (max: {})",
                column_index,
                self.column_contexts.len()
            ))
        })?;

        TdsValueSerializer::serialize_value(self.packet_writer, value, ctx).await?;

        Ok(())
    }

    /// Get mutable access to the packet writer (for pre-serialized bytes).
    ///
    /// This allows external code to write pre-serialized TDS bytes directly
    /// to the packet without going through write_column_value.
    ///
    /// # Safety
    ///
    /// Caller must ensure the bytes written are valid TDS wire format for
    /// the expected column types, or SQL Server will reject the data.
    pub fn packet_writer(&mut self) -> &mut PacketWriter<'a> {
        self.packet_writer
    }

    /// Write pre-serialized TDS wire format bytes directly to the packet.
    ///
    /// This is a convenience method for writing raw TDS bytes that have been
    /// serialized externally (e.g., by Python code). It uses the internal
    /// TdsPacketWriter trait to write the bytes.
    ///
    /// # Safety
    ///
    /// Caller must ensure the bytes are valid TDS wire format for the expected
    /// column types, or SQL Server will reject the data.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Pre-serialized TDS wire format bytes
    ///
    /// # Errors
    ///
    /// Returns an error if network transmission fails.
    pub async fn write_raw_bytes(&mut self, bytes: &[u8]) -> TdsResult<()> {
        self.packet_writer.write_async(bytes).await
    }

    /// Begin a new row (for zero-copy bulk load).
    /// Writes the ROW token.
    pub(crate) async fn begin_row(&mut self) -> TdsResult<()> {
        if !self.metadata_written {
            return Err(Error::ProtocolError(
                "Must call begin() before begin_row()".to_string(),
            ));
        }

        // Write ROW token
        self.packet_writer.write_byte_async(TOKEN_ROW).await?;

        Ok(())
    }

    /// End the current row (for zero-copy bulk load).
    /// Increments row counter.
    pub(crate) fn end_row(&mut self) {
        self.rows_written += 1;

        trace!(
            "StreamingBulkLoadWriter: Row {} written (zero-copy)",
            self.rows_written
        );
    }

    /// Get the number of columns in the metadata.
    pub(crate) fn column_count(&self) -> usize {
        self.column_metadata.len()
    }

    /// Write a single row using zero-copy BulkLoadRow trait.
    ///
    /// This method provides zero-copy bulk insert by allowing the row
    /// to serialize directly to the packet writer.
    ///
    /// # Arguments
    ///
    /// * `row` - Row implementing BulkLoadRow trait
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `begin()` has not been called yet
    /// - Network errors occur during transmission
    /// - Type conversion errors occur
    pub async fn write_row_zerocopy<R>(&mut self, row: &R) -> TdsResult<()>
    where
        R: BulkLoadRow,
    {
        if !self.metadata_written {
            return Err(Error::ProtocolError(
                "Must call begin() before write_row_zerocopy()".to_string(),
            ));
        }

        // Write ROW token
        self.packet_writer.write_byte_async(TOKEN_ROW).await?;

        // Let the row serialize itself
        let mut column_index = 0usize;
        row.write_to_packet(self, &mut column_index).await?;

        // Verify completeness
        if column_index != self.column_metadata.len() {
            return Err(Error::UsageError(format!(
                "Incomplete row: expected {} columns, wrote {}",
                self.column_metadata.len(),
                column_index
            )));
        }

        // Increment row counter
        self.rows_written += 1;

        trace!(
            "StreamingBulkLoadWriter: Row {} written (zero-copy)",
            self.rows_written
        );

        Ok(())
    }

    /// End streaming - write DONE token and finalize packet.
    ///
    /// This consumes the writer and returns the number of rows written.
    ///
    /// # Returns
    ///
    /// The number of rows successfully written to the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Network errors occur during transmission
    pub async fn end(mut self) -> TdsResult<u64> {
        if !self.metadata_written {
            return Err(Error::ProtocolError(
                "Must call begin() before end()".to_string(),
            ));
        }

        // Write DONE token
        self.write_done_token_internal().await?;

        // Finalize packet
        self.packet_writer.finalize().await?;

        debug!(
            "StreamingBulkLoadWriter: Completed - {} rows written",
            self.rows_written
        );

        Ok(self.rows_written)
    }

    /// Internal method to write metadata.
    async fn write_metadata_internal(&mut self) -> TdsResult<()> {
        eprintln!(
            "DEBUG write_metadata_internal: {} columns",
            self.column_metadata.len()
        );
        for (i, col_meta) in self.column_metadata.iter().enumerate() {
            eprintln!(
                "  Column {}: name='{}', tds_type=0x{:02X}, sql_type={:?}",
                i, col_meta.column_name, col_meta.tds_type, col_meta.sql_type
            );
        }

        self.packet_writer
            .write_byte_async(TOKEN_COLMETADATA)
            .await?;

        // Column count (2 bytes)
        let column_count = self.column_metadata.len();
        eprintln!(
            "DEBUG: Writing COLMETADATA token: 0x{:02X}",
            TOKEN_COLMETADATA
        );
        eprintln!(
            "DEBUG: Writing column count: {} (0x{:04X})",
            column_count, column_count
        );
        self.packet_writer
            .write_u16_async(column_count as u16)
            .await?;

        // Write each column descriptor
        // Cache metadata length to avoid borrow conflicts
        let metadata_len = self.column_metadata.len();
        for i in 0..metadata_len {
            // Clone individual metadata item to avoid holding immutable borrow
            // This is acceptable since we only do it once during metadata phase
            let col_meta = self.column_metadata[i].clone();
            self.write_column_descriptor_internal(&col_meta).await?;
        }

        // DEBUG: Capture and display the COMPLETE packet buffer state after all metadata written
        eprintln!("\n========== DEBUG: Attempting to get cursor ==========");
        // Try a simpler approach - just log what we know was written
        eprintln!("Metadata writing completed. Expected sequence:");
        eprintln!(
            "  81 01 00 | 00 00 00 00 | 09 00 | A7 | FF FF | 00 00 00 00 00 | 04 | 64 00 61 00 74 00 61 00"
        );
        eprintln!(
            "  ^token   ^UserType     ^Flags  ^Type ^Len   ^Collation       ^Len ^'d' 'a' 't' 'a'"
        );
        eprintln!("====================================================\n");

        Ok(())
    }

    /// Internal method to write column descriptor.
    async fn write_column_descriptor_internal(
        &mut self,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()> {
        eprintln!(
            "DEBUG: write_column_descriptor_internal: name='{}', tds_type=0x{:02X}",
            col_meta.column_name, col_meta.tds_type
        );

        // Collect all metadata bytes for debugging
        let mut metadata_bytes = Vec::new();

        // User type (4 bytes) - always 0 for standard types (TDS 7.2+)
        eprintln!("DEBUG: Writing UserType: 0x00000000 (4 bytes)");
        metadata_bytes.extend_from_slice(&0u32.to_le_bytes());
        self.packet_writer.write_u32_async(0).await?;

        // Flags (2 bytes)
        let mut flags: u16 = 0x0008; // Updatability flag
        if col_meta.is_nullable {
            flags |= 0x0001; // Nullable
        }
        if col_meta.is_identity {
            flags |= 0x0010; // Identity
        }
        eprintln!("DEBUG: Writing Flags: 0x{:04X} (2 bytes)", flags);
        metadata_bytes.extend_from_slice(&flags.to_le_bytes());
        self.packet_writer.write_u16_async(flags).await?;

        // TDS type byte
        eprintln!(
            "DEBUG: Writing TDS Type: 0x{:02X} (1 byte)",
            col_meta.tds_type
        );
        metadata_bytes.push(col_meta.tds_type);
        self.packet_writer
            .write_byte_async(col_meta.tds_type)
            .await?;

        // Type-specific info
        self.write_type_info_internal(col_meta).await?;

        // Column name (B_VARCHAR format)
        let name_utf16: Vec<u16> = col_meta.column_name.encode_utf16().collect();
        eprintln!(
            "DEBUG: Writing column name length: 0x{:02X} ({} UTF-16 code units)",
            (name_utf16.len() & 0xFF) as u8,
            name_utf16.len()
        );
        metadata_bytes.push((name_utf16.len() & 0xFF) as u8);
        self.packet_writer
            .write_byte_async((name_utf16.len() & 0xFF) as u8)
            .await?;
        for c in name_utf16 {
            metadata_bytes.extend_from_slice(&c.to_le_bytes());
            self.packet_writer.write_u16_async(c).await?;
        }

        // Print complete metadata hex dump for this column
        eprintln!(
            "DEBUG: Complete column metadata bytes ({} bytes):",
            metadata_bytes.len()
        );
        let hex_str: String = metadata_bytes
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!("  {}", hex_str);

        Ok(())
    }

    /// Internal method to write type info.
    async fn write_type_info_internal(
        &mut self,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()> {
        eprintln!(
            "DEBUG bulk_load: write_type_info_internal: tds_type=0x{:02X}, sql_type={:?}, is_plp={}",
            col_meta.tds_type,
            col_meta.sql_type,
            col_meta.is_plp()
        );
        eprintln!(
            "  Checking against TdsDataType::Json=0x{:02X}, TdsDataType::BigVarChar=0x{:02X}",
            TdsDataType::Json as u8,
            TdsDataType::BigVarChar as u8
        );

        match col_meta.tds_type {
            // DECIMAL/NUMERIC - precision and scale
            x if x == TdsDataType::Decimal as u8
                || x == TdsDataType::Numeric as u8
                || x == TdsDataType::DecimalN as u8
                || x == TdsDataType::NumericN as u8 => {
                self.packet_writer
                    .write_byte_async(col_meta.length as u8)
                    .await?;
                self.packet_writer
                    .write_byte_async(col_meta.precision)
                    .await?;
                self.packet_writer.write_byte_async(col_meta.scale).await?;
            }

            // Fixed-length types - NO type info needed
            x if x == TdsDataType::Int1 as u8       // TINYINT
                || x == TdsDataType::Bit as u8      // BIT
                || x == TdsDataType::Int2 as u8     // SMALLINT
                || x == TdsDataType::Int4 as u8     // INT
                || x == TdsDataType::DateTim4 as u8 // SMALLDATETIME
                || x == TdsDataType::Flt4 as u8     // REAL
                || x == TdsDataType::Money as u8    // MONEY
                || x == TdsDataType::DateTime as u8 // DATETIME
                || x == TdsDataType::Flt8 as u8     // FLOAT
                || x == TdsDataType::Int8 as u8     // BIGINT
            => {
                // These are fixed-length types, no additional type info
            }

            // INTN, FLTN, BITN, MONEYN, DATETIMEN - length byte
            x if x == TdsDataType::IntN as u8
                || x == TdsDataType::FltN as u8
                || x == TdsDataType::BitN as u8
                || x == TdsDataType::MoneyN as u8
                || x == TdsDataType::DateTimeN as u8 => {
                self.packet_writer
                    .write_byte_async(col_meta.length as u8)
                    .await?;
            }

            // VARCHAR/CHAR types - length + collation
            x if x == TdsDataType::VarChar as u8
                || x == TdsDataType::Char as u8
                || x == TdsDataType::BigVarChar as u8
                || x == TdsDataType::BigChar as u8 => {
                eprintln!("DEBUG: BigVarChar/VarChar path: tds_type=0x{:02X}, is_plp={}, collation={:?}", 
                    x, col_meta.is_plp(), col_meta.collation);
                
                if col_meta.is_plp() {
                    eprintln!("DEBUG: Writing Length: 0xFFFF (PLP marker, 2 bytes)");
                    self.packet_writer.write_u16_async(0xFFFF).await?;
                } else {
                    eprintln!("DEBUG: Writing Length: 0x{:04X} (2 bytes)", col_meta.length);
                    self.packet_writer
                        .write_u16_async(col_meta.length as u16)
                        .await?;
                }

                if let Some(collation) = col_meta.collation {
                    eprintln!("DEBUG: Writing collation bytes:");
                    eprintln!("  info=0x{:08X} (bytes: {:02X} {:02X} {:02X} {:02X})", 
                        collation.info, 
                        (collation.info & 0xFF) as u8,
                        ((collation.info >> 8) & 0xFF) as u8,
                        ((collation.info >> 16) & 0xFF) as u8,
                        ((collation.info >> 24) & 0xFF) as u8);
                    eprintln!("  sort_id=0x{:02X}", collation.sort_id);
                    self.packet_writer.write_u32_async(collation.info).await?;
                    self.packet_writer
                        .write_byte_async(collation.sort_id)
                        .await?;
                } else {
                    eprintln!("DEBUG: No collation provided, writing default 0x00000409");
                    self.packet_writer.write_u32_async(0x00000409).await?;
                    self.packet_writer.write_byte_async(0).await?;
                }
            }

            // NVARCHAR/NCHAR types - length + collation
            x if x == TdsDataType::NChar as u8
                || x == TdsDataType::NVarChar as u8 => {
                eprintln!("DEBUG bulk_load: Writing NVarChar metadata: tds_type=0x{:02X}, is_plp={}, collation={:?}, length={}", 
                    x, col_meta.is_plp(), col_meta.collation, col_meta.length);
                
                if col_meta.is_plp() {
                    self.packet_writer.write_u16_async(0xFFFF).await?;
                } else {
                    self.packet_writer
                        .write_u16_async(col_meta.length as u16)
                        .await?;
                }

                if let Some(collation) = col_meta.collation {
                    eprintln!("DEBUG: Writing collation: info=0x{:08X}, sort_id={}", collation.info, collation.sort_id);
                    self.packet_writer.write_u32_async(collation.info).await?;
                    self.packet_writer
                        .write_byte_async(collation.sort_id)
                        .await?;
                } else {
                    eprintln!("DEBUG: Using default collation: info=0x{:08X}, sort_id={}", 
                        self.default_collation.info, self.default_collation.sort_id);
                    // Use connection's default collation (matches .NET SqlBulkCopy behavior)
                    self.packet_writer
                        .write_u32_async(self.default_collation.info)
                        .await?;
                    self.packet_writer
                        .write_byte_async(self.default_collation.sort_id)
                        .await?;
                }
            }

            // VARBINARY/BINARY types - length
            x if x == TdsDataType::VarBinary as u8
                || x == TdsDataType::Binary as u8
                || x == TdsDataType::BigVarBinary as u8
                || x == TdsDataType::BigBinary as u8 => {
                if col_meta.is_plp() {
                    self.packet_writer.write_u16_async(0xFFFF).await?;
                } else {
                    self.packet_writer
                        .write_u16_async(col_meta.length as u16)
                        .await?;
                }
            }

            // XML - schema info
            x if x == TdsDataType::Xml as u8 => {
                self.packet_writer.write_byte_async(0).await?;
            }

            // JSON - schema info (similar to XML, no schema support yet)
            x if x == TdsDataType::Json as u8 => {
                self.packet_writer.write_byte_async(0).await?;
            }

            // Time types - scale only
            x if x == TdsDataType::TimeN as u8
                || x == TdsDataType::DateTime2N as u8
                || x == TdsDataType::DateTimeOffsetN as u8 => {
                trace!("Writing TIME type metadata: tds_type=0x{:02X}, length={}, scale={}", 
                       col_meta.tds_type, col_meta.length, col_meta.scale);
                self.packet_writer.write_byte_async(col_meta.scale).await?;
            }

            // DATE - no type info
            x if x == TdsDataType::DateN as u8 => {}

            // UNIQUEIDENTIFIER (GUIDTYPE) - requires length byte (always 16)
            x if x == TdsDataType::Guid as u8 => {
                self.packet_writer.write_byte_async(16).await?;
            }

            _ => {
                return Err(Error::ProtocolError(format!(
                    "Unsupported TDS type for bulk copy: 0x{:02X}",
                    col_meta.tds_type
                )));
            }
        }

        Ok(())
    }

    /// Internal method to write DONE token.
    async fn write_done_token_internal(&mut self) -> TdsResult<()> {
        self.packet_writer.write_byte_async(TOKEN_DONE).await?;
        self.packet_writer.write_u16_async(0x0000).await?; // Status
        self.packet_writer.write_u16_async(0x0000).await?; // CurCmd
        self.packet_writer.write_u32_async(0).await?; // Row count (client sends 4 bytes)

        Ok(())
    }
}

/// Helper function to build the INSERT BULK SQL command.
///
/// This is used by both `BulkLoadMessage` and streaming bulk copy operations.
///
/// # Arguments
///
/// * `table_name` - Destination table name
/// * `column_metadata` - Column metadata for the bulk load
/// * `options` - Bulk copy options
///
/// # Returns
///
/// The INSERT BULK SQL command string
pub(crate) fn build_insert_bulk_command(
    table_name: &str,
    column_metadata: &[BulkCopyColumnMetadata],
    options: &BulkCopyOptions,
) -> String {
    let mut command = format!("INSERT BULK {table_name} (");

    for (i, col_meta) in column_metadata.iter().enumerate() {
        if i > 0 {
            command.push_str(", ");
        }

        // Column name
        command.push_str(&format!("[{}] ", col_meta.column_name));

        // Type definition
        let type_def = col_meta.get_sql_type_definition();
        command.push_str(&type_def);
    }

    command.push(')');

    // Add WITH clause for options
    let mut option_list = Vec::new();
    if options.keep_nulls {
        option_list.push("KEEP_NULLS");
    }
    if options.table_lock {
        option_list.push("TABLOCK");
    }
    if options.check_constraints {
        option_list.push("CHECK_CONSTRAINTS");
    }
    if options.fire_triggers {
        option_list.push("FIRE_TRIGGERS");
    }
    if options.keep_identity {
        option_list.push("KEEP_IDENTITY");
    }

    if !option_list.is_empty() {
        command.push_str(" WITH (");
        command.push_str(&option_list.join(", "));
        command.push(')');
    }

    command
}

// Include additional unit tests from separate test file
#[cfg(test)]
#[path = "bulk_load_tests.rs"]
mod bulk_load_tests;
