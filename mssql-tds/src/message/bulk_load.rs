// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Bulk load message implementation for SQL Server bulk copy protocol.
//!
//! This module implements the TDS bulk load protocol for high-performance data insertion.
//! It follows the .NET SqlBulkCopy implementation pattern from TdsParser.WriteBulkCopyMetaData
//! and WriteBulkCopyValue methods.

use super::messages::{PacketType, Request};
use crate::connection::bulk_copy::BulkCopyOptions;
use crate::core::TdsResult;
use crate::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, EncodingType, SqlDbType};
use crate::datatypes::column_values::ColumnValues;
use crate::error::Error;
use crate::read_write::packet_writer::{PacketWriter, TdsPacketWriter};
use async_trait::async_trait;
use tracing::{debug, trace};

// TDS Token types
const TOKEN_COLMETADATA: u8 = 0x81;
const TOKEN_ROW: u8 = 0xD1;
const TOKEN_NBCROW: u8 = 0xD2; // Null Bitmap Compressed Row
const TOKEN_DONE: u8 = 0xFD;

// NULL markers for different type classes
const FIXEDNULL: u8 = 0x00;
const VARNULL: u16 = 0xFFFF;
const PLP_NULL: u64 = 0xFFFFFFFFFFFFFFFF;
const PLP_UNKNOWN: u64 = 0xFFFFFFFFFFFFFFFE;
const PLP_TERMINATOR: u32 = 0x00000000;

/// Bulk load message for transmitting bulk copy data.
///
/// This message encapsulates column metadata and row data for the TDS bulk load protocol.
pub(crate) struct BulkLoadMessage {
    /// Destination table name
    pub table_name: String,

    /// Column metadata
    pub column_metadata: Vec<BulkCopyColumnMetadata>,

    /// Row data (batch)
    pub rows: Vec<Vec<ColumnValues>>,

    /// Bulk copy options
    pub options: BulkCopyOptions,
}

impl BulkLoadMessage {
    /// Create a new bulk load message.
    pub fn new(
        table_name: String,
        column_metadata: Vec<BulkCopyColumnMetadata>,
        rows: Vec<Vec<ColumnValues>>,
        options: BulkCopyOptions,
    ) -> Self {
        Self {
            table_name,
            column_metadata,
            rows,
            options,
        }
    }

    /// Build the "INSERT BULK" SQL command that must be sent before the bulk data.
    ///
    /// This matches .NET's SqlBulkCopy.AnalyzeTargetAndCreateUpdateBulkCommand() behavior.
    /// Format: INSERT BULK table_name (col1 type1, col2 type2, ...) [WITH (options)]
    pub fn build_insert_bulk_command(&self) -> String {
        let mut command = format!("INSERT BULK {} (", self.table_name);

        for (i, col_meta) in self.column_metadata.iter().enumerate() {
            if i > 0 {
                command.push_str(", ");
            }

            // Column name
            command.push_str(&format!("[{}] ", col_meta.column_name));

            // Type definition
            let type_def = self.get_sql_type_definition(col_meta);
            command.push_str(&type_def);
        }

        command.push(')');

        // Add WITH clause for options
        let mut options = Vec::new();
        if self.options.keep_nulls {
            options.push("KEEP_NULLS");
        }
        if self.options.table_lock {
            options.push("TABLOCK");
        }
        if self.options.check_constraints {
            options.push("CHECK_CONSTRAINTS");
        }
        if self.options.fire_triggers {
            options.push("FIRE_TRIGGERS");
        }
        if self.options.keep_identity {
            options.push("KEEP_IDENTITY");
        }

        if !options.is_empty() {
            command.push_str(" WITH (");
            command.push_str(&options.join(", "));
            command.push(')');
        }

        command
    }

    /// Get SQL type definition string for a column.
    fn get_sql_type_definition(&self, col_meta: &BulkCopyColumnMetadata) -> String {
        // Reuse the implementation from BulkCopyColumnMetadata
        col_meta.get_sql_type_definition()
    }

    /// Write column metadata block.
    ///
    /// Based on .NET TdsParser.WriteBulkCopyMetaData (lines 11498-11724).
    /// Writes the COLMETADATA token followed by column descriptors.
    async fn write_metadata<'a, 'b>(&'a self, writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Write COLMETADATA token
        writer.write_byte_async(TOKEN_COLMETADATA).await?;

        // Write column count as 2 bytes (u16)
        debug!(
            "Writing COLMETADATA: token=0x81, column_count={} (2 bytes)",
            self.column_metadata.len()
        );
        writer
            .write_u16_async(self.column_metadata.len() as u16)
            .await?;

        // Note: NO CEK (Column Encryption Key) table is sent when column encryption is not negotiated
        // The CEK table is only sent when the connection has negotiated column encryption support

        // Write each column descriptor
        for col_meta in &self.column_metadata {
            self.write_column_descriptor(writer, col_meta).await?;
        }

        Ok(())
    }

    /// Write a single column descriptor.
    ///
    /// Each column descriptor includes:
    /// - User type (2 or 4 bytes depending on type)
    /// - Flags (2 bytes, only for non-INT types)
    /// - Type info (type-specific)
    /// - Column name
    ///
    /// Based on .NET SqlBulkCopy packet capture:
    /// - INT type: UserType(2 bytes: 0x0008) + TDS Type(1 byte) + ColName
    /// - Other types: UserType(4 bytes: 0x00000000) + Flags(2 bytes) + TDS Type + Type Info + ColName
    async fn write_column_descriptor<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        trace!(
            "Writing column descriptor for '{}': sql_type={:?}, tds_type=0x{:02X}, nullable={}, length={}",
            col_meta.column_name,
            col_meta.sql_type,
            col_meta.tds_type,
            col_meta.is_nullable,
            col_meta.length
        );

        // Based on .NET packet capture analysis when CEK is NOT negotiated:
        // .NET WriteBulkCopyMetaData writes UserType as 4 bytes for ALL columns
        // Per TdsParser.cs line 11522: WriteInt(0x0, stateObj) - always 4 bytes
        //
        // Format: UserType(4) + Flags(2) + TDS Type(1) + Type Info + ColName

        // User type (4 bytes) - always 0 for standard types
        trace!("Writing UserType: 0x00000000 (4 bytes)");
        writer.write_u32_async(0).await?;

        // Flags (2 bytes)
        let mut flags: u16 = 0x0008; // Updatability flag (always set in bulk copy)
        if col_meta.is_nullable {
            flags |= 0x0001; // Nullable
        }
        if col_meta.is_identity {
            flags |= 0x0010; // Identity
        }
        trace!(
            "Writing Flags: 0x{:04X} (nullable={}, identity={})",
            flags, col_meta.is_nullable, col_meta.is_identity
        );
        writer.write_u16_async(flags).await?;

        // TDS type byte
        trace!("Writing TDS Type: 0x{:02X}", col_meta.tds_type);
        writer.write_byte_async(col_meta.tds_type).await?;

        // Type-specific info (length, precision, scale, collation, etc.)
        trace!("Writing type info...");
        self.write_type_info(writer, col_meta).await?;

        // Column name (B_VARCHAR format)
        // Length byte (number of UTF-16 characters) + UTF-16LE string
        let name_utf16: Vec<u16> = col_meta.column_name.encode_utf16().collect();
        trace!(
            "Writing column name: length={}, name='{}'",
            name_utf16.len(),
            col_meta.column_name
        );
        writer
            .write_byte_async((name_utf16.len() & 0xFF) as u8)
            .await?;
        for c in name_utf16 {
            writer.write_u16_async(c).await?;
        }

        Ok(())
    }

    /// Write type-specific information after the TDS type byte.
    ///
    /// Different types require different additional information based on the TDS type:
    /// - Fixed-length types (0x30-0x3F): no additional info
    /// - Variable-length nullable types (0x26 INTN, 0x6D FLTN, etc.): length byte
    /// - String types: max length + collation
    /// - Decimal/Numeric: precision + scale
    ///
    /// This matches .NET's WriteTokenLength logic which uses the TDS type byte to
    /// determine what additional info to write.
    async fn write_type_info<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Check TDS type byte to determine what type info to write
        // This matches .NET's WriteTokenLength logic
        //
        // TDS type categories:
        // - Fixed-length (0x30-0x3F): No type info (length encoded in type byte)
        // - Variable-length nullable (0x26 INTN, 0x6D FLTN, 0x68 BITN, etc.): Length byte
        // - Variable-length strings: Length + collation
        // - PLP types: 0xFFFF

        match col_meta.tds_type {
            // DECIMAL/NUMERIC (0x37, 0x3F, 0x6A, 0x6C) - precision and scale
            // Check these first since 0x3F could be ambiguous
            0x37 | 0x3F | 0x6A | 0x6C => {
                // Length (always 17 for max precision)
                writer.write_byte_async(17).await?;
                // Precision
                writer.write_byte_async(col_meta.precision).await?;
                // Scale
                writer.write_byte_async(col_meta.scale).await?;
            }

            // Fixed-length types (0x30-0x3F range except 0x37, 0x3F above) - NO type info needed
            0x30 | // INT1 (TINYINT)
            0x32 | // BIT
            0x34 | // INT2 (SMALLINT)
            0x38 | // INT4 (INT)
            0x3A | // DATETIM4 (SMALLDATETIME)
            0x3B | // FLT4 (REAL)
            0x3C | // MONEY
            0x3D | // DATETIME
            0x3E | // FLT8 (FLOAT)
            0x7F   // INT8 (BIGINT)
            => {
                // These are fixed-length types, no additional type info
                trace!("Fixed-length type, no type info written");
            }

            // INTN (0x26) - nullable integer, needs length byte
            0x26 => {
                let len = match col_meta.sql_type {
                    SqlDbType::TinyInt => 1,
                    SqlDbType::SmallInt => 2,
                    SqlDbType::Int => 4,
                    SqlDbType::BigInt => 8,
                    _ => col_meta.length as u8,
                };
                trace!("Writing INTN length: {}", len);
                writer.write_byte_async(len).await?;
            }

            // FLTN (0x6D) - nullable float, needs length byte
            0x6D => {
                let len = match col_meta.sql_type {
                    SqlDbType::Real => 4,
                    SqlDbType::Float => 8,
                    _ => col_meta.length as u8,
                };
                trace!("Writing FLTN length: {}", len);
                writer.write_byte_async(len).await?;
            }

            // BITN (0x68) - nullable bit, needs length byte
            0x68 => {
                trace!("Writing BITN length: 1");
                writer.write_byte_async(1).await?;
            }

            // MONEYN (0x6E) - nullable money, needs length byte
            0x6E => {
                let len = match col_meta.sql_type {
                    SqlDbType::SmallMoney => 4,
                    SqlDbType::Money => 8,
                    _ => col_meta.length as u8,
                };
                writer.write_byte_async(len).await?;
            }

            // DATETIMEN (0x6F) - nullable datetime, needs length byte
            0x6F => {
                let len = match col_meta.sql_type {
                    SqlDbType::SmallDateTime => 4,
                    SqlDbType::DateTime => 8,
                    _ => col_meta.length as u8,
                };
                writer.write_byte_async(len).await?;
            }

            // VARCHAR/CHAR types (0x27, 0x2F, 0xA7, 0xAF) - length + collation
            0x27 | 0x2F | 0xA7 | 0xAF => {
                // Max length (2 bytes)
                if col_meta.is_plp() {
                    writer.write_u16_async(0xFFFF).await?;
                } else {
                    writer.write_u16_async(col_meta.length as u16).await?;
                }

                // Collation (5 bytes)
                if let Some(collation) = col_meta.collation {
                    writer.write_u32_async(collation.info).await?;
                    writer.write_byte_async(collation.sort_id).await?;
                } else {
                    // Default collation
                    writer.write_u32_async(0x00000409).await?;
                    writer.write_byte_async(0).await?;
                }
            }

            // NVARCHAR/NCHAR types (0xE7, 0xEF) - length + collation
            0xE7 | 0xEF => {
                // Max length in BYTES (2 bytes) - for NVARCHAR this is characters * 2
                if col_meta.is_plp() {
                    trace!("Writing NVARCHAR/NCHAR max length: 0xFFFF (PLP)");
                    writer.write_u16_async(0xFFFF).await?;
                } else {
                    trace!("Writing NVARCHAR/NCHAR max length: {} bytes", col_meta.length);
                    writer.write_u16_async(col_meta.length as u16).await?;
                }

                // Collation (5 bytes)
                if let Some(collation) = col_meta.collation {
                    trace!("Writing collation: info=0x{:08X}, sort_id=0x{:02X}", collation.info, collation.sort_id);
                    writer.write_u32_async(collation.info).await?;
                    writer.write_byte_async(collation.sort_id).await?;
                } else {
                    trace!("Writing default collation: 0x00000409, sort_id=0x00");
                    // Default collation
                    writer.write_u32_async(0x00000409).await?;
                    writer.write_byte_async(0).await?;
                }
            }

            // VARBINARY/BINARY types (0x25, 0x2D, 0xA5, 0xAD) - length
            0x25 | 0x2D | 0xA5 | 0xAD => {
                // Max length (2 bytes)
                if col_meta.is_plp() {
                    writer.write_u16_async(0xFFFF).await?;
                } else {
                    writer.write_u16_async(col_meta.length as u16).await?;
                }
            }

            // XML (0xF1) - schema info
            0xF1 => {
                // Schema info (1 byte = 0 for no schema)
                writer.write_byte_async(0).await?;
            }

            // Time types (0x29, 0x2A, 0x2B) - scale
            0x29..=0x2B => {
                // Scale (1 byte)
                writer.write_byte_async(col_meta.scale).await?;
            }

            // DATE (0x28) - no type info
            0x28 => {
                // No additional type info
            }

            // UNIQUEIDENTIFIER (0x24) - no type info (fixed 16 bytes)
            0x24 => {
                // No additional type info
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

    /// Write all rows in the batch.
    async fn write_rows<'a, 'b>(&'a self, writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        debug!("Writing {} rows", self.rows.len());
        for (i, row) in self.rows.iter().enumerate() {
            trace!("Writing row {}", i);
            self.write_row(writer, row).await?;
        }
        debug!("All rows written");
        Ok(())
    }

    /// Write a single row.
    ///
    /// Each row starts with a ROW token followed by column values.
    async fn write_row<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        row: &[ColumnValues],
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Write ROW token
        writer.write_byte_async(TOKEN_ROW).await?;

        // Write each column value
        if row.len() != self.column_metadata.len() {
            return Err(Error::ProtocolError(format!(
                "Row column count ({}) does not match metadata count ({})",
                row.len(),
                self.column_metadata.len()
            )));
        }

        for (i, value) in row.iter().enumerate() {
            let col_meta = &self.column_metadata[i];
            self.write_value(writer, value, col_meta).await?;
        }

        Ok(())
    }

    /// Write a single column value.
    ///
    /// Based on .NET TdsParser.WriteBulkCopyValue (lines 11725-11950).
    /// Handles null encoding and type-specific serialization.
    async fn write_value<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        value: &ColumnValues,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Handle NULL values first
        if matches!(value, ColumnValues::Null) {
            return self.write_null(writer, col_meta).await;
        }

        // Write non-null values
        // Check if we're using fixed-length or nullable types based on TDS type byte
        // Fixed types (0x30-0x3F, 0x7F): Write value directly, NO length byte
        // Nullable types (0x26 INTN, 0x6D FLTN, etc.): Write length byte then value
        let is_fixed_type = matches!(col_meta.tds_type, 0x30..=0x3F | 0x7F);

        match value {
            ColumnValues::TinyInt(v) => {
                // Could be INT1 (0x30 fixed) or INTN (0x26 nullable)
                if !is_fixed_type {
                    writer.write_byte_async(1).await?; // Length byte for INTN
                }
                writer.write_byte_async(*v).await?;
            }

            ColumnValues::SmallInt(v) => {
                // Could be INT2 (0x34 fixed) or INTN (0x26 nullable)
                if !is_fixed_type {
                    writer.write_byte_async(2).await?; // Length byte for INTN
                }
                writer.write_i16_async(*v).await?;
            }

            ColumnValues::Int(v) => {
                // Could be INT4 (0x38 fixed) or INTN (0x26 nullable)
                if !is_fixed_type {
                    writer.write_byte_async(4).await?; // Length byte for INTN
                }
                writer.write_i32_async(*v).await?;
            }

            ColumnValues::BigInt(v) => {
                // Could be INT8 (0x7F fixed) or INTN (0x26 nullable)
                if !is_fixed_type {
                    writer.write_byte_async(8).await?; // Length byte for INTN
                }
                writer.write_i64_async(*v).await?;
            }

            ColumnValues::Real(v) => {
                // Could be FLT4 (0x3B fixed) or FLTN (0x6D nullable)
                if !is_fixed_type {
                    writer.write_byte_async(4).await?; // Length byte for FLTN
                }
                let bytes = v.to_le_bytes();
                writer.write_async(&bytes).await?;
            }

            ColumnValues::Float(v) => {
                // Could be FLT8 (0x3E fixed) or FLTN (0x6D nullable)
                if !is_fixed_type {
                    writer.write_byte_async(8).await?; // Length byte for FLTN
                }
                let bytes = v.to_le_bytes();
                writer.write_async(&bytes).await?;
            }

            ColumnValues::Bit(v) => {
                // Could be BIT (0x32 fixed) or BITN (0x68 nullable)
                if !is_fixed_type {
                    writer.write_byte_async(1).await?; // Length byte for BITN
                }
                writer.write_byte_async(if *v { 1 } else { 0 }).await?;
            }

            ColumnValues::Decimal(parts) | ColumnValues::Numeric(parts) => {
                // Decimal: sign (1 byte) + data (16 bytes max)
                writer
                    .write_byte_async(if parts.is_positive { 1 } else { 0 })
                    .await?;

                // Write integer parts as little-endian bytes
                let mut bytes = vec![0u8; 16];
                for (i, &part) in parts.int_parts.iter().enumerate() {
                    let part_bytes = part.to_le_bytes();
                    bytes[i * 4..(i + 1) * 4].copy_from_slice(&part_bytes);
                }
                writer.write_async(&bytes).await?;
            }

            ColumnValues::String(sql_string) => {
                let s = sql_string.to_utf8_string();
                self.write_string_value(writer, &s, col_meta).await?;
            }

            ColumnValues::Bytes(bytes) => {
                self.write_binary_value(writer, bytes, col_meta).await?;
            }

            ColumnValues::Uuid(uuid) => {
                // UNIQUEIDENTIFIER is 16 bytes
                writer.write_async(uuid.as_bytes()).await?;
            }

            ColumnValues::Xml(xml) => {
                // XML as PLP
                self.write_plp_bytes(writer, &xml.bytes).await?;
            }

            ColumnValues::Json(json) => {
                // JSON is stored as UTF-8 NVARCHAR(MAX)
                let json_str = json.as_string();
                let bytes = json_str.as_bytes();
                self.write_plp_bytes(writer, bytes).await?;
            }

            // Date/Time types
            ColumnValues::Date(date) => {
                writer.write_byte_async(3).await?; // Length
                let days_bytes = date.get_days().to_le_bytes();
                writer.write_async(&days_bytes[0..3]).await?;
            }

            ColumnValues::Time(time) => {
                // Time: scale determines length (3-5 bytes)
                let length = Self::time_length_from_scale(time.scale);
                writer.write_byte_async(length).await?;

                let time_bytes = time.time_nanoseconds.to_le_bytes();
                writer.write_async(&time_bytes[0..length as usize]).await?;
            }

            ColumnValues::DateTime2(dt2) => {
                // DateTime2: time + date
                let time_length = Self::time_length_from_scale(dt2.time.scale);
                let total_length = time_length + 3; // time + 3 bytes for days
                writer.write_byte_async(total_length).await?;

                let time_bytes = dt2.time.time_nanoseconds.to_le_bytes();
                writer
                    .write_async(&time_bytes[0..time_length as usize])
                    .await?;

                let days_bytes = dt2.days.to_le_bytes();
                writer.write_async(&days_bytes[0..3]).await?;
            }

            ColumnValues::DateTimeOffset(dto) => {
                // DateTimeOffset: time + date + offset
                let time_length = Self::time_length_from_scale(dto.datetime2.time.scale);
                let total_length = time_length + 3 + 2; // time + days + offset
                writer.write_byte_async(total_length).await?;

                let time_bytes = dto.datetime2.time.time_nanoseconds.to_le_bytes();
                writer
                    .write_async(&time_bytes[0..time_length as usize])
                    .await?;

                let days_bytes = dto.datetime2.days.to_le_bytes();
                writer.write_async(&days_bytes[0..3]).await?;

                writer.write_i16_async(dto.offset).await?;
            }

            ColumnValues::DateTime(dt) => {
                // DateTime: 4 bytes days + 4 bytes time
                writer.write_i32_async(dt.days).await?;
                writer.write_u32_async(dt.time).await?;
            }

            ColumnValues::SmallDateTime(sdt) => {
                // SmallDateTime: 2 bytes days + 2 bytes minutes
                writer.write_u16_async(sdt.days).await?;
                writer.write_u16_async(sdt.time).await?;
            }

            ColumnValues::Money(money) => {
                // Money: 8 bytes (MSB, LSB)
                writer.write_i32_async(money.msb_part).await?;
                writer.write_i32_async(money.lsb_part).await?;
            }

            ColumnValues::SmallMoney(sm) => {
                // SmallMoney: 4 bytes
                writer.write_i32_async(sm.int_val).await?;
            }

            ColumnValues::Null => {
                // Already handled above
                unreachable!();
            }
        }

        Ok(())
    }

    /// Write a NULL value with appropriate encoding for the column type.
    async fn write_null<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Check if this is a nullable type (INTN, FLTN, BITN, MONEYN, DATETIMEN)
        // These types use a length byte of 0x00 to represent NULL
        match col_meta.sql_type {
            SqlDbType::TinyInt
            | SqlDbType::SmallInt
            | SqlDbType::Int
            | SqlDbType::BigInt
            | SqlDbType::Bit
            | SqlDbType::Real
            | SqlDbType::Float
            | SqlDbType::Money
            | SqlDbType::SmallMoney
            | SqlDbType::DateTime
            | SqlDbType::SmallDateTime => {
                // Nullable type NULL: length byte = 0x00
                writer.write_byte_async(0x00).await?;
            }
            _ => {
                // Other types
                if col_meta.is_plp() {
                    // PLP NULL: 8 bytes of 0xFF
                    writer.write_u64_async(PLP_NULL).await?;
                } else if col_meta.length_type.is_fixed() {
                    // Fixed-length type NULL: 0x00
                    writer.write_byte_async(FIXEDNULL).await?;
                } else {
                    // Variable-length type NULL: 0xFFFF
                    writer.write_u16_async(VARNULL).await?;
                }
            }
        }
        Ok(())
    }

    /// Write a string value with proper encoding.
    async fn write_string_value<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        s: &str,
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        let encoding = col_meta.encoding.as_ref().unwrap_or(&EncodingType::Utf16Le);
        let bytes = encoding.encode(s);

        if col_meta.is_plp() {
            // PLP string
            self.write_plp_bytes(writer, &bytes).await?;
        } else {
            // Variable-length string with 2-byte length prefix
            writer.write_u16_async(bytes.len() as u16).await?;
            writer.write_async(&bytes).await?;
        }

        Ok(())
    }

    /// Write a binary value.
    async fn write_binary_value<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        bytes: &[u8],
        col_meta: &BulkCopyColumnMetadata,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        if col_meta.is_plp() {
            // PLP binary
            self.write_plp_bytes(writer, bytes).await?;
        } else {
            // Variable-length binary with 2-byte length prefix
            writer.write_u16_async(bytes.len() as u16).await?;
            writer.write_async(bytes).await?;
        }

        Ok(())
    }

    /// Write PLP (Partial Length Prefix) data.
    ///
    /// PLP format:
    /// - 8 bytes: total length (or PLP_UNKNOWN for streaming)
    /// - Chunks: 4 bytes chunk length + data
    /// - 4 bytes: terminator (0x00000000)
    async fn write_plp_bytes<'a, 'b>(
        &'a self,
        writer: &'a mut PacketWriter<'b>,
        bytes: &[u8],
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Write total length (known)
        writer.write_u64_async(bytes.len() as u64).await?;

        // Write data in chunks (8KB max per chunk)
        const CHUNK_SIZE: usize = 8000;
        for chunk in bytes.chunks(CHUNK_SIZE) {
            writer.write_u32_async(chunk.len() as u32).await?;
            writer.write_async(chunk).await?;
        }

        // Write terminator
        writer.write_u32_async(PLP_TERMINATOR).await?;

        Ok(())
    }

    /// Calculate time length from scale.
    fn time_length_from_scale(scale: u8) -> u8 {
        match scale {
            0..=2 => 3,
            3..=4 => 4,
            5..=7 => 5,
            _ => 5,
        }
    }

    /// Write DONE token to complete the bulk load.
    ///
    /// IMPORTANT: Client DONE token uses 4-byte (Int32) row count, while server DONE token uses 8-byte (Int64).
    /// This matches .NET TdsParser behavior where client sends WriteInt(0) but server returns TryReadInt64().
    async fn write_done_token<'a, 'b>(&'a self, writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        writer.write_byte_async(TOKEN_DONE).await?;
        // Status: 0x0000 (no flags - this is client-side terminator)
        // .NET sends all zeros for the client DONE token
        writer.write_u16_async(0x0000).await?;
        // CurCmd: 0x0000 (no specific command)
        writer.write_u16_async(0x0000).await?;
        // Row count: 0 (client sends 4 bytes, not 8!)
        // .NET: WriteInt(0, stateObj) = 4 bytes
        // Server response uses 8 bytes: TryReadInt64(out longCount)
        writer.write_u32_async(0).await?;

        Ok(())
    }
}

#[async_trait]
impl Request for BulkLoadMessage {
    fn packet_type(&self) -> PacketType {
        PacketType::BulkLoad
    }

    async fn serialize<'a, 'b>(&'a self, packet_writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        // NOTE: Table name is NOT sent here. It must be sent via "INSERT BULK table_name (...)"
        // SQL command first, then this bulk load packet follows with the data.
        // See .NET implementation: SqlBulkCopy.AnalyzeTargetAndCreateUpdateBulkCommand()

        // Write column metadata
        self.write_metadata(packet_writer).await?;

        // Write row data
        self.write_rows(packet_writer).await?;

        // Write DONE token to signal end of bulk load data
        self.write_done_token(packet_writer).await?;

        // Finalize packet
        packet_writer.finalize().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::bulk_copy_metadata::{SqlDbType, TypeLength};

    #[test]
    fn test_time_length_from_scale() {
        assert_eq!(BulkLoadMessage::time_length_from_scale(0), 3);
        assert_eq!(BulkLoadMessage::time_length_from_scale(2), 3);
        assert_eq!(BulkLoadMessage::time_length_from_scale(3), 4);
        assert_eq!(BulkLoadMessage::time_length_from_scale(4), 4);
        assert_eq!(BulkLoadMessage::time_length_from_scale(5), 5);
        assert_eq!(BulkLoadMessage::time_length_from_scale(7), 5);
    }

    #[test]
    fn test_bulk_load_message_creation() {
        let metadata = vec![
            BulkCopyColumnMetadata::new("id", SqlDbType::Int, 0x38)
                .with_length(4, TypeLength::Fixed(4)),
            BulkCopyColumnMetadata::new("name", SqlDbType::NVarChar, 0xE7)
                .with_length(100, TypeLength::Variable(100)),
        ];

        use crate::datatypes::sql_string::SqlString;

        let rows = vec![vec![
            ColumnValues::Int(1),
            ColumnValues::String(SqlString::from_utf8_string("test".to_string())),
        ]];

        let msg = BulkLoadMessage::new(
            "TestTable".to_string(),
            metadata,
            rows,
            BulkCopyOptions::default(),
        );

        assert_eq!(msg.table_name, "TestTable");
        assert_eq!(msg.column_metadata.len(), 2);
        assert_eq!(msg.rows.len(), 1);
    }
}
