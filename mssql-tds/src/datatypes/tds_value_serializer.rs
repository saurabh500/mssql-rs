// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TDS value serialization utilities for RPC parameters and bulk copy operations.
//!
//! This module provides shared value encoding logic that can be used by both
//! RPC parameter serialization (SqlType) and bulk copy ROW token serialization.
//! It separates the concerns of type metadata encoding from value encoding.

use crate::core::TdsResult;
use crate::datatypes::column_values::ColumnValues;
use crate::error::Error;
use crate::io::packet_writer::{PacketWriter, TdsPacketWriter, TdsPacketWriterUnchecked};

// NULL markers for different type classes
const NULL_LENGTH: u8 = 0x00;
const VARNULL: u16 = 0xFFFF;
const PLP_NULL: u64 = 0xFFFFFFFFFFFFFFFF;
const PLP_TERMINATOR: u32 = 0x00000000;

/// Context for value serialization, containing type metadata needed for encoding.
///
/// This struct encapsulates the TDS type information required to properly encode
/// a value without duplicating the type metadata itself.
#[derive(Debug, Clone)]
pub struct TdsTypeContext {
    /// TDS type byte (e.g., 0x26 for INTN, 0xE7 for NVARCHAR)
    pub tds_type: u8,

    /// Maximum type size (for nullable types: 1/2/4/8 for INTN, 4/8 for FLTN, etc.)
    pub max_size: u8,

    /// Whether this is a PLP (Partial Length Prefix) type (MAX types)
    pub is_plp: bool,

    /// For Decimal/Numeric: precision
    pub precision: Option<u8>,

    /// For Decimal/Numeric/Time/DateTime2/DateTimeOffset: scale
    pub scale: Option<u8>,

    /// Whether the type is nullable (affects NULL encoding)
    pub is_nullable: bool,
}

impl TdsTypeContext {
    /// Check if this is a fixed-length type (no length prefix needed).
    pub fn is_fixed_type(&self) -> bool {
        // Fixed types: INT1-INT8, BIT, FLT4, FLT8, DATETIME, MONEY, etc.
        matches!(self.tds_type, 0x30..=0x3F | 0x7F)
    }
}

/// Main value serializer - writes ONLY the value bytes (no type metadata).
pub struct TdsValueSerializer;

impl TdsValueSerializer {
    /// Serialize a value using the provided type context.
    ///
    /// This writes ONLY the value portion:
    /// - For nullable types (INTN, FLTN): length byte + value bytes
    /// - For fixed types (INT4, FLT8): value bytes only
    /// - For variable-length types: length prefix + value bytes
    /// - For PLP types: total_length + chunks + terminator
    ///
    /// Type metadata (TDS type byte, max_size, precision, scale, collation)
    /// must be written separately by the caller.
    #[inline]
    pub async fn serialize_value<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: &ColumnValues,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        match value {
            ColumnValues::Null => Self::serialize_null(writer, ctx).await,
            ColumnValues::Int(v) => Self::serialize_int(writer, *v, ctx).await,
            _ => Err(Error::UnimplementedFeature {
                feature: format!("Value serialization not implemented for type: {:?}", value),
                context: "serialization".to_string(),
            }),
        }
    }

    /// Serialize a NULL value using the appropriate NULL marker for the type.
    #[inline(always)]
    pub async fn serialize_null<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Check type class and write appropriate NULL marker
        match ctx.tds_type {
            // Nullable types (INTN, FLTN, BITN, MONEYN, DATETIMEN, NumericN, Guid, DateN) use length = 0x00
            0x26 | 0x6D | 0x68 | 0x6E | 0x6F | 0x6C | 0x24 | 0x28 => {
                writer.write_byte_async(NULL_LENGTH).await?;
            }
            _ => {
                // Other types depend on length classification
                if ctx.is_plp {
                    // PLP NULL: 8 bytes of 0xFF
                    writer.write_u64_async(PLP_NULL).await?;
                } else if ctx.is_fixed_type() {
                    // Fixed-length types cannot be NULL - must use nullable variant (INTN, FLTN, etc.)
                    return Err(Error::UsageError(format!(
                        "Cannot serialize NULL for fixed-length type 0x{:02X}.",
                        ctx.tds_type
                    )));
                } else {
                    // Variable-length NULL: 0xFFFF
                    writer.write_u16_async(VARNULL).await?;
                }
            }
        }
        Ok(())
    }

    #[inline(always)]
    async fn serialize_int<'a, 'b>(
        writer: &'a mut PacketWriter<'b>,
        value: i32,
        ctx: &TdsTypeContext,
    ) -> TdsResult<()>
    where
        'b: 'a,
    {
        // Phase 1 Optimization: Batch writes for fixed types
        if !ctx.is_fixed_type() {
            // Ensure space for length byte + value (5 bytes total)
            match writer.has_space(5) {
                false => {
                    writer.write_byte_async(4).await?; // Length for INTN
                    writer.write_i32_async(value).await?;
                }
                true => {
                    writer.write_byte_unchecked(4); // Length for INTN
                    writer.write_i32_unchecked(value);
                }
            }
        } else {
            // Fixed type - just write value (4 bytes)
            match writer.has_space(4) {
                false => {
                    writer.write_i32_async(value).await?;
                }
                true => {
                    writer.write_i32_unchecked(value);
                }
            }
        }
        Ok(())
    }
}
