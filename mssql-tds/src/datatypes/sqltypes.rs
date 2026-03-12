// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use uuid::Uuid;

use crate::datatypes::column_values::{
    ColumnValues, DEFAULT_VARTIME_SCALE, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset,
    SqlMoney, SqlSmallDateTime, SqlSmallMoney, SqlTime, SqlXml,
};
use crate::datatypes::sql_json::SqlJson;
use crate::datatypes::sql_vector::SqlVector;
use crate::datatypes::tds_value_serializer::{TdsTypeContext, TdsValueSerializer};
use crate::{
    core::TdsResult,
    datatypes::{
        decoder::DecimalParts,
        sql_string::SqlString,
        sqldatatypes::{
            FixedLengthTypes, TdsDataType, VECTOR_HEADER_SIZE, VECTOR_MAX_DIMENSIONS,
            VectorBaseType, VectorLayoutFormat, VectorLayoutVersion,
        },
    },
    error::Error,
    io::packet_writer::{PacketWriter, TdsPacketWriter},
    token::tokens::SqlCollation,
};

#[derive(Debug, PartialEq, Clone)]
pub enum SqlType {
    Bit(Option<bool>),
    TinyInt(Option<u8>),
    SmallInt(Option<i16>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Real(Option<f32>),
    Float(Option<f64>),
    Decimal(Option<DecimalParts>),
    Numeric(Option<DecimalParts>),
    Money(Option<SqlMoney>),
    SmallMoney(Option<SqlSmallMoney>),

    Time(Option<SqlTime>),
    DateTime2(Option<SqlDateTime2>),
    DateTimeOffset(Option<SqlDateTimeOffset>),
    SmallDateTime(Option<SqlSmallDateTime>),
    DateTime(Option<SqlDateTime>),
    Date(Option<SqlDate>),

    /// Represents a Varchar with a specifiied length.
    NVarchar(Option<SqlString>, u16),

    /// Represents a Varchar with MAX length.
    NVarcharMax(Option<SqlString>),

    Varchar(Option<SqlString>, u16),
    VarcharMax(Option<SqlString>),

    VarBinary(Option<Vec<u8>>, u16),
    VarBinaryMax(Option<Vec<u8>>),

    Binary(Option<Vec<u8>>, u16),
    Char(Option<SqlString>, u16),
    NChar(Option<SqlString>, u16),

    Text(Option<SqlString>),
    NText(Option<SqlString>),

    Json(Option<SqlJson>),

    Xml(Option<SqlXml>),
    Uuid(Option<Uuid>),

    /// Parameters: (data, dimensions, base_type)
    /// Although SqlVector has dimension & base type information, we also pass it separately so
    /// that we can serialize NULL vector parameters (where SqlVector=None) with correct metadata.
    Vector(Option<SqlVector>, u16, VectorBaseType),
    // To be added in future
    // Variant
    // TVP
}

type NullableTdsType = TdsDataType;

// The maximum length of a variable length type in TDS is 8000 bytes.
pub(crate) const VAR_TDS_MAX_LENGTH: u16 = 8000u16;

// The length of a NULL value in TDS is 65535 bytes for variable length types.
pub(crate) const MAX_U16_LENGTH: u16 = 65535u16;

// The length of a NULL value in TDS is 0 bytes.
pub(crate) const NULL_LENGTH: u8 = 0u8;

// The fixed size for Decimal in TDS is 17 bytes.
pub(crate) const DECIMAL_FIXED_SIZE: u8 = 17;

// The short data length which signifies that the data is being sent as PLP (Partial Length Packet).
pub(crate) const MAX_SHORT_DATA_LENGTH: u16 = 0xFFFF;

pub(crate) const PLP_TERMINATOR_CHUNK_LEN: u32 = 0x00000000;

pub(crate) const PLP_UNKNOWN_LENGTH: u64 = 0xFFFF_FFFF_FFFF_FFFE;

pub(crate) const PLP_NULL: u64 = 0xFFFF_FFFF_FFFF_FFFF;

pub(crate) const NO_XML_SCHEMA: u8 = 0x00;

impl SqlType {
    fn get_nullable_type(&self) -> NullableTdsType {
        match self {
            SqlType::Bit(_)
            | SqlType::TinyInt(_)
            | SqlType::SmallInt(_)
            | SqlType::Int(_)
            | SqlType::BigInt(_) => TdsDataType::IntN,
            SqlType::Real(_) | SqlType::Float(_) => TdsDataType::FltN,
            SqlType::Decimal(_) => TdsDataType::NumericN,
            SqlType::Numeric(_) => TdsDataType::NumericN,
            SqlType::NVarchar(_, _) => TdsDataType::NVarChar,
            SqlType::VarBinary(_items, _size) => TdsDataType::BigVarBinary,
            SqlType::Binary(_items, _) => TdsDataType::BigBinary,
            SqlType::Char(_, _) => TdsDataType::BigChar,
            SqlType::NChar(_, _) => TdsDataType::NChar,
            SqlType::Text(_) => TdsDataType::Text,
            SqlType::NText(_) => TdsDataType::NText,
            SqlType::Json(_) => TdsDataType::Json,

            SqlType::Time(_) => TdsDataType::TimeN,
            SqlType::DateTime2(_) => TdsDataType::DateTime2N,
            SqlType::DateTimeOffset(_) => TdsDataType::DateTimeOffsetN,
            SqlType::DateTime(_) => TdsDataType::DateTimeN,
            SqlType::Date(_) => TdsDataType::DateN,
            SqlType::SmallDateTime(_) => TdsDataType::DateTimeN,
            SqlType::NVarcharMax(_) => TdsDataType::NVarChar,
            SqlType::Varchar(_, _) => TdsDataType::BigVarChar,
            SqlType::VarcharMax(_) => TdsDataType::BigVarChar,
            SqlType::VarBinaryMax(_) => TdsDataType::BigVarBinary,
            SqlType::Xml(_) => TdsDataType::Xml,
            SqlType::Uuid(_) => TdsDataType::Guid,
            SqlType::Money(_) => TdsDataType::MoneyN,
            SqlType::SmallMoney(_) => TdsDataType::MoneyN,
            SqlType::Vector(_, _, _) => TdsDataType::Vector,
        }
    }

    fn get_fixed_length_size(&self) -> usize {
        let fixed_length_type = FixedLengthTypes::try_from(self);
        assert!(
            fixed_length_type.is_ok(),
            "SqlType is not a fixed length type."
        );
        fixed_length_type.unwrap().get_len()
    }

    /// Convert this SqlType to a ColumnValues (for value serialization) and a TdsTypeContext.
    /// Returns (ColumnValues, TdsTypeContext).
    fn to_column_value_and_context(
        &self,
        db_collation: &SqlCollation,
    ) -> (ColumnValues, TdsTypeContext) {
        let nullable_type = self.get_nullable_type();
        let tds_type = nullable_type as u8;

        // Common context for nullable RPC types: always nullable, never fixed-length
        // (RPC value data always has length prefixes)
        let base_ctx = TdsTypeContext {
            tds_type,
            max_size: 0,
            is_plp: false,
            is_fixed_length: false,
            precision: None,
            scale: None,
            collation: None,
            is_nullable: true,
        };

        match self {
            // Fixed-size integer/float types
            SqlType::Bit(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Bit(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 1,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::TinyInt(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::TinyInt(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 1,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::SmallInt(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::SmallInt(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 2,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::Int(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Int(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 4,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::BigInt(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::BigInt(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 8,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::Real(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Real(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 4,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::Float(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Float(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 8,
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Decimal/Numeric - RPC always sends 17 bytes, so use precision >= 29
            SqlType::Decimal(opt) | SqlType::Numeric(opt) => {
                let cv = match opt {
                    Some(v) => {
                        if matches!(self, SqlType::Numeric(_)) {
                            ColumnValues::Numeric(v.clone())
                        } else {
                            ColumnValues::Decimal(v.clone())
                        }
                    }
                    None => ColumnValues::Null,
                };
                // Use precision 38 to ensure TdsValueSerializer writes 17 bytes (matching RPC behavior)
                let ctx = TdsTypeContext {
                    max_size: DECIMAL_FIXED_SIZE as usize,
                    precision: Some(38),
                    scale: Some(0),
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Money types
            SqlType::Money(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Money(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 8,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::SmallMoney(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::SmallMoney(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 4,
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Date/Time types
            SqlType::Date(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Date(v.clone()),
                    None => ColumnValues::Null,
                };
                (cv, base_ctx)
            }
            SqlType::Time(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Time(v.clone()),
                    None => ColumnValues::Null,
                };
                (cv, base_ctx)
            }
            SqlType::DateTime(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::DateTime(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 8,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::SmallDateTime(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::SmallDateTime(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 4,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::DateTime2(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::DateTime2(v.clone()),
                    None => ColumnValues::Null,
                };
                (cv, base_ctx)
            }
            SqlType::DateTimeOffset(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::DateTimeOffset(v.clone()),
                    None => ColumnValues::Null,
                };
                (cv, base_ctx)
            }

            // UUID
            SqlType::Uuid(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Uuid(*v),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: 16,
                    ..base_ctx
                };
                (cv, ctx)
            }

            // String types - NVarchar
            SqlType::NVarchar(opt, param_len) => {
                let max_size = 4000u16;
                let param_len = if *param_len > max_size {
                    MAX_U16_LENGTH
                } else {
                    *param_len * 2
                };
                let is_plp = param_len == MAX_U16_LENGTH;
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::NVarChar as u8,
                    max_size: if is_plp {
                        usize::MAX
                    } else {
                        (param_len / 2) as usize
                    },
                    is_plp,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::NVarcharMax(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::NVarChar as u8,
                    max_size: usize::MAX,
                    is_plp: true,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }

            // String types - Varchar
            SqlType::Varchar(opt, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    MAX_U16_LENGTH
                } else {
                    *param_len
                };
                let is_plp = param_len == MAX_U16_LENGTH;
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::BigVarChar as u8,
                    max_size: if is_plp {
                        usize::MAX
                    } else {
                        param_len as usize
                    },
                    is_plp,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::VarcharMax(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::BigVarChar as u8,
                    max_size: usize::MAX,
                    is_plp: true,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Char/NChar
            SqlType::Char(opt, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    MAX_U16_LENGTH
                } else {
                    *param_len
                };
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::BigChar as u8,
                    max_size: param_len as usize,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::NChar(opt, param_len) => {
                let param_len = if *param_len > 4000 {
                    MAX_U16_LENGTH
                } else {
                    *param_len * 2
                };
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::NChar as u8,
                    max_size: (param_len / 2) as usize,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Text/NText (legacy LOB types)
            SqlType::Text(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::Text as u8,
                    max_size: usize::MAX,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::NText(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::String(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    tds_type: TdsDataType::NText as u8,
                    max_size: usize::MAX,
                    collation: Some(*db_collation),
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Binary types
            SqlType::Binary(opt, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    u16::MAX
                } else {
                    *param_len
                };
                let cv = match opt {
                    Some(v) => ColumnValues::Bytes(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: param_len as usize,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::VarBinary(opt, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    u16::MAX
                } else {
                    *param_len
                };
                let is_plp = param_len == u16::MAX;
                let cv = match opt {
                    Some(v) => ColumnValues::Bytes(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: param_len as usize,
                    is_plp,
                    ..base_ctx
                };
                (cv, ctx)
            }
            SqlType::VarBinaryMax(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Bytes(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: usize::MAX,
                    is_plp: true,
                    ..base_ctx
                };
                (cv, ctx)
            }

            // XML
            SqlType::Xml(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Xml(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: usize::MAX,
                    is_plp: true,
                    ..base_ctx
                };
                (cv, ctx)
            }

            // JSON
            SqlType::Json(opt) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Json(v.clone()),
                    None => ColumnValues::Null,
                };
                let ctx = TdsTypeContext {
                    max_size: usize::MAX,
                    is_plp: true,
                    ..base_ctx
                };
                (cv, ctx)
            }

            // Vector
            SqlType::Vector(opt, dimensions, base_type) => {
                let cv = match opt {
                    Some(v) => ColumnValues::Vector(v.clone()),
                    None => ColumnValues::Null,
                };
                let element_size = base_type.element_size_bytes() as u16;
                let exact_size = (VECTOR_HEADER_SIZE as u16) + (*dimensions * element_size);
                let ctx = TdsTypeContext {
                    max_size: exact_size as usize,
                    ..base_ctx
                };
                (cv, ctx)
            }
        }
    }

    /// Write RPC type metadata preamble, then delegate value serialization to TdsValueSerializer.
    pub(crate) async fn serialize(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
    ) -> TdsResult<()> {
        // JSON needs special handling: TdsValueSerializer converts to UTF-16LE for bulk copy,
        // but RPC sends raw UTF-8 bytes with TDS type 0xF4.
        if let SqlType::Json(json) = self {
            return self.serialize_json(packet_writer, json).await;
        }

        // Step 1: Write the RPC type metadata preamble
        self.write_rpc_type_metadata(packet_writer, db_collation)
            .await?;

        // Step 2: Convert to ColumnValues + TdsTypeContext and serialize value
        let (column_value, ctx) = self.to_column_value_and_context(db_collation);
        TdsValueSerializer::serialize_value(packet_writer, &column_value, &ctx).await?;

        Ok(())
    }

    /// Write the RPC type metadata preamble (type byte, max_size, precision/scale/collation).
    /// This is RPC-specific and must stay exactly as-is for protocol compatibility.
    async fn write_rpc_type_metadata(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
    ) -> TdsResult<()> {
        let nullable_type = self.get_nullable_type();

        match self {
            // Fixed-size integer types: type byte + max_size byte
            SqlType::Bit(_)
            | SqlType::TinyInt(_)
            | SqlType::SmallInt(_)
            | SqlType::Int(_)
            | SqlType::BigInt(_)
            | SqlType::Real(_)
            | SqlType::Float(_) => {
                let type_size = self.get_fixed_length_size();
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(type_size as u8).await?;
            }

            // Decimal/Numeric: type byte + 17 + precision + scale
            SqlType::Decimal(opt) | SqlType::Numeric(opt) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(DECIMAL_FIXED_SIZE).await?;
                match opt {
                    Some(v) => {
                        packet_writer.write_byte_async(v.precision).await?;
                        packet_writer.write_byte_async(v.scale).await?;
                    }
                    None => {
                        packet_writer.write_byte_async(1).await?;
                        packet_writer.write_byte_async(0).await?;
                    }
                }
            }

            // Money types: type byte + size byte
            SqlType::Money(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(8u8).await?;
            }
            SqlType::SmallMoney(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(4u8).await?;
            }

            // UUID: type byte + 16
            SqlType::Uuid(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(16u8).await?;
            }

            // DateTime: type byte + 8
            SqlType::DateTime(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(8u8).await?;
            }

            // SmallDateTime: type byte + 4
            SqlType::SmallDateTime(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(4u8).await?;
            }

            // Date: type byte only (no size byte)
            SqlType::Date(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
            }

            // Time: type byte + scale
            SqlType::Time(opt) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                let scale = match opt {
                    Some(t) => t.get_scale(),
                    None => DEFAULT_VARTIME_SCALE,
                };
                packet_writer.write_byte_async(scale).await?;
            }

            // DateTime2: type byte + scale
            SqlType::DateTime2(opt) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                let scale = match opt {
                    Some(dt2) => dt2.time.get_scale(),
                    None => DEFAULT_VARTIME_SCALE,
                };
                packet_writer.write_byte_async(scale).await?;
            }

            // DateTimeOffset: type byte + scale
            SqlType::DateTimeOffset(opt) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                let scale = match opt {
                    Some(dto) => dto.datetime2.time.get_scale(),
                    None => DEFAULT_VARTIME_SCALE,
                };
                packet_writer.write_byte_async(scale).await?;
            }

            // NVarchar: type byte + param_len(u16) + collation(5 bytes)
            SqlType::NVarchar(_, param_len) => {
                let max_size = 4000u16;
                let param_len = if *param_len > max_size {
                    MAX_U16_LENGTH
                } else {
                    *param_len * 2
                };
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(param_len).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
            }
            SqlType::NVarcharMax(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(MAX_U16_LENGTH).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
            }

            // Varchar: type byte + param_len(u16) + collation(5 bytes)
            SqlType::Varchar(_, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    MAX_U16_LENGTH
                } else {
                    *param_len
                };
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(param_len).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
            }
            SqlType::VarcharMax(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(MAX_U16_LENGTH).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
            }

            // Char: type byte + param_len(u16) + collation(5 bytes)
            SqlType::Char(_, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    MAX_U16_LENGTH
                } else {
                    *param_len
                };
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(param_len).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
            }
            SqlType::NChar(_, param_len) => {
                let param_len = if *param_len > 4000 {
                    MAX_U16_LENGTH
                } else {
                    *param_len * 2
                };
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(param_len).await?;
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
            }

            // Text/NText: type byte + u32 max_size + collation(5 bytes) + table name parts
            SqlType::Text(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u32_async(0x7FFFFFFF).await?; // max size
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
                // No table name parts for RPC parameters
                packet_writer.write_byte_async(0).await?; // num parts = 0
            }
            SqlType::NText(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u32_async(0x7FFFFFFF).await?; // max size
                packet_writer.write_u32_async(db_collation.info).await?;
                packet_writer.write_byte_async(db_collation.sort_id).await?;
                // No table name parts for RPC parameters
                packet_writer.write_byte_async(0).await?; // num parts = 0
            }

            // Binary types: type byte + param_len(u16)
            SqlType::Binary(_, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    u16::MAX
                } else {
                    *param_len
                };
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(param_len).await?;
            }
            SqlType::VarBinary(_, param_len) => {
                let param_len = if *param_len > VAR_TDS_MAX_LENGTH {
                    u16::MAX
                } else {
                    *param_len
                };
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(param_len).await?;
            }
            SqlType::VarBinaryMax(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_u16_async(u16::MAX).await?;
            }

            // XML: type byte + no_schema byte
            SqlType::Xml(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
                packet_writer.write_byte_async(NO_XML_SCHEMA).await?;
            }

            // JSON: type byte only
            SqlType::Json(_) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;
            }

            // Vector: type byte + exact_size(u16) + base_type byte
            SqlType::Vector(sql_vector, dimensions, base_type) => {
                packet_writer.write_byte_async(nullable_type as u8).await?;

                if *dimensions > VECTOR_MAX_DIMENSIONS {
                    return Err(Error::UsageError(format!(
                        "Vector dimensions {} exceeds maximum supported dimensions {}",
                        dimensions, VECTOR_MAX_DIMENSIONS
                    )));
                }

                if let Some(vector) = sql_vector {
                    let actual_base_type = vector.base_type();
                    if actual_base_type != *base_type {
                        return Err(Error::TypeConversionError(format!(
                            "Vector base type mismatch: declared {:?}, but vector has {:?}",
                            base_type, actual_base_type
                        )));
                    }
                    let actual_dimensions = vector.dimension_count();
                    if actual_dimensions != *dimensions {
                        return Err(Error::TypeConversionError(format!(
                            "Vector dimension mismatch: declared {}, but vector has {}",
                            dimensions, actual_dimensions
                        )));
                    }
                }

                let element_size = base_type.element_size_bytes() as u16;
                let exact_size = (VECTOR_HEADER_SIZE as u16) + (*dimensions * element_size);
                packet_writer.write_u16_async(exact_size).await?;
                packet_writer.write_byte_async(*base_type as u8).await?;
            }
        }

        Ok(())
    }

    async fn serialize_json(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        json: &Option<SqlJson>,
    ) -> TdsResult<()> {
        let nullable_type: NullableTdsType = self.get_nullable_type();
        packet_writer.write_byte_async(nullable_type as u8).await?;
        let optional_sqljson = match json {
            Some(binary) => Some(binary),
            None => None,
        };

        match optional_sqljson {
            Some(sqljson) => {
                let data = &sqljson.bytes;

                // Write unknown length for PLP.
                packet_writer.write_u64_async(PLP_UNKNOWN_LENGTH).await?;

                let data_len = data.len();

                // Write the data chunk length, which is the same as PLP length.
                packet_writer.write_u32_async(data_len as u32).await?;

                packet_writer.write_async(data).await?;

                // Write a zero-length PLP chunk terminator to signal the end of the PLP stream.
                packet_writer.write_u32_async(0).await?;
            }
            None => {
                packet_writer.write_u64_async(PLP_NULL).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn encode_vector_header(
        packet_writer: &mut PacketWriter<'_>,
        dimension_count: u16,
        base_type: VectorBaseType,
    ) -> TdsResult<()> {
        packet_writer
            .write_byte_async(VectorLayoutFormat::V1 as u8)
            .await?;
        packet_writer
            .write_byte_async(VectorLayoutVersion::V1 as u8)
            .await?;
        packet_writer.write_u16_async(dimension_count).await?;
        packet_writer.write_byte_async(base_type as u8).await?;
        packet_writer.write_byte_async(0x00).await?; // reserved
        packet_writer.write_byte_async(0x00).await?; // reserved
        packet_writer.write_byte_async(0x00).await?; // reserved
        Ok(())
    }
}

/// Calculate the byte length for time-based types based on scale value.
///
/// This mapping is defined in the TDS protocol documentation:
/// - Scale 0-2: 3 bytes
/// - Scale 3-4: 4 bytes  
/// - Scale 5-7: 5 bytes
pub(crate) fn get_time_length_from_scale(scale: u8) -> TdsResult<u8> {
    match scale {
        0..=2 => Ok(0x03),
        3 | 4 => Ok(0x04),
        5..=7 => Ok(0x05),
        _ => Err(Error::UsageError(format!(
            "Invalid scale for Time type: {scale}"
        ))),
    }
}

impl TryFrom<&SqlType> for FixedLengthTypes {
    type Error = Error;

    fn try_from(value: &SqlType) -> TdsResult<FixedLengthTypes> {
        match value {
            SqlType::Bit(_) => Ok(FixedLengthTypes::Int1),
            SqlType::TinyInt(_) => Ok(FixedLengthTypes::Int1),
            SqlType::SmallInt(_) => Ok(FixedLengthTypes::Int2),
            SqlType::Int(_) => Ok(FixedLengthTypes::Int4),
            SqlType::BigInt(_) => Ok(FixedLengthTypes::Int8),
            SqlType::Real(_) => Ok(FixedLengthTypes::Flt4),
            SqlType::Float(_) => Ok(FixedLengthTypes::Flt8),
            _ => Err(Error::UsageError(
                "SqlType is not a fixed length type.".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod json_tests {
    use std::io::Cursor;

    use bytes::Buf;

    use crate::{
        datatypes::{
            sql_json::SqlJson,
            sqldatatypes::TdsDataType,
            sqltypes::{PLP_NULL, PLP_UNKNOWN_LENGTH, SqlType},
        },
        io::{
            packet_reader::tests::MockNetworkReaderWriter,
            packet_writer::{PacketWriter, TdsPacketWriter},
        },
        message::messages::PacketType,
    };

    #[tokio::test]
    async fn test_write_json() {
        let json_str = "[\"abc\",\"ghi\",\"def\"]";
        let sqljson: SqlJson = json_str.to_string().into();

        let mut copied_bytes = Vec::new();

        copied_bytes.extend_from_slice(sqljson.bytes.as_slice());

        let byte_len = sqljson.bytes.len();

        let val = Some(sqljson);
        let sqltypejson = SqlType::Json(val.clone());

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypejson
            .serialize_json(&mut packet_writer, &val)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Json as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u64_le(), PLP_UNKNOWN_LENGTH);
        assert_eq!(test_cursor.get_u32_le(), byte_len as u32); // Chunk len
        let mut written_bytes = vec![0u8; byte_len];
        test_cursor.copy_to_slice(&mut written_bytes);
        assert_eq!(written_bytes, copied_bytes);
    }

    #[tokio::test]
    async fn test_write_null_json() {
        let sqltypejson = SqlType::Json(None);

        let mut mock_reader_writer = MockNetworkReaderWriter::default();

        let mut packet_writer = PacketWriter::new(
            PacketType::TabularResult,
            &mut mock_reader_writer,
            None,
            None,
        );

        sqltypejson
            .serialize_json(&mut packet_writer, &None)
            .await
            .unwrap();
        packet_writer.finalize().await.unwrap();

        let payload = mock_reader_writer.get_written_data();
        let mut test_cursor = Cursor::new(payload);
        test_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        assert_eq!(test_cursor.get_u8(), TdsDataType::Json as u8); // Valdate tds type
        assert_eq!(test_cursor.get_u64_le(), PLP_NULL);
    }
}
