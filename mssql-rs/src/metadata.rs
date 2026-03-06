// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use mssql_tds::datatypes::sqldatatypes::TdsDataType;
use mssql_tds::query::metadata::ColumnMetadata as TdsColumnMetadata;

/// Simplified column metadata exposed to users.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub collation: Option<String>,
}

/// Logical data types exposed by `mssql-rs`.
///
/// Maps the ~45 TDS wire types into 11 semantic categories.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Bool,
    Int,
    Float,
    Decimal { precision: u8, scale: u8 },
    String { max_length: Option<u32> },
    Binary { max_length: Option<u32> },
    DateTime { scale: u8 },
    Uuid,
    Xml,
    Json,
    Vector { dimensions: Option<u32> },
}

const VECTOR_HEADER_SIZE: usize = 8;

impl From<&TdsColumnMetadata> for ColumnMetadata {
    fn from(meta: &TdsColumnMetadata) -> Self {
        let data_type = map_data_type(meta);
        let collation = meta.get_collation().map(|c| c.to_string());
        ColumnMetadata {
            name: meta.column_name.clone(),
            data_type,
            nullable: meta.is_nullable(),
            collation,
        }
    }
}

fn map_data_type(meta: &TdsColumnMetadata) -> DataType {
    use mssql_tds::datatypes::sqldatatypes::TypeInfoVariant;

    match meta.data_type {
        // Boolean
        TdsDataType::Bit | TdsDataType::BitN => DataType::Bool,

        // Integer
        TdsDataType::Int1
        | TdsDataType::Int2
        | TdsDataType::Int4
        | TdsDataType::Int8
        | TdsDataType::IntN => DataType::Int,

        // Float
        TdsDataType::Flt4 | TdsDataType::Flt8 | TdsDataType::FltN => DataType::Float,

        // Decimal / Numeric
        TdsDataType::Decimal
        | TdsDataType::Numeric
        | TdsDataType::DecimalN
        | TdsDataType::NumericN => {
            if let TypeInfoVariant::VarLenPrecisionScale(_, _, precision, scale) =
                meta.type_info.type_info_variant
            {
                DataType::Decimal { precision, scale }
            } else {
                DataType::Decimal {
                    precision: 18,
                    scale: 0,
                }
            }
        }

        // Money → treated as Decimal
        TdsDataType::Money | TdsDataType::Money4 | TdsDataType::MoneyN => DataType::Decimal {
            precision: 19,
            scale: 4,
        },

        // String types
        TdsDataType::VarChar
        | TdsDataType::BigVarChar
        | TdsDataType::BigChar
        | TdsDataType::Char
        | TdsDataType::Text
        | TdsDataType::NText
        | TdsDataType::NVarChar
        | TdsDataType::NChar => {
            let max_len = meta.type_info.length;
            let max_length = if max_len == 0 || max_len == usize::MAX {
                None
            } else {
                Some(max_len as u32)
            };
            DataType::String { max_length }
        }

        // Binary types
        TdsDataType::VarBinary
        | TdsDataType::BigVarBinary
        | TdsDataType::BigBinary
        | TdsDataType::Binary
        | TdsDataType::Image => {
            let max_len = meta.type_info.length;
            let max_length = if max_len == 0 || max_len == usize::MAX {
                None
            } else {
                Some(max_len as u32)
            };
            DataType::Binary { max_length }
        }

        // Date/Time
        TdsDataType::DateTime
        | TdsDataType::DateTim4
        | TdsDataType::DateTimeN
        | TdsDataType::DateN => DataType::DateTime { scale: 0 },

        TdsDataType::TimeN | TdsDataType::DateTime2N | TdsDataType::DateTimeOffsetN => {
            let scale = match meta.type_info.type_info_variant {
                TypeInfoVariant::VarLenScale(_, s) => s,
                _ => 7,
            };
            DataType::DateTime { scale }
        }

        // UUID
        TdsDataType::Guid => DataType::Uuid,

        // XML
        TdsDataType::Xml => DataType::Xml,

        // JSON
        TdsDataType::Json => DataType::Json,

        // Vector
        TdsDataType::Vector => {
            let length = meta.type_info.length;
            let dimensions = if length > VECTOR_HEADER_SIZE {
                // Float32 element size = 4
                Some(((length - VECTOR_HEADER_SIZE) / 4) as u32)
            } else {
                None
            };
            DataType::Vector { dimensions }
        }

        // Fallback
        TdsDataType::SsVariant | TdsDataType::Udt | TdsDataType::Void | TdsDataType::None => {
            DataType::Binary { max_length: None }
        }
    }
}
