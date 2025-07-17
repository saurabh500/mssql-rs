// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use mssql_tds::{
    datatypes::{
        column_values::{SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlTime},
        decoder::DecimalParts,
        sqldatatypes::TdsDataType,
    },
    query::metadata::ColumnMetadata,
    token::tokens::SqlCollation,
};
use napi::bindgen_prelude::BigInt;

use crate::connection::RowDataType;

#[napi(object)]
#[derive(Debug, Clone)]
pub struct CollationMetadata {
    pub info: u32,
    pub lcid_language_id: i32,
    pub col_flags: u8,
    pub sort_id: u8,
    pub is_utf8: bool,
}

impl From<SqlCollation> for CollationMetadata {
    fn from(collation: SqlCollation) -> Self {
        CollationMetadata {
            info: collation.info,
            lcid_language_id: collation.lcid_language_id(),
            col_flags: collation.col_flags,
            sort_id: collation.sort_id(),
            is_utf8: collation.utf8(),
        }
    }
}

#[napi]
pub enum SqlDataTypes {
    Void = 0x1F,
    Image = 0x22,
    Text = 0x23,
    Guid = 0x24,
    VarBinary = 0x25,
    VarChar = 0x27,
    Date,
    Time,
    DateTime2 = 0x2A,
    DateTimeOffset = 0x2B,
    Binary = 0x2D,
    Char = 0x2F,
    Int1 = 0x30,
    Bit = 0x32,
    Int2 = 0x34,
    Decimal = 0x37,
    Int4 = 0x38,
    SmallDateTime = 0x3A,
    Flt4 = 0x3B,
    Money = 0x3C,
    DateTime = 0x3D,
    Flt8 = 0x3E,
    Numeric = 0x3F,
    SsVariant = 0x62,
    NText = 0x63,
    FltN = 0x6D,
    Money4 = 0x7A,
    Int8 = 0x7F,
    BigVarBinary = 0xA5,
    BigVarChar = 0xA7,
    BigBinary = 0xAD,
    BigChar = 0xAF,
    NVarChar = 0xE7,
    NChar = 0xEF,
    Udt = 0xF0,
    Xml = 0xF1,
    Json = 0xF4,
}

impl TryFrom<TdsDataType> for SqlDataTypes {
    fn try_from(value: TdsDataType) -> Result<Self, Self::Error> {
        match value {
            TdsDataType::Void => Ok(SqlDataTypes::Void),
            TdsDataType::Image => Ok(SqlDataTypes::Image),
            TdsDataType::Text => Ok(SqlDataTypes::Text),
            TdsDataType::Guid => Ok(SqlDataTypes::Guid),
            TdsDataType::VarBinary => Ok(SqlDataTypes::VarBinary),
            TdsDataType::VarChar => Ok(SqlDataTypes::VarChar),
            TdsDataType::Binary => Ok(SqlDataTypes::Binary),
            TdsDataType::Char => Ok(SqlDataTypes::Char),
            TdsDataType::Int1 => Ok(SqlDataTypes::Int1),
            TdsDataType::Bit => Ok(SqlDataTypes::Bit),
            TdsDataType::Int2 => Ok(SqlDataTypes::Int2),
            TdsDataType::Decimal => Ok(SqlDataTypes::Decimal),
            TdsDataType::Int4 => Ok(SqlDataTypes::Int4),
            TdsDataType::Flt4 => Ok(SqlDataTypes::Flt4),
            TdsDataType::Money => Ok(SqlDataTypes::Money),
            TdsDataType::DateTime => Ok(SqlDataTypes::DateTime),
            TdsDataType::Flt8 => Ok(SqlDataTypes::Flt8),
            TdsDataType::Numeric => Ok(SqlDataTypes::Numeric),
            TdsDataType::SsVariant => Ok(SqlDataTypes::SsVariant),
            TdsDataType::NText => Ok(SqlDataTypes::NText),
            TdsDataType::Money4 => Ok(SqlDataTypes::Money4),
            TdsDataType::Int8 => Ok(SqlDataTypes::Int8),
            TdsDataType::BigVarBinary => Ok(SqlDataTypes::BigVarBinary),
            TdsDataType::BigVarChar => Ok(SqlDataTypes::BigVarChar),
            TdsDataType::BigBinary => Ok(SqlDataTypes::BigBinary),
            TdsDataType::BigChar => Ok(SqlDataTypes::BigChar),
            TdsDataType::NVarChar => Ok(SqlDataTypes::NVarChar),
            TdsDataType::NChar => Ok(SqlDataTypes::NChar),
            TdsDataType::Udt => Ok(SqlDataTypes::Udt),
            TdsDataType::Xml => Ok(SqlDataTypes::Xml),
            TdsDataType::Json => Ok(SqlDataTypes::Json),
            TdsDataType::DateTim4 => Ok(SqlDataTypes::SmallDateTime),
            TdsDataType::IntN => Err(()),
            TdsDataType::DateN => Err(()),
            TdsDataType::TimeN => Err(()),
            TdsDataType::DateTime2N => Err(()),
            TdsDataType::DateTimeOffsetN => Err(()),
            TdsDataType::DateTimeN => Err(()),
            TdsDataType::FltN => Err(()),
            TdsDataType::BitN => Err(()),
            TdsDataType::DecimalN => Err(()),
            TdsDataType::NumericN => Err(()),
            TdsDataType::MoneyN => Err(()),
            TdsDataType::None => unreachable!(),
        }
    }

    type Error = ();
}

#[napi(object)]
pub struct RowItem {
    pub metadata: Metadata,
    pub row_val: RowDataType,
}

#[napi(object)]
pub struct NapiSqlDateTime {
    pub days: i32,
    pub time: u32,
}

#[napi(object)]
pub struct NapiSqlTime {
    pub time_nanoseconds: BigInt,
    pub scale: u8,
}

impl From<SqlTime> for NapiSqlTime {
    fn from(sql_time: SqlTime) -> Self {
        NapiSqlTime {
            time_nanoseconds: BigInt::from(sql_time.time_nanoseconds),
            scale: sql_time.scale,
        }
    }
}

#[napi(object)]
pub struct NapiSqlDateTime2 {
    pub days: u32,
    pub time: NapiSqlTime,
}

impl From<SqlDateTime2> for NapiSqlDateTime2 {
    fn from(datetime2: SqlDateTime2) -> Self {
        NapiSqlDateTime2 {
            days: datetime2.days,
            time: NapiSqlTime::from(datetime2.time),
        }
    }
}

#[napi(object)]
pub struct NapiSqlDateTimeOffset {
    pub datetime2: NapiSqlDateTime2,
    pub offset: i16,
}

impl From<SqlDateTimeOffset> for NapiSqlDateTimeOffset {
    fn from(datetime_offset: SqlDateTimeOffset) -> Self {
        NapiSqlDateTimeOffset {
            datetime2: NapiSqlDateTime2::from(datetime_offset.datetime2),
            offset: datetime_offset.offset,
        }
    }
}

#[napi(object)]
pub struct NapiSqlMoney {
    pub lsb_part: i32, // LSB
    pub msb_part: i32, // MSB - Only populated for Money, 0 for SmallMoney
}

impl From<SqlMoney> for NapiSqlMoney {
    fn from(value: SqlMoney) -> Self {
        NapiSqlMoney {
            lsb_part: value.lsb_part,
            msb_part: value.msb_part,
        }
    }
}

#[napi(object)]
pub struct NapiDecimalParts {
    pub is_positive: bool,
    pub scale: u8,
    pub precision: u8,
    pub int_parts: Vec<i32>,
}

impl From<DecimalParts> for NapiDecimalParts {
    fn from(decimal_parts: DecimalParts) -> Self {
        NapiDecimalParts {
            is_positive: decimal_parts.is_positive,
            scale: decimal_parts.scale,
            precision: decimal_parts.precision,
            int_parts: decimal_parts.int_parts,
        }
    }
}

#[napi(object)]
pub struct Metadata {
    pub name: String,
    pub data_type: SqlDataTypes,
    pub encoding: Option<CollationMetadata>,
}

impl Clone for Metadata {
    fn clone(&self) -> Self {
        Metadata {
            name: self.name.clone(),
            data_type: self.data_type,
            encoding: self.encoding.clone(),
        }
    }
}

impl From<ColumnMetadata> for Metadata {
    fn from(column_metadata: ColumnMetadata) -> Self {
        From::from(&column_metadata)
    }
}

impl From<&ColumnMetadata> for Metadata {
    fn from(column_metadata: &ColumnMetadata) -> Self {
        let sql_type = {
            let tried_type = column_metadata.data_type.try_into();
            match tried_type {
                Ok(sql_type) => sql_type,
                Err(_) => match column_metadata.data_type {
                    TdsDataType::IntN => match column_metadata.type_info.length {
                        1 => SqlDataTypes::Int1,
                        2 => SqlDataTypes::Int2,
                        4 => SqlDataTypes::Int4,
                        8 => SqlDataTypes::Int8,
                        len => unreachable!("Unsupported IntN length: {}", len),
                    },
                    TdsDataType::DateN => SqlDataTypes::Date,
                    TdsDataType::TimeN => SqlDataTypes::Time,
                    TdsDataType::DateTimeN => match column_metadata.type_info.length {
                        4 => SqlDataTypes::SmallDateTime,
                        8 => SqlDataTypes::DateTime,
                        _ => unreachable!(
                            "Unsupported DateTimeN length: {}",
                            column_metadata.type_info.length
                        ),
                    },
                    TdsDataType::DateTime2N => SqlDataTypes::DateTime2,
                    TdsDataType::DateTimeOffsetN => SqlDataTypes::DateTimeOffset,
                    TdsDataType::BitN => SqlDataTypes::Bit,
                    TdsDataType::DecimalN => SqlDataTypes::Decimal,
                    TdsDataType::NumericN => SqlDataTypes::Numeric,
                    TdsDataType::MoneyN => match column_metadata.type_info.length {
                        4 => SqlDataTypes::Money4,
                        8 => SqlDataTypes::Money,
                        _ => unreachable!(),
                    },
                    TdsDataType::None => unreachable!(),
                    _ => panic!("Unsupported SQL data type: {:?}", column_metadata.data_type),
                },
            }
        };

        Metadata {
            name: column_metadata.column_name.clone(),
            data_type: sql_type,
            encoding: column_metadata.get_collation().map(Into::into),
        }
    }
}
