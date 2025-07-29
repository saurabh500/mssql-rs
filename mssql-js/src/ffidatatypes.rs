// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::str::FromStr;

use mssql_tds::{
    datatypes::{
        column_values::{
            ColumnValues, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney,
            SqlSmallDateTime, SqlTime,
        },
        decoder::DecimalParts,
        sql_string::{EncodingType, SqlString},
        sqldatatypes::TdsDataType,
        sqltypes::SqlType,
    },
    query::metadata::ColumnMetadata,
    token::tokens::SqlCollation,
};
use napi::{
    Error,
    bindgen_prelude::{Buffer, Null},
};
use uuid::Uuid;

use crate::{
    connection::RowDataType,
    datatypes::datetime::{NapiSqlDateTime, NapiSqlDateTime2, NapiSqlDateTimeOffset, NapiSqlTime},
};

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

#[derive(Debug, PartialEq, Eq)]
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

impl From<NapiSqlMoney> for SqlMoney {
    fn from(napi_sql_money: NapiSqlMoney) -> Self {
        SqlMoney {
            lsb_part: napi_sql_money.lsb_part,
            msb_part: napi_sql_money.msb_part,
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

impl From<NapiDecimalParts> for DecimalParts {
    fn from(napi_decimal_parts: NapiDecimalParts) -> Self {
        DecimalParts {
            is_positive: napi_decimal_parts.is_positive,
            scale: napi_decimal_parts.scale,
            precision: napi_decimal_parts.precision,
            int_parts: napi_decimal_parts.int_parts,
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
                    TdsDataType::FltN => match column_metadata.type_info.length {
                        4 => SqlDataTypes::Flt4,
                        8 => SqlDataTypes::Flt8,
                        _ => unreachable!(),
                    },
                    TdsDataType::Flt4 => SqlDataTypes::Flt4,
                    TdsDataType::Flt8 => SqlDataTypes::Flt8,
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

#[napi(object)]
pub struct Parameter {
    pub name: String,
    pub data_type: SqlDataTypes,
    pub value: RowDataType,
    // Applicable to Varchar, NVarChar, VarBinary, NVarBinary, and similar types
    pub length: Option<u32>,
}

/// Values are converted from Parameter to SqlType to be sent over the wire.
impl TryFrom<Parameter> for SqlType {
    fn try_from(param: Parameter) -> Result<SqlType, Error> {
        match param.value {
            RowDataType::A(f64val) => match param.data_type {
                SqlDataTypes::Flt4 => {
                    if *f64val < f32::MIN as f64 || *f64val > f32::MAX as f64 {
                        return Err(Error::from_reason(format!(
                            "Value {:?} out of range for F32",
                            *f64val
                        )));
                    }
                    Ok(SqlType::Real(Some(*f64val as f32)))
                }
                SqlDataTypes::Flt8 => Ok(SqlType::Float(Some(*f64val))),
                _ => Err(Error::from_reason(format!(
                    "Invalid data_type for RowDataType::A: {:?}. Only Flt4 and Flt8 are allowed.",
                    param.data_type
                ))),
            },
            RowDataType::B(v) => {
                if !matches!(
                    param.data_type,
                    SqlDataTypes::Int1
                        | SqlDataTypes::Int2
                        | SqlDataTypes::Int4
                        | SqlDataTypes::Int8
                        | SqlDataTypes::Date
                ) {
                    return Err(Error::from_reason(format!(
                        "Invalid data_type for number: {:?}. Only smallint, tinyint, int and bigint are allowed. Value {:?}",
                        param.data_type, v
                    )));
                }
                match param.data_type {
                    SqlDataTypes::Int1 => {
                        if v < u8::MIN as i32 || v > u8::MAX as i32 {
                            return Err(Error::from_reason(format!(
                                "Value {v} out of range for Int1"
                            )));
                        }
                        Ok(SqlType::TinyInt(Some(v as u8)))
                    }
                    SqlDataTypes::Int2 => {
                        if v < i16::MIN as i32 || v > i16::MAX as i32 {
                            return Err(Error::from_reason(format!(
                                "Value {v} out of range for Int2"
                            )));
                        }
                        Ok(SqlType::SmallInt(Some(v as i16)))
                    }
                    SqlDataTypes::Int4 => Ok(SqlType::Int(Some(v))),
                    SqlDataTypes::Int8 => Ok(SqlType::BigInt(Some(v as i64))),
                    SqlDataTypes::Date => {
                        // Conversion to u32 is safe
                        let sql_date = SqlDate::create(v as u32).map_err(|e| {
                            Error::from_reason(format!(
                                "Failed to create SqlDate from u32 value: {e}"
                            ))
                        })?;
                        Ok(SqlType::Date(Some(sql_date)))
                    }
                    _ => Err(Error::from_reason(format!(
                        "Invalid data_type for RowDataType::A: {:?}. Only Int1, Int2, Int4, Int8 are allowed.",
                        param.data_type
                    ))),
                }
            }

            RowDataType::C(bigint) => {
                let (i64val, is_lossless) = bigint.get_i64();
                if !is_lossless {
                    return Err(Error::from_reason(format!(
                        "BigInt value {i64val} is not lossless. A value out of range of i64 was provided."
                    )));
                }
                if !matches!(param.data_type, SqlDataTypes::Int8) {
                    return Err(Error::from_reason(format!(
                        "Invalid data_type for RowDataType::B: {:?}. Only Int8 is allowed.",
                        param.data_type
                    )));
                }
                Ok(SqlType::BigInt(Some(i64val)))
            }
            RowDataType::D(bit_val) => {
                if !matches!(param.data_type, SqlDataTypes::Bit) {
                    return Err(Error::from_reason(format!(
                        "Invalid data_type for RowDataType::C: {:?}. Only Bit is allowed.",
                        param.data_type
                    )));
                }
                Ok(SqlType::Bit(Some(bit_val)))
            }
            RowDataType::E(buffer) => match param.data_type {
                SqlDataTypes::VarChar => {
                    let bytes: Vec<u8> = buffer.to_vec();
                    Ok(SqlType::VarcharMax(Some(SqlString::new(
                        bytes,
                        EncodingType::DelayedSet,
                    ))))
                }
                SqlDataTypes::NVarChar => {
                    let bytes: Vec<u8> = buffer.to_vec();
                    Ok(SqlType::NVarcharMax(Some(SqlString::new(
                        bytes,
                        EncodingType::DelayedSet,
                    ))))
                }
                _ => todo!("Buffer subtype not implemeted"),
            },
            RowDataType::F(_) => get_null_sql_type(&param),
            RowDataType::G(napi_sql_date_time) => match param.data_type {
                SqlDataTypes::DateTime => {
                    let sql_datetime: SqlDateTime = napi_sql_date_time.into();
                    Ok(SqlType::DateTime(Some(sql_datetime)))
                }
                SqlDataTypes::SmallDateTime => {
                    let sql_small_datetime: SqlSmallDateTime = napi_sql_date_time.try_into()?;
                    Ok(SqlType::SmallDateTime(Some(sql_small_datetime)))
                }
                _ => Err(Error::from_reason(format!(
                    "Invalid data_type for RowDataType::F: {:?}. Only DateTime is allowed.",
                    param.data_type
                ))),
            },
            RowDataType::H(v) => {
                // Check if the data_type is date
                if matches!(param.data_type, SqlDataTypes::Date) {
                    // Convert the u32 value to SqlDate
                    let sql_date = SqlDate::create(v).map_err(|e| {
                        Error::from_reason(format!("Failed to create SqlDate from u32 value: {e}"))
                    })?;
                    return Ok(SqlType::Date(Some(sql_date)));
                }
                todo!(
                    "u32 {} value conversion for {:?} not supported.",
                    v,
                    param.data_type
                );
            }
            RowDataType::I(napi_sql_time) => {
                if !matches!(param.data_type, SqlDataTypes::Time) {
                    return Err(Error::from_reason(format!(
                        "Invalid data_type for RowDataType::H: {:?}. Only Time is allowed.",
                        param.data_type
                    )));
                }
                let sql_time = SqlTime::try_from(napi_sql_time)?;
                Ok(SqlType::Time(Some(sql_time)))
            }
            RowDataType::J(napi_sql_datetime2) => match param.data_type {
                SqlDataTypes::DateTime2 => {
                    let sql_datetime2: SqlDateTime2 = napi_sql_datetime2.try_into()?;
                    Ok(SqlType::DateTime2(Some(sql_datetime2)))
                }
                _ => Err(Error::from_reason(format!(
                    "Invalid data_type for RowDataType::J: {:?}. Only DateTime2 is allowed.",
                    param.data_type
                ))),
            },
            RowDataType::K(napi_date_time_offset) => match param.data_type {
                SqlDataTypes::DateTimeOffset => {
                    let sql_datetime_offset: SqlDateTimeOffset =
                        napi_date_time_offset.try_into()?;
                    Ok(SqlType::DateTimeOffset(Some(sql_datetime_offset)))
                }
                _ => Err(Error::from_reason(format!(
                    "Invalid data_type for RowDataType::K: {:?}. Only DateTimeOffset is allowed.",
                    param.data_type
                ))),
            },
            RowDataType::L(napi_sql_money) => {
                if !matches!(param.data_type, SqlDataTypes::Money | SqlDataTypes::Money4) {
                    return Err(Error::from_reason(format!(
                        "Invalid data_type for RowDataType::L: {:?}. Only Money and Money4 are allowed.",
                        param.data_type
                    )));
                }
                Ok(SqlType::Money(Some(napi_sql_money.into())))
            }
            RowDataType::M(decimal_parts) => {
                if !matches!(
                    param.data_type,
                    SqlDataTypes::Decimal | SqlDataTypes::Numeric
                ) {
                    return Err(Error::from_reason(format!(
                        "Invalid data_type for RowDataType::L: {:?}. Only Decimal and Numeric are allowed.",
                        param.data_type
                    )));
                }
                Ok(SqlType::Decimal(Some(decimal_parts.into())))
            }
            RowDataType::N(uuid) => match param.data_type {
                SqlDataTypes::Guid => {
                    let uuid = Uuid::from_str(&uuid).map_err(|uuid| {
                        Error::from_reason(format!("Failed to convert String to Uuid: {uuid}"))
                    })?;
                    Ok(SqlType::Uuid(Some(uuid)))
                }
                _ => Err(Error::from_reason(format!(
                    "Invalid data_type for RowDataType::N: {:?}. Only Guid is allowed.",
                    param.data_type
                ))),
            },
        }
    }

    type Error = napi::Error;
}

fn get_null_sql_type(param: &Parameter) -> Result<SqlType, Error> {
    match param.data_type {
        SqlDataTypes::Int1 | SqlDataTypes::Int2 | SqlDataTypes::Int4 | SqlDataTypes::Int8 => {
            Ok(SqlType::Int(None))
        }
        SqlDataTypes::Bit => Ok(SqlType::Bit(None)),
        SqlDataTypes::Decimal | SqlDataTypes::Numeric => Ok(SqlType::Decimal(None)),
        SqlDataTypes::Money | SqlDataTypes::Money4 => Ok(SqlType::Money(None)),
        SqlDataTypes::Void => Err(Error::from_reason("Void type not implemented")),
        SqlDataTypes::Image => Err(Error::from_reason("Image conversion not implemented")),
        SqlDataTypes::Text => Ok(SqlType::Text(None)),
        SqlDataTypes::Guid => Ok(SqlType::Uuid(None)),
        SqlDataTypes::VarBinary => Ok(SqlType::VarBinary(None, 0)),
        SqlDataTypes::VarChar => Ok(SqlType::Varchar(None, 0)),
        SqlDataTypes::Date => Ok(SqlType::Date(None)),
        SqlDataTypes::Time => Ok(SqlType::Time(None)),
        SqlDataTypes::DateTime2 => Ok(SqlType::DateTime2(None)),
        SqlDataTypes::DateTimeOffset => Ok(SqlType::DateTimeOffset(None)),
        SqlDataTypes::Binary => Ok(SqlType::Binary(None, 0)),
        SqlDataTypes::Char => Ok(SqlType::Char(None, 0)),
        SqlDataTypes::SmallDateTime => Ok(SqlType::SmallDateTime(None)),
        SqlDataTypes::Flt4 => Ok(SqlType::Real(None)),
        SqlDataTypes::DateTime => Ok(SqlType::DateTime(None)),
        SqlDataTypes::Flt8 => Ok(SqlType::Float(None)),
        SqlDataTypes::SsVariant => Err(Error::from_reason("SSVariant conversion not implemented")),
        SqlDataTypes::NText => Ok(SqlType::NText(None)),
        SqlDataTypes::FltN => Ok(SqlType::Float(None)),
        SqlDataTypes::BigVarBinary => Ok(SqlType::VarBinary(None, 0)),
        SqlDataTypes::BigVarChar => Ok(SqlType::Varchar(None, 0)),
        SqlDataTypes::BigBinary => Ok(SqlType::Binary(None, 0)),
        SqlDataTypes::BigChar => Ok(SqlType::Char(None, 0)),
        SqlDataTypes::NVarChar => Ok(SqlType::NVarchar(None, 0)),
        SqlDataTypes::NChar => Ok(SqlType::NChar(None, 0)),
        SqlDataTypes::Udt => Err(Error::from_reason("Udt conversion not implemented")),
        SqlDataTypes::Xml => Ok(SqlType::Xml(None)),
        SqlDataTypes::Json => Ok(SqlType::Json(None)),
    }
}

pub(crate) fn transform_row(row: Vec<ColumnValues>) -> Vec<RowDataType> {
    let mut ret_val: Vec<RowDataType> = Vec::with_capacity(row.len());
    for col in row {
        match col {
            ColumnValues::Int(v) => ret_val.push(RowDataType::B(v)),
            ColumnValues::Uuid(uuid) => ret_val.push(RowDataType::N(uuid.to_string())),
            ColumnValues::Bit(v) => ret_val.push(RowDataType::D(v)),
            ColumnValues::BigInt(v) => ret_val.push(RowDataType::C(v.into())),
            ColumnValues::TinyInt(v) => ret_val.push(RowDataType::H(v.into())),
            ColumnValues::SmallInt(v) => ret_val.push(RowDataType::B(v.into())),
            ColumnValues::Real(v) => ret_val.push(RowDataType::A(v.into())),
            ColumnValues::Float(v) => ret_val.push(RowDataType::A(v.into())),
            ColumnValues::Decimal(decimal_parts) => {
                ret_val.push(RowDataType::M(decimal_parts.into()));
            }
            ColumnValues::Numeric(decimal_parts) => {
                ret_val.push(RowDataType::M(decimal_parts.into()));
            }
            ColumnValues::String(sql_string) => {
                ret_val.push(RowDataType::E(Buffer::from(sql_string.bytes)))
            }
            ColumnValues::DateTime(sql_date_time) => {
                ret_val.push(RowDataType::G(NapiSqlDateTime {
                    days: sql_date_time.days,
                    time: sql_date_time.time,
                }));
            }
            ColumnValues::Date(sql_date) => {
                ret_val.push(RowDataType::H(sql_date.get_days()));
            }
            ColumnValues::Time(_time) => ret_val.push(RowDataType::I(NapiSqlTime::from(_time))),
            ColumnValues::DateTime2(_date_time2) => {
                ret_val.push(RowDataType::J(NapiSqlDateTime2::from(_date_time2)))
            }
            ColumnValues::DateTimeOffset(date_time_offset) => ret_val.push(RowDataType::K(
                NapiSqlDateTimeOffset::from(date_time_offset),
            )),
            ColumnValues::SmallDateTime(sql_small_date_time) => {
                ret_val.push(RowDataType::G(NapiSqlDateTime {
                    days: sql_small_date_time.days.into(),
                    time: sql_small_date_time.time.into(),
                }));
            }
            ColumnValues::SmallMoney(sql_small_money) => {
                ret_val.push(RowDataType::B(sql_small_money.int_val))
            }
            ColumnValues::Money(sql_money) => ret_val.push(RowDataType::L(sql_money.into())),
            ColumnValues::Bytes(items) => ret_val.push(RowDataType::E(Buffer::from(items))),
            ColumnValues::Xml(sql_xml) => ret_val.push(RowDataType::E(Buffer::from(sql_xml.bytes))),
            ColumnValues::Null => ret_val.push(RowDataType::F(Null)),
            ColumnValues::Json(sql_json) => {
                ret_val.push(RowDataType::E(Buffer::from(sql_json.bytes)))
            }
        }
    }
    ret_val
}
