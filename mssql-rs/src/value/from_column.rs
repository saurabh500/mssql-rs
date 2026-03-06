// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::str::FromStr;

use bigdecimal::BigDecimal;
use mssql_tds::datatypes::column_values::ColumnValues;

use super::Value;
use crate::DateTime;

impl From<ColumnValues> for Value {
    fn from(cv: ColumnValues) -> Self {
        match cv {
            ColumnValues::Null => Value::Null,

            // Bool
            ColumnValues::Bit(b) => Value::Bool(b),

            // Int — widen all integer types to i64
            ColumnValues::TinyInt(v) => Value::Int(v as i64),
            ColumnValues::SmallInt(v) => Value::Int(v as i64),
            ColumnValues::Int(v) => Value::Int(v as i64),
            ColumnValues::BigInt(v) => Value::Int(v),

            // Float — widen f32 to f64
            ColumnValues::Real(v) => Value::Float(v as f64),
            ColumnValues::Float(v) => Value::Float(v),

            // Decimal / Numeric — parse via Display impl
            ColumnValues::Decimal(parts) | ColumnValues::Numeric(parts) => {
                let s = parts.to_string();
                match BigDecimal::from_str(&s) {
                    Ok(d) => Value::Decimal(d),
                    Err(_) => Value::String(s),
                }
            }

            // Money → assemble into BigDecimal
            ColumnValues::Money(m) => {
                let lsb_in_i64 = (m.lsb_part as i64) & 0x00000000FFFFFFFF;
                let raw = lsb_in_i64 | ((m.msb_part as i64) << 32);
                let d = BigDecimal::new(raw.into(), 4);
                Value::Decimal(d)
            }
            ColumnValues::SmallMoney(m) => {
                let d = BigDecimal::new(m.int_val.into(), 4);
                Value::Decimal(d)
            }

            // String — all text types coalesce to String
            ColumnValues::String(s) => Value::String(s.to_utf8_string()),

            // Binary
            ColumnValues::Bytes(b) => Value::Binary(b),

            // DateTime — all temporal types coalesce via From impls
            ColumnValues::Date(d) => Value::DateTime(DateTime::from(d)),
            ColumnValues::Time(t) => Value::DateTime(DateTime::from(t)),
            ColumnValues::DateTime(dt) => Value::DateTime(DateTime::from(dt)),
            ColumnValues::DateTime2(dt2) => Value::DateTime(DateTime::from(dt2)),
            ColumnValues::DateTimeOffset(dto) => Value::DateTime(DateTime::from(dto)),
            ColumnValues::SmallDateTime(sdt) => Value::DateTime(DateTime::from(sdt)),

            // UUID
            ColumnValues::Uuid(u) => Value::Uuid(u),

            // XML
            ColumnValues::Xml(x) => Value::Xml(x.as_string()),

            // JSON
            ColumnValues::Json(j) => Value::Json(j.as_string()),

            // Vector
            ColumnValues::Vector(v) => {
                Value::Vector(v.as_f32().map(|s| s.to_vec()).unwrap_or_default())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_coalesces() {
        let v: Value = ColumnValues::Null.into();
        assert!(v.is_null());
    }

    #[test]
    fn int_widens() {
        let v: Value = ColumnValues::TinyInt(42).into();
        assert_eq!(v, Value::Int(42));
    }

    #[test]
    fn float_widens() {
        let v: Value = ColumnValues::Real(1.5).into();
        assert_eq!(v, Value::Float(1.5));
    }

    #[test]
    fn bit_to_bool() {
        let v: Value = ColumnValues::Bit(true).into();
        assert_eq!(v, Value::Bool(true));
    }
}
