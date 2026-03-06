// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::DateTime;
use crate::error::{Error, Result};
use crate::value::Value;
use bigdecimal::BigDecimal;
use uuid::Uuid;

/// Trait for converting a [`Value`] into a concrete Rust type.
///
/// Built-in implementations cover primitive types, `String`, `Vec<u8>`,
/// `BigDecimal`, `Uuid`, `DateTime`, and `Option<T>` for nullable columns.
///
/// # Example
///
/// ```ignore
/// let id: i64 = row.get(0)?;
/// let name: Option<String> = row.get(1)?;
/// ```
pub trait FromValue: Sized {
    fn from_value(value: Value) -> Result<Self>;
}

impl FromValue for bool {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Bool(b) => Ok(b),
            other => Err(type_err("bool", &other)),
        }
    }
}

macro_rules! impl_int_from_value {
    ($($ty:ty),+) => { $(
        impl FromValue for $ty {
            fn from_value(value: Value) -> Result<Self> {
                match value {
                    Value::Int(v) => <$ty>::try_from(v)
                        .map_err(|_| Error::TypeConversion(
                            format!("i64 value {} out of range for {}", v, stringify!($ty))
                        )),
                    other => Err(type_err(stringify!($ty), &other)),
                }
            }
        }
    )+ };
}

impl_int_from_value!(i8, i16, i32, i64, u8, u16, u32);

impl FromValue for f32 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Float(v) => Ok(v as f32),
            other => Err(type_err("f32", &other)),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Float(v) => Ok(v),
            other => Err(type_err("f64", &other)),
        }
    }
}

impl FromValue for String {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::String(s) => Ok(s),
            Value::Xml(s) => Ok(s),
            Value::Json(s) => Ok(s),
            other => Err(type_err("String", &other)),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Binary(b) => Ok(b),
            other => Err(type_err("Vec<u8>", &other)),
        }
    }
}

impl FromValue for BigDecimal {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Decimal(d) => Ok(d),
            other => Err(type_err("BigDecimal", &other)),
        }
    }
}

impl FromValue for Uuid {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Uuid(u) => Ok(u),
            other => Err(type_err("Uuid", &other)),
        }
    }
}

impl FromValue for DateTime {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::DateTime(dt) => Ok(dt),
            other => Err(type_err("DateTime", &other)),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            other => T::from_value(other).map(Some),
        }
    }
}

fn type_err(target: &str, value: &Value) -> Error {
    let variant = match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int(_) => "Int",
        Value::Float(_) => "Float",
        Value::Decimal(_) => "Decimal",
        Value::String(_) => "String",
        Value::Binary(_) => "Binary",
        Value::DateTime(_) => "DateTime",
        Value::Uuid(_) => "Uuid",
        Value::Xml(_) => "Xml",
        Value::Json(_) => "Json",
        Value::Vector(_) => "Vector",
    };
    Error::TypeConversion(format!("cannot convert Value::{variant} to {target}"))
}
