// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod from_column;

use bigdecimal::BigDecimal;
use uuid::Uuid;

use crate::DateTime;

/// A SQL Server column value coalesced into one of 13 semantic variants.
///
/// Wire-level types (25 `ColumnValues` variants) are mapped into these
/// categories following the coalescing rules in research decision R2.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(BigDecimal),
    String(String),
    Binary(Vec<u8>),
    DateTime(DateTime),
    Uuid(Uuid),
    Xml(String),
    Json(String),
    Vector(Vec<f32>),
}

impl Value {
    /// Returns `true` if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}
