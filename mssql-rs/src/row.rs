// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::from_value::FromValue;
use crate::metadata::ColumnMetadata;
use crate::value::Value;

/// A single row from a result set.
///
/// Supports random-access by index or column name, and sequential
/// column streaming via [`next_column`](Row::next_column).
pub struct Row {
    pub(crate) columns: Vec<Value>,
    pub(crate) metadata: Arc<Vec<ColumnMetadata>>,
    pub(crate) stream_pos: Option<usize>,
}

impl Row {
    /// Get a typed value by zero-based column index.
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T> {
        let val = self.value(index)?.clone();
        T::from_value(val)
    }

    /// Get a typed value by column name (case-insensitive).
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T> {
        let index = self
            .metadata
            .iter()
            .position(|m| m.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::TypeConversion(format!("column '{}' not found", name)))?;
        self.get(index)
    }

    /// Get the raw [`Value`] by zero-based index without conversion.
    pub fn value(&self, index: usize) -> Result<&Value> {
        self.columns.get(index).ok_or_else(|| {
            Error::TypeConversion(format!(
                "column index {} out of range (len {})",
                index,
                self.columns.len()
            ))
        })
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the row has zero columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Column metadata for this row's result set.
    pub fn metadata(&self) -> &[ColumnMetadata] {
        &self.metadata
    }

    /// Convert the row into a `Vec<Value>`, consuming it.
    pub fn into_values(self) -> Vec<Value> {
        self.columns
    }

    /// Yield the next column value in column order.
    ///
    /// Returns `None` after the last column. Mutually exclusive with
    /// random-access methods within the same `Row` instance.
    pub fn next_column(&mut self) -> Result<Option<Value>> {
        let pos = self.stream_pos.get_or_insert(0);
        if *pos >= self.columns.len() {
            return Ok(None);
        }
        let val = self.columns[*pos].clone();
        *pos += 1;
        Ok(Some(val))
    }
}
