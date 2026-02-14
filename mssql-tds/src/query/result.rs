// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::datatypes::column_values::ColumnValues;
use crate::query::metadata::ColumnMetadata;
use crate::token::tokenitems::ReturnValueStatus;
use crate::token::tokens::ReturnValueToken;

/// Represents a return value from a stored procedure or prepared statement.
/// This includes output parameters and return status values.
#[derive(Debug, Clone)]
pub struct ReturnValue {
    pub param_ordinal: u16,
    pub param_name: String,
    pub value: ColumnValues,
    pub column_metadata: Box<ColumnMetadata>,
    pub status: ReturnValueStatus,
}

impl From<ReturnValueToken> for ReturnValue {
    fn from(token: ReturnValueToken) -> Self {
        ReturnValue {
            param_ordinal: token.param_ordinal,
            param_name: token.param_name,
            value: token.value,
            column_metadata: token.column_metadata,
            status: token.status,
        }
    }
}
