// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use mssql_tds::connection::tds_client::ResultSet as TdsResultSet;
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};

use crate::client::Client;
use crate::error::{Error, Result};
use crate::metadata::ColumnMetadata;
use crate::result_set::ResultSet;
use crate::value::Value;

/// Convert a `Value` into a TDS `SqlType` for parameter binding.
pub(crate) fn value_to_sql_type(value: &Value) -> Result<SqlType> {
    match value {
        Value::Null => Ok(SqlType::NVarcharMax(None)),
        Value::Bool(v) => Ok(SqlType::Bit(Some(*v))),
        Value::Int(v) => Ok(SqlType::BigInt(Some(*v))),
        Value::Float(v) => Ok(SqlType::Float(Some(*v))),
        Value::Decimal(d) => {
            let s = d.to_string();
            let parts = DecimalParts::from_string(&s, 38, 18)
                .map_err(|e| Error::TypeConversion(format!("decimal conversion: {e}")))?;
            Ok(SqlType::Decimal(Some(parts)))
        }
        Value::String(s) => Ok(SqlType::NVarcharMax(Some(SqlString::from_utf8_string(
            s.clone(),
        )))),
        Value::Binary(b) => Ok(SqlType::VarBinaryMax(Some(b.clone()))),
        Value::DateTime(_) => Err(Error::TypeConversion(
            "DateTime parameters not yet supported — use string literals".into(),
        )),
        Value::Uuid(u) => Ok(SqlType::Uuid(Some(*u))),
        Value::Xml(s) => Ok(SqlType::NVarcharMax(Some(SqlString::from_utf8_string(
            s.clone(),
        )))),
        Value::Json(s) => Ok(SqlType::NVarcharMax(Some(SqlString::from_utf8_string(
            s.clone(),
        )))),
        Value::Vector(_) => Err(Error::TypeConversion(
            "Vector parameters not yet supported".into(),
        )),
    }
}

/// Build a `Vec<RpcParameter>` from named parameter pairs.
pub(crate) fn build_rpc_params(params: &[(&str, Value)]) -> Result<Vec<RpcParameter>> {
    params
        .iter()
        .map(|(name, value)| {
            let sql_value = value_to_sql_type(value)?;
            Ok(RpcParameter::new(
                Some(name.to_string()),
                StatusFlags::NONE,
                sql_value,
            ))
        })
        .collect()
}

impl Client {
    /// Execute a parameterized query via `sp_executesql`.
    ///
    /// Parameters are named (e.g. `@p1`, `@name`) and passed as a slice of
    /// `(&str, Value)` pairs. This prevents SQL injection by sending values
    /// as bound parameters rather than inlined text.
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<ResultSet<'_>> {
        self.drain_pending().await?;

        let rpc_params = build_rpc_params(params)?;
        self.inner
            .execute_sp_executesql(
                sql.to_string(),
                rpc_params,
                self.command_timeout,
                Some(&self.cancel_handle),
            )
            .await?;

        let tds_meta = self.inner.get_metadata();
        let metadata: Vec<ColumnMetadata> = tds_meta.iter().map(ColumnMetadata::from).collect();

        Ok(ResultSet::new(self, Arc::new(metadata)))
    }

    /// Execute a parameterized query and collect all rows.
    pub async fn query_collect_with_params(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<Vec<Vec<Value>>> {
        let rs = self.query_with_params(sql, params).await?;
        rs.collect_rows().await
    }
}
