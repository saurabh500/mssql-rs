// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use mssql_tds::{
    connection::tds_client::{ResultSet, ResultSetClient, TdsClient},
    message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
};
use napi::bindgen_prelude::{BigInt, Buffer, Either15, Null};
use tokio::sync::Mutex;

use crate::{
    datatypes::datetime::{NapiSqlDateTime, NapiSqlDateTime2, NapiSqlDateTimeOffset, NapiSqlTime},
    ffidatatypes::{
        CollationMetadata, Metadata, NapiDecimalParts, NapiSqlMoney, Parameter, RowItem,
        transform_row,
    },
};

pub(crate) type RowDataType = Either15<
    i32,                   // A
    BigInt,                // B
    bool,                  // C
    Buffer,                // D
    Null,                  // E
    NapiSqlDateTime,       // F
    u32,                   // G
    NapiSqlTime,           // H
    NapiSqlDateTime,       // I
    NapiSqlDateTime2,      // J
    NapiSqlDateTimeOffset, // K
    NapiSqlMoney,          // L
    NapiDecimalParts,      // M
    f64,                   // N
    String,                // O
>;

#[napi]
pub struct Connection {
    pub(crate) tds_client: Arc<Mutex<TdsClient>>,
    pub(crate) collation: Option<CollationMetadata>,
}

#[napi]
impl Connection {
    #[napi]
    pub fn get_collation(&self) -> Option<CollationMetadata> {
        self.collation.clone()
    }

    #[napi]
    pub async fn execute(&self, query: String) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.execute(query, None, None).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to execute query: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn execute_with_params(
        &self,
        query: String,
        params: Vec<Parameter>,
    ) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;

        let rpc_params: Result<Vec<RpcParameter>, napi::Error> = params
            .into_iter()
            .map(|p| {
                let param_name = p.name.clone();
                let param_value = match p.try_into() {
                    Ok(value) => value,
                    Err(e) => {
                        return Err(napi::Error::from_reason(format!(
                            "Parameter conversion failed: {e}"
                        )));
                    }
                };
                Ok(RpcParameter::new(
                    Some(param_name),
                    StatusFlags::NONE,
                    param_value,
                ))
            })
            .collect();

        let rpc_params = rpc_params?;
        let result = client
            .execute_sp_executesql(query, rpc_params, None, None)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to execute query with parameters: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn next_row2(&self) -> napi::Result<Option<Vec<RowItem>>> {
        let mut client = self.tds_client.lock().await;

        let result_set = client.get_current_resultset();
        // Check if the client has a result set.
        if result_set.is_none() {
            return Ok(None);
        }
        let result_set = result_set.unwrap();
        let next_row = result_set
            .next_row()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Failed to get next row: {e}")))?;
        let md = result_set.get_metadata();
        match next_row {
            Some(row) => {
                let transformed_row = transform_row(row);
                let col_count = transformed_row.len();
                let mut row_items: Vec<RowItem> = Vec::with_capacity(col_count);
                for (i, item) in transformed_row.into_iter().enumerate() {
                    if let Some(meta) = md.get(i) {
                        let metadata: Metadata = meta.into();
                        let row_item = RowItem {
                            metadata,
                            row_val: item,
                        };
                        row_items.push(row_item);
                    } else {
                        return Err(napi::Error::from_reason(format!(
                            "Metadata length mismatch: expected at least {col_count}, found {}",
                            md.len()
                        )));
                    }
                }
                Ok(Some(row_items))
            }
            None => Ok(None),
        }
    }

    // We dont use RowDataType directly as the return type. The binding generation cannot generate aliases for Rust Types,
    // or replace them. Hence to have compilable typings, its necessary that Either* is directly used in the return type.
    #[napi]
    pub async fn next_row_in_resultset(
        &self,
    ) -> napi::Result<
        Option<
            Vec<
                Either15<
                    i32,
                    BigInt,
                    bool,
                    Buffer,
                    Null,
                    NapiSqlDateTime,
                    u32,
                    NapiSqlTime,
                    NapiSqlDateTime,
                    NapiSqlDateTime2,
                    NapiSqlDateTimeOffset,
                    NapiSqlMoney,
                    NapiDecimalParts,
                    f64,
                    String,
                >,
            >,
        >,
    > {
        let mut client = self.tds_client.lock().await;

        let result_set = client.get_current_resultset();
        // Check if the client has a result set.
        if result_set.is_none() {
            return Ok(None);
        }
        let result_set = result_set.unwrap();
        let next_row = result_set
            .next_row()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Failed to get next row: {e}")))?;

        match next_row {
            Some(row) => {
                let transformed_row = transform_row(row);
                Ok(Some(transformed_row))
            }
            None => Ok(None),
        }
    }

    #[napi]
    pub async fn get_metadata(&self) -> napi::Result<Option<Vec<Metadata>>> {
        let mut client = self.tds_client.lock().await;
        let result_set = client.get_current_resultset();
        // Check if the client has a result set.
        if result_set.is_none() {
            return Ok(None);
        }
        let result_set = result_set.unwrap();
        let metadata = result_set.get_metadata();

        let metadata_vec: Vec<Metadata> = metadata.iter().map(|m| m.into()).collect();
        Ok(Some(metadata_vec))
    }

    #[napi]
    pub async fn next_result_set(&self) -> napi::Result<bool> {
        let mut client = self.tds_client.lock().await;
        let result = client.move_to_next().await;
        match result {
            Ok(has_next) => Ok(has_next),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to get next result set: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn close_query(&self) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.close_query().await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to close query: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.close_connection().await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to close connection: {e}"
            ))),
        }
    }
}
