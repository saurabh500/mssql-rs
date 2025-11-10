// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::{fmt::Debug, sync::Arc};

use mssql_tds::{
    connection::tds_client::{ResultSet, ResultSetClient, TdsClient},
    message::{
        parameters::rpc_parameters::{RpcParameter, StatusFlags},
        transaction_management::TransactionIsolationLevel,
    },
};
use napi::bindgen_prelude::{BigInt, Buffer, Either14, Null};
use tokio::sync::Mutex;
use tracing::instrument;

use crate::{
    datatypes::datetime::{
        NapiF64, NapiSqlDateTime, NapiSqlDateTime2, NapiSqlDateTimeOffset, NapiSqlTime,
    },
    ffidatatypes::{
        CollationMetadata, Metadata, NapiDecimalParts, NapiSqlMoney, OutputParams, Parameter,
        ParameterDirection, transform_row,
    },
};

/// The ordering is super important here as it defines the order in which napi will try to convert from one number
/// to another.
pub(crate) type RowDataType = Either14<
    NapiF64,               // A
    i32,                   // B
    BigInt,                // C
    bool,                  // D
    Buffer,                // E
    Null,                  // F
    NapiSqlDateTime,       // G
    u32,                   // H
    NapiSqlTime,           // I
    NapiSqlDateTime2,      // J
    NapiSqlDateTimeOffset, // K
    NapiSqlMoney,          // L
    NapiDecimalParts,      // M
    String,                // N
>;

#[napi]
pub struct Connection {
    pub(crate) tds_client: Arc<Mutex<TdsClient>>,
    pub(crate) collation: Option<CollationMetadata>,
}

impl Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection").finish()
    }
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

    /// Executes a stored procedure with named parameters.
    /// The execution executes and positions the client to the first result set.
    #[napi]
    pub async fn execute_proc(
        &self,
        stored_proc_name: String,
        named_params: Vec<Parameter>,
    ) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;

        let named_params: Result<Vec<RpcParameter>, napi::Error> = named_params
            .into_iter()
            .map(|p| {
                let options = match &p.direction {
                    ParameterDirection::Input => StatusFlags::NONE,
                    ParameterDirection::Output => StatusFlags::BY_REF_VALUE,
                };
                let param_name = p.name.clone();
                let param_value = match p.try_into() {
                    Ok(value) => value,
                    Err(e) => {
                        return Err(napi::Error::from_reason(format!(
                            "Parameter conversion failed: {e}"
                        )));
                    }
                };
                Ok(RpcParameter::new(Some(param_name), options, param_value))
            })
            .collect();

        let named_params = named_params?;

        let result = client
            .execute_stored_procedure(stored_proc_name, None, Some(named_params), None, None)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to execute stored proc: {e}"
            ))),
        }
    }

    // We dont use RowDataType directly as the return type. The binding generation cannot generate aliases for Rust Types,
    // or replace them. Hence to have compilable typings, its necessary that Either* is directly used in the return type.
    #[napi]
    #[instrument]
    #[allow(clippy::type_complexity)]
    pub async fn next_row_in_resultset(
        &self,
    ) -> napi::Result<
        Option<
            Vec<
                Either14<
                    NapiF64,               // A
                    i32,                   // B
                    BigInt,                // C
                    bool,                  // D
                    Buffer,                // E
                    Null,                  // F
                    NapiSqlDateTime,       // G
                    u32,                   // H
                    NapiSqlTime,           // I
                    NapiSqlDateTime2,      // J
                    NapiSqlDateTimeOffset, // K
                    NapiSqlMoney,          // L
                    NapiDecimalParts,      // M
                    String,                // N
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
    #[instrument]
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
    #[instrument]
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
    #[instrument]
    pub async fn get_return_values(&self) -> napi::Result<Option<Vec<OutputParams>>> {
        let client = self.tds_client.lock().await;
        let return_values = client.get_return_values();
        let output_params: Vec<OutputParams> =
            return_values.into_iter().map(OutputParams::from).collect();
        Ok(Some(output_params))
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

    #[napi]
    pub async fn begin_transaction(
        &self,
        isolation_level: NapiIsolationLevel,
        savepoint_name: Option<String>,
    ) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client
            .begin_transaction(isolation_level.into(), savepoint_name)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to begin transaction: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn commit_transaction(&self) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.commit_transaction(None, None).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to commit transaction: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn rollback_transaction(&self, name: Option<String>) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.rollback_transaction(name, None).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to rollback transaction: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn save_transaction(&self, savepoint_name: String) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.save_transaction(savepoint_name).await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to save transaction: {e}"
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
#[napi]
pub enum NapiIsolationLevel {
    NoChange = 0x00,
    ReadUncommitted = 0x01,
    ReadCommitted = 0x02,
    RepeatableRead = 0x03,
    Serializable = 0x04,
    Snapshot = 0x05,
}

impl From<NapiIsolationLevel> for TransactionIsolationLevel {
    fn from(level: NapiIsolationLevel) -> Self {
        match level {
            NapiIsolationLevel::NoChange => TransactionIsolationLevel::NoChange,
            NapiIsolationLevel::ReadUncommitted => TransactionIsolationLevel::ReadUncommitted,
            NapiIsolationLevel::ReadCommitted => TransactionIsolationLevel::ReadCommitted,
            NapiIsolationLevel::RepeatableRead => TransactionIsolationLevel::RepeatableRead,
            NapiIsolationLevel::Serializable => TransactionIsolationLevel::Serializable,
            NapiIsolationLevel::Snapshot => TransactionIsolationLevel::Snapshot,
        }
    }
}
