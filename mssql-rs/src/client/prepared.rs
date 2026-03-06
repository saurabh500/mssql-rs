// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use mssql_tds::connection::tds_client::ResultSet as TdsResultSet;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};

use crate::client::Client;
use crate::error::Result;
use crate::metadata::ColumnMetadata;
use crate::result_set::ResultSet;
use crate::value::Value;

use super::parameterized::value_to_sql_type;

/// A prepared statement handle.
///
/// Created via [`Client::prepare`]. The statement is executed with
/// [`execute`](PreparedStatement::execute) and should be closed with
/// [`close`](PreparedStatement::close). If dropped without being closed, the
/// handle is enqueued on the owning `Client` for deferred `sp_unprepare`
/// (research R8).
pub struct PreparedStatement<'a> {
    handle: i32,
    client: &'a mut Client,
    closed: bool,
}

impl<'a> PreparedStatement<'a> {
    pub(crate) fn new(handle: i32, client: &'a mut Client) -> Self {
        Self {
            handle,
            client,
            closed: false,
        }
    }

    /// Execute the prepared statement with positional parameter values.
    pub async fn execute(&mut self, params: &[Value]) -> Result<ResultSet<'_>> {
        let rpc_params: Vec<RpcParameter> = params
            .iter()
            .map(|v| {
                let sql_value = value_to_sql_type(v)?;
                Ok(RpcParameter::new(None, StatusFlags::NONE, sql_value))
            })
            .collect::<Result<_>>()?;

        self.client
            .inner
            .execute_sp_execute(
                self.handle,
                Some(rpc_params),
                None,
                self.client.command_timeout,
                Some(&self.client.cancel_handle),
            )
            .await?;

        let tds_meta = self.client.inner.get_metadata();
        let metadata: Vec<ColumnMetadata> = tds_meta.iter().map(ColumnMetadata::from).collect();

        Ok(ResultSet::new(self.client, Arc::new(metadata)))
    }

    /// Close the prepared statement by calling `sp_unprepare` on the server.
    pub async fn close(mut self) -> Result<()> {
        self.closed = true;
        self.client
            .inner
            .execute_sp_unprepare(self.handle, None, None)
            .await?;
        Ok(())
    }
}

impl<'a> Drop for PreparedStatement<'a> {
    fn drop(&mut self) {
        if !self.closed {
            tracing::debug!(
                handle = self.handle,
                "PreparedStatement dropped without close — deferring sp_unprepare"
            );
            self.client.pending_unprepare.push(self.handle);
        }
    }
}

impl Client {
    /// Prepare a SQL statement on the server via `sp_prepare`.
    ///
    /// The returned [`PreparedStatement`] can be executed repeatedly with
    /// different parameter values. Parameter declarations are inferred from
    /// the provided template parameters.
    pub async fn prepare(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<PreparedStatement<'_>> {
        self.drain_pending().await?;

        let rpc_params = super::parameterized::build_rpc_params(params)?;
        let handle = self
            .inner
            .execute_sp_prepare(
                sql.to_string(),
                rpc_params,
                self.command_timeout,
                Some(&self.cancel_handle),
            )
            .await?;

        Ok(PreparedStatement::new(handle, self))
    }
}
