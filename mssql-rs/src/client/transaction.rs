// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use mssql_tds::connection::tds_client::ResultSet as TdsResultSet;
use mssql_tds::message::transaction_management::TransactionIsolationLevel;

use crate::client::Client;
use crate::error::Result;
use crate::metadata::ColumnMetadata;
use crate::result_set::ResultSet;
use crate::value::Value;

use super::prepared::PreparedStatement;

/// Transaction isolation level for [`Client::begin_transaction_with_isolation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

impl From<IsolationLevel> for TransactionIsolationLevel {
    fn from(level: IsolationLevel) -> Self {
        match level {
            IsolationLevel::ReadUncommitted => TransactionIsolationLevel::ReadUncommitted,
            IsolationLevel::ReadCommitted => TransactionIsolationLevel::ReadCommitted,
            IsolationLevel::RepeatableRead => TransactionIsolationLevel::RepeatableRead,
            IsolationLevel::Serializable => TransactionIsolationLevel::Serializable,
            IsolationLevel::Snapshot => TransactionIsolationLevel::Snapshot,
        }
    }
}

/// An active transaction on a SQL Server connection.
///
/// Created via [`Client::begin_transaction`] or
/// [`Client::begin_transaction_with_isolation`]. Operations are executed
/// within the transaction scope via [`query`](Transaction::query),
/// [`query_with_params`](Transaction::query_with_params), and
/// [`prepare`](Transaction::prepare).
///
/// Must be explicitly committed with [`commit`](Transaction::commit) or
/// rolled back with [`rollback`](Transaction::rollback). If dropped without
/// either, a deferred rollback is triggered on the next `Client` operation.
pub struct Transaction<'a> {
    client: &'a mut Client,
    committed: bool,
    rolled_back: bool,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Self {
        Self {
            client,
            committed: false,
            rolled_back: false,
        }
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<()> {
        self.committed = true;
        self.client.inner.commit_transaction(None, None).await?;
        Ok(())
    }

    /// Roll back the transaction.
    pub async fn rollback(mut self) -> Result<()> {
        self.rolled_back = true;
        self.client.inner.rollback_transaction(None, None).await?;
        Ok(())
    }

    /// Execute a SQL query within this transaction.
    pub async fn query(&mut self, sql: &str) -> Result<ResultSet<'_>> {
        self.client
            .inner
            .execute(
                sql.to_string(),
                self.client.command_timeout,
                Some(&self.client.cancel_handle),
            )
            .await?;

        let tds_meta = self.client.inner.get_metadata();
        let metadata: Vec<ColumnMetadata> = tds_meta.iter().map(ColumnMetadata::from).collect();

        Ok(ResultSet::new(self.client, Arc::new(metadata)))
    }

    /// Execute a parameterized query within this transaction.
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<ResultSet<'_>> {
        let rpc_params = super::parameterized::build_rpc_params(params)?;

        self.client
            .inner
            .execute_sp_executesql(
                sql.to_string(),
                rpc_params,
                self.client.command_timeout,
                Some(&self.client.cancel_handle),
            )
            .await?;

        let tds_meta = self.client.inner.get_metadata();
        let metadata: Vec<ColumnMetadata> = tds_meta.iter().map(ColumnMetadata::from).collect();

        Ok(ResultSet::new(self.client, Arc::new(metadata)))
    }

    /// Prepare a statement within this transaction.
    pub async fn prepare(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<PreparedStatement<'_>> {
        let rpc_params = super::parameterized::build_rpc_params(params)?;

        let handle = self
            .client
            .inner
            .execute_sp_prepare(
                sql.to_string(),
                rpc_params,
                self.client.command_timeout,
                Some(&self.client.cancel_handle),
            )
            .await?;

        Ok(PreparedStatement::new(handle, self.client))
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            tracing::debug!("Transaction dropped without commit/rollback — deferring rollback");
            self.client.pending_rollback = true;
        }
    }
}

impl Client {
    /// Begin a transaction with the default isolation level (`ReadCommitted`).
    pub async fn begin_transaction(&mut self) -> Result<Transaction<'_>> {
        self.drain_pending().await?;
        self.inner
            .begin_transaction(TransactionIsolationLevel::ReadCommitted, None)
            .await?;
        Ok(Transaction::new(self))
    }

    /// Begin a transaction with a specific isolation level.
    pub async fn begin_transaction_with_isolation(
        &mut self,
        level: IsolationLevel,
    ) -> Result<Transaction<'_>> {
        self.drain_pending().await?;
        self.inner.begin_transaction(level.into(), None).await?;
        Ok(Transaction::new(self))
    }
}
