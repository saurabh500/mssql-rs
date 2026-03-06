// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod connection;
mod parameterized;
pub mod prepared;
mod query;
pub mod transaction;

use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::core::CancelHandle;

use crate::error::Result;

/// SQL Server client wrapping the TDS protocol layer.
pub struct Client {
    pub(crate) inner: TdsClient,
    pub(crate) cancel_handle: CancelHandle,
    pub(crate) command_timeout: Option<u32>,
    pub(crate) pending_rollback: bool,
    pub(crate) pending_unprepare: Vec<i32>,
}

impl Client {
    /// Cancel any in-flight query.
    pub fn cancel(&self) {
        let child = self.cancel_handle.child_handle();
        child.cancel();
    }

    /// Close the connection.
    pub async fn close(mut self) -> Result<()> {
        self.inner.close_connection().await?;
        Ok(())
    }

    /// Drain any deferred cleanup (pending rollback / unprepare) before
    /// executing a new operation.
    pub(crate) async fn drain_pending(&mut self) -> Result<()> {
        // Close any open batch from a previous partially-consumed result set
        self.inner.close_query().await?;
        if self.pending_rollback {
            self.pending_rollback = false;
            self.inner.rollback_transaction(None, None).await?;
        }
        for handle in std::mem::take(&mut self.pending_unprepare) {
            let _ = self.inner.execute_sp_unprepare(handle, None, None).await;
        }
        Ok(())
    }
}
