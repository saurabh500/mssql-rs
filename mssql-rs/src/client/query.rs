// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use mssql_tds::connection::tds_client::ResultSet as TdsResultSet;

use crate::client::Client;
use crate::error::Result;
use crate::metadata::ColumnMetadata;
use crate::result_set::ResultSet;
use crate::value::Value;

impl Client {
    /// Execute a SQL query, returning the first result set.
    pub async fn query(&mut self, sql: &str) -> Result<ResultSet<'_>> {
        self.drain_pending().await?;

        self.inner
            .execute(
                sql.to_string(),
                self.command_timeout,
                Some(&self.cancel_handle),
            )
            .await?;

        let tds_meta = self.inner.get_metadata();
        let metadata: Vec<ColumnMetadata> = tds_meta.iter().map(ColumnMetadata::from).collect();

        Ok(ResultSet::new(self, Arc::new(metadata)))
    }

    /// Execute a query and collect all rows into a 2D collection.
    pub async fn query_collect(&mut self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let rs = self.query(sql).await?;
        rs.collect_rows().await
    }
}
