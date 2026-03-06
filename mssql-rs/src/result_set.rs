// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::Stream;
use mssql_tds::connection::tds_client::{
    ResultSet as TdsResultSet, ResultSetClient as TdsResultSetClient,
};

use crate::client::Client;
use crate::error::{Error, Result};
use crate::metadata::ColumnMetadata;
use crate::row::Row;
use crate::value::Value;

type NextRowFuture<'a> = BoxFuture<
    'a,
    (
        std::result::Result<
            Option<Vec<mssql_tds::datatypes::column_values::ColumnValues>>,
            mssql_tds::error::Error,
        >,
        &'a mut Client,
    ),
>;

/// A result set returned from a query.
///
/// Implements [`futures::Stream`] yielding `Result<Row>` items. Also provides
/// [`collect_rows`](ResultSet::collect_rows) for convenience collection and
/// [`next_result_set`](ResultSet::next_result_set) for multi-result batches.
pub struct ResultSet<'a> {
    client: Option<&'a mut Client>,
    metadata: Arc<Vec<ColumnMetadata>>,
    finished: bool,
    pending: Option<NextRowFuture<'a>>,
}

impl<'a> ResultSet<'a> {
    pub(crate) fn new(client: &'a mut Client, metadata: Arc<Vec<ColumnMetadata>>) -> Self {
        let finished = metadata.is_empty();
        Self {
            client: Some(client),
            metadata,
            finished,
            pending: None,
        }
    }

    /// Column metadata for this result set.
    pub fn metadata(&self) -> &[ColumnMetadata] {
        &self.metadata
    }

    /// Drain all remaining rows into a `Vec<Vec<Value>>`.
    pub async fn collect_rows(mut self) -> Result<Vec<Vec<Value>>> {
        if self.finished {
            return Ok(Vec::new());
        }
        let client = self.client.take().expect("client present");
        let mut rows = Vec::new();
        while let Some(raw) = client.inner.next_row().await? {
            let values: Vec<Value> = raw.into_iter().map(Value::from).collect();
            rows.push(values);
        }
        self.finished = true;
        Ok(rows)
    }

    /// Advance to the next result set. Returns `None` if no more sets.
    pub async fn next_result_set(mut self) -> Result<Option<ResultSet<'a>>> {
        let client = match self.client.take() {
            Some(c) => c,
            None => return Ok(None),
        };

        // Drain unread rows
        if !self.finished {
            while client.inner.next_row().await?.is_some() {}
        }

        let has_next = client.inner.move_to_next().await?;
        if has_next {
            let tds_meta = client.inner.get_metadata();
            let metadata: Vec<ColumnMetadata> = tds_meta.iter().map(ColumnMetadata::from).collect();
            Ok(Some(ResultSet::new(client, Arc::new(metadata))))
        } else {
            Ok(None)
        }
    }
}

impl<'a> Stream for ResultSet<'a> {
    type Item = Result<Row>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.finished {
            return Poll::Ready(None);
        }

        // Start a new next_row call if none is in progress.
        if this.pending.is_none() {
            let client = this
                .client
                .take()
                .expect("client must be present when not polling");
            let fut = async move {
                let result = client.inner.next_row().await;
                (result, client)
            }
            .boxed();
            this.pending = Some(fut);
        }

        let fut = this.pending.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((result, client)) => {
                this.pending = None;
                match result {
                    Ok(Some(cols)) => {
                        let values: Vec<Value> = cols.into_iter().map(Value::from).collect();
                        let row = Row {
                            columns: values,
                            metadata: this.metadata.clone(),
                            stream_pos: None,
                        };
                        this.client = Some(client);
                        Poll::Ready(Some(Ok(row)))
                    }
                    Ok(None) => {
                        this.finished = true;
                        Poll::Ready(None)
                    }
                    Err(e) => {
                        this.finished = true;
                        Poll::Ready(Some(Err(Error::from(e))))
                    }
                }
            }
        }
    }
}
