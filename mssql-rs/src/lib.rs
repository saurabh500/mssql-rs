// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! # mssql-rs
//!
//! Ergonomic Rust client for SQL Server, built on the `mssql-tds` protocol
//! library.
//!
//! ## Features
//!
//! - **ODBC connection strings**: case-insensitive key-value parsing, 12 standard keys
//! - **Ad-hoc and parameterized queries**: `query`, `query_with_params`
//! - **Row streaming**: `ResultSet` implements [`futures::Stream`]
//! - **Convenience collection**: `query_collect` drains all rows into `Vec<Vec<Value>>`
//! - **Type-safe extraction**: [`FromValue`] trait with built-in impls for Rust primitives
//! - **Prepared statements**: `sp_prepare` / `sp_execute` / `sp_unprepare` lifecycle
//! - **Transactions**: `begin_transaction`, `commit`, `rollback` with isolation levels
//! - **Cancellation & timeouts**: in-flight cancel via [`Client::cancel`], per-query timeout
//! - **Multiple result sets**: `next_result_set` for batch queries
//!
//! ## Quick Example
//!
//! ```ignore
//! use mssql_rs::{Client, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut client = Client::connect(
//!         "Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword123"
//!     ).await?;
//!
//!     let rows = client.query_collect("SELECT 1 AS val").await?;
//!     println!("{:?}", rows);
//!
//!     client.close().await
//! }
//! ```

mod client;
mod column_stream;
mod datetime;
mod error;
mod from_value;
mod metadata;
mod result_set;
mod row;
mod value;

pub use client::Client;
pub use client::prepared::PreparedStatement;
pub use client::transaction::{IsolationLevel, Transaction};
pub use column_stream::ColumnStream;
pub use datetime::DateTime;
pub use error::{Error, Result};
pub use from_value::FromValue;
pub use metadata::{ColumnMetadata, DataType};
pub use result_set::ResultSet;
pub use row::Row;
pub use value::Value;
