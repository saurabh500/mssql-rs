// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Mock TDS Server for testing
//!
//! This crate provides a mock TDS (Tabular Data Stream) server that can be used
//! for testing SQL Server client implementations. The mock server implements a
//! subset of the TDS protocol sufficient for basic connection and query testing.
//!
//! # Example
//!
//! ```no_run
//! use mssql_mock_tds::MockTdsServer;
//!
//! #[tokio::main]
//! async fn main() {
//!     let server = MockTdsServer::new("127.0.0.1:1433").await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

pub mod protocol;
pub mod query_response;
pub mod server;

pub use query_response::{
    ColumnDefinition, ColumnValue, QueryRegistry, QueryResponse, Row, SqlDataType,
};
pub use server::MockTdsServer;
