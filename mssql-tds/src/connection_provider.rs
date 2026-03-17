// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Connection provider factory for creating [`TdsClient`](crate::connection::tds_client::TdsClient) instances.
//!
//! Use [`TdsConnectionProvider::create_client()`](tds_connection_provider::TdsConnectionProvider::create_client)
//! to establish a connection from a data source string.

pub mod tds_connection_provider;
