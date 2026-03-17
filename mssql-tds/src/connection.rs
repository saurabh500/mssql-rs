// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Connection management types for TDS protocol communication with SQL Server.
//!
//! Key types:
//! - [`tds_client::TdsClient`] — primary client for executing queries and managing connections
//! - [`client_context::ClientContext`] — connection configuration (credentials, encryption, timeouts)
//! - [`bulk_copy::BulkCopy`] — bulk data loading

pub mod bulk_copy;
pub mod bulk_copy_state;
pub mod client_context;
pub mod connection_actions;
pub mod datasource_parser;
pub(crate) mod execution_context;
pub mod metadata_retriever;
pub mod odbc_authentication_transformer;
pub mod odbc_authentication_validator;
pub mod odbc_supported_auth_keywords;
pub mod tds_client;
pub mod transport;
