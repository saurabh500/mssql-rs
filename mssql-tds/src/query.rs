// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Result set reading and row iteration.
//!
//! This module contains the types used to describe query output:
//!
//! - [`metadata::ColumnMetadata`] — per-column type and flag information
//!   returned with every result set.
//! - [`result::ReturnValue`] — output parameters and return-status values
//!   from stored procedures.
//!
//! Row-level iteration is driven by the [`ResultSet`](crate::ResultSet) and
//! [`ResultSetClient`](crate::ResultSetClient) traits implemented on
//! [`TdsClient`](crate::TdsClient).

pub mod metadata;
pub mod result;
