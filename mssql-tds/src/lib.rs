// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

pub mod connection;
pub mod connection_provider;
pub mod core;
pub mod datatypes;
pub mod error;
pub(crate) mod handler;
pub(crate) mod io;
pub mod message;
pub mod query;
pub mod security;
pub mod sql_identifier;
pub(crate) mod ssrp;
pub mod token;

// Expose internal APIs for fuzzing
#[cfg(fuzzing)]
pub mod fuzz_support;
