// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TDS protocol message types.
//!
//! Each sub-module corresponds to a TDS message kind: SQL batches, RPC
//! requests, login/prelogin handshakes, bulk-load streams, and transaction
//! management envelopes. Messages implement the [`messages::Request`] trait
//! for serialization into TDS packets.

pub(crate) mod batch;
pub mod bulk_load;
pub mod parameters;

mod features;
pub(crate) mod headers;
pub mod rpc;

pub(crate) mod attention;
pub(crate) mod login;
pub mod login_options;
pub mod messages;
pub(crate) mod prelogin;
pub mod transaction_management;
