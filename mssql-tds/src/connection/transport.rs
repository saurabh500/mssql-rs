// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

pub(crate) mod buffers;
pub(crate) mod certificate_validator;
#[cfg(windows)]
pub(crate) mod localdb;
#[cfg(windows)]
pub(crate) mod named_pipes;
pub mod network_transport;
pub mod ssl_handler;
pub mod tds_transport;
