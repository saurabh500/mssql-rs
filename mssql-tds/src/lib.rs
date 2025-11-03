// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(dead_code)]
pub mod connection;
pub mod connection_provider;
pub mod core;
pub mod datatypes;
pub mod error;
pub mod handler;
pub mod message;
pub mod query;
pub mod read_write;
pub mod token;

// Expose internal APIs for fuzzing
#[cfg(fuzzing)]
pub mod fuzz_support {
    pub use crate::read_write::packet_reader::TdsPacketReader;
    pub use crate::read_write::token_stream::ParserContext;
    pub use crate::token::parsers::{DoneTokenParser, TokenParser};
    pub use crate::token::tokens::Tokens;
}
