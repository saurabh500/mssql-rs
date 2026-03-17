// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz test for TokenStreamReader
//!
//! This fuzzer tests the complete TokenStreamReader with arbitrary byte inputs to find:
//! - Panics or crashes when parsing various token types
//! - Infinite loops or hangs
//! - Unexpected behavior with malformed token streams
//!
//! Token Stream Format:
//! - First byte: Token type (e.g., 0xFD for DONE, 0xAB for INFO, etc.)
//! - Following bytes: Token-specific data (varies by token type)
//!
//! The fuzzer will try various token types and data combinations to ensure
//! robust error handling across all token parsers in the TokenStreamReader.
//!
//! Run with: RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_token_stream

#![no_main]

use libfuzzer_sys::fuzz_target;
use mssql_tds::fuzz_support::{
    FuzzReader, GenericTokenParserRegistry, ParserContext, TdsTokenStreamReader, TokenStreamReader,
};

fuzz_target!(|data: &[u8]| {
    // We need at least 1 byte for the token type
    if data.is_empty() {
        return;
    }

    // Limit input size to avoid excessive memory consumption and timeouts
    if data.len() > 1024 {
        return;
    }

    // Create a tokio runtime to execute async code
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let reader = FuzzReader::new(data);
        let parser_registry = Box::new(GenericTokenParserRegistry::default());
        let mut token_stream = TokenStreamReader::new(reader, parser_registry);
        let context = ParserContext::default();

        // Test the TokenStreamReader.receive_token() method
        // This is the main entry point that orchestrates token parsing
        // The fuzzer will try to trigger panics or unexpected behavior
        let _ = token_stream.receive_token(&context, None, None).await;

        // Try to receive multiple tokens to test token stream continuity
        let _ = token_stream.receive_token(&context, None, None).await;
    });
});
