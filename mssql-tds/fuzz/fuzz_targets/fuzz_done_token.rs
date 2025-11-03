// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz test for DoneTokenParser.parse() method
//! 
//! This fuzzer tests the actual DoneTokenParser with arbitrary byte inputs to find:
//! - Panics or crashes
//! - Infinite loops or hangs
//! - Unexpected behavior with malformed data
//!
//! DoneToken structure (12 bytes total):
//! - Bytes 0-1: status (u16, little-endian) - bitflags for DONE status
//! - Bytes 2-3: current_command (u16, little-endian) - command type
//! - Bytes 4-11: row_count (u64, little-endian) - number of rows affected
//!
//! Run with: RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_done_token

#![no_main]

use libfuzzer_sys::fuzz_target;
use mssql_tds::fuzz_support::{DoneTokenParser, ParserContext, TdsPacketReader, TokenParser, Tokens};
use mssql_tds::core::TdsResult;
use std::io::{Error, ErrorKind};

/// Simple reader that wraps fuzz input data
struct FuzzReader {
    data: Vec<u8>,
    position: usize,
}

impl FuzzReader {
    fn new(data: &[u8]) -> Self {
        Self {
            data: data.to_vec(),
            position: 0,
        }
    }
}

#[async_trait::async_trait]
impl TdsPacketReader for FuzzReader {
    async fn read_byte(&mut self) -> TdsResult<u8> {
        if self.position >= self.data.len() {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "EOF",
            )));
        }
        let byte = self.data[self.position];
        self.position += 1;
        Ok(byte)
    }

    async fn read_uint16(&mut self) -> TdsResult<u16> {
        let mut buf = [0u8; 2];
        self.read_bytes(&mut buf).await?;
        Ok(u16::from_le_bytes(buf))
    }

    async fn read_uint64(&mut self) -> TdsResult<u64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(u64::from_le_bytes(buf))
    }

    async fn read_bytes(&mut self, buf: &mut [u8]) -> TdsResult<usize> {
        if self.position + buf.len() > self.data.len() {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "EOF",
            )));
        }
        buf.copy_from_slice(&self.data[self.position..self.position + buf.len()]);
        self.position += buf.len();
        Ok(buf.len())
    }

    // Stub implementations for methods not used by DoneTokenParser
    async fn read_int16(&mut self) -> TdsResult<i16> { unimplemented!() }
    async fn read_int16_big_endian(&mut self) -> TdsResult<i16> { unimplemented!() }
    async fn read_int32(&mut self) -> TdsResult<i32> { unimplemented!() }
    async fn read_int32_big_endian(&mut self) -> TdsResult<i32> { unimplemented!() }
    async fn read_int64(&mut self) -> TdsResult<i64> { unimplemented!() }
    async fn read_int64_big_endian(&mut self) -> TdsResult<i64> { unimplemented!() }
    async fn read_uint24(&mut self) -> TdsResult<u32> { unimplemented!() }
    async fn read_int24(&mut self) -> TdsResult<i32> { unimplemented!() }
    async fn read_uint32(&mut self) -> TdsResult<u32> { unimplemented!() }
    async fn read_uint40(&mut self) -> TdsResult<u64> { unimplemented!() }
    async fn read_float32(&mut self) -> TdsResult<f32> { unimplemented!() }
    async fn read_float64(&mut self) -> TdsResult<f64> { unimplemented!() }
    async fn read_u8_varbyte(&mut self) -> TdsResult<Vec<u8>> { unimplemented!() }
    async fn read_u16_varbyte(&mut self) -> TdsResult<Vec<u8>> { unimplemented!() }
    async fn read_varchar_u16_length(&mut self) -> TdsResult<Option<String>> { unimplemented!() }
    async fn read_varchar_u8_length(&mut self) -> TdsResult<String> { unimplemented!() }
    async fn read_varchar_byte_len(&mut self) -> TdsResult<String> { unimplemented!() }
    async fn read_unicode(&mut self, _: usize) -> TdsResult<String> { unimplemented!() }
    async fn read_unicode_with_byte_length(&mut self, _: usize) -> TdsResult<String> { unimplemented!() }
    async fn skip_bytes(&mut self, _: usize) -> TdsResult<()> { unimplemented!() }
    async fn cancel_read_stream(&mut self) -> TdsResult<()> { Ok(()) }
    fn reset_reader(&mut self) { self.position = 0; }
}

fuzz_target!(|data: &[u8]| {
    // DoneToken requires exactly 12 bytes
    if data.len() != 12 {
        return;
    }

    // Create a tokio runtime to execute async code
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    rt.block_on(async {
        let mut reader = FuzzReader::new(data);
        let parser = DoneTokenParser {};
        let context = ParserContext::default();
        
        // Test the actual DoneTokenParser.parse() method
        // This should never panic, only return Ok or Err
        let _ = parser.parse(&mut reader, &context).await;
    });
});
