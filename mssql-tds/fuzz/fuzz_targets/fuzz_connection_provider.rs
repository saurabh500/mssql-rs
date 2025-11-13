// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz test for TdsConnectionProvider
//!
//! This fuzzer tests the actual TdsConnectionProvider::create_client_with_transport() API
//! by injecting a mock transport with fuzzed server responses.
//!
//! What it tests:
//! - Connection establishment with fuzzed prelogin responses
//! - Login handshake with fuzzed tokens
//! - Feature negotiation with malformed data
//! - Error handling during connection setup
//! - Timeout and cancellation behavior
//! - Redirection handling with fuzzed routing tokens
//!
//! Run with: RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider

#![no_main]

use libfuzzer_sys::fuzz_target;
use mssql_tds::connection::client_context::ClientContext;
use mssql_tds::fuzz_support::{MockTransport, TdsConnectionProvider, TdsPacketReader};
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

    async fn read_int16_big_endian(&mut self) -> TdsResult<i16> {
        let mut buf = [0u8; 2];
        self.read_bytes(&mut buf).await?;
        Ok(i16::from_be_bytes(buf))
    }

    async fn read_int32_big_endian(&mut self) -> TdsResult<i32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf).await?;
        Ok(i32::from_be_bytes(buf))
    }

    async fn read_int64_big_endian(&mut self) -> TdsResult<i64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(i64::from_be_bytes(buf))
    }

    async fn read_uint40(&mut self) -> TdsResult<u64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf[..5]).await?;
        Ok(u64::from_le_bytes(buf))
    }

    async fn read_float32(&mut self) -> TdsResult<f32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf).await?;
        Ok(f32::from_le_bytes(buf))
    }

    async fn read_float64(&mut self) -> TdsResult<f64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(f64::from_le_bytes(buf))
    }

    async fn read_uint16(&mut self) -> TdsResult<u16> {
        let mut buf = [0u8; 2];
        self.read_bytes(&mut buf).await?;
        Ok(u16::from_le_bytes(buf))
    }

    async fn read_uint32(&mut self) -> TdsResult<u32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf).await?;
        Ok(u32::from_le_bytes(buf))
    }

    async fn read_uint64(&mut self) -> TdsResult<u64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(u64::from_le_bytes(buf))
    }

    async fn read_int16(&mut self) -> TdsResult<i16> {
        let mut buf = [0u8; 2];
        self.read_bytes(&mut buf).await?;
        Ok(i16::from_le_bytes(buf))
    }

    async fn read_int24(&mut self) -> TdsResult<i32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf[..3]).await?;
        Ok(i32::from_le_bytes(buf))
    }

    async fn read_uint24(&mut self) -> TdsResult<u32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf[..3]).await?;
        Ok(u32::from_le_bytes(buf))
    }

    async fn read_int32(&mut self) -> TdsResult<i32> {
        let mut buf = [0u8; 4];
        self.read_bytes(&mut buf).await?;
        Ok(i32::from_le_bytes(buf))
    }

    async fn read_int64(&mut self) -> TdsResult<i64> {
        let mut buf = [0u8; 8];
        self.read_bytes(&mut buf).await?;
        Ok(i64::from_le_bytes(buf))
    }

    async fn read_bytes(&mut self, buf: &mut [u8]) -> TdsResult<usize> {
        // Use checked arithmetic to prevent overflow
        let end_position = self.position.checked_add(buf.len()).ok_or_else(|| {
            mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "buffer length causes position overflow",
            ))
        })?;
        
        if end_position > self.data.len() {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "EOF",
            )));
        }
        buf.copy_from_slice(&self.data[self.position..end_position]);
        self.position = end_position;
        Ok(buf.len())
    }

    async fn read_u8_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        let len = self.read_byte().await? as usize;
        const MAX_ALLOC: usize = 1024 * 1024;
        if len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; len];
        self.read_bytes(&mut buf).await?;
        Ok(buf)
    }

    async fn read_u16_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        let len = self.read_uint16().await? as usize;
        const MAX_ALLOC: usize = 1024 * 1024;
        if len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; len];
        self.read_bytes(&mut buf).await?;
        Ok(buf)
    }

    async fn read_varchar_u16_length(&mut self) -> TdsResult<Option<String>> {
        let len = self.read_uint16().await?;
        if len == 0xFFFF {
            return Ok(None);
        }
        let byte_len = (len as usize) * 2;
        const MAX_ALLOC: usize = 1024 * 1024;
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        String::from_utf16(&buf.chunks_exact(2).map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])).collect::<Vec<u16>>())
            .map(Some)
            .map_err(|e| mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, e)))
    }

    async fn read_varchar_u8_length(&mut self) -> TdsResult<String> {
        let len = self.read_byte().await? as usize;
        let byte_len = len * 2;
        const MAX_ALLOC: usize = 1024 * 1024;
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        String::from_utf16(&buf.chunks_exact(2).map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])).collect::<Vec<u16>>())
            .map_err(|e| mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, e)))
    }

    async fn read_varchar_byte_len(&mut self) -> TdsResult<String> {
        let byte_len = self.read_byte().await? as usize;
        const MAX_ALLOC: usize = 1024 * 1024;
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        String::from_utf16(&buf.chunks_exact(2).map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])).collect::<Vec<u16>>())
            .map_err(|e| mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, e)))
    }

    async fn read_unicode(&mut self, string_length: usize) -> TdsResult<String> {
        let byte_len = string_length * 2;
        const MAX_ALLOC: usize = 1024 * 1024;
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        String::from_utf16(&buf.chunks_exact(2).map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])).collect::<Vec<u16>>())
            .map_err(|e| mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, e)))
    }

    async fn read_unicode_with_byte_length(&mut self, byte_length: usize) -> TdsResult<String> {
        const MAX_ALLOC: usize = 1024 * 1024;
        if byte_length > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_length, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_length];
        self.read_bytes(&mut buf).await?;
        String::from_utf16(&buf.chunks_exact(2).map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]])).collect::<Vec<u16>>())
            .map_err(|e| mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, e)))
    }

    async fn skip_bytes(&mut self, skip_count: usize) -> TdsResult<()> {
        // Use checked arithmetic to prevent overflow
        let new_position = self.position.checked_add(skip_count).ok_or_else(|| {
            mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidInput,
                "skip_count causes position overflow",
            ))
        })?;
        
        if new_position > self.data.len() {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "EOF",
            )));
        }
        self.position = new_position;
        Ok(())
    }

    async fn cancel_read_stream(&mut self) -> TdsResult<()> {
        // No-op for fuzzing
        Ok(())
    }

    fn reset_reader(&mut self) {
        self.position = 0;
    }
}

fuzz_target!(|data: &[u8]| {
    // Need at least some data to work with
    if data.is_empty() {
        return;
    }

    // Run the fuzzing
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        fuzz_connection_provider(data).await;
    });
});

async fn fuzz_connection_provider(data: &[u8]) {
    // Create a fuzz reader with the input data
    let reader = Box::new(FuzzReader::new(data));
    let packet_size = 4096;

    // Create a mock transport with fuzzed data
    let transport = MockTransport::new(reader, packet_size);

    // Create a minimal client context for testing
    let context = ClientContext::default();

    // Try to create a client with the fuzzed transport
    // This exercises the entire connection flow including prelogin, login,
    // feature negotiation, and error handling
    let result = TdsConnectionProvider::create_client_with_transport(context, transport).await;

    // We don't care about the result, just that it doesn't panic
    // The fuzzer is looking for panics, not successful connections
    let _ = result;
}
