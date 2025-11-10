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
    GenericTokenParserRegistry, ParserContext, TdsPacketReader, TdsTokenStreamReader,
    TokenStreamReader,
};
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

    async fn read_u8_varbyte(&mut self) -> TdsResult<Vec<u8>> {
        let len = self.read_byte().await? as usize;
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
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
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
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

    async fn read_varchar_u8_length(&mut self) -> TdsResult<String> {
        let len = self.read_byte().await? as usize;
        let byte_len = len.checked_mul(2).ok_or_else(|| {
            mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("String length {} * 2 overflows", len),
            ))
        })?; // Unicode characters are 2 bytes
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        
        // Try to decode as UTF-16LE
        let chars: Vec<u16> = buf
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        String::from_utf16(&chars).map_err(|_| {
            mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, "Invalid UTF-16"))
        })
    }

    async fn read_varchar_u16_length(&mut self) -> TdsResult<Option<String>> {
        let len = self.read_uint16().await?;
        if len == 0xFFFF {
            return Ok(None);
        }
        
        let byte_len = (len as usize).checked_mul(2).ok_or_else(|| {
            mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("String length {} * 2 overflows", len),
            ))
        })?;
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        
        let chars: Vec<u16> = buf
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        String::from_utf16(&chars)
            .map(Some)
            .map_err(|_| {
                mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, "Invalid UTF-16"))
            })
    }

    async fn read_unicode(&mut self, char_count: usize) -> TdsResult<String> {
        let byte_len = char_count.checked_mul(2).ok_or_else(|| {
            mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("String char_count {} * 2 overflows", char_count),
            ))
        })?;
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        
        let chars: Vec<u16> = buf
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        String::from_utf16(&chars).map_err(|_| {
            mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, "Invalid UTF-16"))
        })
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

    // Stub implementations for methods not commonly used
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

    async fn read_uint24(&mut self) -> TdsResult<u32> {
        let mut buf = [0u8; 3];
        self.read_bytes(&mut buf).await?;
        Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], 0]))
    }

    async fn read_int24(&mut self) -> TdsResult<i32> {
        let mut buf = [0u8; 3];
        self.read_bytes(&mut buf).await?;
        let value = i32::from_le_bytes([buf[0], buf[1], buf[2], 0]);
        // Sign extend if negative
        if buf[2] & 0x80 != 0 {
            Ok(value | 0xFF000000u32 as i32)
        } else {
            Ok(value)
        }
    }

    async fn read_uint40(&mut self) -> TdsResult<u64> {
        let mut buf = [0u8; 5];
        self.read_bytes(&mut buf).await?;
        Ok(u64::from_le_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], 0, 0, 0]))
    }

    async fn read_varchar_byte_len(&mut self) -> TdsResult<String> {
        let byte_len = self.read_byte().await? as usize;
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        
        let chars: Vec<u16> = buf
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        String::from_utf16(&chars).map_err(|_| {
            mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, "Invalid UTF-16"))
        })
    }

    async fn read_unicode_with_byte_length(&mut self, byte_len: usize) -> TdsResult<String> {
        // Prevent capacity overflow
        const MAX_ALLOC: usize = 1024 * 1024; // 1MB
        if byte_len > MAX_ALLOC {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Allocation size {} exceeds max {}", byte_len, MAX_ALLOC),
            )));
        }
        let mut buf = vec![0u8; byte_len];
        self.read_bytes(&mut buf).await?;
        
        let chars: Vec<u16> = buf
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        
        String::from_utf16(&chars).map_err(|_| {
            mssql_tds::error::Error::Io(Error::new(ErrorKind::InvalidData, "Invalid UTF-16"))
        })
    }

    async fn skip_bytes(&mut self, count: usize) -> TdsResult<()> {
        if self.position + count > self.data.len() {
            return Err(mssql_tds::error::Error::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "EOF",
            )));
        }
        self.position += count;
        Ok(())
    }

    async fn cancel_read_stream(&mut self) -> TdsResult<()> {
        Ok(())
    }

    fn reset_reader(&mut self) {
        self.position = 0;
    }
}

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
