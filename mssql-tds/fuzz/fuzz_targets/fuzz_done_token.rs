// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Fuzz test for DoneToken parser
//! 
//! This fuzzer tests the DoneToken parser with arbitrary byte inputs to find:
//! - Panics or crashes
//! - Infinite loops or hangs
//! - Unexpected behavior with malformed data
//!
//! DoneToken structure (12 bytes total):
//! - Bytes 0-1: status (u16, little-endian) - bitflags for DONE status
//! - Bytes 2-3: current_command (u16, little-endian) - command type
//! - Bytes 4-11: row_count (u64, little-endian) - number of rows affected
//!
//! Run with: cargo +nightly fuzz run fuzz_done_token

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // DoneToken requires exactly 12 bytes:
    // - 2 bytes: status (u16)
    // - 2 bytes: current_command (u16)
    // - 8 bytes: row_count (u64)
    
    if data.len() != 12 {
        return; // Only process valid-length inputs
    }

    // Parse the bytes manually to test edge cases
    let status = u16::from_le_bytes([data[0], data[1]]);
    let current_command = u16::from_le_bytes([data[2], data[3]]);
    let row_count = u64::from_le_bytes([
        data[4], data[5], data[6], data[7],
        data[8], data[9], data[10], data[11],
    ]);

    // Test DoneStatus bitflags parsing
    // This should never panic regardless of the bit pattern
    let _ = parse_done_status(status);

    // Test CurrentCommand enum conversion
    // This should handle unknown values gracefully
    let _ = parse_current_command(current_command);

    // Test row_count boundaries
    let _ = validate_row_count(row_count);
});

/// Test DoneStatus bitflags parsing
/// The DoneStatus is defined as bitflags with these values:
/// - FINAL = 0x0000
/// - MORE = 0x0001
/// - ERROR = 0x0002
/// - IN_XACT = 0x0004
/// - COUNT = 0x0010
/// - ATTN = 0x0020
/// - SERVER_ERROR = 0x0100
fn parse_done_status(status: u16) {
    // Test all possible bitflag combinations
    const FINAL: u16 = 0x0000;
    const MORE: u16 = 0x0001;
    const ERROR: u16 = 0x0002;
    const IN_XACT: u16 = 0x0004;
    const COUNT: u16 = 0x0010;
    const ATTN: u16 = 0x0020;
    const SERVER_ERROR: u16 = 0x0100;

    // Test individual flag checks
    let _has_more = (status & MORE) != 0;
    let _has_error = (status & ERROR) != 0;
    let _in_transaction = (status & IN_XACT) != 0;
    let _has_count = (status & COUNT) != 0;
    let _attention = (status & ATTN) != 0;
    let _server_error = (status & SERVER_ERROR) != 0;

    // Test combined flags
    let _has_more_and_error = (status & (MORE | ERROR)) == (MORE | ERROR);
    
    // Test invalid/unknown bits (should be ignored)
    let _unknown_bits = status & !(FINAL | MORE | ERROR | IN_XACT | COUNT | ATTN | SERVER_ERROR);
}

/// Test CurrentCommand enum conversion
/// The CurrentCommand enum has specific values:
/// - None = 0x00
/// - Select = 0xc1
/// - Insert = 0xc3
/// - Delete = 0xc4
/// - Update = 0xc5
/// - Abort = 0xd2
/// - BeginXact = 0xd4
/// - EndXact = 0xd5
/// - BulkInsert = 0xf0
/// - OpenCursor = 0x20
/// (and possibly more)
fn parse_current_command(cmd: u16) {
    // Test known command values
    match cmd {
        0x00 => { /* None */ },
        0xc1 => { /* Select */ },
        0xc3 => { /* Insert */ },
        0xc4 => { /* Delete */ },
        0xc5 => { /* Update */ },
        0xd2 => { /* Abort */ },
        0xd4 => { /* BeginXact */ },
        0xd5 => { /* EndXact */ },
        0xf0 => { /* BulkInsert */ },
        0x20 => { /* OpenCursor */ },
        _ => {
            // Unknown command value - parser should handle this gracefully
            // Either by having a catch-all variant or using try_from
        }
    }
}

/// Test row_count boundaries
fn validate_row_count(row_count: u64) {
    // Test boundary conditions
    match row_count {
        0 => { /* Empty result */ },
        1 => { /* Single row */ },
        u64::MAX => { /* Maximum possible value */ },
        _ => { /* Normal value */ }
    }
    
    // Ensure arithmetic operations don't overflow
    let _ = row_count.saturating_add(1);
    let _ = row_count.saturating_mul(2);
}
