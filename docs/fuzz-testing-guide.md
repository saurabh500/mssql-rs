# Fuzz Testing Setup for DoneToken Parser

## Overview

This document explains the fuzz testing setup for the `DoneToken` parser in the mssql-tds repository.

## What is Fuzz Testing?

Fuzz testing (or fuzzing) is an automated software testing technique that involves providing invalid, unexpected, or random data as inputs to a program. The goal is to find:

- **Crashes**: Panics, assertions, segfaults
- **Hangs**: Infinite loops, deadlocks  
- **Memory issues**: Buffer overflows, use-after-free
- **Logic errors**: Incorrect behavior on edge cases

## Why Fuzz the DoneToken Parser?

The `DoneToken` parser is a critical component that processes DONE packets from SQL Server. These packets signal:
- Completion of a command (TokenType::Done = 0xFD)
- Completion of a stored procedure (TokenType::DoneProc = 0xFE)
- Completion of a statement in a procedure (TokenType::DoneInProc = 0xFF)

A malformed DONE packet could potentially:
- Cause the client to hang or crash
- Misinterpret error states
- Incorrectly report row counts
- Fail to detect transaction boundaries

## DoneToken Structure

The DoneToken parser expects exactly 12 bytes:

```
Offset | Size | Field           | Type | Description
-------|------|-----------------|------|----------------------------------
0      | 2    | status          | u16  | Bitflags for DONE status
2      | 2    | current_command | u16  | Type of SQL command executed
4      | 8    | row_count       | u64  | Number of rows affected
```

### Status Bitflags (u16)

```rust
const FINAL        = 0x0000;  // Final result in batch
const MORE         = 0x0001;  // More results to follow
const ERROR        = 0x0002;  // Error occurred
const IN_XACT      = 0x0004;  // Inside a transaction
const COUNT        = 0x0010;  // Row count is valid
const ATTN         = 0x0020;  // Attention acknowledged
const SERVER_ERROR = 0x0100;  // Server error occurred
```

### Current Command Enum (u16)

```rust
None       = 0x00
Select     = 0xc1
Insert     = 0xc3
Delete     = 0xc4
Update     = 0xc5
Abort      = 0xd2
BeginXact  = 0xd4
EndXact    = 0xd5
BulkInsert = 0xf0
OpenCursor = 0x20
```

## Fuzz Test Implementation

### Location
```
mssql-tds/fuzz/
├── Cargo.toml                         # Fuzz test package config
├── README.md                          # Fuzzing documentation
├── .gitignore                         # Ignore artifacts/corpus
├── fuzz_targets/
│   └── fuzz_done_token.rs             # DoneToken fuzz target
└── corpus/
    └── fuzz_done_token/
        ├── seed1                       # FINAL, SELECT, 0 rows
        ├── seed2                       # MORE, INSERT, 1 row
        ├── seed3                       # ERROR, UPDATE, 100 rows
        └── ...                         # More seed cases
```

### Fuzzer Strategy

The fuzzer uses **structure-aware fuzzing**:

1. **Input Validation**: Only processes exactly 12-byte inputs
2. **Component Testing**: Tests each field separately:
   - Status bitflags parsing
   - Command enum conversion  
   - Row count boundary conditions

3. **Edge Cases**:
   - Unknown bitflag combinations
   - Invalid command values
   - Boundary row counts (0, 1, MAX)

### Code Structure

```rust
fuzz_target!(|data: &[u8]| {
    if data.len() != 12 {
        return; // Only test valid-length inputs
    }

    // Parse bytes manually
    let status = u16::from_le_bytes([data[0], data[1]]);
    let current_command = u16::from_le_bytes([data[2], data[3]]);
    let row_count = u64::from_le_bytes([data[4..12]]);

    // Test each component
    parse_done_status(status);          // Bitflags
    parse_current_command(current_command); // Enum
    validate_row_count(row_count);       // Boundaries
});
```

## Running the Fuzzer

### Prerequisites

```powershell
# Install nightly Rust
rustup install nightly

# Install cargo-fuzz
cargo install cargo-fuzz
```

### Basic Usage

```powershell
cd c:\work\mssql-tds\mssql-tds

# Run fuzz test (Ctrl+C to stop)
cargo +nightly fuzz run fuzz_done_token

# Run for 60 seconds
cargo +nightly fuzz run fuzz_done_token -- -max_total_time=60

# Run with 4 parallel workers
cargo +nightly fuzz run fuzz_done_token -- -jobs=4

# Use seed corpus
cargo +nightly fuzz run fuzz_done_token corpus/fuzz_done_token/
```

### Advanced Options

```powershell
# Limit memory per worker to 2GB
cargo +nightly fuzz run fuzz_done_token -- -rss_limit_mb=2048

# Run only on specific CPU cores
cargo +nightly fuzz run fuzz_done_token -- -fork=2

# Print stats every 10 seconds
cargo +nightly fuzz run fuzz_done_token -- -print_final_stats=1
```

## Expected Results

### Success Indicators

- **No crashes**: Parser handles all inputs gracefully
- **High exec/s**: Fuzzer is making progress (>1000 exec/s is good)
- **Growing corpus**: Fuzzer finds interesting test cases

### If a Crash is Found

1. **Crash file saved**: `fuzz/artifacts/fuzz_done_token/crash-<hash>`

2. **Reproduce the crash**:
```powershell
cargo +nightly fuzz run fuzz_done_token fuzz/artifacts/fuzz_done_token/crash-<hash>
```

3. **Examine the input**:
```powershell
# View as hex
cargo +nightly fuzz fmt fuzz_done_token fuzz/artifacts/fuzz_done_token/crash-<hash>

# Or manually
Get-Content fuzz/artifacts/fuzz_done_token/crash-<hash> -Encoding Byte
```

4. **Add to test suite**: Create a regression test

## Seed Corpus

The seed corpus contains known-good test cases to guide fuzzing:

| File | Status | Command | Rows | Description |
|------|--------|---------|------|-------------|
| seed1 | FINAL (0x0000) | SELECT (0xc1) | 0 | Typical empty SELECT result |
| seed2 | MORE (0x0001) | INSERT (0xc3) | 1 | Single-row INSERT with more results |
| seed3 | ERROR (0x0002) | UPDATE (0xc5) | 100 | UPDATE with error |

The fuzzer will:
1. Start with these seeds
2. Mutate them (flip bits, change bytes)
3. Discover new interesting inputs
4. Add them to the corpus automatically

## Coverage Analysis

To see what code paths are exercised:

```powershell
# Generate coverage report
cargo +nightly fuzz coverage fuzz_done_token

# View with coverage tool (requires llvm-cov)
cargo cov -- show target/*/release/fuzz_done_token \
    --format=html \
    --instr-profile=fuzz/coverage/fuzz_done_token/coverage.profdata \
    > coverage.html
```

## Next Steps

### More Fuzz Targets

Consider fuzzing other token parsers:

1. **ErrorToken**: Tests error message parsing
2. **InfoToken**: Tests informational messages
3. **ColMetadataToken**: Tests column metadata (complex)
4. **RowToken**: Tests row data parsing (very complex)

### Integration with CI/CD

Add to your build pipeline:

```yaml
- name: Fuzz Test
  run: |
    cargo +nightly fuzz run fuzz_done_token -- -max_total_time=300
```

### Dictionary-Based Fuzzing

Create a dictionary of interesting values:

```
# fuzz/fuzz.dict
status_final="\x00\x00"
status_more="\x01\x00"
status_error="\x02\x00"
cmd_select="\xc1\x00"
cmd_insert="\xc3\x00"
rows_zero="\x00\x00\x00\x00\x00\x00\x00\x00"
rows_max="\xff\xff\xff\xff\xff\xff\xff\xff"
```

Then run:
```powershell
cargo +nightly fuzz run fuzz_done_token -- -dict=fuzz/fuzz.dict
```

## Resources

- [Rust Fuzz Book](https://rust-fuzz.github.io/book/)
- [libFuzzer Tutorial](https://llvm.org/docs/LibFuzzer.html)
- [cargo-fuzz GitHub](https://github.com/rust-fuzz/cargo-fuzz)
- [TDS Protocol Spec](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/)

## Troubleshooting

### "error: no such subcommand: `fuzz`"
Install cargo-fuzz: `cargo install cargo-fuzz`

### "error: toolchain 'nightly' is not installed"
Install nightly: `rustup install nightly`

### Fuzzer runs too slow
- Reduce max_len: `--max_len=12`
- Disable ASAN: build in release mode
- Use more workers: `-jobs=8`

### Out of memory
- Limit RSS: `-rss_limit_mb=2048`
- Reduce workers: `-jobs=1`
- Clear corpus: `rm -rf corpus/fuzz_done_token/*`
