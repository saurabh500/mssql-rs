# Fuzz Testing for mssql-tds

This directory contains fuzz tests for the mssql-tds TDS protocol parser using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

## Setup

1. Install the nightly Rust toolchain:
```powershell
rustup install nightly
```

2. Install cargo-fuzz:
```powershell
cargo install cargo-fuzz
```

## Running Fuzz Tests

**⚠️ Windows Limitation**: cargo-fuzz with libFuzzer currently has runtime DLL dependency issues on Windows (error 0xc0000135 STATUS_DLL_NOT_FOUND). This is a known limitation of libFuzzer on Windows with MSVC.

### Recommended Solutions for Windows:
1. **Use WSL2**: Run fuzzing in Windows Subsystem for Linux (best option)
   ```bash
   wsl
   cd /mnt/c/work/mssql-tds/mssql-tds
   cargo +nightly fuzz run fuzz_done_token
   ```

2. **Use Docker**: Run in a Linux container
   ```powershell
   docker run -v ${PWD}:/workspace -it rust:latest
   cd /workspace/mssql-tds
   cargo install cargo-fuzz
   cargo +nightly fuzz run fuzz_done_token
   ```

3. **CI/CD**: Run fuzz tests on Linux-based CI (GitHub Actions, Azure Pipelines)

### Fuzz the DoneToken Parser

The `fuzz_done_token` target tests the DONE token parser with random byte inputs:

```powershell
# Run with default settings
cargo +nightly fuzz run fuzz_done_token

# Run for a specific duration (e.g., 60 seconds)
cargo +nightly fuzz run fuzz_done_token -- -max_total_time=60

# Run with more jobs (parallel workers)
cargo +nightly fuzz run fuzz_done_token -- -jobs=4

# Run with a seed corpus
cargo +nightly fuzz run fuzz_done_token corpus/fuzz_done_token/
```

### Coverage

To see code coverage:

```powershell
cargo +nightly fuzz coverage fuzz_done_token
```

### Minimizing Test Cases

If a crash is found, minimize the test case:

```powershell
cargo +nightly fuzz cmin fuzz_done_token
```

## Test Targets

### fuzz_done_token

Tests the DoneToken parser which handles DONE packets (0xFD, 0xFE, 0xFF token types).

**Input Format:** 12 bytes
- Bytes 0-1: status flags (u16, little-endian)
  - MORE = 0x0001
  - ERROR = 0x0002
  - IN_XACT = 0x0004
  - COUNT = 0x0010
  - ATTN = 0x0020
  - SERVER_ERROR = 0x0100

- Bytes 2-3: current_command (u16, little-endian)
  - None = 0x00
  - Select = 0xc1
  - Insert = 0xc3
  - Delete = 0xc4
  - Update = 0xc5
  - etc.

- Bytes 4-11: row_count (u64, little-endian)

**What it tests:**
- Bitflag parsing (DoneStatus)
- Enum conversion (CurrentCommand)
- Boundary conditions for row_count
- No panics on arbitrary byte patterns

## Creating New Fuzz Targets

1. Add a new binary to `Cargo.toml`:
```toml
[[bin]]
name = "fuzz_new_token"
path = "fuzz_targets/fuzz_new_token.rs"
test = false
doc = false
bench = false
```

2. Create the fuzz target in `fuzz_targets/fuzz_new_token.rs`:
```rust
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Your fuzzing logic here
});
```

3. Run it:
```powershell
cargo +nightly fuzz run fuzz_new_token
```

## Interpreting Results

### No Crashes
If fuzzing completes without crashes, the parser handles malformed input gracefully.

### Crash Found
If a crash is found:
1. The crashing input is saved in `fuzz/artifacts/fuzz_target_name/`
2. Examine the crash: `cargo +nightly fuzz fmt fuzz_target_name <crash_file>`
3. Debug: Add the test case to the test suite

### Common Issues to Look For
- **Panics**: Unwrap() on None, out-of-bounds access
- **Infinite loops**: Parser gets stuck on certain byte patterns
- **Memory issues**: Excessive allocations, buffer overflows
- **Logic errors**: Incorrect state transitions

## Best Practices

1. **Start Simple**: Fuzz simple, self-contained parsers first
2. **Define Input Structure**: Document the expected byte format
3. **Add to CI/CD**: Run fuzz tests periodically to catch regressions
4. **Corpus Management**: Save interesting inputs to improve coverage
5. **Minimize Before Fixing**: Always minimize crash cases before fixing

## Resources

- [Rust Fuzz Book](https://rust-fuzz.github.io/book/)
- [cargo-fuzz Guide](https://github.com/rust-fuzz/cargo-fuzz)
- [libFuzzer Documentation](https://llvm.org/docs/LibFuzzer.html)
