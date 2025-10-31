# Fuzz Testing on Windows - Known Limitations

## Issue

cargo-fuzz with libFuzzer has a known limitation on Windows with the MSVC toolchain. When attempting to run fuzz tests, you'll encounter:

```
error: process didn't exit successfully (exit code: 0xc0000135, STATUS_DLL_NOT_FOUND)
Error: Fuzz target exited with exit code: 0xc0000135
```

This error code `0xc0000135` (`STATUS_DLL_NOT_FOUND`) indicates that required LLVM/libFuzzer runtime DLLs are missing. This is a fundamental compatibility issue between libFuzzer and Windows MSVC builds.

## Root Cause

libFuzzer is part of the LLVM project and was primarily designed for Clang/Linux environments. While Rust's cargo-fuzz supports Windows, it requires:
- LLVM compiler runtime DLLs that aren't automatically available with MSVC builds
- Specific instrumentation that works better with Clang than MSVC
- Address Sanitizer (ASAN) which has limited Windows MSVC support

## Solutions

### Option 1: Windows Subsystem for Linux (WSL) ⭐ **Recommended**

The easiest and most effective solution is to use WSL:

```bash
# From PowerShell
wsl

# Inside WSL
cd /mnt/c/work/mssql-tds/mssql-tds

# Install Rust if not already installed
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup install nightly
cargo install cargo-fuzz

# Run fuzz tests
cargo +nightly fuzz run fuzz_done_token -- -max_total_time=60
```

**Pros:**
- Native Linux environment for fuzzing
- Full libFuzzer functionality
- Easy setup
- Shares Windows filesystem

**Cons:**
- Requires WSL installation

### Option 2: Docker

Run fuzzing in a Docker container:

```powershell
# Build a fuzzing container
docker run -v ${PWD}:/workspace -it rust:latest bash

# Inside container
cd /workspace/mssql-tds
rustup install nightly
cargo install cargo-fuzz
cargo +nightly fuzz run fuzz_done_token
```

**Pros:**
- Isolated Linux environment
- Reproducible across systems
- Can be integrated into CI/CD

**Cons:**
- Requires Docker installation
- Volume mounting on Windows can be slow

### Option 3: Property-Based Testing (Windows-Native) ⭐ **Available Now**

We've created Windows-compatible property-based tests that validate the same logic as fuzz tests:

```powershell
cargo test --test property_test_done_token
```

This runs 5 comprehensive tests covering:
- **1,000 pseudo-random test cases** - Random byte patterns using LCG
- **8 curated edge cases** - All zeros, all ones, alternating patterns, valid tokens
- **12 status flag combinations** - All important bitflag combinations
- **8 command enum values** - Known commands (SELECT, INSERT, etc.) + unknowns
- **8 row count boundaries** - 0, 1, 100, 1000, u32::MAX, u64::MAX/2, u64::MAX-1, u64::MAX

**Pros:**
- ✅ Works natively on Windows
- ✅ No additional tools required
- ✅ Fast execution (completes in milliseconds)
- ✅ Already integrated into test suite
- ✅ Covers the same validation logic as fuzz tests

**Cons:**
- ❌ Not coverage-guided (doesn't discover new interesting inputs automatically)
- ❌ Limited to ~1,000 iterations vs continuous fuzzing
- ❌ Requires manual test case design

**When to use:**
- ✅ Local development on Windows
- ✅ Quick validation of parser robustness
- ✅ Pre-commit checks
- ✅ CI/CD on Windows runners

### Option 4: CI/CD (Automated)

Run fuzz tests automatically on Linux-based CI:

**GitHub Actions:**
```yaml
name: Fuzz Tests
on: [push, pull_request]
jobs:
  fuzz:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: nightly
      - run: cargo install cargo-fuzz
      - run: cargo +nightly fuzz run fuzz_done_token -- -max_total_time=300
```

**Azure Pipelines:**
```yaml
- task: Bash@3
  displayName: 'Run Fuzz Tests'
  inputs:
    targetType: 'inline'
    script: |
      rustup install nightly
      cargo install cargo-fuzz
      cd mssql-tds
      cargo +nightly fuzz run fuzz_done_token -- -max_total_time=300
```

**Pros:**
- Automated testing on every commit
- Linux environment for full libFuzzer support
- No local setup required

**Cons:**
- Feedback loop is slower than local testing
- Limited fuzzing time in CI (usually 5-10 minutes max)

### Option 5: Clang on Windows (Advanced)

Install the full LLVM/Clang toolchain on Windows and configure Rust to use it:

```powershell
# Install LLVM
choco install llvm

# Configure Rust to use Clang
$env:CC="clang"
$env:CXX="clang++"
cargo +nightly fuzz run fuzz_done_token -- -linker=lld-link
```

**Pros:**
- Native Windows fuzzing possible
- Full libFuzzer functionality

**Cons:**
- ❌ Complex setup
- ❌ Toolchain compatibility issues
- ❌ Not officially supported workflow
- ❌ Not recommended by cargo-fuzz maintainers

## Recommendation

For **local development on Windows**:
1. ✅ Use property-based tests: `cargo test --test property_test_done_token`
2. ✅ Use WSL for longer fuzz campaigns

For **CI/CD**:
1. ✅ Add fuzz tests to Linux-based CI runners
2. ✅ Run property-based tests on Windows runners for quick validation

For **comprehensive fuzzing**:
1. ✅ Use WSL or Docker for multi-hour fuzz campaigns
2. ✅ Let CI run extended fuzzing overnight

## What We've Accomplished

Despite the Windows limitation, we've successfully created:

✅ **Fuzz infrastructure** - Ready to run on Linux/WSL/Docker/CI
- `fuzz/Cargo.toml` - Fuzz test package configuration
- `fuzz/fuzz_targets/fuzz_done_token.rs` - DoneToken fuzzer
- `fuzz/corpus/fuzz_done_token/` - 9 seed test cases

✅ **Documentation** - Comprehensive guides
- `fuzz/README.md` - Quick reference with Windows notes
- `docs/fuzz-testing-guide.md` - 288-line comprehensive guide
- `fuzz/WINDOWS_NOTES.md` - This file

✅ **Windows-compatible tests** - Working alternative
- `tests/property_test_done_token.rs` - 5 comprehensive tests
- Validates same logic as fuzz tests
- Passes all tests on Windows

✅ **Tooling** - Installed and ready
- cargo-fuzz v0.13.1
- Rust nightly 1.93.0-nightly

## Verification

The property-based tests confirm that the DoneToken parser validation logic is robust:

```
running 5 tests
test test_done_token_all_flag_combinations ... ok
test test_done_token_no_panic_on_arbitrary_bytes ... ok
test test_done_token_row_count_boundaries ... ok
test test_done_token_command_values ... ok
test test_done_token_random_patterns ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured
```

This validates:
- ✅ No panics on arbitrary byte patterns
- ✅ Bitflag combinations handled correctly
- ✅ Unknown command enums don't crash
- ✅ Boundary row counts work properly
- ✅ 1,000+ random inputs processed successfully

## Next Steps

1. **Integrate into CI/CD**: Add Linux-based fuzz tests to your pipeline
2. **Extend Coverage**: Create fuzz targets for other tokens (ErrorToken, InfoToken, etc.)
3. **Long-Running Fuzzing**: Run extended fuzz campaigns (hours/days) in WSL to discover edge cases
4. **Corpus Management**: Save interesting inputs discovered during fuzzing

## References

- [cargo-fuzz Known Issues](https://github.com/rust-fuzz/cargo-fuzz/issues?q=is%3Aissue+windows)
- [libFuzzer on Windows](https://github.com/rust-fuzz/cargo-fuzz/issues/277)
- [Rust Fuzz Book](https://rust-fuzz.github.io/book/)
