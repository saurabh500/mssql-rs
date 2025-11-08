# Fuzzing Setup and Usage

This directory contains the fuzzing infrastructure for MSSQL-TDS token stream parsing.

## Quick Start on a New Machine

1. **Navigate to the project directory:**
   ```bash
   cd mssql-tds/mssql-tds
   ```

2. **Run the setup and fuzz script:**
   ```bash
   ./fuzz/run-fuzz.sh [duration_in_seconds] [target_name]
   ```

   Examples:
   ```bash
   # Run for 60 seconds (default)
   ./fuzz/run-fuzz.sh

   # Run for 5 minutes
   ./fuzz/run-fuzz.sh 300

   # Run specific target for 2 minutes
   ./fuzz/run-fuzz.sh 120 fuzz_token_stream
   ```

## What the Script Does

The `run-fuzz.sh` script automatically:

1. ✅ Checks if Rust nightly toolchain is installed (installs if missing)
2. ✅ Checks if cargo-fuzz is installed (installs if missing)
3. ✅ Extracts the corpus from `corpus-fuzz_token_stream.tar.gz` if needed
4. ✅ Runs the fuzzer for the specified duration
5. ✅ Reports any crashes found
6. ✅ Shows corpus statistics

## Files in This Directory

- **`run-fuzz.sh`** - Main script to set up and run fuzzing
- **`corpus-fuzz_token_stream.tar.gz`** - Compressed corpus (135 files, 6KB compressed)
- **`Cargo.toml`** - Fuzz targets configuration
- **`fuzz_targets/`** - Fuzz target implementations
  - `fuzz_token_stream.rs` - Main token stream fuzzer
- **`corpus/`** - Uncompressed corpus files (extracted automatically)
- **`artifacts/`** - Crash files (if any are found)

## Manual Setup (Alternative)

If you prefer manual setup:

```bash
# Install nightly toolchain
rustup toolchain install nightly

# Install cargo-fuzz
cargo +nightly install cargo-fuzz

# Extract corpus (from mssql-tds/mssql-tds directory)
cd fuzz
tar -xzf corpus-fuzz_token_stream.tar.gz
cd ..

# Run fuzzer
cd fuzz
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_token_stream -- \
    -max_total_time=60 \
    -timeout=10
```

## Corpus Management

### Minimize Corpus

After fuzzing, minimize the corpus to remove redundant test cases:

```bash
cd fuzz
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz cmin fuzz_token_stream
```

### Compress Updated Corpus

If you want to save the updated corpus:

```bash
cd fuzz
tar -czf corpus-fuzz_token_stream.tar.gz corpus/fuzz_token_stream/
```

## Continuous Fuzzing

For longer fuzzing sessions (e.g., overnight):

```bash
cd fuzz
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_token_stream -- \
    -max_total_time=28800  # 8 hours
```

## If Crashes Are Found

1. Crashes are saved in `fuzz/artifacts/fuzz_token_stream/crash-*`
2. Reproduce the crash:
   ```bash
   cd fuzz
   RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_token_stream \
       artifacts/fuzz_token_stream/crash-HASH -- -runs=1
   ```
3. Document the bug in `../fuzz-bugs-found.md` following the existing format
4. Fix the bug in the main codebase
5. Verify the fix:
   ```bash
   # Test that the crash no longer occurs
   RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_token_stream \
       artifacts/fuzz_token_stream/crash-HASH -- -runs=1
   ```
6. Re-run fuzzing to confirm no new crashes

## CI/CD Integration

To run fuzzing in CI:

```bash
# Short run for PR validation (30 seconds)
./fuzz/run-fuzz.sh 30

# Check exit code (non-zero if crashes found)
if [ $? -ne 0 ]; then
    echo "Fuzzing found crashes!"
    exit 1
fi
```

## Resources

- [cargo-fuzz documentation](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [libFuzzer options](https://llvm.org/docs/LibFuzzer.html#options)
- Bug tracking: See `../fuzz-bugs-found.md`
