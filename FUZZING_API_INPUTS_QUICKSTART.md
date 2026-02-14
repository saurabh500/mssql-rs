# API Input Fuzzing - Quick Start

## Summary

I've created two new fuzz targets that focus on **API input fuzzing** to complement the existing server-response fuzzers:

### New Fuzz Targets

1. **`fuzz_api_inputs`** - Tests validation and handling of user-provided inputs
   - Query strings with malicious/malformed SQL
   - Parameter names with special characters
   - Parameter values with extreme sizes
   - Stored procedure names with edge cases
   - Transaction names with boundary values

2. **`fuzz_parameter_encoding`** - Tests serialization and encoding logic
   - UTF-16 encoding of strings
   - Size calculations and integer overflows
   - Binary data handling
   - Float special values (NaN, infinity)

## What's Different

| Aspect | Server Response Fuzzers | API Input Fuzzers |
|--------|------------------------|-------------------|
| **Target** | Protocol parsing | Input validation & encoding |
| **Input Source** | Simulated server | Simulated user/application |
| **Bug Types** | Protocol violations, parsing crashes | SQL injection, buffer overflows, encoding bugs |
| **Examples** | Malformed DONE token | 10MB query string, invalid UTF-16 |

## Quick Test

```bash
cd mssql-tds/fuzz

# Test API inputs (30 seconds)
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs -- -max_total_time=30

# Test parameter encoding (30 seconds)
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_parameter_encoding -- -max_total_time=30
```

## What Gets Fuzzed

### Query Strings
```rust
// Examples of fuzzed inputs:
"SELECT * FROM users WHERE id = '\0\0\0'"
"'; DROP TABLE users; --"
"SELECT " + "A" * 10000
"SELECT 你好世界"  // Unicode
```

### Parameter Values
```rust
// Extreme values:
NVarchar(Some("X".repeat(10000)), 50)  // String exceeds declared length
VarBinary(Some(vec![0u8; 100000]), 100)  // Binary exceeds length
Float(Some(f64::NAN))  // Special float values
```

### Parameter Names
```rust
// Edge cases:
Some("@" + "\0" * 100)  // Null bytes
Some("'; DROP TABLE--")  // SQL injection attempt
Some("参数")  // Unicode  
None  // Missing name
```

## Files Added

- `/home/saurabh/work/mssql-tds/mssql-tds/fuzz/fuzz_targets/fuzz_api_inputs.rs`
- `/home/saurabh/work/mssql-tds/mssql-tds/fuzz/fuzz_targets/fuzz_parameter_encoding.rs`
- `/home/saurabh/work/mssql-tds/FUZZING_API_INPUTS.md` (comprehensive documentation)

## Next Steps

1. **Run Extended Fuzzing**
   ```bash
   # Overnight run with 4 parallel workers
   RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs -- \
       -max_total_time=28800 -fork=4 -ignore_crashes=1
   ```

2. **Add to CI/CD**
   - Run each fuzzer for 5 minutes in CI
   - Save crash artifacts
   - Block on new crashes

3. **Enhance Fuzzers** (future)
   - Add dictionary for SQL keywords
   - Test connection context fuzzing
   - Add bulk copy fuzzing

## Documentation

See `FUZZING_API_INPUTS.md` for:
- Detailed architecture
- Integration with existing fuzzers
- Debugging crashes
- Performance considerations
- CI/CD integration examples
