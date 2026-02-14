# API Input Fuzzing

## Overview

The API input fuzzers test **user-provided inputs** to the public APIs, complementing the existing server-response fuzzers. While `fuzz_connection_provider`, `fuzz_tds_client`, and `fuzz_token_stream` test malformed server responses, these new fuzzers test the robustness of client-side input validation and encoding.

## New Fuzz Targets

### 1. `fuzz_api_inputs`

Tests validation and handling of user inputs without actually executing against a server.

**What it tests:**
- Query strings with random/malformed SQL
- Parameter values with extreme sizes
- Invalid UTF-8/UTF-16 in strings
- Parameter names with special characters
- Stored procedure names with edge cases
- Transaction names with boundary values
- Special SQL characters and injection patterns

**Run with:**
```bash
cd mssql-tds/fuzz
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs
```

**Scenarios tested:**
1. **ExecuteQuery** - Fuzzes SQL query strings
2. **ExecuteParameterizedQuery** - Fuzzes query + parameters (sp_executesql)
3. **ExecuteStoredProcedure** - Fuzzes procedure name + parameters
4. **TransactionOps** - Fuzzes transaction names

### 2. `fuzz_parameter_encoding`

Tests the actual serialization and encoding of parameters to TDS wire format.

**What it tests:**
- SqlType serialization correctness
- Buffer management in packet writing
- Integer overflow in length calculations
- UTF-16 encoding edge cases
- Decimal encoding with extreme precision/scale
- Size estimation accuracy
- Batch parameter encoding

**Run with:**
```bash
cd mssql-tds/fuzz
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_parameter_encoding
```

**What it catches:**
- Buffer overflows in encoding logic
- Integer overflows in size calculations
- Invalid UTF-16 conversions
- Decimal encoding bugs
- Type-specific serialization issues

## Why API Input Fuzzing Matters

### Different Bug Classes

Server-response fuzzing catches protocol-level bugs:
- Malformed TDS packets
- Invalid token sequences
- Protocol state violations

API input fuzzing catches application-level bugs:
- SQL injection vulnerabilities
- Buffer overflows from user data
- Encoding bugs in parameter handling
- Integer overflows from extreme values
- Logic errors in validation

### Real-World Attack Vectors

These fuzzers simulate actual attack scenarios:

1. **Malicious Query Strings**
   - SQL injection attempts
   - Extremely long queries
   - Queries with null bytes
   - Unicode normalization attacks

2. **Extreme Parameter Values**
   - Oversized strings (> 4000 chars)
   - Very large binary data
   - Decimal values with max precision
   - Negative lengths or sizes
   - Special characters in names

3. **Invalid Encodings**
   - Invalid UTF-8 sequences
   - Unpaired UTF-16 surrogates
   - Null bytes in strings
   - Control characters

## Architecture

### Input Sanitization

Both fuzzers implement sanitization to prevent timeouts:

```rust
const MAX_STRING_LEN: usize = 4096;  // Prevent timeout from huge strings
const MAX_BINARY_LEN: usize = 2048;  // Limit binary data size
const MAX_PARAMS: usize = 20;         // Limit parameter count
```

This allows the fuzzer to explore more interesting edge cases instead of spending time on obviously invalid inputs.

### Arbitrary-based Generation

Uses the `arbitrary` crate for structured fuzzing:

```rust
#[derive(Debug, Arbitrary)]
enum FuzzScenario {
    ExecuteQuery(FuzzQuery),
    ExecuteParameterizedQuery { ... },
    ExecuteStoredProcedure(FuzzStoredProcedure),
    TransactionOps(FuzzTransaction),
}
```

This generates well-formed but potentially malicious inputs that exercise real code paths.

## Integration with Existing Fuzzers

### Complete Coverage Matrix

| Fuzzer | Target | Input Type | Focus |
|--------|--------|------------|-------|
| `fuzz_token_stream` | Token parser | Server TDS tokens | Protocol parsing |
| `fuzz_tds_client` | TdsClient | Server responses | Query execution flow |
| `fuzz_connection_provider` | Connection | Server handshake | Connection establishment |
| **`fuzz_api_inputs`** | **Public APIs** | **User inputs** | **Input validation** |
| **`fuzz_parameter_encoding`** | **Serialization** | **User parameters** | **Wire format encoding** |

### Complementary Strategies

1. **Server Response Fuzzing** (existing)
   - Tests: "What if the server sends garbage?"
   - Catches: Protocol handling bugs
   - Example: Malformed DONE token

2. **API Input Fuzzing** (new)
   - Tests: "What if the user sends garbage?"
   - Catches: Input validation bugs
   - Example: Query with 1MB of special characters

## Running the Fuzzers

### Quick Test (1 minute each)

```bash
cd mssql-tds/fuzz

# Test API input validation
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs -- -max_total_time=60

# Test parameter encoding
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_parameter_encoding -- -max_total_time=60
```

### Extended Fuzzing (overnight)

```bash
# Run with multiple forks for parallel fuzzing
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs -- \
    -max_total_time=28800 \
    -fork=4 \
    -ignore_crashes=1

RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_parameter_encoding -- \
    -max_total_time=28800 \
    -fork=4 \
    -ignore_crashes=1
```

### Corpus Management

The fuzzer automatically saves interesting inputs:

```
mssql-tds/fuzz/corpus/fuzz_api_inputs/
mssql-tds/fuzz/corpus/fuzz_parameter_encoding/
```

Crashes are saved to:

```
mssql-tds/fuzz/artifacts/fuzz_api_inputs/
mssql-tds/fuzz/artifacts/fuzz_parameter_encoding/
```

## Expected Findings

### Common Issues to Watch For

1. **String Handling**
   - Buffer overflows from oversized strings
   - Invalid UTF-8/UTF-16 conversions
   - Null byte handling
   - Memory allocation failures

2. **Integer Overflows**
   - Length calculations: `len * 2` for UTF-16
   - Size estimations for buffer allocation
   - Parameter count accumulation

3. **Type Conversions**
   - Decimal precision/scale validation
   - Floating point edge cases (NaN, infinity)
   - Binary data length mismatches

4. **Parameter Encoding**
   - Type-specific serialization bugs
   - NULL value handling
   - Output parameter flag handling

## Future Enhancements

### Potential Additional Fuzzers

1. **`fuzz_connection_context`**
   - Fuzz ClientContext fields
   - Test connection string parsing
   - Validate authentication parameters

2. **`fuzz_result_deserialization`**
   - Fuzz result set data with extreme values
   - Test column metadata handling
   - Validate type conversions

3. **`fuzz_bulk_copy`**
   - Fuzz bulk insert data
   - Test large batch sizes
   - Validate column mapping

### Dictionary-Based Fuzzing

Add dictionaries for SQL-specific fuzzing:

```
mssql-tds/fuzz/dict/sql_keywords.dict
mssql-tds/fuzz/dict/special_chars.dict
mssql-tds/fuzz/dict/unicode_edge_cases.dict
```

## Debugging Crashes

When a crash is found:

```bash
# Reproduce the crash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs \
    artifacts/fuzz_api_inputs/crash-XXXXX

# Get a backtrace
RUST_BACKTRACE=1 RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs \
    artifacts/fuzz_api_inputs/crash-XXXXX

# Minimize the crash input
cargo +nightly fuzz tmin fuzz_api_inputs artifacts/fuzz_api_inputs/crash-XXXXX
```

## Performance Considerations

### Timeout Prevention

The fuzzers use aggressive size limits to prevent timeouts:
- Strings: max 4KB
- Binary data: max 2KB  
- Parameter count: max 20

This keeps execution time under control while still testing interesting edge cases.

### Memory Limits

The fuzzer automatically limits memory usage:
- RSS limit: 2GB per process
- Timeout: 20 seconds per input (default)

## CI/CD Integration

### Recommended Pipeline

```yaml
# .github/workflows/fuzz.yml
- name: Run API input fuzzers
  run: |
    cd mssql-tds/fuzz
    
    # Quick smoke test (5 min each)
    RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_api_inputs -- -max_total_time=300
    RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_parameter_encoding -- -max_total_time=300
    
    # Upload any crashes as artifacts
    if [ -d artifacts ]; then
      tar czf fuzz-crashes.tar.gz artifacts/
    fi
```

## References

- [libFuzzer Documentation](https://llvm.org/docs/LibFuzzer.html)
- [cargo-fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz.html)
- [Arbitrary Crate](https://docs.rs/arbitrary/)
- [TDS Protocol Specification](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/)
