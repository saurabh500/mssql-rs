# TDS Connection Provider Fuzzing

## Overview

The connection provider fuzzing suite consists of three separate fuzzers that test different aspects of `TdsConnectionProvider`:

1. **fuzz_connection_provider** - Original fuzzer testing complete connection flow
2. **fuzz_connection_provider_network** - Tests server response variations with fixed ClientContext
3. **fuzz_connection_provider_context** - Tests ClientContext variations with minimal server responses

This separation allows focused testing of specific attack surfaces and more efficient fuzzing.

## Fuzzers

### 1. fuzz_connection_provider (Original)
Tests the complete `TdsConnectionProvider::create_client_with_transport()` API by injecting fuzzed server responses through a MockTransport.

**What It Tests:**
- Prelogin handshake with malformed responses
- Login sequence with various packet combinations
- Feature negotiation with invalid data
- Connection timeout handling
- Protocol edge cases with mixed inputs

**Use Case:** General-purpose fuzzing of connection establishment

### 2. fuzz_connection_provider_network (New)
Isolates testing of server response handling by using a fixed default ClientContext and fuzzing only the network data.

**What It Tests:**
- Prelogin handshake with malformed/fuzzed responses
- Login sequence with various packet combinations
- Feature negotiation with invalid data
- Connection timeout handling
- Cancellation during connection

### Protocol Edge Cases
- Malformed TDS packets during connection
- Invalid token sequences in login response
- Corrupted authentication data
- Unexpected server responses
- Connection redirection with fuzzed routing info

### Error Handling
- Graceful handling of truncated packets
- Recovery from protocol violations
- Proper cleanup on connection failure
- Memory safety with malformed input

## Implementation

### Architecture

The fuzzer uses a multi-layer approach:

```
Fuzzer Input → FuzzReader → MockTransport → TdsConnectionProvider
     (raw bytes)   (TdsPacketReader)   (TdsTransport)   (public API)
```

1. **FuzzReader**: Implements `TdsPacketReader` trait, wraps raw fuzz input bytes
2. **MockTransport**: Simulates TDS server responses using FuzzReader
3. **TdsConnectionProvider**: Real production code being tested

### Key Components

#### FuzzReader
- Reads fuzzed data as TDS packets
- Implements all packet reading methods
- Returns EOF when input exhausted
- Protects against excessive allocations

#### MockTransport
- Implements `TdsTransport` trait
- Uses `TokenStreamReader` for token parsing
- Provides fake network writer (no-op)
- Enables testing without network I/O

#### Test Flow
```rust
// Create reader from fuzz input
let reader = Box::new(FuzzReader::new(data));

// Create mock transport
let transport = MockTransport::new(reader, 4096);

// Create default context
let context = ClientContext::default();

// Test the actual public API
let result = TdsConnectionProvider::create_client_with_transport(
    context, 
    transport
).await;

// We don't care about the result, just that it doesn't panic
let _ = result;
```

### Implementation Changes

To enable fuzzing, we added a fuzzing-only method to `TdsConnectionProvider`:

```rust
#[cfg(fuzzing)]
pub async fn create_client_with_transport<T>(
    context: ClientContext,
    transport: T,
) -> TdsResult<TdsClient>
where
    T: TdsTransport + NetworkReaderWriter + TdsTokenStreamReader + TdsPacketReader + 'static
{
    // Same code path as normal connections, but with injected transport
    let (transport, negotiated_settings, execution_context) =
        Self::connect_with_transport(&context, &context.transport_context, transport).await?;
    Ok(TdsClient::new(transport, negotiated_settings, execution_context))
}
```

This method:
- Only compiled when `fuzzing` cfg is enabled
- Takes a generic transport (MockTransport for fuzzing, NetworkTransport for production)
- Exercises the same internal connection logic as real connections
- Allows testing without network dependencies

## Running the Fuzzers

### Original Connection Provider Fuzzer
```bash
cd mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider
```

### Network Response Fuzzer
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_network
```

### ClientContext Fuzzer
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_context
```

### With Time Limit
```bash
# Any fuzzer can use time limits
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_network -- -max_total_time=3600
```

### With Multiple Workers
```bash
# Any fuzzer can use multiple workers
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_context -- -workers=4
```

### Run All Three in Parallel
```bash
# Terminal 1
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider -- -max_total_time=3600 &

# Terminal 2
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_network -- -max_total_time=3600 &

# Terminal 3
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider_context -- -max_total_time=3600 &
```

## Expected Behavior

The fuzzer should:
- ✅ Handle all malformed input gracefully (return errors, not panic)
- ✅ Never cause memory unsafety
- ✅ Never have unbounded allocations
- ✅ Properly clean up resources on error
- ✅ Test the actual public API used by real applications

## Benefits

1. **Real API Testing**: Tests the actual public API that applications use
2. **Focused Testing**: Separate fuzzers allow targeting specific attack surfaces
3. **Efficient Fuzzing**: Isolated testing finds bugs faster than mixed fuzzing
4. **Comprehensive Coverage**: Combined coverage of protocol, client config, and integration
5. **Protocol Robustness**: Finds edge cases in TDS protocol handling
6. **Memory Safety**: Discovers potential buffer overflows or allocation issues
7. **No Dependencies**: Runs without needing a SQL Server instance

## Why Three Fuzzers?

**Separation of Concerns:**
- Network fuzzer focuses on protocol parsing without ClientContext noise
- Context fuzzer focuses on configuration validation without protocol noise
- Original fuzzer provides integration testing of both together

**Better Fuzzing Efficiency:**
- Smaller input space per fuzzer means faster convergence
- Each fuzzer can build a specialized corpus
- Easier to reproduce and debug issues

**Clear Responsibility:**
- Network fuzzer finds protocol bugs
- Context fuzzer finds configuration bugs
- Original fuzzer finds interaction bugs

## Technical Details

### Memory Safety
- All allocations are bounded by `MAX_ALLOC` constant (1MB)
- String parsing from UTF-16 handles invalid sequences gracefully
- Buffer overruns protected by length checks

### Timeout Handling
- Fuzzer uses tokio runtime but doesn't set connection timeouts
- Allows fuzzer to focus on protocol issues rather than timing

### Comparison with Previous Approach

**Previous (Incorrect) Approach:**
- Tested internal `SessionHandler` directly
- Did not test the public API
- Missed timeout handling and redirection logic
- Did not exercise `TdsConnectionProvider::create_client()`

**Current (Correct) Approach:**
- Tests `TdsConnectionProvider::create_client_with_transport()` public API
- Exercises all connection logic including timeouts and redirection
- Uses dependency injection via trait to enable testing
- Tests the same code path that real applications use

## Future Improvements

Potential enhancements:
1. Generate more realistic packet structures for network fuzzer
2. Add corpus of real server responses for mutation-based fuzzing
3. Track code coverage to identify untested paths
4. Fuzz specific authentication methods (NTLM, Kerberos, etc.)
5. Test connection redirection scenarios specifically
6. Add combined fuzzer that fuzzes both ClientContext and network responses
7. Add dictionary files for SQL keywords and common configuration values

## Related Documentation

- [Fuzzing Quick Start](FUZZING_QUICKSTART.md) - How to run all fuzzers
- [Token Stream Fuzzing](FUZZING_TOKEN_STREAM_IMPLEMENTATION.md) - Low-level token parsing fuzzing
- [TDS Client Fuzzing](FUZZING_TDS_CLIENT_IMPLEMENTATION.md) - Query execution fuzzing
