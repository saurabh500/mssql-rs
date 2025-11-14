# TDS Connection Provider Fuzzing

## Overview

The connection provider fuzzer tests the `TdsConnectionProvider::create_client_with_transport()` API by injecting fuzzed server responses through a MockTransport. This allows comprehensive testing of the connection establishment flow without requiring a real SQL Server.

## What It Tests

### Connection Establishment
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

## Running the Fuzzer

### Basic Run
```bash
cd mssql-tds
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider
```

### With Time Limit
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider -- -max_total_time=3600
```

### With Multiple Workers
```bash
RUSTFLAGS="--cfg fuzzing" cargo +nightly fuzz run fuzz_connection_provider -- -workers=4
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
2. **Comprehensive Coverage**: Exercises prelogin, login, feature negotiation, error handling
3. **Protocol Robustness**: Finds edge cases in TDS protocol handling
4. **Memory Safety**: Discovers potential buffer overflows or allocation issues
5. **No Dependencies**: Runs without needing a SQL Server instance

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
1. Generate more realistic packet structures for better fuzzing quality
2. Add corpus of real server responses for mutation-based fuzzing
3. Track code coverage to identify untested paths
4. Fuzz specific authentication methods (NTLM, Kerberos, etc.)
5. Test connection redirection scenarios specifically

## Related Documentation

- [Fuzzing Quick Start](FUZZING_QUICKSTART.md) - How to run all fuzzers
- [Token Stream Fuzzing](FUZZING_TOKEN_STREAM_IMPLEMENTATION.md) - Low-level token parsing fuzzing
- [TDS Client Fuzzing](FUZZING_TDS_CLIENT_IMPLEMENTATION.md) - Query execution fuzzing
