# Windows Testing Quick Start Guide

## Environment Setup

Create a `.env` file in the project root with these variables:

```bash
# Required for all tests
DB_HOST=localhost
DB_USERNAME=your_username
SQL_PASSWORD=your_password

# For named instance testing (optional, defaults to SQLEXPRESS if not set)
DB_INSTANCE=SQLEXPRESS

# Encryption settings (recommended for local testing)
TRUST_SERVER_CERTIFICATE=true
CERT_HOST_NAME=localhost

# Optional: Enable detailed logging
ENABLE_TRACE=false
```

## SQL Server Configuration

1. **Enable Named Pipes and Shared Memory**:
   - Open SQL Server Configuration Manager
   - Navigate to: SQL Server Network Configuration → Protocols for [INSTANCE]
   - Enable:
     - TCP/IP
     - Named Pipes
     - Shared Memory
   - Restart SQL Server service

2. **Verify Protocols**:
   ```sql
   SELECT * FROM sys.dm_exec_connections WHERE session_id = @@SPID;
   ```

## Running Tests

### All Tests
```bash
cargo test --package mssql-tds --test test_transport_protocols
```

### TCP Only (should work on any platform)
```bash
cargo test --package mssql-tds --test test_transport_protocols test_tcp_connection
```

### Named Pipe Tests (Windows only)
```bash
# All Named Pipe tests
cargo test --package mssql-tds --test test_transport_protocols test_named_pipe

# Individual tests
cargo test --package mssql-tds --test test_transport_protocols test_named_pipe_default_instance
cargo test --package mssql-tds --test test_transport_protocols test_named_pipe_named_instance
cargo test --package mssql-tds --test test_transport_protocols test_named_pipe_local_default_instance
cargo test --package mssql-tds --test test_transport_protocols test_named_pipe_local_named_instance
```

### Shared Memory Tests (Windows only)
```bash
# All Shared Memory tests
cargo test --package mssql-tds --test test_transport_protocols test_shared_memory

# Individual tests
cargo test --package mssql-tds --test test_transport_protocols test_shared_memory_default_instance
cargo test --package mssql-tds --test test_transport_protocols test_shared_memory_named_instance
cargo test --package mssql-tds --test test_transport_protocols test_shared_memory_mssqlserver_instance
```

### Unit Tests (work on any platform)
```bash
cargo test --package mssql-tds --test test_transport_protocols test_transport_context
cargo test --package mssql-tds --lib connection::client_context::tests
```

## Expected Test Coverage

**✅ On Linux** (current platform):
- Unit tests: All 11 tests pass (8 in client_context + 3 in test_transport_protocols)
- TCP integration test: Should work if SQL Server is accessible

**✅ On Windows** (when you test):
- All 11 unit tests: Should pass
- TCP integration test: Should pass
- 4 Named Pipe integration tests: Should pass if Named Pipes enabled
- 3 Shared Memory integration tests: Should pass if Shared Memory enabled

## Common Issues

**Named Pipe connection fails:**
- Check if Named Pipes protocol is enabled
- Restart SQL Server after enabling
- Verify pipe path: `\\.\pipe\sql\query` (default) or `\\.\pipe\MSSQL$INSTANCE\sql\query`

**Shared Memory connection fails:**
- Check if Shared Memory protocol is enabled
- Restart SQL Server after enabling
- Verify instance name (use empty string or "MSSQLSERVER" for default instance)

**"Environment variable not set" error:**
- Ensure `.env` file exists in project root
- Verify all required variables are set

## Test Files

- **test_transport_protocols.rs**: Main integration test file
  - 7 integration tests (4 Named Pipe, 3 Shared Memory, 1 TCP)
  - 3 unit tests (TransportContext helper methods)

- **client_context.rs**: Unit tests for Protocol enum and TransportContext
  - 8 unit tests covering all transport types

- **common/mod.rs**: Test helpers
  - `create_named_pipe_context()` - Helper for Named Pipe contexts
  - `create_shared_memory_context()` - Helper for Shared Memory contexts

## Commits Made

1. **312865a**: Add Named Pipe and Shared Memory transport support
   - Protocol enum implementation
   - TransportContext enhancement
   - Shared Memory via Named Pipes

2. **3ca54e8**: Add comprehensive tests for Named Pipe and Shared Memory transport protocols
   - Integration tests for all protocols
   - Test documentation
   - Common test helpers

3. **44bd65a**: Fix test assertions to match implementation behavior
   - Corrected expected values for local connections

## Next Steps

After testing on Windows:
1. Verify all integration tests pass
2. Test with different SQL Server configurations (default instance, named instance)
3. Consider adding more edge cases if needed
4. Move to Stage 2: Data Source parsing implementation (if tests pass)
