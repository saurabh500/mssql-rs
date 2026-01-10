# No-Protocol Connection Resolution Tests

This test suite validates ODBC-compatible behavior for datasource connections when no explicit protocol is specified.

## Test Coverage

### 1. **Explicit Protocol Tests (Baseline)**
- `test_explicit_tcp_with_port` - TCP with explicit port
- `test_explicit_named_pipe` - Named Pipe with explicit path

### 2. **No-Protocol Resolution Tests**
- `test_no_protocol_localhost_default_port` - localhost without protocol/port
- `test_no_protocol_dot_local` - "." shorthand
- `test_no_protocol_127_0_0_1` - Loopback IP
- `test_no_protocol_with_explicit_port` - Port without protocol (should default to TCP)

### 3. **Protocol Fallback Tests** (Windows Only)
- `test_protocol_fallback_order_with_encryption_on` - Verifies protocol precedence:
  1. Shared Memory (local only)
  2. TCP
  3. Named Pipes
- `test_named_pipe_auto_format` - UNC path auto-detection (`\\server\pipe\path`)

### 4. **SSRP Error Handling Tests**
- `test_instance_name_without_port_returns_error` - Named instance requires SSRP
- `test_named_instance_with_named_pipe_format_returns_error` - tcp:server\instance error

### 5. **ODBC Compatibility Tests**
- `test_port_takes_priority_over_instance` - Port ignores instance name
- `test_various_localhost_formats` - localhost, ., 127.0.0.1, (local)
- `test_no_protocol_uses_default_1433` - Defaults to port 1433
- `test_whitespace_handling` - Trims whitespace like ODBC
- `test_case_insensitive_protocol` - TCP/tcp/Tcp all work
- `test_empty_instance_name_ignored` - server\ without instance

## Prerequisites

1. **Local SQL Server** running on `localhost:1433`
2. **Protocols enabled**: TCP/IP, Named Pipes, Shared Memory
3. **SQL Authentication**: sa account with password

## Environment Variables

```powershell
$env:SQL_PASSWORD = "YourPassword"
$env:DB_USERNAME = "sa"
$env:DB_HOST = "localhost"
```

## Running Tests

### Run All Tests
```powershell
.\run-no-protocol-tests.ps1
```

### Run Individual Test
```powershell
$env:SQL_PASSWORD = "YourPassword"
$env:DB_USERNAME = "sa"
cargo test test_protocol_fallback --test test_no_protocol_resolution -- --nocapture
```

### Run with Debug Logging
```powershell
$env:RUST_LOG = "debug"
cargo test <test_name> --test test_no_protocol_resolution -- --nocapture
```

## Expected Behavior

### When No Protocol Specified

**Local Server** (localhost, ., 127.0.0.1):
1. Try Shared Memory first (Windows only)
2. If fails, try TCP on default port 1433
3. If fails, try Named Pipes

**Remote Server**:
1. Try TCP on default port 1433
2. If fails, try Named Pipes (Windows only)

### When Protocol Specified

- **tcp:server,port** - Use TCP explicitly
- **np:path** - Use Named Pipes explicitly
- **server,port** (no protocol) - Auto-defaults to TCP
- **\\server\pipe\path** (UNC) - Auto-detected as Named Pipe

### Named Instance Behavior

- **server\INSTANCE** - Returns error (SSRP not implemented)
- **server\INSTANCE,port** - Ignores instance, uses port
- **tcp:server,port** - Works (explicit protocol + port)

## Test Status

✅ Explicit protocol connections  
✅ No-protocol default port connections  
✅ Protocol fallback logic  
✅ SSRP error handling  
✅ ODBC compatibility (port priority, localhost detection, etc.)  
⚠️ Shared Memory transport (stub exists, needs implementation)  
⚠️ Named instance resolution (requires SSRP implementation)

## Architecture Notes

The implementation follows ODBC's approach:

1. **Parsing**: [client_context.rs](../mssql-tds/src/connection/client_context.rs)
   - Detects protocol prefix (tcp:, np:, etc.)
   - Extracts port from comma separator
   - Identifies named pipe UNC paths
   - Parses instance names from backslash

2. **Protocol Resolution**: [tds_connection_provider.rs](../mssql-tds/src/connection_provider/tds_connection_provider.rs)
   - `resolve_transport_contexts()` - Builds protocol list
   - `has_explicit_transport()` - Checks for explicit protocol/port
   - `is_local_server()` - Detects localhost variations
   - `needs_ssrp_query()` - Determines if SSRP needed (stub)

3. **SSRP Stub**: [ssrp.rs](../mssql-tds/src/ssrp.rs)
   - `get_instance_info()` - Query SQL Browser (not implemented)
   - `get_admin_port()` - Query DAC port (not implemented)
   - Returns clear error messages directing users to explicit protocols

## Known Limitations

1. **SSRP Not Implemented**: Named instances (`server\INSTANCE`) return error
   - Workaround: Use explicit port (`tcp:server,1433`)
   
2. **Shared Memory Not Implemented**: Listed in protocol order but not functional
   - Falls back to TCP automatically

3. **LocalDB Not Tested**: Special handling exists but not validated

## Future Work

- Implement SSRP protocol for named instance resolution
- Implement Shared Memory transport (Windows only)
- Add more error codes (SNIE_45, SNIE_46, SNIE_28)
- Test with clustered servers
- Test with Azure SQL Database
