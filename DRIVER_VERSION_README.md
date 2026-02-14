# Driver Name and Version Feature - Quick Reference

## What Was Implemented

Support for setting the mssql-python driver name and version in TDS login packets.

## For Python Package Maintainers

To use this feature in the mssql-python package, pass the driver version when creating a connection:

```python
import mssql_py_core
from mssql_python import __version__  # Your package version

context = {
    "server": "localhost",
    "user_name": "sa", 
    "password": "password",
    "database": "master",
    "driver_version": __version__  # IMPORTANT: Add this line
}

conn = mssql_py_core.PyCoreConnection(context)
```

## What Gets Sent in TDS Login Packet

When connecting with the above code:
- **library_name**: "mssql-python" (automatically set)
- **client_prog_ver**: Encoded version number (e.g., "1.2.3" → 0x01020003)
- **application_name**: "mssql-python" (default, can be overridden)

## Version Format

The `driver_version` parameter must be a string in the format: `"major.minor.build"`

Examples:
- ✅ "1.0.0"
- ✅ "2.5.1234"
- ✅ "255.255.65535"
- ❌ "1.2" (missing build number)
- ❌ "v1.0.0" (should not include 'v' prefix)

## Backward Compatibility

The `driver_version` parameter is **optional**. If not provided:
- Connection works normally
- `client_prog_ver` is set to 0 (default behavior)

## Files Modified

1. `mssql-tds/src/connection/client_context.rs` - Core version encoding logic
2. `mssql-py-core/src/connection.rs` - Python binding integration
3. `mssql-tds/src/message/login.rs` - TDS login packet population

## Testing

Run the tests:
```bash
# Rust unit tests
cargo test encode_driver_version

# Python integration tests
pytest mssql-py-core/tests/test_driver_info.py
```

## Documentation

- `IMPLEMENTATION_SUMMARY.md` - Detailed implementation notes
- `DRIVER_VERSION_IMPLEMENTATION.md` - Technical specification

## Questions?

Refer to the work item: https://sqlclientdrivers.visualstudio.com/web/wi.aspx?pcguid=7207cf78-9b57-4b4b-b274-c803cac0efe0&id=42218
