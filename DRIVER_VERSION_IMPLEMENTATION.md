# Driver Name and Version Support for mssql-python

## Overview

This implementation adds support for setting the driver name and version information in the TDS login packet when connecting from the Python `mssql-python` driver.

## Changes Made

### 1. ClientContext Enhancement (mssql-tds)

Added a new optional field to `ClientContext`:
```rust
pub driver_version: Option<String>
```

This field stores the driver version string in the format "major.minor.build" (e.g., "1.0.0", "2.5.1234").

### 2. Version Encoding

Implemented `encode_driver_version()` method in `ClientContext` that:
- Parses the version string "major.minor.build"
- Encodes it into a 32-bit integer using the format:
  - Bits 24-31: Major version (8 bits)
  - Bits 16-23: Minor version (8 bits)
  - Bits 0-15: Build number (16 bits)
- Returns 0 if no version is provided or parsing fails

Example encodings:
- "1.2.3" → 0x01020003
- "2.5.1234" → 0x020504D2
- "255.255.65535" → 0xFFFFFFFF

### 3. Login Packet Update

Modified `LoginRequestModel::from_context()` to use the encoded driver version:
```rust
client_prog_ver: context.encode_driver_version(),
```

This populates the `client_prog_ver` field in the TDS login packet with the encoded version.

### 4. Python Wrapper (mssql-py-core)

Updated `dict_to_client_context()` to:

1. **Set library_name to "mssql-python"**:
   ```rust
   context.library_name = "mssql-python".to_string();
   ```
   This ensures the driver name in the login packet is "mssql-python".

2. **Accept optional driver_version parameter**:
   ```rust
   let driver_version = dict
       .get_item("driver_version")?
       .and_then(|v| v.extract::<String>().ok());
   context.driver_version = driver_version;
   ```

## Usage from Python

The mssql-python package can now pass the driver version when creating a connection:

```python
import mssql_py_core

context = {
    "server": "localhost",
    "user_name": "sa",
    "password": "password",
    "database": "master",
    "driver_version": "1.0.0"  # Optional, format: "major.minor.build"
}

conn = mssql_py_core.PyCoreConnection(context)
```

If `driver_version` is not provided, the `client_prog_ver` will be set to 0 (which is the default behavior).

## Behavior

### Driver Name (library_name)
- **Always** set to "mssql-python" when connecting from Python (mssql-py-core)
- This is the client interface name sent in the TDS login packet

### Driver Version (client_prog_ver)
- Determined by the optional `driver_version` parameter from Python
- If not provided: 0 (default)
- If provided: Encoded as described above

### Application Name
- Already defaults to "mssql-python" in the Python wrapper (line 142 of connection.rs)
- Can be overridden by the caller via the `application_name` parameter

## TDS Specification Compliance

The implementation follows the TDS protocol specification:
- `library_name`: Unicode string field in the login packet (also known as client interface name)
- `client_prog_ver`: 4-byte (DWORD) field in the login packet containing the encoded driver version

## Tests

Added comprehensive unit tests in `client_context.rs`:
- `test_encode_driver_version_valid`: Tests basic version encoding
- `test_encode_driver_version_max_values`: Tests maximum values (255.255.65535)
- `test_encode_driver_version_none`: Tests behavior when no version is provided
- `test_encode_driver_version_invalid`: Tests handling of invalid version strings
- `test_encode_driver_version_realistic`: Tests realistic version number

## Integration with mssql-python

The mssql-python package (in its separate repository) should:

1. Import its own version (e.g., from `__version__`)
2. Pass it to the connection context as `driver_version`

Example:
```python
from mssql_python import __version__

context = {
    "server": server,
    "user_name": user,
    "password": pwd,
    "driver_version": __version__  # e.g., "1.0.0"
}

conn = mssql_py_core.PyCoreConnection(context)
```

## Backward Compatibility

All changes are backward compatible:
- The `driver_version` parameter is optional
- If not provided, behavior is identical to before (client_prog_ver = 0)
- Existing Python code will continue to work without modifications
