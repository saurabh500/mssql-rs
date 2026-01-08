# JSON Bulk Copy Comparison: .NET vs Python/Rust

## Issue Summary
Python/Rust bulk copy with JSON columns is failing because it's serializing JSON data as `NVarChar(MAX)` (TDS type 0xE7) with UTF-16 LE encoding, instead of using the native JSON type (TDS type 0xF4) with UTF-8 encoding like .NET does.

## Test Results

### Python Test Output
- **8 failed, 190 passed, 4 skipped in 34.02s**
- All JSON tests are failing with: `json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)`
- When reading back data, SQL Server returns: `Json(Json: {"name":"Alice","age":30})` instead of just the JSON string

### Specific Failing Tests
1. `test_cursor_bulkcopy_json_basic` - JSON strings
2. `test_cursor_bulkcopy_json_from_dict` - Python dicts
3. `test_cursor_bulkcopy_json_from_list` - Python lists
4. `test_cursor_bulkcopy_json_nested_objects` - Nested structures
5. `test_cursor_bulkcopy_json_special_characters` - Special chars
6. `test_cursor_bulkcopy_json_empty_structures` - Empty objects/arrays
7. `test_cursor_bulkcopy_json_numeric_types` - Numeric types
8. `test_cursor_bulkcopy_json_boolean_values` - Boolean values

## TDS Trace Comparison

### .NET Implementation (WORKING) ✅

**Metadata:**
```
[BULKCOPY METADATA] Column 0: 'data' (Type: 35, TdsType: 0xF4)
Metadata bytes: 81 01 00 00 00 00 00 09 00 A7 FF FF 00 00 00 00 00 04 64 00 61 00 74 00 61 00
```
- **Type 35 = JSON** (native SQL Server 2025 JSON type)
- **TDS Type: 0xF4**
- 8-byte metadata sequence for JSON type

**Data Serialization:**
```
Bulk insert packet (TDS message type 0x07):
CA 03 00 00 5B 7B 22 49 64 22 3A 31 2C 22 4E 61 6D 65 22 3A 22 F0 A9 B8 BD...

Decoded UTF-8:
[{"Id":1,"Name":"𩸽json\u0915","Email":"abc@test.com","PhoneNumber":"1234567890"}]
```
- **Encoding: UTF-8** (single-byte for ASCII, multi-byte for Unicode)
- Direct JSON string sent in UTF-8 format
- Length prefix: `CA 03 00 00` = 970 bytes

### Python/Rust Implementation (FAILING) ❌

**Metadata:**
```
DEBUG bulk_load: column 'json_data' tds_type=0xE7 sql_type=NVarChar length=-1 is_plp=true
```
- **Type: NVarChar (not JSON!)**
- **TDS Type: 0xE7** (NVarChar PLP)
- Treating JSON column as string column

**Data Serialization:**
```
DEBUG serialize_json: ctx.tds_type=0xE7, ctx.is_plp=true
DEBUG serialize_json: data_len=88, bytes[0..min(20,len)]=[123, 0, 34, 0, 110, 0, 97, 0, 109, 0, 101, 0, 34, 0, 58, 0, 32, 0, 34, 0]

Decoded UTF-16 LE:
{   \0  "   \0  n   \0  a   \0  m   \0  e   \0  "   \0  :   \0      \0  "   \0
123 0   34  0   110 0   97  0   109 0   101 0   34  0   58  0   32  0   34  0

= {"name": "Alice"...
```
- **Encoding: UTF-16 LE** (2 bytes per character with null bytes)
- Sent as PLP (Partially Length Prefixed) data stream
- Uses PLP_UNKNOWNLEN marker and chunk structure
- **Wrong encoding for JSON type!**

**Data Retrieved from SQL Server:**
```
'Json(Json: {"name":"Alice","age":30})'
```
SQL Server wraps the data in a `Json()` type wrapper because it detects the column is JSON type but receives string data.

## Root Cause Analysis

### Problem
The Rust TDS implementation is not recognizing or handling the native JSON type (0xF4) correctly:

1. **Column Metadata Parsing**: When reading table metadata, JSON columns (Type 35, TDS 0xF4) are being mapped to NVarChar (0xE7)
2. **Data Serialization**: JSON values are serialized as UTF-16 LE strings (NVarChar) instead of UTF-8 JSON
3. **Type Mapping**: The `sql_type=NVarChar` shows the type mapper doesn't have a JSON case

### Expected Behavior (from .NET)
1. Recognize JSON type from metadata (Type 35, TDS 0xF4)
2. Serialize Python dicts/lists as JSON strings using UTF-8 encoding
3. Send with proper JSON type metadata (0xF4) and length prefix
4. No PLP encoding for JSON - use simple length-prefixed format

## Required Fixes

### 1. Add JSON Type Recognition
- Parse TDS type 0xF4 from column metadata
- Map SQL Server Type 35 to JSON enum variant
- Handle 8-byte JSON metadata sequence

### 2. Add JSON Serialization Logic
- Detect JSON columns in bulk copy metadata
- Serialize Python dicts/lists to JSON strings using `serde_json`
- Use UTF-8 encoding (not UTF-16 LE)
- Send with length prefix: `[4-byte length][UTF-8 JSON data]`

### 3. Update Type Coercion
- In `serialize_json` function, check if `ctx.tds_type == 0xF4`
- If JSON type: serialize to UTF-8 JSON string
- If NVarChar type: use current UTF-16 LE PLP encoding

## Testing Notes

### .NET Test (WORKING)
```bash
cd /home/saurabh/work/dotnetbcp/BulkCopyBenchmark
ENABLE_TRACE=1 ~/.dotnet/dotnet run -c Release
```
**Result:** ✅ 1 row successfully bulk copied with native JSON type

### Python Test (FAILING)
```bash
cd /home/saurabh/planning/mssql-tds
ENABLE_TRACE=1 bash dev/test-python.sh
```
**Result:** ❌ 8 failed JSON tests - data inserted but wrapped as `Json(Json: {...})`

## File Locations

### .NET Reference
- Test: `/home/saurabh/parallelwork/sqlclient/src/Microsoft.Data.SqlClient/tests/ManualTests/SQL/JsonTest/JsonBulkCopyTest.cs`
- Implementation: `Microsoft.Data.SqlClient` using `SqlDbTypeExtensions.Json`
- Working repro: `/home/saurabh/work/dotnetbcp/BulkCopyBenchmark/Program.cs`

### Python/Rust Implementation
- Tests: `/home/saurabh/planning/mssql-tds/mssql-py-core/tests/test_bulkcopy_json.py`
- Rust TDS lib: `/home/saurabh/planning/mssql-tds/tds/` (needs JSON type support)
- Serialization: Look for `serialize_json` function and `tds_type=0xE7` mapping

## Next Steps

1. ✅ **COMPLETED**: Compare TDS traces and identify root cause
2. **TODO**: Find Rust code that parses column metadata and maps types
3. **TODO**: Add TDS type 0xF4 (JSON) to type enum
4. **TODO**: Implement JSON serialization with UTF-8 encoding
5. **TODO**: Test with Python bulk copy tests
6. **TODO**: Verify all 8 JSON tests pass
