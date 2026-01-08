# Binary Bulk Copy Implementation Summary

## Overview
Successfully implemented binary (BINARY/VARBINARY) type support for bulk copy operations in the Python mssql-py-core library.

## Implementation Details

### 1. Python Type Conversion
**File:** `mssql-py-core/src/types.rs`
- Added support for converting Python `bytes` objects to `ColumnValues::Bytes`
- Added support for converting Python `bytearray` objects to `ColumnValues::Bytes`
- Import added: `PyByteArray` from `pyo3::types`

### 2. TDS Value Serialization
**File:** `mssql-tds/src/datatypes/tds_value_serializer.rs`

#### TdsTypeContext Enhancement
- Added `is_fixed_length` field to distinguish between BINARY(n) and VARBINARY(n) types
- BINARY(n) is fixed-length (TypeLength::Fixed)
- VARBINARY(n) is variable-length (TypeLength::Variable)

#### serialize_bytes() Implementation
Implemented three encoding paths based on type metadata:

1. **PLP Types (MAX types):**
   - Write 8-byte total length
   - Write 4-byte chunk length
   - Write actual data bytes
   - Write 4-byte terminator (0x00000000)

2. **Fixed-Length BINARY(n):**
   - Write exactly n bytes (no length prefix)
   - Write actual data bytes
   - Pad with zeros if data_len < n
   - Matches .NET SqlBulkCopy behavior

3. **Variable-Length VARBINARY(n):**
   - Write 2-byte length prefix (actual data length)
   - Write actual data bytes
   - No padding

### 3. Metadata Handling
**File:** `mssql-tds/src/message/bulk_load.rs`
- Modified context creation to set `is_fixed_length` from `BulkCopyColumnMetadata.length_type.is_fixed()`

## TDS Protocol Details

### Type Bytes
- `0xAD`: BINARY/VARBINARY types
- `0xA5`: VARBINARY types (alternative encoding)

### ROW Token Encoding
- **BINARY(n)**: Writes exactly n bytes with NO length prefix
- **VARBINARY(n)**: Writes 2-byte length prefix + variable data
- **VARBINARY(MAX)**: Uses PLP encoding (8-byte length + chunked data + terminator)

## Test Coverage

### Test File
`mssql-py-core/tests/test_bulkcopy_binary.py` (683 lines, 12 tests)

### Tests Implemented
1. ✅ **test_cursor_bulkcopy_binary_basic**: Basic insertion of byte arrays
2. ✅ **test_cursor_bulkcopy_binary_padding**: BINARY(n) zero-padding validation
3. ✅ **test_cursor_bulkcopy_binary_varbinary_no_padding**: VARBINARY(n) no-padding validation
4. ✅ **test_cursor_bulkcopy_binary_too_large**: BINARY size overflow error
5. ✅ **test_cursor_bulkcopy_varbinary_too_large**: VARBINARY size overflow error
6. ✅ **test_cursor_bulkcopy_binary_null_handling**: NULL value insertion
7. ✅ **test_cursor_bulkcopy_binary_null_to_non_nullable**: NULL to NOT NULL error
8. ✅ **test_cursor_bulkcopy_string_to_binary_fails**: String type rejection
9. ✅ **test_cursor_bulkcopy_int_to_binary_fails**: Integer type rejection
10. ✅ **test_cursor_bulkcopy_binary_with_explicit_encoding**: string.encode() support
11. ✅ **test_cursor_bulkcopy_binary_empty_bytes**: Empty bytes handling
12. ✅ **test_cursor_bulkcopy_binary_bytearray_type**: bytearray type support

### Test Results
```
12 passed in 2.06s
```

## Behavior Comparison with .NET

### Similarities (Matching .NET SqlBulkCopy)
- ✅ Only accepts byte[] (Python: bytes/bytearray)
- ✅ Rejects string-to-binary conversion
- ✅ Rejects number-to-binary conversion
- ✅ BINARY(n) pads with zeros to fixed size n
- ✅ VARBINARY(n) stores exact byte count (no padding)
- ✅ Size validation (data_len ≤ schema_size)
- ✅ NULL handling for nullable columns
- ✅ Error on NULL to NOT NULL columns

### Differences
- Python accepts both `bytes` (immutable) and `bytearray` (mutable)
- .NET only has `byte[]` (similar to Python's bytearray)

## Key Technical Challenges Resolved

### 1. Build System Issue
**Problem:** `maturin develop` cached old .so files and didn't update them despite successful compilation

**Solution:** Use full reinstallation cycle:
```bash
pip uninstall -y mssql-py-core
maturin build
pip install target/wheels/mssql_py_core*.whl --force-reinstall
```

### 2. TDS Protocol Encoding
**Problem:** Initially wrote 2-byte length prefix for all non-PLP types, causing "premature end-of-message" error

**Solution:** Distinguished BINARY (fixed-length, no prefix) from VARBINARY (variable-length, with prefix) using `is_fixed_length` flag from metadata

### 3. Type Detection
**Problem:** TDS type byte 0xAD can represent either BINARY or VARBINARY

**Solution:** Used `BulkCopyColumnMetadata.length_type` (TypeLength::Fixed vs TypeLength::Variable) to determine encoding

## Files Modified

1. **mssql-py-core/src/types.rs**
   - Added PyByteArray import
   - Added bytes type conversion (lines ~148-154)
   - Added bytearray type conversion (lines ~156-162)

2. **mssql-tds/src/datatypes/tds_value_serializer.rs**
   - Added is_fixed_length field to TdsTypeContext (line ~43)
   - Added Bytes match arm in serialize_value (line ~100)
   - Implemented serialize_bytes function (lines ~1058-1120)

3. **mssql-tds/src/message/bulk_load.rs**
   - Added is_fixed_length initialization in context creation (line ~127)

4. **mssql-py-core/tests/test_bulkcopy_binary.py**
   - Created new test file with 12 comprehensive tests (683 lines)

## References

### .NET Testing
- **File:** `/home/saurabh/work/dotnetbcp/BulkCopyBenchmark/Program.cs`
- **Documentation:** `/home/saurabh/work/dotnetbcp/BINARY_COERCION_TEST_RESULTS.md`

### TDS Protocol
- BINARY(n): Fixed-length, type 0xAD, writes n bytes directly in ROW token
- VARBINARY(n): Variable-length, type 0xA5 or 0xAD, writes 2-byte length + data
- PLP types: 8-byte total length + chunked data + 4-byte terminator

## Conclusion

The implementation successfully replicates .NET SqlBulkCopy behavior for binary types, passing all 12 test cases covering:
- Basic functionality
- Edge cases (empty bytes, NULL handling)
- Error cases (size overflow, type mismatches)
- Type variants (bytes vs bytearray)
- Padding behavior (BINARY vs VARBINARY)
