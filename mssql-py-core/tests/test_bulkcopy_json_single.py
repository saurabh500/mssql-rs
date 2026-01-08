# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for JSON data type - single column test matching .NET."""
import pytest
import mssql_py_core
import json


@pytest.mark.integration
def test_cursor_bulkcopy_json_single_column(client_context):
    """Test cursor bulkcopy with single JSON column, matching .NET test structure EXACTLY."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with single JSON column - EXACTLY matching .NET test
    # .NET creates: "CREATE TABLE tablename (data json)"
    table_name = "BulkCopyJsonSingleColumnTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (data JSON)")

    # Prepare test data - EXACTLY matching .NET test format
    # .NET generates: [{"Id":1,"Name":"𩸽jsonक29"}, {"Id":2,"Name":"𩸽jsonक27"}, ...]
    # Generate exactly 30 records like .NET does
    records = []
    for i in range(1, 31):
        records.append({
            "Id": i,
            "Name": f"𩸽jsonक{i}"  # Same UTF-8 chars as .NET test
        })
    
    # Serialize to JSON string (not indented, to match .NET's bulk copy format)
    json_data = json.dumps(records, ensure_ascii=False)
    
    print(f"DEBUG: JSON data length: {len(json_data)} bytes")
    print(f"DEBUG: JSON data (first 100 chars): {json_data[:100]}")
    
    # .NET bulk copies a SINGLE ROW with the entire JSON array
    data = [(json_data,)]

    # Execute bulk copy with single column - matching .NET's SqlBulkCopy
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "data"),
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 1
    assert result["batch_count"] == 1
    
    # Verify data was inserted - EXACTLY matching .NET test verification
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    assert row_count == 1, f"Expected 1 row, got {row_count}"

    # Verify JSON data integrity - matching .NET test
    cursor.execute(f"SELECT data FROM {table_name}")
    result_data = cursor.fetchone()[0]
    assert result_data is not None
    
    # Verify it's valid JSON by parsing it
    parsed = json.loads(result_data)
    assert isinstance(parsed, list)
    assert len(parsed) == 30, f"Expected 30 records, got {len(parsed)}"
    
    # Verify first record matches expected format
    first_record = parsed[0]
    assert first_record["Id"] == 1
    assert "𩸽jsonक" in first_record["Name"]
    
    print(f"✅ Bulk copy successful: {row_count} row inserted")
    print(f"✅ JSON array contains {len(parsed)} records")
    print(f"✅ First record: {first_record}")
