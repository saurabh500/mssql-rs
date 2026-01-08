# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test JSON bulk copy against Azure SQL Database - matching .NET test exactly."""
import pytest
import mssql_py_core
import json
import os


def get_azure_credentials():
    """Get Azure SQL credentials matching .NET test."""
    password_file = "/tmp/azpassword"
    if not os.path.exists(password_file):
        pytest.skip(f"Azure password file not found: {password_file}")
    
    with open(password_file, 'r') as f:
        password = f.read().strip()
    
    return {
        "server": "saurabhsingh.database.windows.net",
        "user_name": "saurabh",
        "password": password,
        "database": "drivers",
        "trust_server_certificate": True,
        "encryption": "Mandatory",  # Azure SQL requires encryption
    }


@pytest.mark.integration
@pytest.mark.azure
def test_cursor_bulkcopy_json_azure():
    """Test cursor bulkcopy with JSON against Azure SQL - EXACTLY matching .NET test."""
    
    client_context = get_azure_credentials()
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with single JSON column - EXACTLY matching .NET test
    table_name = "json_bulkcopy_dest_python_test_v3"  # Changed to v3 for final fresh test
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (data JSON)")
    print(f"✅ Created table: {table_name}")

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
    
    print(f"📊 JSON data length: {len(json_data)} bytes")
    print(f"📊 JSON data (first 100 chars): {json_data[:100]}")
    
    # .NET bulk copies a SINGLE ROW with the entire JSON array
    data = [(json_data,)]

    print("\n🚀 Starting bulk copy operation...")
    print("=" * 70)
    
    # Execute bulk copy with single column - matching .NET's SqlBulkCopy
    try:
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
        
        print(f"✅ Bulk copy successful!")
        print(f"   Rows copied: {result['rows_copied']}")
        print(f"   Batch count: {result['batch_count']}")
        
        # Verify results
        assert result is not None
        assert result["rows_copied"] == 1
        assert result["batch_count"] == 1
        
        # Verify data was inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        print(f"\n📊 Verification:")
        print(f"   Total rows in table: {row_count}")
        assert row_count == 1, f"Expected 1 row, got {row_count}"

        # Verify JSON data integrity
        cursor.execute(f"SELECT data FROM {table_name}")
        result_data = cursor.fetchone()[0]
        assert result_data is not None
        
        # Verify it's valid JSON by parsing it
        parsed = json.loads(result_data)
        assert isinstance(parsed, list)
        assert len(parsed) == 30, f"Expected 30 records, got {len(parsed)}"
        print(f"   Records in JSON array: {len(parsed)}")
        
        # Verify first record matches expected format
        first_record = parsed[0]
        assert first_record["Id"] == 1
        assert "𩸽jsonक" in first_record["Name"]
        print(f"   First record: {first_record}")
        
        print("\n✅ ALL TESTS PASSED - JSON bulk copy works on Azure SQL!")
        
    except Exception as e:
        print(f"\n❌ Bulk copy failed: {e}")
        raise
    finally:
        # Cleanup
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"\n🧹 Cleaned up table: {table_name}")
        except:
            pass
