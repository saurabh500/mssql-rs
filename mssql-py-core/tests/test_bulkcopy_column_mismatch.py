# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for column count mismatch scenarios.

Tests the behavior when bulk copying data where:
1. Source data has more columns than the target table
2. Source data has fewer columns than the target table

According to expected behavior:
- Extra columns in the source should be dropped/ignored
- Missing columns should result in NULL values (if nullable) or defaults
"""

import pytest
import mssql_py_core


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_more_columns_than_table(client_context):
    """Test bulk copy where source has more columns than the target table.
    
    The extra columns should be dropped and the bulk copy should succeed.
    Only the columns specified in column_mappings should be inserted.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "BulkCopyMoreColumnsTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT)"
    )

    # Source data has 5 columns, but table only has 3
    # Extra columns (indices 3 and 4) should be ignored via column_mappings
    data = [
        (1, 100, 30, 999, 888),
        (2, 200, 25, 999, 888),
        (3, 300, 35, 999, 888),
        (4, 400, 28, 999, 888),
    ]

    # Execute bulk copy with explicit column mappings for first 3 columns only
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "id"),       # Map source column 0 to 'id'
                (1, "value1"),   # Map source column 1 to 'value1'
                (2, "value2"),   # Map source column 2 to 'value2'
                # Columns 3 and 4 are NOT mapped, so they're dropped
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4, "Expected 4 rows to be copied"
    assert result["batch_count"] >= 1

    # Verify data was inserted correctly (only first 3 columns)
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 4, "Expected 4 rows in table"
    assert rows[0] == (1, 100, 30)
    assert rows[1] == (2, 200, 25)
    assert rows[2] == (3, 300, 35)
    assert rows[3] == (4, 400, 28)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_fewer_columns_than_table(client_context):
    """Test bulk copy where source has fewer columns than the target table.
    
    Missing columns should be filled with NULL (if nullable).
    The bulk copy should succeed.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns (value2 is nullable)
    table_name = "BulkCopyFewerColumnsTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT NULL)"
    )

    # Source data has only 2 columns (id, value1) - missing 'value2'
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
        (4, 400),
    ]

    # Execute bulk copy with mappings for only 2 columns
    # 'value2' column is not mapped, so it should get NULL values
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "id"),       # Map source column 0 to 'id'
                (1, "value1"),   # Map source column 1 to 'value1'
                # 'value2' is not mapped, should be NULL
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4, "Expected 4 rows to be copied"
    assert result["batch_count"] >= 1

    # Verify data was inserted with NULL for missing 'value2' column
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 4, "Expected 4 rows in table"
    assert rows[0] == (1, 100, None), "value2 should be NULL"
    assert rows[1] == (2, 200, None), "value2 should be NULL"
    assert rows[2] == (3, 300, None), "value2 should be NULL"
    assert rows[3] == (4, 400, None), "value2 should be NULL"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_auto_mapping_with_extra_columns(client_context):
    """Test bulk copy with auto-mapping when source has more columns than table.
    
    Without explicit column_mappings, auto-mapping should use the first N columns
    where N is the number of columns in the target table.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "BulkCopyAutoMapExtraTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value1 INT, value2 INT)"
    )

    # Source data has 5 columns, table has 3
    # Auto-mapping should use first 3 columns
    data = [
        (1, 100, 30, 777, 666),
        (2, 200, 25, 777, 666),
        (3, 300, 35, 777, 666),
    ]

    # Execute bulk copy WITHOUT explicit column mappings
    # Auto-mapping should map first 3 columns to table's 3 columns
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3, "Expected 3 rows to be copied"

    # Verify data was inserted correctly (first 3 columns only)
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3, "Expected 3 rows in table"
    assert rows[0] == (1, 100, 30)
    assert rows[1] == (2, 200, 25)
    assert rows[2] == (3, 300, 35)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_partial_column_mapping(client_context):
    """Test bulk copy with partial column mapping.
    
    Tests the flexibility to specify only some columns explicitly,
    skipping columns in the middle.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "BulkCopyPartialMapTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT NULL, value2 INT)"
    )

    # Source data with id and value2 (no value1)
    data = [
        (1, 30),
        (2, 25),
        (3, 35),
    ]

    # Map columns 0 and 1 to 'id' and 'value2', skipping 'value1'
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "id"),       # Map source column 0 to 'id'
                (1, "value2"),   # Map source column 1 to 'value2'
                # 'value1' is not mapped, should be NULL
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3, "Expected 3 rows to be copied"

    # Verify data with NULL value1
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3, "Expected 3 rows in table"
    assert rows[0] == (1, None, 30), "value1 should be NULL"
    assert rows[1] == (2, None, 25), "value1 should be NULL"
    assert rows[2] == (3, None, 35), "value1 should be NULL"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_with_default_values(client_context):
    """Test bulk copy with fewer columns when table has default values.
    
    Missing columns with default values should use those defaults.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a table with default values (using INT types only)
    table_name = "BulkCopyDefaultsTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT PRIMARY KEY,
            value1 INT,
            status INT DEFAULT 999,
            counter INT DEFAULT 0
        )"""
    )

    # Source data has only id and value1
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Map only id and value1, letting status and counter use defaults
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "id"),
                (1, "value1"),
                # status and counter not mapped - should use defaults
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3, "Expected 3 rows to be copied"

    # Verify defaults were applied
    cursor.execute(
        f"SELECT id, value1, status, counter FROM {table_name} ORDER BY id"
    )
    rows = cursor.fetchall()

    assert len(rows) == 3, "Expected 3 rows in table"
    
    for row in rows:
        assert row[2] == 999, f"Status should be 999 (default), got {row[2]}"
        assert row[3] == 0, f"Counter should be 0 (default), got {row[3]}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_empty_source(client_context):
    """Test bulk copy with empty source data.
    
    Should handle gracefully without errors.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with INT columns
    table_name = "BulkCopyEmptyTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT)"
    )

    # Empty source data
    data = []

    # Execute bulk copy with empty data
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 0, "Expected 0 rows to be copied"

    # Verify no rows were inserted
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 0, "Expected no rows in table"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="ADO #40933: Bulk copy does not support column count mismatch scenarios")
@pytest.mark.integration
def test_bulkcopy_column_order_mismatch(client_context):
    """Test bulk copy where source column order differs from table column order.
    
    With explicit column mappings, the order shouldn't matter.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with columns in order: id, value1, value2
    table_name = "BulkCopyOrderTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT)"
    )

    # Source data with columns in order: value2, id, value1 (different from table)
    data = [
        (30, 1, 100),    # value2, id, value1
        (25, 2, 200),
        (35, 3, 300),
    ]

    # Map columns explicitly to handle order mismatch
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "value2"),   # Source column 0 -> table column 'value2'
                (1, "id"),       # Source column 1 -> table column 'id'
                (2, "value1"),   # Source column 2 -> table column 'value1'
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3, "Expected 3 rows to be copied"

    # Verify data was mapped correctly despite order mismatch
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3, "Expected 3 rows in table"
    assert rows[0] == (1, 100, 30)
    assert rows[1] == (2, 200, 25)
    assert rows[2] == (3, 300, 35)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
