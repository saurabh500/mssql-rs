# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Test bulk copy auto-mapping with fewer source columns than destination."""

import pytest
import mssql_py_core


@pytest.mark.integration
def test_bulkcopy_auto_mapping_fewer_columns(client_context):
    """Test bulk copy with auto-mapping when source has fewer columns than table.
    
    Without explicit column_mappings, auto-mapping should use only the first N columns
    where N is the number of source columns. Unmapped destination columns should be NULL/default.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns (last column nullable)
    table_name = "#BulkCopyAutoFewerTest"
    cursor.execute(
        f"IF OBJECT_ID('tempdb..{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value1 INT, value2 INT NULL)"
    )

    # Source data has only 2 columns, table has 3
    # Auto-mapping should use first 2 columns, leave value2 as NULL
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Execute bulk copy WITHOUT explicit column mappings
    # Auto-mapping should map first 2 source columns to first 2 table columns
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

    # Verify data was inserted correctly (first 2 columns only, value2 NULL)
    cursor.execute(f"SELECT id, value1, value2 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3, "Expected 3 rows in table"
    assert rows[0] == (1, 100, None), "value2 should be NULL"
    assert rows[1] == (2, 200, None), "value2 should be NULL"
    assert rows[2] == (3, 300, None), "value2 should be NULL"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
