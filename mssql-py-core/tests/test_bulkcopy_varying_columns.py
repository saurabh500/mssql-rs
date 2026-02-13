# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for varying column counts in iterator.

Tests the behavior when bulk copying data where different rows
have different numbers of columns in the iterator.

According to expected behavior:
- The first row establishes the authoritative column count
- Subsequent rows with different column counts should error out
- On error, cancellation should be sent to SQL Server
"""

import pytest
import mssql_py_core


@pytest.mark.integration
def test_bulkcopy_subsequent_row_has_more_columns(client_context):
    """Test bulk copy where a subsequent row has more columns than the first row.
    
    The first row establishes the expected column count. When a subsequent row
    has more columns, the bulk copy should fail with an appropriate error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "#BulkCopyVaryingColumnsMoreTest"
    cursor.execute(
        f"IF OBJECT_ID('tempdb..{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT)"
    )

    # First row has 3 columns, second row has 4 columns (one extra)
    # This should fail because the column count varies
    def varying_data_generator():
        yield (1, 100, 30)      # First row: 3 columns (establishes the count)
        yield (2, 200, 25, 999) # Second row: 4 columns (ERROR: too many)

    # Execute bulk copy - should fail
    with pytest.raises(Exception) as exc_info:
        cursor.bulkcopy(
            table_name,
            varying_data_generator(),
            batch_size=1000,
            timeout=30,
        )

    # Verify error message mentions column count mismatch
    error_message = str(exc_info.value).lower()
    assert "column" in error_message or "expected" in error_message, \
        f"Error message should mention column count issue, got: {exc_info.value}"

    # Verify connection is still usable (attention was sent properly)
    cursor.execute("SELECT 1 as test_col")
    result = cursor.fetchone()
    assert result == (1,), "Connection should still be usable after bulk copy error"

    # Cleanup
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.close()


@pytest.mark.integration
def test_bulkcopy_subsequent_row_has_fewer_columns(client_context):
    """Test bulk copy where a subsequent row has fewer columns than the first row.
    
    The first row establishes the expected column count. When a subsequent row
    has fewer columns, the bulk copy should fail with an appropriate error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "#BulkCopyVaryingColumnsFewerTest"
    cursor.execute(
        f"IF OBJECT_ID('tempdb..{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT PRIMARY KEY, value1 INT, value2 INT)"
    )

    # First row has 3 columns, second row has 2 columns (one missing)
    # This should fail because the column count varies
    def varying_data_generator():
        yield (1, 100, 30)  # First row: 3 columns (establishes the count)
        yield (2, 200)      # Second row: 2 columns (ERROR: too few)

    # Execute bulk copy - should fail
    with pytest.raises(Exception) as exc_info:
        cursor.bulkcopy(
            table_name,
            varying_data_generator(),
            batch_size=1000,
            timeout=30,
        )

    # Verify error message mentions column count mismatch
    error_message = str(exc_info.value).lower()
    assert "column" in error_message or "expected" in error_message, \
        f"Error message should mention column count issue, got: {exc_info.value}"

    # Verify connection is still usable (attention was sent properly)
    cursor.execute("SELECT 1 as test_col")
    result = cursor.fetchone()
    assert result == (1,), "Connection should still be usable after bulk copy error"

    # Cleanup
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.close()


@pytest.mark.integration
def test_bulkcopy_varying_columns_within_batch(client_context):
    """Test bulk copy with varying columns within a single batch.
    
    Tests that column count validation works correctly even when
    all rows are in a single batch.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "#BulkCopyVaryingBatchTest"
    cursor.execute(
        f"IF OBJECT_ID('tempdb..{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value1 INT, value2 INT)"
    )

    # Multiple rows with varying columns - should fail on the second row
    data = [
        (1, 100, 30),     # First row: 3 columns (correct)
        (2, 200, 25, 99), # Second row: 4 columns (ERROR)
        (3, 300, 35),     # Third row: would be correct but shouldn't get here
    ]

    # Execute bulk copy - should fail
    with pytest.raises(Exception) as exc_info:
        cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,  # Large batch to ensure all rows in one batch
            timeout=30,
        )

    # Verify error message mentions column count mismatch
    error_message = str(exc_info.value).lower()
    assert "column" in error_message or "expected" in error_message, \
        f"Error message should mention column count issue, got: {exc_info.value}"

    # Verify connection is still usable
    cursor.execute("SELECT 1 as test_col")
    result = cursor.fetchone()
    assert result == (1,), "Connection should still be usable after bulk copy error"

    # Cleanup
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.close()


@pytest.mark.integration
def test_bulkcopy_consistent_columns_success(client_context):
    """Test bulk copy with consistent column counts (should succeed).
    
    This is a control test to verify that bulk copy works correctly when
    all rows have the same number of columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with 3 INT columns
    table_name = "#BulkCopyConsistentTest"
    cursor.execute(
        f"IF OBJECT_ID('tempdb..{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value1 INT, value2 INT)"
    )

    # All rows have 3 columns - should succeed
    data = [
        (1, 100, 30),
        (2, 200, 25),
        (3, 300, 35),
        (4, 400, 28),
    ]

    # Execute bulk copy - should succeed
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4, "Expected 4 rows to be copied"

    # Verify data was inserted correctly
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 4, "Expected 4 rows in table"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
