# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for PyCoreCursor functionality."""
import pytest
import mssql_py_core


def test_cursor_import():
    """Test that the module can be imported."""
    assert hasattr(mssql_py_core, 'PyCoreCursor')


def test_cursor_creation():
    """Test cursor creation (when connection is implemented)."""
    # TODO: Add connection creation and cursor tests once PyConnection is implemented
    # Example:
    # conn = mssql_py_core.PyConnection(...)
    # cursor = conn.cursor()
    # assert cursor is not None
    pass


def test_cursor_repr():
    """Test cursor string representation (when available)."""
    # TODO: Test cursor repr once we can create cursor instances
    pass


@pytest.mark.integration
def test_cursor_execute(client_context):
    """Test cursor execute method."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 AS value")
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == 1
    conn.close()


@pytest.mark.integration
def test_cursor_fetchall(client_context):
    """Test cursor fetchall method."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3")
    results = cursor.fetchall()
    assert results is not None
    assert len(results) == 3
    assert results[0][0] == 1
    assert results[1][0] == 2
    assert results[2][0] == 3
    conn.close()


@pytest.mark.asyncio
async def test_cursor_fetchmany():
    """Test cursor fetchmany method."""
    # TODO: Implement once connection and execute are functional
    pass


def test_cursor_close():
    """Test cursor close method."""
    # TODO: Implement once we can create cursor instances
    pass


@pytest.mark.integration
def test_cursor_bulkcopy(client_context):
    """Test cursor bulkcopy method with two integer columns."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()
    
    # Create a test table with two int columns
    table_name = "BulkCopyTestTable"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")
    
    # Prepare test data - two columns, both int
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]
    
    # Execute bulk copy with column mappings
    result = cursor.bulkcopy(
        table_name, 
        iter(data), 
        kwargs={
            'batch_size': 1000, 
            'timeout': 30,
            'column_mappings': [(0, 'id'), (1, 'value')]  # Map tuple positions to columns
        }
    )
    
    # Verify results
    assert result is not None
    assert result['rows_copied'] == 3
    assert result['batch_count'] == 1
    assert 'elapsed_time' in result
    
    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == 100
    assert rows[1][0] == 2 and rows[1][1] == 200
    assert rows[2][0] == 3 and rows[2][1] == 300
    
    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.skip(reason="Bulk copy API is stubbed, not yet implemented")
def test_cursor_bulkcopy_with_options():
    """Test cursor bulkcopy with various options."""
    # TODO: Test bulkcopy with different options once implemented
    # Options to test:
    # - batch_size
    # - timeout
    # - column_mappings
    # - keep_identity
    # - check_constraints
    # - table_lock
    # - keep_nulls
    # - fire_triggers
    pass


@pytest.mark.skip(reason="Bulk copy API is stubbed, not yet implemented")
def test_cursor_bulkcopy_column_mappings():
    """Test cursor bulkcopy with column mappings."""
    # TODO: Test bulkcopy with column mappings once implemented
    # Test both name-based and ordinal-based mappings:
    # column_mappings = [
    #     ('source_id', 'id'),
    #     (1, 'name'),
    # ]
    pass


@pytest.mark.skip(reason="Bulk copy API is stubbed, not yet implemented")
def test_cursor_bulkcopy_empty_data():
    """Test cursor bulkcopy with empty data source."""
    # TODO: Test bulkcopy behavior with empty iterator once implemented
    pass


@pytest.mark.skip(reason="Bulk copy API is stubbed, not yet implemented")
def test_cursor_bulkcopy_error_handling():
    """Test cursor bulkcopy error handling."""
    # TODO: Test error cases once implemented:
    # - Invalid table name
    # - Type mismatches
    # - Constraint violations
    # - Network errors
    pass
