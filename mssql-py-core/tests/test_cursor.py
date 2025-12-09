# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for PyCoreCursor functionality."""
import pytest
import mssql_py_core


def test_cursor_import():
    """Test that the module can be imported."""
    assert hasattr(mssql_py_core, "PyCoreCursor")


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


@pytest.mark.integration
def test_cursor_fetchmany(client_context):
    """Test cursor fetchmany method."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Execute a query that returns multiple rows
    cursor.execute(
        "SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5"
    )

    # Fetch first 2 rows
    results = cursor.fetchmany(2)
    assert results is not None
    assert len(results) == 2
    assert results[0][0] == 1
    assert results[1][0] == 2

    # Fetch next 2 rows
    results = cursor.fetchmany(2)
    assert len(results) == 2
    assert results[0][0] == 3
    assert results[1][0] == 4

    # Fetch remaining rows (should only get 1)
    results = cursor.fetchmany(2)
    assert len(results) == 1
    assert results[0][0] == 5

    # Try to fetch more (should get empty list)
    results = cursor.fetchmany(2)
    assert results is not None
    assert len(results) == 0

    conn.close()


def test_cursor_close():
    """Test cursor close method."""
    # TODO: Implement once we can create cursor instances
    pass


# NOTE: Bulk copy tests have been moved to separate files by data type:
# - test_bulkcopy_int.py - INT data type tests
# Future files will include:
# - test_bulkcopy_smallint.py - SMALLINT data type tests
# - test_bulkcopy_bigint.py - BIGINT data type tests
# - test_bulkcopy_varchar.py - VARCHAR data type tests
# - etc.


@pytest.mark.skip(reason="Bulk copy API is stubbed, not yet implemented")
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
