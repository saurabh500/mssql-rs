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
    """Test cursor bulkcopy method with two integer columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two int columns
    table_name = "BulkCopyTestTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data - two columns, both int
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "id"),
                (1, "value"),
            ],  # Map tuple positions to columns
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == 100
    assert rows[1][0] == 2 and rows[1][1] == 200


@pytest.mark.integration
def test_cursor_bulkcopy_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable int columns
    table_name = "BulkCopyAutoMapTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data - two columns, both int, with NULL values
    data = [
        (1, 100),
        (2, None),  # NULL value in second column
        (None, 300),  # NULL value in first column
        (4, 400),
    ]

    # Execute bulk copy WITHOUT column mappings - should auto-generate
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly, including NULL values
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY COALESCE(id, 999)")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == 100
    assert rows[1][0] == 2 and rows[1][1] is None  # Verify NULL in value column
    assert rows[2][0] == 4 and rows[2][1] == 400
    assert rows[3][0] is None and rows[3][1] == 300  # Verify NULL in id column

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_string_to_int_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to int columns.

    Tests type coercion when source data contains numeric strings but
    destination columns are INT type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two int columns
    table_name = "BulkCopyStringToIntTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data - strings containing valid numbers
    data = [
        ("1", "100"),
        ("2", "200"),
        ("3", "300"),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly and converted to integers
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == 100
    assert rows[1][0] == 2 and rows[1][1] == 200
    assert rows[2][0] == 3 and rows[2][1] == 300

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable int column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable int column
    table_name = "#BulkCopyNonNullableTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT NOT NULL)")

    # Prepare test data with a null value
    data = [
        (1,),
        (None,),  # This should trigger a conversion error
        (3,),
    ]

    # Execute bulk copy and expect a ValueError
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
        )
        # If we get here, no error was raised
        print(f"No error raised. Result: {result}")
    except ValueError as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Expected ValueError caught: {e}")

    # Verify that an error was raised with appropriate message
    assert error_raised, "Expected a ValueError to be raised for null value in non-nullable column"
    assert "conversion" in error_message or "null" in error_message, \
        f"Expected conversion error, got: {error_message}"
    assert "non-nullable" in error_message, \
        f"Expected 'non-nullable' in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_invalid_string_to_int_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to int.

    Tests that client-side type coercion properly validates string-to-int conversion
    and fails with an appropriate error when the string is not a valid integer.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with int columns
    table_name = "#BulkCopyInvalidStringTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data with invalid string that cannot be parsed as integer
    data = [
        ("1", "100"),
        ("not_a_number", "200"),  # This should trigger a conversion error
        ("3", "300"),
    ]

    # Execute bulk copy and expect a client-side ValueError
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
        )
        # If we get here, no error was raised
        print(f"No error raised. Result: {result}")
    except ValueError as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Client-side ValueError caught: {e}")

    # Verify that an error was raised with appropriate message about conversion failure
    assert error_raised, "Expected a ValueError to be raised for invalid string-to-int conversion"
    assert "cannot convert" in error_message or "conversion" in error_message, \
        f"Expected conversion error message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert "invalid digit" in error_message or "not_a_number" in error_message, \
        f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_integer_overflow_to_int_column(client_context):
    """Test cursor bulkcopy with Python integer larger than INT max to INT column.

    When a Python number exceeds i32::MAX (2,147,483,647), it should be rejected
    during type coercion when the target column is INT type.
    
    Expected behavior:
    - Python int <= i32::MAX converts to ColumnValues::Int successfully
    - Python int > i32::MAX triggers range validation error during coercion
    - Error raised: "Python integer ... out of range for INT column"
    
    This ensures we catch overflow errors early during type conversion rather than
    allowing silent conversion to BigInt which would fail later or lose data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with INT column (max value: 2,147,483,647)
    table_name = "#BulkCopyIntOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data with Python integer larger than i32::MAX
    # Python integers are arbitrary precision, but SQL INT is limited:
    # - i32::MIN = -2,147,483,648
    # - i32::MAX = 2,147,483,647
    data = [
        (1, 100),
        (2, 2147483648),  # i32::MAX + 1 = 2,147,483,648, exceeds INT range
        (3, 300),
    ]

    # Execute bulk copy and expect range validation error
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
        )
        # If we get here, no error was raised (unexpected)
        print(f"No error raised. Result: {result}")
    except (ValueError, RuntimeError) as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Expected error caught: {e}")

    # Verify that an error was raised about integer overflow
    assert error_raised, "Expected an error for Python integer exceeding INT column range"
    assert "out of range" in error_message, \
        f"Expected 'out of range' error, got: {error_message}"
    assert "int" in error_message, \
        f"Expected INT column type in error, got: {error_message}"
    # The error message should mention the actual value that overflowed
    assert "2147483648" in error_message, \
        f"Expected overflow value in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
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
