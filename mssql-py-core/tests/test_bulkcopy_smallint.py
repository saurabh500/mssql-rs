# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for SMALLINT data type."""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_smallint_basic(client_context):
    """Test cursor bulkcopy method with two smallint columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two smallint columns
    table_name = "BulkCopyTestTableSmallInt"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id SMALLINT, value SMALLINT)")

    # Prepare test data - two columns, both smallint
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
def test_cursor_bulkcopy_smallint_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable smallint columns
    table_name = "BulkCopyAutoMapTableSmallInt"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id SMALLINT, value SMALLINT)")

    # Prepare test data - two columns, both smallint, with NULL values
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
def test_cursor_bulkcopy_smallint_string_to_smallint_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to smallint columns.

    Tests type coercion when source data contains numeric strings but
    destination columns are SMALLINT type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two smallint columns
    table_name = "BulkCopyStringToSmallIntTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id SMALLINT, value SMALLINT)")

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

    # Verify data was inserted correctly and converted to smallint
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
def test_cursor_bulkcopy_smallint_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable smallint column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable smallint column
    table_name = "#BulkCopyNonNullableTableSmallInt"
    cursor.execute(f"CREATE TABLE {table_name} (id SMALLINT NOT NULL)")

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
    assert (
        error_raised
    ), "Expected a ValueError to be raised for null value in non-nullable column"
    assert (
        "conversion" in error_message or "null" in error_message
    ), f"Expected conversion error, got: {error_message}"
    assert (
        "non-nullable" in error_message
    ), f"Expected 'non-nullable' in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smallint_invalid_string_to_smallint_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to smallint.

    Tests that client-side type coercion properly validates string-to-smallint conversion
    and fails with an appropriate error when the string is not a valid integer.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with smallint columns
    table_name = "#BulkCopyInvalidStringTableSmallInt"
    cursor.execute(f"CREATE TABLE {table_name} (id SMALLINT, value SMALLINT)")

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
    assert (
        error_raised
    ), "Expected a ValueError to be raised for invalid string-to-smallint conversion"
    assert (
        "cannot convert" in error_message or "conversion" in error_message
    ), f"Expected conversion error message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert (
        "invalid digit" in error_message or "not_a_number" in error_message
    ), f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smallint_overflow_to_smallint_column(client_context):
    """Test cursor bulkcopy with Python integer larger than SMALLINT max to SMALLINT column.

    When a Python number exceeds i16::MAX (32,767), it should be rejected
    during type coercion when the target column is SMALLINT type.

    Expected behavior:
    - Python int <= i16::MAX converts to ColumnValues::SmallInt successfully
    - Python int > i16::MAX triggers range validation error during coercion
    - Error raised: "Python integer ... out of range for SMALLINT column"

    This ensures we catch overflow errors early during type conversion rather than
    allowing silent conversion to larger integer types which would fail later or lose data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with SMALLINT column (max value: 32,767)
    table_name = "#BulkCopySmallIntOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id SMALLINT, value SMALLINT)")

    # Prepare test data with Python integer larger than i16::MAX
    # Python integers are arbitrary precision, but SQL SMALLINT is limited:
    # - i16::MIN = -32,768
    # - i16::MAX = 32,767
    data = [
        (1, 100),
        (2, 32768),  # i16::MAX + 1 = 32,768, exceeds SMALLINT range
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
    assert (
        error_raised
    ), "Expected an error for Python integer exceeding SMALLINT column range"
    assert (
        "out of range" in error_message
    ), f"Expected 'out of range' error, got: {error_message}"
    assert (
        "smallint" in error_message
    ), f"Expected SMALLINT column type in error, got: {error_message}"
    # The error message should mention the actual value that overflowed
    assert (
        "32768" in error_message
    ), f"Expected overflow value in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()
