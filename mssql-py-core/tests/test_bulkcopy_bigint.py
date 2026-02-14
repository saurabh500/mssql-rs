# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for BIGINT data type."""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_bigint_basic(client_context):
    """Test cursor bulkcopy method with two bigint columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two bigint columns
    table_name = "BulkCopyTestTableBigInt"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

    # Prepare test data - two columns, both bigint
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "value"),
        ],  # Map tuple positions to columns
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
def test_cursor_bulkcopy_bigint_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable bigint columns
    table_name = "BulkCopyAutoMapTableBigInt"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

    # Prepare test data - two columns, both bigint, with NULL values
    data = [
        (1, 100),
        (2, None),  # NULL value in second column
        (None, 300),  # NULL value in first column
        (4, 400),
    ]

    # Execute bulk copy WITHOUT column mappings - should auto-generate
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_bigint_string_to_bigint_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to bigint columns.

    Tests type coercion when source data contains numeric strings but
    destination columns are BIGINT type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two bigint columns
    table_name = "BulkCopyStringToBigIntTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

    # Prepare test data - strings containing valid numbers
    data = [
        ("1", "100"),
        ("2", "200"),
        ("3", "300"),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly and converted to bigint
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
def test_cursor_bulkcopy_bigint_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable bigint column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable bigint column
    table_name = "#BulkCopyNonNullableBigIntTable"
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT NOT NULL)")

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
            table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_bigint_invalid_string_to_bigint_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to bigint.

    Tests that client-side type coercion properly validates string-to-bigint conversion
    and fails with an appropriate error when the string is not a valid integer.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with bigint columns
    table_name = "#BulkCopyInvalidStringBigIntTable"
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

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
            table_name, iter(data), batch_size=1000, timeout=30
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
    ), "Expected a ValueError to be raised for invalid string-to-bigint conversion"
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
def test_cursor_bulkcopy_bigint_large_values(client_context):
    """Test cursor bulkcopy with large Python integers that fit in BIGINT range.

    Tests that Python integers larger than INT max but within BIGINT range
    are properly converted and stored in BIGINT columns.

    BIGINT range:
    - i64::MIN = -9,223,372,036,854,775,808
    - i64::MAX = 9,223,372,036,854,775,807
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with BIGINT columns
    table_name = "#BulkCopyBigIntLargeTable"
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

    # Prepare test data with large Python integers within i64 range
    # Values exceed i32::MAX (2,147,483,647) but fit in i64
    data = [
        (1, 2147483648),  # i32::MAX + 1
        (2, 9223372036854775807),  # i64::MAX
        (3, -9223372036854775808),  # i64::MIN
        (4, 5000000000),  # Large positive value
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == 2147483648
    assert rows[1][0] == 2 and rows[1][1] == 9223372036854775807
    assert rows[2][0] == 3 and rows[2][1] == -9223372036854775808
    assert rows[3][0] == 4 and rows[3][1] == 5000000000

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_bigint_overflow_to_bigint_column(client_context):
    """Test cursor bulkcopy with Python integer larger than BIGINT max to BIGINT column.

    When a Python number exceeds i64::MAX (9,223,372,036,854,775,807), it should be rejected
    during type coercion when the target column is BIGINT type.

    Expected behavior:
    - Python int <= i64::MAX converts to ColumnValues::BigInt successfully
    - Python int > i64::MAX triggers range validation error during coercion
    - Error raised: "Python integer ... out of range for BIGINT column"

    This ensures we catch overflow errors early during type conversion.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with BIGINT column (max value: 9,223,372,036,854,775,807)
    table_name = "#BulkCopyBigIntOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

    # Prepare test data with Python integer larger than i64::MAX
    # Python integers are arbitrary precision, but SQL BIGINT is limited:
    # - i64::MIN = -9,223,372,036,854,775,808
    # - i64::MAX = 9,223,372,036,854,775,807
    data = [
        (1, 100),
        (2, 9223372036854775808),  # i64::MAX + 1, exceeds BIGINT range
        (3, 300),
    ]

    # Execute bulk copy and expect range validation error
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), batch_size=1000, timeout=30
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
    ), "Expected an error for Python integer exceeding BIGINT column range"
    assert (
        "out of range" in error_message
    ), f"Expected 'out of range' error, got: {error_message}"
    assert (
        "bigint" in error_message
    ), f"Expected BIGINT column type in error, got: {error_message}"
    # The error message should mention the actual value that overflowed
    assert (
        "9223372036854775808" in error_message
    ), f"Expected overflow value in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()
