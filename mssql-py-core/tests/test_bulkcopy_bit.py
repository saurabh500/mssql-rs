# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for BIT data type."""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_bit_basic(client_context):
    """Test cursor bulkcopy method with two bit columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two bit columns
    table_name = "BulkCopyTestTableBit"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT, flag2 BIT)")

    # Prepare test data - two columns, both bit (using bool and int representations)
    data = [
        (True, False),
        (False, True),
        (1, 0),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "flag1"),
            (1, "flag2"),
        ],  # Map tuple positions to columns
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly (BIT columns return as bool in Python)
    cursor.execute(f"SELECT flag1, flag2 FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == True and rows[0][1] == False
    assert rows[1][0] == False and rows[1][1] == True
    assert rows[2][0] == True and rows[2][1] == False


@pytest.mark.integration
def test_cursor_bulkcopy_bit_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable bit columns
    table_name = "BulkCopyAutoMapTableBit"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT, flag2 BIT)")

    # Prepare test data - two columns, both bit, with NULL values
    data = [
        (True, False),
        (False, None),  # NULL value in second column
        (None, True),  # NULL value in first column
        (1, 0),
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
    cursor.execute(f"SELECT flag1, flag2 FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == True and rows[0][1] == False
    assert rows[1][0] == False and rows[1][1] is None  # Verify NULL in flag2 column
    assert rows[2][0] is None and rows[2][1] == True  # Verify NULL in flag1 column
    assert rows[3][0] == True and rows[3][1] == False

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_bit_string_to_bit_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to bit columns.

    Tests type coercion when source data contains string representations of boolean values
    but destination columns are BIT type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two bit columns
    table_name = "BulkCopyStringToBitTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT, flag2 BIT)")

    # Prepare test data - strings containing valid bit values
    data = [
        ("1", "0"),
        ("0", "1"),
        ("True", "False"),
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

    # Verify data was inserted correctly and converted to bit
    cursor.execute(f"SELECT flag1, flag2 FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == True and rows[0][1] == False
    assert rows[1][0] == False and rows[1][1] == True
    assert rows[2][0] == True and rows[2][1] == False

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_bit_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable bit column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable bit column
    table_name = "#BulkCopyNonNullableTableBit"
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT NOT NULL)")

    # Prepare test data with a null value
    data = [
        (True,),
        (None,),  # This should trigger a conversion error
        (False,),
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
def test_cursor_bulkcopy_bit_invalid_string_to_bit_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to bit.

    Tests that client-side type coercion properly validates string-to-bit conversion
    and fails with an appropriate error when the string is not a valid boolean value.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with bit columns
    table_name = "#BulkCopyInvalidStringTableBit"
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT, flag2 BIT)")

    # Prepare test data with invalid string that cannot be parsed as bit
    data = [
        ("1", "0"),
        ("not_a_boolean", "1"),  # This should trigger a conversion error
        ("0", "1"),
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
    ), "Expected a ValueError to be raised for invalid string-to-bit conversion"
    assert (
        "cannot convert" in error_message or "conversion" in error_message
    ), f"Expected conversion error message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert (
        "not_a_boolean" in error_message or "invalid" in error_message
    ), f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_bit_integer_values(client_context):
    """Test cursor bulkcopy with various integer values to BIT column.

    Tests that non-zero integers convert to True (1) and zero converts to False (0).
    This matches SQL Server's BIT type behavior.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with bit columns
    table_name = "#BulkCopyBitIntegerTable"
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT, flag2 BIT)")

    # Prepare test data with various integer values
    # In SQL Server, any non-zero integer converts to 1 (True)
    data = [
        (0, 1),
        (2, 0),  # 2 should convert to True
        (100, -1),  # Both should convert to True
        (-5, 0),  # -5 should convert to True
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1

    # Verify data was inserted correctly with proper conversion
    cursor.execute(f"SELECT flag1, flag2 FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == False and rows[0][1] == True  # 0, 1
    assert rows[1][0] == True and rows[1][1] == False  # 2 -> True, 0
    assert rows[2][0] == True and rows[2][1] == True  # 100 -> True, -1 -> True
    assert rows[3][0] == True and rows[3][1] == False  # -5 -> True, 0

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_bit_boolean_values(client_context):
    """Test cursor bulkcopy with explicit Python boolean values to BIT column.

    Tests that Python True and False values correctly map to BIT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with bit columns
    table_name = "#BulkCopyBitBooleanTable"
    cursor.execute(f"CREATE TABLE {table_name} (flag1 BIT, flag2 BIT)")

    # Prepare test data with explicit boolean values
    data = [
        (True, False),
        (False, True),
        (True, True),
        (False, False),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT flag1, flag2 FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == True and rows[0][1] == False
    assert rows[1][0] == False and rows[1][1] == True
    assert rows[2][0] == True and rows[2][1] == True
    assert rows[3][0] == False and rows[3][1] == False

    # Close connection - temp table will be automatically dropped
    conn.close()
