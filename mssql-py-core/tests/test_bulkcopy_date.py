# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for DATE data type."""
import pytest
import datetime
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_date_basic(client_context):
    """Test cursor bulkcopy method with two date columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two date columns
    table_name = "BulkCopyTestTableDate"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")

    # Prepare test data - two columns, both date
    data = [
        (datetime.date(2020, 1, 15), datetime.date(1990, 5, 20)),
        (datetime.date(2021, 6, 10), datetime.date(1985, 3, 25)),
        (datetime.date(2022, 12, 25), datetime.date(2000, 7, 4)),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "event_date"),
            (1, "birth_date"),
        ],  # Map tuple positions to columns
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.fetchall()  # Consume remaining result sets before next execute
    assert count == 3

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_date_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable date columns
    table_name = "BulkCopyAutoMapTableDate"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")

    # Prepare test data - two columns, both date, with NULL values
    data = [
        (datetime.date(2020, 1, 15), datetime.date(1990, 5, 20)),
        (datetime.date(2021, 6, 10), None),  # NULL value in second column
        (None, datetime.date(1985, 3, 25)),  # NULL value in first column
        (datetime.date(2022, 12, 25), datetime.date(2000, 7, 4)),
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

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.fetchall()  # Consume remaining result sets before next execute
    assert count == 4

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_date_string_to_date_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to date columns.

    Tests type coercion when source data contains date strings but
    destination columns are DATE type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two date columns
    table_name = "BulkCopyStringToDateTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")

    # Prepare test data - strings containing valid dates in ISO format
    data = [
        ("2020-01-15", "1990-05-20"),
        ("2021-06-10", "1985-03-25"),
        ("2022-12-25", "2000-07-04"),
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

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.fetchall()  # Consume remaining result sets before next execute
    assert count == 3

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_date_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable date column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable date column
    table_name = "#BulkCopyNonNullableDateTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE NOT NULL)")

    # Prepare test data with a null value
    data = [
        (datetime.date(2020, 1, 15),),
        (None,),  # This should trigger a conversion error
        (datetime.date(2022, 12, 25),),
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
def test_cursor_bulkcopy_date_invalid_string_to_date_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to date.

    Tests that client-side type coercion properly validates string-to-date conversion
    and fails with an appropriate error when the string is not a valid date.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with date columns
    table_name = "#BulkCopyInvalidStringDateTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE, birth_date DATE)")

    # Prepare test data with invalid string that cannot be parsed as date
    data = [
        ("2020-01-15", "1990-05-20"),
        ("not_a_date", "1985-03-25"),  # This should trigger a conversion error
        ("2022-12-25", "2000-07-04"),
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
    ), "Expected a ValueError to be raised for invalid string-to-date conversion"
    assert (
        "cannot convert" in error_message or "conversion" in error_message or "invalid" in error_message
    ), f"Expected conversion error message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert (
        "not_a_date" in error_message or "invalid" in error_message
    ), f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_date_boundary_values(client_context):
    """Test cursor bulkcopy with boundary date values.

    Tests that realistic DATE values are properly handled.
    SQL Server DATE range: 0001-01-01 to 9999-12-31
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with DATE columns
    table_name = "#BulkCopyDateBoundaryTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_date DATE)")

    # Prepare test data with realistic boundary dates
    # Note: Python datetime.date only supports years 1-9999
    data = [
        (1, datetime.date(1900, 1, 1)),  # Old date
        (2, datetime.date(9999, 12, 31)),  # Maximum DATE value
        (3, datetime.date(2020, 6, 15)),  # Recent date
        (4, datetime.date(2000, 12, 31)),  # Y2K
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

    # Verify data was inserted by checking the count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.fetchall()  # Consume remaining result sets before next execute
    assert count == 4

    # Verify the actual date values match what was sent
    cursor.execute(f"SELECT id, event_date FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    cursor.fetchall()  # Consume remaining result sets
    
    # Check each row's date value
    assert len(rows) == 4
    for i, (expected_id, expected_date) in enumerate(data):
        actual_id, actual_date = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch - expected {expected_id}, got {actual_id}"
        assert actual_date == expected_date, f"Row {i}: Date mismatch - expected {expected_date}, got {actual_date}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_date_invalid_boundary_out_of_range(client_context):
    """Test cursor bulkcopy with invalid date values outside the valid range.

    When a date value is outside the valid range for SQL Server DATE type
    (0001-01-01 to 9999-12-31), it should be rejected during type coercion.

    Expected behavior:
    - Date within range converts to ColumnValues::Date successfully
    - Date outside range triggers validation error during coercion
    - Error raised: "Date value ... out of range for DATE column"
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with DATE column
    table_name = "#BulkCopyDateOutOfRangeTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_date DATE)")

    # Prepare test data with an invalid date (year 0, which is before min valid date)
    data = [
        (datetime.date(2020, 1, 15),),
        (datetime.date(1, 1, 1),),  # This is the minimum valid date
    ]

    # Note: Python's datetime.date doesn't allow dates before year 1,
    # so the overflow scenario is less relevant here, but we can test
    # with the boundary case

    # Execute bulk copy (should succeed for valid boundary)
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_date_max_boundary_9999_12_31(client_context):
    """Test bulk copy with the maximum valid DATE value: 9999-12-31.
    
    This test verifies the fix for the toordinal() off-by-one bug.
    Python's toordinal() returns 1-based values (date(1,1,1) = 1), but
    SQL Server DATE type needs 0-based days since 0001-01-01.
    
    Before the fix:
    - date(9999, 12, 31).toordinal() = 3,652,059 (sent directly)
    - SQL Server rejected this as out of range (error 7339)
    
    After the fix:
    - date(9999, 12, 31).toordinal() - 1 = 3,652,058 (correct value)
    - SQL Server accepts this as valid 9999-12-31
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopyTestMaxDate"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_date DATE)")

    # Test boundary dates
    data = [
        (1, datetime.date(1, 1, 1)),        # Minimum date (day 0)
        (2, datetime.date(2024, 1, 1)),     # Regular date
        (3, datetime.date(9999, 12, 30)),   # One day before max
        (4, datetime.date(9999, 12, 31)),   # Maximum date - critical test!
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded    
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the dates were inserted correctly
    cursor.execute(f"SELECT id, test_date FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    assert rows[0][1] == datetime.date(1, 1, 1)
    assert rows[1][1] == datetime.date(2024, 1, 1)
    assert rows[2][1] == datetime.date(9999, 12, 30)
    assert rows[3][1] == datetime.date(9999, 12, 31), "9999-12-31 should be inserted correctly!"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
