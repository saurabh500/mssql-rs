# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for SMALLDATETIME data type."""
import pytest
import datetime
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_basic(client_context):
    """Test cursor bulkcopy method with two smalldatetime columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two smalldatetime columns
    table_name = "BulkCopyTestTableSmallDateTime"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME, birth_time SMALLDATETIME)")

    # Prepare test data - two columns, both smalldatetime
    data = [
        (datetime.datetime(2020, 1, 15, 10, 30), datetime.datetime(1990, 5, 20, 14, 45)),
        (datetime.datetime(2021, 6, 10, 8, 15), datetime.datetime(2000, 3, 25, 16, 20)),
        (datetime.datetime(2022, 12, 25, 23, 59), datetime.datetime(2010, 7, 4, 12, 0)),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "event_time"),
            (1, "birth_time"),
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
def test_cursor_bulkcopy_smalldatetime_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable smalldatetime columns
    table_name = "BulkCopyAutoMapTableSmallDateTime"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME, birth_time SMALLDATETIME)")

    # Prepare test data - two columns, both smalldatetime, with NULL values
    data = [
        (datetime.datetime(2020, 1, 15, 10, 30), datetime.datetime(1990, 5, 20, 14, 45)),
        (datetime.datetime(2021, 6, 10, 8, 15), None),  # NULL value in second column
        (None, datetime.datetime(2000, 3, 25, 16, 20)),  # NULL value in first column
        (datetime.datetime(2022, 12, 25, 23, 59), datetime.datetime(2010, 7, 4, 12, 0)),
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
def test_cursor_bulkcopy_smalldatetime_string_to_datetime_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to smalldatetime columns.

    Tests type coercion when source data contains datetime strings but
    destination columns are SMALLDATETIME type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two smalldatetime columns
    table_name = "BulkCopyStringToSmallDateTimeTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME, birth_time SMALLDATETIME)")

    # Prepare test data - strings containing valid datetimes in ISO format
    data = [
        ("2020-01-15 10:30:00", "1990-05-20 14:45:00"),
        ("2021-06-10 08:15:00", "2000-03-25 16:20:00"),
        ("2022-12-25 23:59:00", "2010-07-04 12:00:00"),
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
def test_cursor_bulkcopy_smalldatetime_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable smalldatetime column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable smalldatetime column
    table_name = "#BulkCopyNonNullableSmallDateTimeTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME NOT NULL)")

    # Prepare test data with a null value
    data = [
        (datetime.datetime(2020, 1, 15, 10, 30),),
        (None,),  # This should trigger a conversion error
        (datetime.datetime(2022, 12, 25, 23, 59),),
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
def test_cursor_bulkcopy_smalldatetime_invalid_string_to_datetime_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to smalldatetime.

    Tests that client-side type coercion properly validates string-to-datetime conversion
    and fails with an appropriate error when the string is not a valid datetime.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with smalldatetime columns
    table_name = "#BulkCopyInvalidStringSmallDateTimeTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME, birth_time SMALLDATETIME)")

    # Prepare test data with invalid string that cannot be parsed as datetime
    data = [
        ("2020-01-15 10:30:00", "1990-05-20 14:45:00"),
        ("not_a_datetime", "2000-03-25 16:20:00"),  # This should trigger a conversion error
        ("2022-12-25 23:59:00", "2010-07-04 12:00:00"),
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
    ), "Expected a ValueError to be raised for invalid string-to-datetime conversion"
    assert (
        "cannot convert" in error_message or "conversion" in error_message or "invalid" in error_message
    ), f"Expected conversion error message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert (
        "not_a_datetime" in error_message or "invalid" in error_message
    ), f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_boundary_values(client_context):
    """Test cursor bulkcopy with boundary smalldatetime values.

    Tests that realistic SMALLDATETIME values are properly handled.
    SQL Server SMALLDATETIME range: 1900-01-01 00:00:00 to 2079-06-06 23:59:59
    Accuracy: 1 minute
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with SMALLDATETIME columns
    table_name = "#BulkCopySmallDateTimeBoundaryTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time SMALLDATETIME)")

    # Prepare test data with realistic boundary datetimes
    data = [
        (1, datetime.datetime(1900, 1, 1, 0, 0)),  # Minimum SMALLDATETIME value
        (2, datetime.datetime(2079, 6, 6, 23, 59)),  # Maximum SMALLDATETIME value
        (3, datetime.datetime(2020, 6, 15, 12, 30)),  # Recent datetime
        (4, datetime.datetime(2000, 1, 1, 0, 0)),  # Y2K
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

    # Verify the actual datetime values match what was sent
    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    cursor.fetchall()  # Consume remaining result sets
    
    # Check each row's datetime value
    assert len(rows) == 4
    for i, (expected_id, expected_datetime) in enumerate(data):
        actual_id, actual_datetime = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch - expected {expected_id}, got {actual_id}"
        assert actual_datetime == expected_datetime, f"Row {i}: DateTime mismatch - expected {expected_datetime}, got {actual_datetime}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_out_of_range(client_context):
    """Test cursor bulkcopy with datetime values outside SMALLDATETIME range.

    When a datetime value is outside the valid range for SQL Server SMALLDATETIME type
    (1900-01-01 00:00:00 to 2079-06-06 23:59:59), it should be rejected during type coercion.

    Expected behavior:
    - DateTime within range converts to ColumnValues::SmallDateTime successfully
    - DateTime outside range triggers validation error during coercion
    - Error raised: "DateTime value ... out of range for SMALLDATETIME column"
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with SMALLDATETIME column
    table_name = "#BulkCopySmallDateTimeOutOfRangeTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME)")

    # Prepare test data with a datetime before the minimum valid value (before 1900)
    data = [
        (datetime.datetime(2020, 1, 15, 10, 30),),
        (datetime.datetime(1899, 12, 31, 23, 59),),  # Before minimum valid SMALLDATETIME
    ]

    # Execute bulk copy and expect a ValueError for out-of-range value
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
    ), "Expected a ValueError to be raised for datetime value outside SMALLDATETIME range"
    assert (
        "out of range" in error_message or "range" in error_message
    ), f"Expected out-of-range error, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_rounding_exceeds_max(client_context):
    """Test that rounding causes SMALLDATETIME to exceed maximum range.
    
    SMALLDATETIME maximum is 2079-06-06 23:59:59. When rounding is applied:
    - 2079-06-06 23:59:30 + rounding (add 1 min) → 2079-06-07 00:00:00 (OUT OF RANGE)
    - 2079-06-06 23:59:45 + rounding (add 1 min) → 2079-06-07 00:00:00 (OUT OF RANGE)
    
    Expected behavior:
    - Validation should detect that after rounding, the datetime exceeds max range
    - Error raised: "DateTime value ... out of range for SMALLDATETIME ... after rounding"
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with SMALLDATETIME column
    table_name = "#BulkCopySmallDateTimeRoundingExceedsMaxTable"
    cursor.execute(f"CREATE TABLE {table_name} (event_time SMALLDATETIME)")

    # Prepare test data with datetime at max boundary that will exceed after rounding
    # 2079-06-06 23:59:30 → rounds to 2079-06-07 00:00:00 (exceeds max 2079-06-06 23:59:59)
    data = [
        (datetime.datetime(2079, 6, 6, 23, 59, 30),),  # Will round up and exceed max
    ]

    # Execute bulk copy and expect a ValueError for out-of-range value after rounding
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
    assert error_raised, "Expected a ValueError to be raised for datetime value that exceeds max after rounding"
    assert "out of range" in error_message or "range" in error_message, f"Expected out-of-range error, got: {error_message}"
    assert "after rounding" in error_message or "rounding" in error_message, f"Expected 'after rounding' in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_max_boundary_2079_06_06(client_context):
    """Test bulk copy with the maximum valid SMALLDATETIME value: 2079-06-06 23:59:59.
    
    This test verifies that the maximum SMALLDATETIME value is handled correctly.
    SQL Server SMALLDATETIME stores date and time with minute precision,
    ranging from 1900-01-01 00:00:00 to 2079-06-06 23:59:59.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopyTestMaxSmallDateTime"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_datetime SMALLDATETIME)")

    # Test boundary datetimes
    data = [
        (1, datetime.datetime(1900, 1, 1, 0, 0)),     # Minimum datetime
        (2, datetime.datetime(2024, 1, 1, 12, 30)),   # Regular datetime
        (3, datetime.datetime(2079, 6, 6, 23, 58)),   # One minute before max
        (4, datetime.datetime(2079, 6, 6, 23, 59)),   # Maximum datetime - critical test!
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the datetimes were inserted correctly
    cursor.execute(f"SELECT id, test_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    assert rows[0][1] == datetime.datetime(1900, 1, 1, 0, 0)
    assert rows[1][1] == datetime.datetime(2024, 1, 1, 12, 30)
    assert rows[2][1] == datetime.datetime(2079, 6, 6, 23, 58)
    assert rows[3][1] == datetime.datetime(2079, 6, 6, 23, 59), "2079-06-06 23:59:59 should be inserted correctly!"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_with_int_columns(client_context):
    """Test cursor bulkcopy with mixed SMALLDATETIME and INT columns.

    Tests bulkcopy when the table has both SMALLDATETIME and INT data types,
    ensuring proper type coercion and column mapping.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with mixed column types
    table_name = "BulkCopyMixedSmallDateTimeIntTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time SMALLDATETIME, count INT)")

    # Prepare test data - mixed INT and SMALLDATETIME columns
    data = [
        (1, datetime.datetime(2020, 1, 15, 10, 30), 100),
        (2, datetime.datetime(2021, 6, 10, 8, 15), 200),
        (3, datetime.datetime(2022, 12, 25, 23, 59), 300),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "event_time"),
            (2, "count"),
        ],
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.fetchall()  # Consume remaining result sets before next execute
    assert count == 3


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_with_seconds_truncation(client_context):
    """Test cursor bulkcopy with datetime values containing seconds.
    
    Python datetime objects support seconds, but SMALLDATETIME only supports
    minute precision. Verify proper rounding behavior (matches SQL Server).
    
    Expected behavior (rounding rule: seconds >= 30 round up):
    - 10:30:00 → 10:30:00 (no seconds, no rounding)
    - 10:30:29 → 10:30:00 (29 < 30, rounds down)
    - 10:30:30 → 10:31:00 (30 >= 30, rounds up)
    - 10:30:59 → 10:31:00 (59 >= 30, rounds up)
    - 10:31:15 → 10:31:00 (15 < 30, rounds down)
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with SMALLDATETIME column
    table_name = "BulkCopySmallDateTimeWithSecondsTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time SMALLDATETIME)")

    # Prepare test data with various second values
    data = [
        (1, datetime.datetime(2024, 1, 15, 10, 30, 0)),   # Exactly on minute boundary
        (2, datetime.datetime(2024, 1, 15, 10, 30, 29)),  # 29 seconds (should truncate to 10:30:00)
        (3, datetime.datetime(2024, 1, 15, 10, 30, 30)),  # 30 seconds (should truncate to 10:30:00)
        (4, datetime.datetime(2024, 1, 15, 10, 30, 59)),  # 59 seconds (should truncate to 10:30:00)
        (5, datetime.datetime(2024, 1, 15, 10, 31, 15)),  # 15 seconds (should truncate to 10:31:00)
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 5

    # Verify data was inserted and check how seconds were handled
    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    # Check each row to see how seconds were handled
    assert len(rows) == 5
    
    # Row 1: 10:30:00 → 10:30:00
    assert rows[0][0] == 1
    assert rows[0][1].hour == 10
    assert rows[0][1].minute == 30
    
    # Row 2: 10:30:29 → should be 10:30:00 (truncated) or 10:30:00 (SQL Server rounds down)
    assert rows[1][0] == 2
    assert rows[1][1].hour == 10
    assert rows[1][1].minute == 30
    
    # Row 3: 10:30:30 → should round up to 10:31:00 (30 seconds rounds up)
    assert rows[2][0] == 3
    assert rows[2][1].hour == 10
    actual_minute_row3 = rows[2][1].minute
    
    # Row 4: 10:30:59 → should round up to 10:31:00 (59 seconds rounds up)
    assert rows[3][0] == 4
    assert rows[3][1].hour == 10
    actual_minute_row4 = rows[3][1].minute
    
    # Row 5: 10:31:15 → should round down to 10:31:00 (15 seconds rounds down)
    assert rows[4][0] == 5
    assert rows[4][1].hour == 10
    assert rows[4][1].minute == 31
    
    # Verify rounding behavior (matching SQL Server/MSSQL behavior)
    # Row 3: 10:30:30 should round up to 10:31 (seconds >= 30)
    assert actual_minute_row3 == 31, f"Expected rounding to :31, got :{actual_minute_row3:02d}"
    
    # Row 4: 10:30:59 should round up to 10:31 (seconds >= 30)
    assert actual_minute_row4 == 31, f"Expected rounding to :31, got :{actual_minute_row4:02d}"
    
    print(f"\nSmallDateTime seconds handling: ROUNDING confirmed (matches SQL Server)")
    print(f"  Input: 10:30:30 → Output: 10:{actual_minute_row3:02d} (rounded up)")
    print(f"  Input: 10:30:59 → Output: 10:{actual_minute_row4:02d} (rounded up)")


@pytest.mark.integration
def test_cursor_bulkcopy_smalldatetime_with_microseconds(client_context):
    """Test cursor bulkcopy with datetime values containing microseconds.
    
    Python datetime objects support microseconds, but SMALLDATETIME only supports
    minute precision. Microseconds are ignored, seconds follow rounding rule.
    
    Results (rounding rule: seconds >= 30 round up):
    - 10:30:00.000000 → 10:30:00 (no rounding needed)
    - 10:30:00.500000 → 10:30:00 (0 seconds, microseconds ignored)
    - 10:30:00.999999 → 10:30:00 (0 seconds, microseconds ignored)
    - 10:30:45.123456 → 10:31:00 (45 seconds rounds up, microseconds ignored)
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopySmallDateTimeWithMicrosecondsTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time SMALLDATETIME)")

    # Prepare test data with microseconds
    data = [
        (1, datetime.datetime(2024, 1, 15, 10, 30, 0, 0)),       # No microseconds
        (2, datetime.datetime(2024, 1, 15, 10, 30, 0, 500000)),  # 500ms
        (3, datetime.datetime(2024, 1, 15, 10, 30, 0, 999999)),  # 999ms
        (4, datetime.datetime(2024, 1, 15, 10, 30, 45, 123456)), # 45.123456 seconds
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify rounding behavior: seconds < 30 keep minute, seconds >= 30 round up
    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    
    # Row 1: 10:30:00.000000 → 10:30:00 (no rounding needed)
    assert rows[0][0] == 1
    assert rows[0][1].hour == 10
    assert rows[0][1].minute == 30
    
    # Row 2: 10:30:00.500000 → 10:30:00 (0 seconds, microseconds ignored)
    assert rows[1][0] == 2
    assert rows[1][1].hour == 10
    assert rows[1][1].minute == 30
    
    # Row 3: 10:30:00.999999 → 10:30:00 (0 seconds, microseconds ignored)
    assert rows[2][0] == 3
    assert rows[2][1].hour == 10
    assert rows[2][1].minute == 30
    
    # Row 4: 10:30:45.123456 → 10:31:00 (45 seconds rounds up)
    assert rows[3][0] == 4
    assert rows[3][1].hour == 10
    assert rows[3][1].minute == 31, f"Row 4: Expected :31 (45 sec rounds up), got :{rows[3][1].minute}"
    
    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()

