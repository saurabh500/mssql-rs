# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for TIME data type."""
import pytest
import datetime
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_time_basic(client_context):
    """Test cursor bulkcopy method with two time columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two time columns
    table_name = "BulkCopyTestTableTime"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (start_time TIME, end_time TIME)")

    # Prepare test data - two columns, both time
    data = [
        (datetime.time(9, 30, 0), datetime.time(17, 45, 30)),
        (datetime.time(8, 15, 45), datetime.time(16, 30, 15)),
        (datetime.time(10, 0, 0), datetime.time(18, 0, 0)),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "start_time"),
            (1, "end_time"),
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
def test_cursor_bulkcopy_time_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable time columns
    table_name = "BulkCopyAutoMapTableTime"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (start_time TIME, end_time TIME)")

    # Prepare test data - two columns, both time, with NULL values
    data = [
        (datetime.time(9, 30, 0), datetime.time(17, 45, 30)),
        (datetime.time(8, 15, 45), None),  # NULL value in second column
        (None, datetime.time(16, 30, 15)),  # NULL value in first column
        (datetime.time(10, 0, 0), datetime.time(18, 0, 0)),
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
def test_cursor_bulkcopy_time_string_to_time_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to time columns.

    Tests type coercion when source data contains time strings but
    destination columns are TIME type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two time columns
    table_name = "BulkCopyStringToTimeTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (start_time TIME, end_time TIME)")

    # Prepare test data - strings containing valid times in ISO format
    data = [
        ("09:30:00", "17:45:30"),
        ("08:15:45", "16:30:15"),
        ("10:00:00", "18:00:00"),
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
def test_cursor_bulkcopy_time_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable time column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable time column
    table_name = "#BulkCopyNonNullableTimeTable"
    cursor.execute(f"CREATE TABLE {table_name} (start_time TIME NOT NULL)")

    # Prepare test data with a null value
    data = [
        (datetime.time(9, 30, 0),),
        (None,),  # This should trigger a conversion error
        (datetime.time(10, 0, 0),),
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
def test_cursor_bulkcopy_time_invalid_string_to_time_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to time.

    Tests that client-side type coercion properly validates string-to-time conversion
    and fails with an appropriate error when the string is not a valid time.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with time columns
    table_name = "#BulkCopyInvalidStringTimeTable"
    cursor.execute(f"CREATE TABLE {table_name} (start_time TIME, end_time TIME)")

    # Prepare test data with invalid string that cannot be parsed as time
    data = [
        ("09:30:00", "17:45:30"),
        ("not_a_time", "16:30:15"),  # This should trigger a conversion error
        ("10:00:00", "18:00:00"),
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
    ), "Expected a ValueError to be raised for invalid string-to-time conversion"
    assert (
        "cannot convert" in error_message or "conversion" in error_message or "invalid" in error_message
    ), f"Expected conversion error message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert (
        "not_a_time" in error_message or "invalid" in error_message
    ), f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_time_boundary_values(client_context):
    """Test cursor bulkcopy with boundary time values.

    Tests that realistic TIME values are properly handled.
    SQL Server TIME range: 00:00:00.0000000 to 23:59:59.9999999
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with TIME columns
    table_name = "#BulkCopyTimeBoundaryTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, start_time TIME)")

    # Prepare test data with realistic boundary times
    data = [
        (1, datetime.time(0, 0, 0)),  # Minimum time (midnight)
        (2, datetime.time(23, 59, 59)),  # Maximum time (one second before midnight)
        (3, datetime.time(12, 0, 0)),  # Noon
        (4, datetime.time(0, 0, 1)),  # One second after midnight
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

    # Verify the actual time values match what was sent
    cursor.execute(f"SELECT id, start_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    cursor.fetchall()  # Consume remaining result sets
    
    # Check each row's time value
    assert len(rows) == 4
    for i, (expected_id, expected_time) in enumerate(data):
        actual_id, actual_time = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch - expected {expected_id}, got {actual_id}"
        assert actual_time == expected_time, f"Row {i}: Time mismatch - expected {expected_time}, got {actual_time}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_time_with_microseconds(client_context):
    """Test cursor bulkcopy with time values that include microseconds.

    Tests that TIME values with fractional seconds are properly handled.
    Python datetime.time supports microseconds, SQL Server TIME supports up to 7 decimal places.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TIME columns
    table_name = "BulkCopyTestTimeMicroseconds"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, precise_time TIME)")

    # Test time values with microseconds
    data = [
        (1, datetime.time(9, 30, 15, 123456)),  # 9:30:15.123456
        (2, datetime.time(14, 45, 30, 999999)),  # 14:45:30.999999
        (3, datetime.time(0, 0, 0, 1)),          # 0:00:00.000001
        (4, datetime.time(23, 59, 59, 999999)),  # 23:59:59.999999
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the times were inserted correctly
    cursor.execute(f"SELECT id, precise_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    # Note: SQL Server TIME has 100ns precision, so we may need to check with some tolerance
    for i, (expected_id, expected_time) in enumerate(data):
        actual_id, actual_time = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch"
        # Compare times (SQL Server may round microseconds differently)
        assert actual_time.hour == expected_time.hour
        assert actual_time.minute == expected_time.minute
        assert actual_time.second == expected_time.second
        # Microseconds may be rounded - check they're close (within 10 microseconds)
        assert abs(actual_time.microsecond - expected_time.microsecond) < 10, \
            f"Row {i}: Microseconds differ significantly - expected {expected_time.microsecond}, got {actual_time.microsecond}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_time_max_boundary_23_59_59(client_context):
    """Test bulk copy with the maximum valid TIME value: 23:59:59.9999999.
    
    This test verifies that the maximum time value is handled correctly.
    SQL Server TIME type supports fractional seconds with up to 100ns precision.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopyTestMaxTime"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_time TIME)")

    # Test boundary times
    data = [
        (1, datetime.time(0, 0, 0)),           # Minimum time
        (2, datetime.time(12, 30, 45)),        # Regular time
        (3, datetime.time(23, 59, 58)),        # One second before max
        (4, datetime.time(23, 59, 59, 999999)),  # Maximum time - critical test!
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the times were inserted correctly
    cursor.execute(f"SELECT id, test_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    assert rows[0][1] == datetime.time(0, 0, 0), "Minimum time should be inserted correctly!"
    assert rows[1][1].hour == 12 and rows[1][1].minute == 30
    assert rows[2][1] == datetime.time(23, 59, 58), "Near-max time should be inserted correctly!"
    # For the maximum time, check hour, minute, second separately due to potential rounding
    assert rows[3][1].hour == 23, "Max time hour should be 23!"
    assert rows[3][1].minute == 59, "Max time minute should be 59!"
    assert rows[3][1].second == 59, "Max time second should be 59!"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_time_various_scales(client_context):
    """Test bulk copy with TIME columns of various scales (0-7).
    
    SQL Server TIME type supports scales from 0 to 7:
    - TIME(0): HH:MM:SS (no fractional seconds)
    - TIME(1): HH:MM:SS.f (0.1 seconds precision)
    - TIME(2): HH:MM:SS.ff (0.01 seconds precision)
    - TIME(3): HH:MM:SS.fff (milliseconds)
    - TIME(4): HH:MM:SS.ffff (0.1 milliseconds)
    - TIME(5): HH:MM:SS.fffff (10 microseconds)
    - TIME(6): HH:MM:SS.ffffff (microseconds)
    - TIME(7): HH:MM:SS.fffffff (100 nanoseconds) - DEFAULT
    
    This test verifies that bulk copy works correctly with all scale values
    and that precision is handled appropriately (truncation/rounding).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TIME columns of different scales
    table_name = "BulkCopyTestTimeScales"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT,
            time_scale0 TIME(0),
            time_scale1 TIME(1),
            time_scale2 TIME(2),
            time_scale3 TIME(3),
            time_scale4 TIME(4),
            time_scale5 TIME(5),
            time_scale6 TIME(6),
            time_scale7 TIME(7)
        )
    """)

    # Prepare test data with high-precision time values
    # Python datetime.time supports microseconds (6 decimal places)
    # SQL Server TIME(7) supports 100ns precision (7 decimal places)
    data = [
        # Test with various microsecond values
        (1, datetime.time(9, 30, 15, 123456), datetime.time(9, 30, 15, 123456),
         datetime.time(9, 30, 15, 123456), datetime.time(9, 30, 15, 123456),
         datetime.time(9, 30, 15, 123456), datetime.time(9, 30, 15, 123456),
         datetime.time(9, 30, 15, 123456), datetime.time(9, 30, 15, 123456)),
        
        # Test with maximum precision
        (2, datetime.time(14, 45, 30, 999999), datetime.time(14, 45, 30, 999999),
         datetime.time(14, 45, 30, 999999), datetime.time(14, 45, 30, 999999),
         datetime.time(14, 45, 30, 999999), datetime.time(14, 45, 30, 999999),
         datetime.time(14, 45, 30, 999999), datetime.time(14, 45, 30, 999999)),
        
        # Test with zero fractional seconds
        (3, datetime.time(12, 0, 0, 0), datetime.time(12, 0, 0, 0),
         datetime.time(12, 0, 0, 0), datetime.time(12, 0, 0, 0),
         datetime.time(12, 0, 0, 0), datetime.time(12, 0, 0, 0),
         datetime.time(12, 0, 0, 0), datetime.time(12, 0, 0, 0)),
        
        # Test with small microsecond values
        (4, datetime.time(8, 15, 45, 1), datetime.time(8, 15, 45, 1),
         datetime.time(8, 15, 45, 1), datetime.time(8, 15, 45, 1),
         datetime.time(8, 15, 45, 1), datetime.time(8, 15, 45, 1),
         datetime.time(8, 15, 45, 1), datetime.time(8, 15, 45, 1)),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the times were inserted and read correctly for each scale
    cursor.execute(f"""
        SELECT id, time_scale0, time_scale1, time_scale2, time_scale3,
               time_scale4, time_scale5, time_scale6, time_scale7
        FROM {table_name} ORDER BY id
    """)
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    
    # Row 1: 9:30:15.123456 - verify truncation at each scale
    row1 = rows[0]
    assert row1[0] == 1
    assert row1[1] == datetime.time(9, 30, 15, 0), "Scale 0: should truncate all fractional seconds"
    assert row1[2].hour == 9 and row1[2].minute == 30 and row1[2].second == 15, "Scale 1: basic time"
    # Scale 1 should be 9:30:15.1 = 9:30:15.100000 (truncation)
    assert row1[2].microsecond == 100000, f"Scale 1: expected 100000 microseconds, got {row1[2].microsecond}"
    # Scale 2 should be 9:30:15.12 = 9:30:15.120000 (truncation)
    assert row1[3].microsecond == 120000, f"Scale 2: expected 120000 microseconds, got {row1[3].microsecond}"
    # Scale 3 should be 9:30:15.123 = 9:30:15.123000 (truncation)
    assert row1[4].microsecond == 123000, f"Scale 3: expected 123000 microseconds, got {row1[4].microsecond}"
    # Scale 4 should be 9:30:15.1234 = 9:30:15.123400 (truncation, precision is 0.0001s = 100µs)
    assert row1[5].microsecond == 123400, f"Scale 4: expected 123400 microseconds, got {row1[5].microsecond}"
    # Scale 5 should be 9:30:15.12345 = 9:30:15.123450 (truncation, precision is 0.00001s = 10µs)
    assert row1[6].microsecond == 123450, f"Scale 5: expected 123450 microseconds, got {row1[6].microsecond}"
    # Scale 6 should be 9:30:15.123456 (exact match, precision is 1µs)
    assert row1[7].microsecond == 123456, f"Scale 6: expected exact 123456 microseconds, got {row1[7].microsecond}"
    # Scale 7 should be 9:30:15.123456 (Python can't represent the extra precision)
    assert row1[8].microsecond == 123456, f"Scale 7: expected 123456 microseconds, got {row1[8].microsecond}"
    
    # Row 2: 14:45:30.999999 - verify rounding/truncation at boundaries
    row2 = rows[1]
    assert row2[0] == 2
    assert row2[1] == datetime.time(14, 45, 31, 0) or row2[1] == datetime.time(14, 45, 30, 0), "Scale 0: may round up"
    assert row2[2].hour == 14 and row2[2].minute == 45, "Scale 1: basic time"
    
    # Row 3: 12:00:00.000000 - no fractional seconds
    row3 = rows[2]
    assert row3[0] == 3
    for i in range(1, 9):
        assert row3[i] == datetime.time(12, 0, 0, 0), f"Scale {i-1}: zero fractional seconds should be exact"
    
    # Row 4: 8:15:45.000001 - very small microsecond value
    row4 = rows[3]
    assert row4[0] == 4
    assert row4[1] == datetime.time(8, 15, 45, 0), "Scale 0: should truncate to zero"
    assert row4[2] == datetime.time(8, 15, 45, 0), "Scale 1: should truncate to zero"
    assert row4[3] == datetime.time(8, 15, 45, 0), "Scale 2: should truncate to zero"
    assert row4[4] == datetime.time(8, 15, 45, 0), "Scale 3: should truncate to zero"
    assert row4[5] == datetime.time(8, 15, 45, 0), "Scale 4: should truncate to zero"
    assert row4[6] == datetime.time(8, 15, 45, 0), "Scale 5: should truncate to zero"
    # Scale 6 and 7 should preserve the 1 microsecond
    assert row4[7].microsecond == 1, f"Scale 6: expected 1 microsecond, got {row4[7].microsecond}"
    assert row4[8].microsecond == 1, f"Scale 7: expected 1 microsecond, got {row4[8].microsecond}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_time_scale0_no_fractional(client_context):
    """Test bulk copy specifically with TIME(0) which has no fractional seconds.
    
    TIME(0) stores only HH:MM:SS with no fractional seconds.
    This test ensures that values are properly truncated/rounded.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TIME(0)
    table_name = "BulkCopyTestTimeScale0"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_time TIME(0))")

    # Test data with various fractional seconds
    data = [
        (1, datetime.time(9, 30, 15, 0)),        # No fractional seconds
        (2, datetime.time(14, 45, 30, 499999)),  # Should round down
        (3, datetime.time(8, 15, 45, 500000)),   # Should round up (or down, depending on implementation)
        (4, datetime.time(12, 0, 0, 123456)),    # Small fractional seconds
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the times - all should have 0 microseconds
    cursor.execute(f"SELECT id, test_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    assert rows[0][1] == datetime.time(9, 30, 15, 0), "Row 1: exact match"
    assert rows[1][1] == datetime.time(14, 45, 30, 0), "Row 2: fractional seconds removed"
    # Row 3 may be 8:15:45 or 8:15:46 depending on rounding
    assert rows[2][1].hour == 8 and rows[2][1].minute == 15, "Row 3: hour and minute preserved"
    assert rows[2][1].second in [45, 46], "Row 3: second may round"
    assert rows[2][1].microsecond == 0, "Row 3: no fractional seconds"
    assert rows[3][1] == datetime.time(12, 0, 0, 0), "Row 4: fractional seconds removed"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_time_scale3_milliseconds(client_context):
    """Test bulk copy with TIME(3) which has millisecond precision.
    
    TIME(3) is commonly used as it matches many application frameworks
    that work with millisecond precision.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TIME(3)
    table_name = "BulkCopyTestTimeScale3"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_time TIME(3))")

    # Test data with millisecond-aligned values
    data = [
        (1, datetime.time(9, 30, 15, 123000)),   # Exactly 123 milliseconds
        (2, datetime.time(14, 45, 30, 456000)),  # Exactly 456 milliseconds
        (3, datetime.time(8, 15, 45, 999000)),   # Exactly 999 milliseconds
        (4, datetime.time(12, 0, 0, 1000)),      # 1 millisecond + extra microseconds
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the times - should preserve milliseconds
    cursor.execute(f"SELECT id, test_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    assert rows[0][1].microsecond == 123000, f"Row 1: expected 123000 microseconds, got {rows[0][1].microsecond}"
    assert rows[1][1].microsecond == 456000, f"Row 2: expected 456000 microseconds, got {rows[1][1].microsecond}"
    assert rows[2][1].microsecond == 999000, f"Row 3: expected 999000 microseconds, got {rows[2][1].microsecond}"
    # Row 4 should truncate to 1000 microseconds (1 millisecond)
    assert rows[3][1].microsecond == 1000, f"Row 4: expected 1000 microseconds, got {rows[3][1].microsecond}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
