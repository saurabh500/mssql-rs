# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for DATETIME2 data type."""

import datetime
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_basic(client_context):
    """Test basic cursor bulkcopy with DATETIME2 data type.

    Tests the basic functionality of bulkcopy with datetime2 values,
    using the default precision (7).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with datetime2 columns (default precision is 7)
    table_name = "BulkCopyTestDateTime2"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_datetime DATETIME2, created_datetime DATETIME2)")

    # Prepare test data - three rows with datetime2 values
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 123456), datetime.datetime(2024, 1, 15, 10, 0, 0, 500000)),
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 999999), datetime.datetime(2024, 2, 20, 15, 0, 0, 0)),
        (3, datetime.datetime(2024, 3, 10, 0, 0, 0, 0), datetime.datetime(2024, 3, 10, 23, 59, 59, 999999)),
    ]

    # Execute bulk copy with explicit column mappings
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
def test_cursor_bulkcopy_datetime2_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping for datetime2.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable datetime2 columns
    table_name = "BulkCopyAutoMapTableDateTime2"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME2, end_datetime DATETIME2)")

    # Prepare test data - two columns, both datetime2, with NULL values
    data = [
        (datetime.datetime(2024, 1, 15, 9, 30, 0, 123456), datetime.datetime(2024, 1, 15, 17, 45, 30, 654321)),
        (datetime.datetime(2024, 2, 20, 8, 15, 45, 111111), None),  # NULL in second column
        (None, datetime.datetime(2024, 2, 20, 16, 30, 15, 222222)),  # NULL in first column
        (datetime.datetime(2024, 3, 10, 10, 0, 0, 0), datetime.datetime(2024, 3, 10, 18, 0, 0, 999999)),
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
def test_cursor_bulkcopy_datetime2_precision_0(client_context):
    """Test cursor bulkcopy with DATETIME2(0) - no fractional seconds.

    Tests datetime2 with precision 0, which stores only whole seconds.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with DATETIME2(0) - no fractional seconds
    table_name = "BulkCopyTestDateTime2Precision0"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_datetime DATETIME2(0))")

    # Prepare test data - microseconds should be truncated/rounded
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 0)),       # Exactly 0 fractional seconds
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 123456)), # Should round to nearest second
        (3, datetime.datetime(2024, 3, 10, 0, 0, 59, 999999)),   # Should round up
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 3

    # Verify the data - precision 0 means no fractional seconds
    cursor.execute(f"SELECT id, event_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    # All fractional seconds should be 0
    for row in rows:
        assert row[1].microsecond == 0, f"Expected 0 microseconds for precision 0, got {row[1].microsecond}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_precision_3(client_context):
    """Test cursor bulkcopy with DATETIME2(3) - millisecond precision.

    Tests datetime2 with precision 3, which stores milliseconds (3 decimal places).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with DATETIME2(3) - millisecond precision
    table_name = "BulkCopyTestDateTime2Precision3"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_datetime DATETIME2(3))")

    # Prepare test data with various millisecond values
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 0)),       # .000 seconds
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 123000)), # .123 seconds
        (3, datetime.datetime(2024, 3, 10, 0, 0, 59, 999000)),   # .999 seconds
        (4, datetime.datetime(2024, 4, 5, 12, 0, 0, 456789)),    # .456789 should round to .457
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the data - precision 3 means milliseconds only
    cursor.execute(f"SELECT id, event_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    # Check that microseconds are rounded to millisecond precision
    for i, (row_id, actual_datetime) in enumerate(rows):
        assert row_id == i + 1
        # Microseconds should be in multiples of 1000 (millisecond precision)
        assert actual_datetime.microsecond % 1000 == 0, \
            f"Row {i+1}: Expected microseconds to be multiple of 1000, got {actual_datetime.microsecond}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_precision_7_default(client_context):
    """Test cursor bulkcopy with DATETIME2(7) - maximum and default precision.

    Tests datetime2 with precision 7 (default), which stores up to 100 nanoseconds precision.
    This is the maximum precision for datetime2.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with DATETIME2(7) - explicit maximum precision
    table_name = "BulkCopyTestDateTime2Precision7"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_datetime DATETIME2(7))")

    # Prepare test data with high-precision fractional seconds
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 0)),       # .0000000 seconds
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 123456)), # .1234560 seconds
        (3, datetime.datetime(2024, 3, 10, 0, 0, 59, 999999)),   # .9999990 seconds
        (4, datetime.datetime(2024, 4, 5, 12, 0, 0, 1)),         # .0000010 seconds (1 microsecond)
        (5, datetime.datetime(2024, 5, 10, 8, 15, 30, 500000)),  # .5000000 seconds
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 5

    # Verify the data - precision 7 preserves microseconds
    cursor.execute(f"SELECT id, event_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 5
    for i, (expected_id, expected_datetime) in enumerate(data):
        actual_id, actual_datetime = rows[i]
        assert actual_id == expected_id
        # With precision 7, microseconds should be preserved
        # Note: datetime2(7) has 100ns precision, Python datetime has microsecond precision
        # So we compare microseconds with potential rounding
        expected_us = expected_datetime.microsecond
        actual_us = actual_datetime.microsecond
        # Allow for potential rounding at the 100ns level (within 1 microsecond)
        assert abs(actual_us - expected_us) <= 1, \
            f"Row {i+1}: Microseconds differ - expected {expected_us}, got {actual_us}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_all_precisions(client_context):
    """Test cursor bulkcopy with all DATETIME2 precision values (0-7).

    Tests datetime2 with each valid precision value to ensure proper handling
    of different fractional second precisions.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Test each precision from 0 to 7
    for precision in range(8):  # 0-7 inclusive
        table_name = f"BulkCopyTestDateTime2Precision{precision}"
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, event_datetime DATETIME2({precision}))")

        # Prepare test data with high-precision fractional seconds
        data = [
            (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 0)),
            (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 123456)),
            (3, datetime.datetime(2024, 3, 10, 0, 0, 59, 999999)),
        ]

        # Execute bulk copy
        result = cursor.bulkcopy(
            table_name, iter(data), batch_size=1000, timeout=30
        )

        # Verify bulk copy succeeded
        assert result is not None, f"Bulk copy failed for precision {precision}"
        assert result["rows_copied"] == 3, f"Expected 3 rows for precision {precision}, got {result['rows_copied']}"

        # Verify data was inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.fetchall()
        assert count == 3, f"Expected 3 rows in table for precision {precision}, got {count}"

        # Cleanup
        cursor.execute(f"DROP TABLE {table_name}")

    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_string_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to datetime2 columns.

    Tests type coercion when source data contains datetime strings but
    destination columns are DATETIME2 type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with datetime2 columns
    table_name = "BulkCopyStringToDateTime2Table"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME2, end_datetime DATETIME2)")

    # Prepare test data - strings containing valid datetimes in ISO format
    data = [
        ("2024-01-15 09:30:00.123456", "2024-01-15 17:45:30.654321"),
        ("2024-02-20 08:15:45.000000", "2024-02-20 16:30:15.999999"),
        ("2024-03-10 10:00:00", "2024-03-10 18:00:00"),  # Without fractional seconds
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
def test_cursor_bulkcopy_datetime2_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable datetime2 column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable datetime2 column
    table_name = "#BulkCopyNonNullableDateTime2Table"
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME2 NOT NULL)")

    # Prepare test data with a null value
    data = [
        (datetime.datetime(2024, 1, 15, 9, 30, 0, 123456),),
        (None,),  # This should trigger a conversion error
        (datetime.datetime(2024, 3, 10, 10, 0, 0, 0),),
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
def test_cursor_bulkcopy_datetime2_invalid_string_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to datetime2.

    Tests that client-side type coercion properly validates string-to-datetime2 conversion
    and fails with an appropriate error when the string is not a valid datetime.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with datetime2 columns
    table_name = "#BulkCopyInvalidStringDateTime2Table"
    cursor.execute(f"CREATE TABLE {table_name} (start_datetime DATETIME2, end_datetime DATETIME2)")

    # Prepare test data with invalid string that cannot be parsed as datetime
    data = [
        ("2024-01-15 09:30:00", "2024-01-15 17:45:30"),
        ("invalid_datetime_string", "2024-02-20 16:30:15"),  # This should trigger a conversion error
        ("2024-03-10 10:00:00", "2024-03-10 18:00:00"),
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
    ), "Expected a ValueError to be raised for invalid string-to-datetime2 conversion"
    assert (
        "cannot convert" in error_message or "conversion" in error_message or "invalid" in error_message
    ), f"Expected conversion error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_boundary_values(client_context):
    """Test cursor bulkcopy with boundary datetime2 values.

    Tests that datetime2 boundary values are properly handled.
    SQL Server DATETIME2 range: 0001-01-01 00:00:00.0000000 to 9999-12-31 23:59:59.9999999
    This is a much wider range than DATETIME (which starts at 1753-01-01).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with DATETIME2 columns
    table_name = "#BulkCopyDateTime2BoundaryTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_datetime DATETIME2)")

    # Prepare test data with boundary datetimes
    data = [
        (1, datetime.datetime(1, 1, 1, 0, 0, 0)),              # Minimum datetime2
        (2, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)), # Maximum datetime2
        (3, datetime.datetime(2024, 6, 15, 12, 0, 0, 123456)),    # Regular datetime
        (4, datetime.datetime(1753, 1, 1, 0, 0, 0)),              # Old DATETIME minimum
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
    cursor.execute(f"SELECT id, test_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    cursor.fetchall()  # Consume remaining result sets
    
    # Check each row's datetime value
    assert len(rows) == 4
    for i, (expected_id, expected_datetime) in enumerate(data):
        actual_id, actual_datetime = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch - expected {expected_id}, got {actual_id}"
        # Compare date and time components (allowing for microsecond rounding)
        assert actual_datetime.year == expected_datetime.year
        assert actual_datetime.month == expected_datetime.month
        assert actual_datetime.day == expected_datetime.day
        assert actual_datetime.hour == expected_datetime.hour
        assert actual_datetime.minute == expected_datetime.minute
        assert actual_datetime.second == expected_datetime.second
        # Allow small difference in microseconds due to precision
        assert abs(actual_datetime.microsecond - expected_datetime.microsecond) <= 1

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_year_one(client_context):
    """Test cursor bulkcopy with year 0001 datetime2 values.

    Tests datetime2's extended range compared to datetime. DATETIME2 can store dates
    starting from 0001-01-01, whereas DATETIME starts at 1753-01-01.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with DATETIME2 columns
    table_name = "BulkCopyTestDateTime2YearOne"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, ancient_datetime DATETIME2)")

    # Test datetime values from year 0001 and other early dates
    data = [
        (1, datetime.datetime(1, 1, 1, 0, 0, 0, 0)),           # Absolute minimum
        (2, datetime.datetime(1, 12, 31, 23, 59, 59, 999999)), # End of year 0001
        (3, datetime.datetime(100, 6, 15, 12, 0, 0, 500000)),  # Year 100
        (4, datetime.datetime(1000, 1, 1, 0, 0, 0, 0)),        # Year 1000
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 4

    # Verify the datetimes were inserted correctly
    cursor.execute(f"SELECT id, ancient_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    for i, (expected_id, expected_datetime) in enumerate(data):
        actual_id, actual_datetime = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch"
        # Verify year is preserved (this is the key test)
        assert actual_datetime.year == expected_datetime.year, \
            f"Row {i}: Year mismatch - expected {expected_datetime.year}, got {actual_datetime.year}"
        assert actual_datetime.month == expected_datetime.month
        assert actual_datetime.day == expected_datetime.day
        assert actual_datetime.hour == expected_datetime.hour
        assert actual_datetime.minute == expected_datetime.minute
        assert actual_datetime.second == expected_datetime.second

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_max_boundary_9999_12_31(client_context):
    """Test bulk copy with the maximum valid DATETIME2 value: 9999-12-31 23:59:59.9999999.
    
    This test verifies that the maximum datetime2 value is handled correctly.
    SQL Server DATETIME2 type has a maximum value of 9999-12-31 23:59:59.9999999.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopyTestMaxDateTime2"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, test_datetime DATETIME2)")

    # Test maximum datetime2 values
    data = [
        (1, datetime.datetime(1, 1, 1, 0, 0, 0)),                # Minimum
        (2, datetime.datetime(2024, 6, 15, 12, 30, 45, 123456)), # Regular
        (3, datetime.datetime(9999, 12, 31, 23, 59, 58, 0)),     # Near maximum
        (4, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)),# Maximum (with max microseconds)
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
    for i, (expected_id, expected_datetime) in enumerate(data):
        actual_id, actual_datetime = rows[i]
        assert actual_id == expected_id, f"Row {i}: ID mismatch"
        assert actual_datetime.year == expected_datetime.year
        assert actual_datetime.month == expected_datetime.month
        assert actual_datetime.day == expected_datetime.day
        assert actual_datetime.hour == expected_datetime.hour
        assert actual_datetime.minute == expected_datetime.minute
        assert actual_datetime.second == expected_datetime.second
        # Allow for potential rounding at the 100ns level
        assert abs(actual_datetime.microsecond - expected_datetime.microsecond) <= 1

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_datetime2_default_precision_without_explicit_scale(client_context):
    """Test that DATETIME2 without explicit precision defaults to scale 7.

    When creating a DATETIME2 column without specifying precision, SQL Server
    defaults to DATETIME2(7), which provides the maximum precision.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with DATETIME2 WITHOUT explicit precision (should default to 7)
    table_name = "BulkCopyTestDateTime2DefaultPrecision"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_datetime DATETIME2)")

    # Prepare test data with high-precision fractional seconds
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 123456)),
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 999999)),
        (3, datetime.datetime(2024, 3, 10, 0, 0, 0, 1)),  # 1 microsecond
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 3

    # Verify the data - default precision should be 7, preserving microseconds
    cursor.execute(f"SELECT id, event_datetime FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    for i, (expected_id, expected_datetime) in enumerate(data):
        actual_id, actual_datetime = rows[i]
        assert actual_id == expected_id
        # With default precision (7), microseconds should be preserved
        expected_us = expected_datetime.microsecond
        actual_us = actual_datetime.microsecond
        # Allow for potential rounding at the 100ns level (within 1 microsecond)
        assert abs(actual_us - expected_us) <= 1, \
            f"Row {i+1}: Microseconds differ - expected {expected_us}, got {actual_us}"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
