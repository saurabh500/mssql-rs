"""
Tests for cursor bulkcopy with DATETIMEOFFSET SQL Server type.

DATETIMEOFFSET is a date and time data type with timezone offset awareness.
- Range: 0001-01-01 00:00:00.0000000 through 9999-12-31 23:59:59.9999999
- Scale: 0 to 7 (fractional seconds precision)
- Timezone offset: -14:00 to +14:00 (stored as minutes from UTC, -840 to +840)
- Storage: DateTime2 (variable) + 2 bytes for offset
- Precision: 100 nanoseconds
"""

import datetime
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_basic(client_context):
    """Test basic cursor bulkcopy with DATETIMEOFFSET type.
    
    Tests basic datetimeoffset insertion with timezone-aware datetime objects.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopyTestDateTimeOffset"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET)")

    # Prepare test data with timezone-aware datetime objects
    utc_tz = datetime.timezone.utc
    est_tz = datetime.timezone(datetime.timedelta(hours=-5))
    ist_tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 45, 123456, tzinfo=utc_tz)),
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 500000, tzinfo=est_tz)),
        (3, datetime.datetime(2024, 3, 10, 18, 15, 0, 0, tzinfo=ist_tz)),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify bulk copy succeeded
    assert result is not None
    assert result["rows_copied"] == 3

    # Verify the data
    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[1][0] == 2
    assert rows[2][0] == 3
    
    # Verify datetime values and timezone offsets are preserved
    assert rows[0][1].year == 2024
    assert rows[0][1].month == 1
    assert rows[0][1].day == 15
    assert rows[0][1].utcoffset() == datetime.timedelta(0)
    
    assert rows[1][1].utcoffset() == datetime.timedelta(hours=-5)
    assert rows[2][1].utcoffset() == datetime.timedelta(hours=5, minutes=30)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_auto_mapping(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET using auto column mapping.
    
    Ensures automatic column mapping works correctly with datetimeoffset columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetAutoMap"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, timestamp_col DATETIMEOFFSET)"
    )

    utc_tz = datetime.timezone.utc
    data = [
        (1, datetime.datetime(2024, 6, 1, 12, 0, 0, tzinfo=utc_tz)),
        (2, datetime.datetime(2024, 6, 2, 13, 30, 15, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))

    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 2
    # Consume any remaining rows
    cursor.fetchall()

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_precision_0(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET(0) - no fractional seconds.
    
    Tests datetimeoffset with precision 0, which stores only whole seconds.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetPrecision0"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET(0))")

    utc_tz = datetime.timezone.utc
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 15, 0, tzinfo=utc_tz)),
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 123456, tzinfo=utc_tz)),
        (3, datetime.datetime(2024, 3, 10, 0, 0, 59, 999999, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    assert result is not None
    assert result["rows_copied"] == 3

    # Verify the data - precision 0 means no fractional seconds
    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    # All fractional seconds should be 0
    for row in rows:
        assert row[1].microsecond == 0, f"Expected 0 microseconds for precision 0, got {row[1].microsecond}"

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_precision_3(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET(3) - millisecond precision."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetPrecision3"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET(3))")

    utc_tz = datetime.timezone.utc
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 45, 123000, tzinfo=utc_tz)),
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 456000, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    # Precision 3 = milliseconds, so microseconds should be rounded to nearest millisecond
    # 123000 microseconds = 123 milliseconds (preserved)
    assert rows[0][1].microsecond % 1000 == 0  # Should be rounded to milliseconds

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_precision_7_default(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET(7) - maximum precision (default)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetPrecision7"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET(7))")

    utc_tz = datetime.timezone.utc
    data = [
        (1, datetime.datetime(2024, 1, 15, 9, 30, 45, 123456, tzinfo=utc_tz)),
        (2, datetime.datetime(2024, 2, 20, 14, 45, 30, 999999, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    # Precision 7 supports 100ns resolution, Python datetime supports microseconds (1us = 10 * 100ns)
    # So microseconds should be preserved
    assert rows[0][1].microsecond == 123456
    assert rows[1][1].microsecond == 999999

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_all_precisions(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET for all precision scales (0-7)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    utc_tz = datetime.timezone.utc
    
    for scale in range(8):  # Scale 0-7
        table_name = f"BulkCopyTestDateTimeOffsetScale{scale}"
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(
            f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET({scale}))"
        )

        data = [
            (1, datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=utc_tz)),
            (2, datetime.datetime(2024, 6, 15, 12, 30, 45, 123456, tzinfo=utc_tz)),
        ]

        result = cursor.bulkcopy(table_name, iter(data))
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        assert count == 2
        # Consume any remaining rows
        cursor.fetchall()

        cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_string_conversion(client_context):
    """Test cursor bulkcopy with string to DATETIMEOFFSET conversion.
    
    Tests that ISO 8601 datetime strings with timezone offsets are correctly
    converted to DATETIMEOFFSET values.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetString"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET)")

    # ISO 8601 format strings with timezone offsets
    data = [
        (1, "2024-01-15T09:30:45+00:00"),
        (2, "2024-02-20T14:45:30-05:00"),
        (3, "2024-03-10T18:15:00+05:30"),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    assert rows[0][1].year == 2024
    assert rows[0][1].month == 1
    assert rows[0][1].utcoffset() == datetime.timedelta(0)
    
    assert rows[1][1].utcoffset() == datetime.timedelta(hours=-5)
    assert rows[2][1].utcoffset() == datetime.timedelta(hours=5, minutes=30)

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_null_to_non_nullable_column(client_context):
    """Test that NULL values are rejected for non-nullable DATETIMEOFFSET columns."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetNonNullable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET NOT NULL)"
    )

    data = [
        (1, datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)),
        (2, None),  # This should cause an error
    ]

    with pytest.raises(ValueError, match="Cannot insert NULL value into non-nullable column"):
        cursor.bulkcopy(table_name, iter(data))


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_invalid_string_conversion(client_context):
    """Test that invalid datetime strings are rejected when converting to DATETIMEOFFSET."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetInvalidString"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET)")

    # Invalid datetime string
    data = [
        (1, "not-a-datetime"),
    ]

    with pytest.raises(Exception):  # Should raise an error
        cursor.bulkcopy(table_name, iter(data))


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_boundary_values(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET boundary values.
    
    Tests minimum and maximum values for DATETIMEOFFSET type with various timezone offsets.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetBoundary"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, boundary_time DATETIMEOFFSET)")

    # Test various timezone offsets at boundaries
    min_tz = datetime.timezone(datetime.timedelta(hours=-14))  # Minimum offset
    max_tz = datetime.timezone(datetime.timedelta(hours=14))   # Maximum offset
    utc_tz = datetime.timezone.utc
    
    data = [
        (1, datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=min_tz)),
        (2, datetime.datetime(2024, 12, 31, 23, 59, 59, 999999, tzinfo=max_tz)),
        (3, datetime.datetime(2024, 6, 15, 12, 30, 0, 0, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, boundary_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    assert rows[0][1].utcoffset() == datetime.timedelta(hours=-14)
    assert rows[1][1].utcoffset() == datetime.timedelta(hours=14)
    assert rows[2][1].utcoffset() == datetime.timedelta(0)

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_year_one(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET values from year 0001.
    
    SQL Server DATETIMEOFFSET supports dates from 0001-01-01, unlike DATETIME
    which starts at 1753-01-01.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetYearOne"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, ancient_time DATETIMEOFFSET)")

    utc_tz = datetime.timezone.utc
    est_tz = datetime.timezone(datetime.timedelta(hours=-5))
    
    data = [
        (1, datetime.datetime(1, 1, 1, 0, 0, 0, 0, tzinfo=utc_tz)),
        (2, datetime.datetime(1, 6, 15, 12, 0, 0, 0, tzinfo=utc_tz)),
        (3, datetime.datetime(1, 12, 31, 23, 59, 59, 0, tzinfo=est_tz)),
        (4, datetime.datetime(2, 1, 1, 0, 0, 0, 0, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 4

    # Verify the datetimes were inserted correctly
    cursor.execute(f"SELECT id, ancient_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 4
    assert rows[0][1].year == 1
    assert rows[0][1].month == 1
    assert rows[0][1].day == 1
    assert rows[0][1].utcoffset() == datetime.timedelta(0)
    
    assert rows[2][1].year == 1
    assert rows[2][1].month == 12
    assert rows[2][1].day == 31
    assert rows[2][1].utcoffset() == datetime.timedelta(hours=-5)

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_max_boundary_9999_12_31(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET maximum date (9999-12-31)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetMaxBoundary"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, future_time DATETIMEOFFSET)")

    utc_tz = datetime.timezone.utc
    pst_tz = datetime.timezone(datetime.timedelta(hours=-8))
    
    data = [
        (1, datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=utc_tz)),
        (2, datetime.datetime(9999, 12, 31, 0, 0, 0, 0, tzinfo=pst_tz)),
        (3, datetime.datetime(9999, 1, 1, 0, 0, 0, 0, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, future_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    assert rows[0][1].year == 9999
    assert rows[0][1].month == 12
    assert rows[0][1].day == 31
    assert rows[0][1].utcoffset() == datetime.timedelta(0)
    
    assert rows[1][1].utcoffset() == datetime.timedelta(hours=-8)

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_default_precision_without_explicit_scale(
    client_context,
):
    """Test cursor bulkcopy with DATETIMEOFFSET without explicit scale specification.
    
    When scale is not specified, SQL Server uses default precision of 7.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetDefaultScale"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    # No explicit scale - defaults to DATETIMEOFFSET(7)
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET)")

    utc_tz = datetime.timezone.utc
    data = [
        (1, datetime.datetime(2024, 1, 1, 0, 0, 0, 0, tzinfo=utc_tz)),
        (2, datetime.datetime(2024, 6, 15, 12, 30, 45, 123456, tzinfo=utc_tz)),
        (3, datetime.datetime(2024, 12, 31, 23, 59, 59, 999999, tzinfo=utc_tz)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 3
    # Default precision 7 should preserve microseconds
    assert rows[1][1].microsecond == 123456
    assert rows[2][1].microsecond == 999999

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_datetimeoffset_mixed_timezones(client_context):
    """Test cursor bulkcopy with DATETIMEOFFSET values in different timezones."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTestDateTimeOffsetMixedTZ"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, event_time DATETIMEOFFSET)")

    # Various timezones
    utc = datetime.timezone.utc
    pst = datetime.timezone(datetime.timedelta(hours=-8))
    est = datetime.timezone(datetime.timedelta(hours=-5))
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    jst = datetime.timezone(datetime.timedelta(hours=9))
    custom = datetime.timezone(datetime.timedelta(hours=3, minutes=45))
    
    data = [
        (1, datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=utc)),
        (2, datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=pst)),
        (3, datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=est)),
        (4, datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=ist)),
        (5, datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=jst)),
        (6, datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=custom)),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 6

    cursor.execute(f"SELECT id, event_time FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 6
    assert rows[0][1].utcoffset() == datetime.timedelta(0)
    assert rows[1][1].utcoffset() == datetime.timedelta(hours=-8)
    assert rows[2][1].utcoffset() == datetime.timedelta(hours=-5)
    assert rows[3][1].utcoffset() == datetime.timedelta(hours=5, minutes=30)
    assert rows[4][1].utcoffset() == datetime.timedelta(hours=9)
    assert rows[5][1].utcoffset() == datetime.timedelta(hours=3, minutes=45)

    cursor.execute(f"DROP TABLE {table_name}")
