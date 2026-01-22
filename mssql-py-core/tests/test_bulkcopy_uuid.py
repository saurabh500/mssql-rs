
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for UNIQUEIDENTIFIER (UUID/GUID) data type using Python end-to-end path.

These tests validate PyCoreCursor.bulkcopy() for UUID columns, ensuring correct mapping
from Python UUID objects to SQL Server UNIQUEIDENTIFIER columns and on-wire serialization
via the Rust core.
"""

import pytest
import mssql_py_core
import uuid

@pytest.mark.integration
def test_cursor_bulkcopy_uuid_basic(client_context):
    """Bulk copy UUID values into a table with UNIQUEIDENTIFIER column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyUuidTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, uuid_col UNIQUEIDENTIFIER NULL)")

    # Test UUIDs - various formats and versions
    test_uuid1 = uuid.UUID("6F9619FF-8B86-D011-B42D-00C04FC964FF")
    test_uuid2 = uuid.UUID("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11")
    test_uuid3 = uuid.UUID("00000000-0000-0000-0000-000000000000")  # NIL UUID
    test_uuid4 = uuid.uuid4()  # Random UUID (v4)
    test_uuid5 = uuid.uuid1()  # Time-based UUID (v1)
    test_uuid6 = uuid.uuid5(uuid.NAMESPACE_DNS, 'example.com')  # Name-based UUID (v5)

    data = [
        (1, test_uuid1),
        (2, test_uuid2),
        (3, None),  # NULL
        (4, test_uuid3),  # NIL UUID
        (5, test_uuid4),  # v4
        (6, test_uuid5),  # v1
        (7, test_uuid6),  # v5
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "uuid_col")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 7

    cursor.execute(f"SELECT id, uuid_col FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 7
  
    # Verify each row - all UUID versions should round-trip correctly
    assert rows[0][0] == 1 and rows[0][1] == test_uuid1
    assert rows[1][0] == 2 and rows[1][1] == test_uuid2
    assert rows[2][0] == 3 and rows[2][1] is None
    assert rows[3][0] == 4 and rows[3][1] == test_uuid3
    assert rows[4][0] == 5 and rows[4][1] == test_uuid4
    assert rows[5][0] == 6 and rows[5][1] == test_uuid5
    assert rows[6][0] == 7 and rows[6][1] == test_uuid6

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_uuid_multiple_columns(client_context):
    """Bulk copy multiple UUID columns."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyMultiUuidTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, uuid1 UNIQUEIDENTIFIER, uuid2 UNIQUEIDENTIFIER, uuid3 UNIQUEIDENTIFIER NULL)")

    uuid1 = uuid.UUID("11111111-1111-1111-1111-111111111111")
    uuid2 = uuid.UUID("22222222-2222-2222-2222-222222222222")
    uuid3 = uuid.UUID("33333333-3333-3333-3333-333333333333")

    data = [
        (1, uuid1, uuid2, uuid3),
        (2, uuid.uuid4(), uuid.uuid4(), None),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "uuid1"), (2, "uuid2"), (3, "uuid3")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, uuid1, uuid2, uuid3 FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2

    # Verify first row
    assert rows[0][0] == 1
    assert rows[0][1] == uuid1
    assert rows[0][2] == uuid2
    assert rows[0][3] == uuid3

    # Verify second row
    assert rows[1][0] == 2
    assert rows[1][3] is None  # uuid3 should be NULL

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_uuid_mixed_types(client_context):
    """Bulk copy UUID with mixed data types."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyMixedUuidTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NVARCHAR(50), uuid_col UNIQUEIDENTIFIER, value DECIMAL(10,2))")

    test_uuid = uuid.UUID("ABCDEF01-2345-6789-ABCD-EF0123456789")

    data = [
        (1, "Test Item 1", test_uuid, 123.45),
        (2, "Test Item 2", uuid.uuid4(), 678.90),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "name"), (2, "uuid_col"), (3, "value")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, name, uuid_col, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2

    # Verify first row
    assert rows[0][0] == 1
    assert rows[0][1] == "Test Item 1"
    assert rows[0][2] == test_uuid
    assert float(rows[0][3]) == 123.45

    # Verify second row
    assert rows[1][0] == 2
    assert rows[1][1] == "Test Item 2"
    assert isinstance(rows[1][2], uuid.UUID)
    assert float(rows[1][3]) == 678.90

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_uuid_string_valid_formats(client_context):
    """Bulk copy UUID strings in valid formats to UNIQUEIDENTIFIER column.

    Valid UUID string formats:
    1. Hyphenated (standard): "6f9619ff-8b86-d011-b42d-00c04fc964ff"
    2. Without hyphens: "6f9619ff8b86d011b42d00c04fc964ff"
    3. With braces: "{6f9619ff-8b86-d011-b42d-00c04fc964ff}"
    4. URN format: "urn:uuid:6f9619ff-8b86-d011-b42d-00c04fc964ff"
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyUuidStringTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, format_name VARCHAR(50), uuid_col UNIQUEIDENTIFIER)")

    # Test various valid UUID string formats
    uuid_hyphenated = "6f9619ff-8b86-d011-b42d-00c04fc964ff"
    uuid_no_hyphens = "6f9619ff8b86d011b42d00c04fc964ff"
    uuid_braces = "{6f9619ff-8b86-d011-b42d-00c04fc964ff}"
    uuid_urn = "urn:uuid:6f9619ff-8b86-d011-b42d-00c04fc964ff"

    # All formats represent the same UUID
    expected_uuid = uuid.UUID(uuid_hyphenated)

    data = [
        (1, "Hyphenated", uuid_hyphenated),
        (2, "No hyphens", uuid_no_hyphens),
        (3, "With braces", uuid_braces),
        (4, "URN format", uuid_urn),
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "format_name"), (2, "uuid_col")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT id, format_name, uuid_col FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4

    # All rows should have the same UUID value (different string formats represent same UUID)
    assert rows[0][1] == "Hyphenated" and rows[0][2] == expected_uuid
    assert rows[1][1] == "No hyphens" and rows[1][2] == expected_uuid
    assert rows[2][1] == "With braces" and rows[2][2] == expected_uuid
    assert rows[3][1] == "URN format" and rows[3][2] == expected_uuid

    cursor.execute(f"DROP TABLE {table_name}")


@pytest.mark.integration
def test_cursor_bulkcopy_uuid_string_invalid_format(client_context):
    """Bulk copy with invalid UUID string format should fail."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyUuidInvalidTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, uuid_col UNIQUEIDENTIFIER)")

    # Invalid UUID string formats
    data = [
        (1, "not-a-uuid"),  # Invalid format
        (2, "12345"),  # Too short
    ]

    # Bulk copy should fail due to invalid UUID format
    error_raised = False
    try:
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "timeout": 30,
                "column_mappings": [(0, "id"), (1, "uuid_col")],
            },
        )
        print(f"No error raised. Result: {result}")
    except (ValueError, RuntimeError) as e:
        error_raised = True
        print(f"Expected error caught: {e}")
    assert error_raised

    # Cleanup can surface a deferred server error; ignore for DROP
    try:
        cursor.execute(f"DROP TABLE {table_name}")
    except Exception:
        pass
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_uuid_invalid_type(client_context):
    """Bulk copy with invalid Python type (list) to UUID column should fail."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyUuidInvalidTypeTest"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, uuid_col UNIQUEIDENTIFIER)")

    # Invalid Python types for UUID column
    data = [
        (1, [1, 2, 3, 4]),  # List is not compatible with UUID
        (2, {"key": "value"}),  # Dict is not compatible with UUID
    ]

    # Bulk copy should fail due to invalid type
    error_raised = False
    try:
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            kwargs={
                "timeout": 30,
                "column_mappings": [(0, "id"), (1, "uuid_col")],
            },
        )
        print(f"No error raised. Result: {result}")
    except (ValueError, RuntimeError) as e:
        error_raised = True
        print(f"Expected error caught: {e}")
    assert error_raised

    # Cleanup can surface a deferred server error; ignore for DROP
    try:
        cursor.execute(f"DROP TABLE {table_name}")
    except Exception:
        pass
    conn.close()
