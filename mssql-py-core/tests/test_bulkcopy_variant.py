# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for SQL_VARIANT data type using Python end-to-end path.

These tests validate PyCoreCursor.bulkcopy() when Python provides various types
that should be wrapped in SQL_VARIANT, ensuring correct mapping and on-wire
serialization via the Rust core.

Note: SQL_VARIANT can contain most SQL Server data types except text, ntext,
      image, timestamp, sql_variant, vector, xml, and json.
"""

import pytest
import mssql_py_core
from datetime import datetime, date, time
from decimal import Decimal
from uuid import UUID


@pytest.mark.integration
def test_cursor_bulkcopy_variant_integers(client_context):
    """Bulk copy various integer types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantIntegers"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, 42),                    # int
        (2, -12345),                # negative int
        (3, 9223372036854775807),   # bigint (max i64)
        (4, 255),                   # tinyint range
        (5, None),                  # NULL
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [(0, "id"), (1, "data")],
        },
    )

    assert result is not None
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5

    # Verify values and base types
    assert rows[0][1] == 42 and rows[0][2] == 'int'
    assert rows[1][1] == -12345 and rows[1][2] == 'int'
    assert rows[2][1] == 9223372036854775807 and rows[2][2] == 'bigint'
    assert rows[3][1] == 255 and rows[3][2] == 'int'
    assert rows[4][1] is None and rows[4][2] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_floats(client_context):
    """Bulk copy floating point types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantFloats"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, 3.14159),
        (2, -2.71828),
        (3, 1.23e10),
        (4, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4

    assert abs(rows[0][1] - 3.14159) < 0.001 and rows[0][2] == 'float'
    assert abs(rows[1][1] - (-2.71828)) < 0.001 and rows[1][2] == 'float'
    assert abs(rows[2][1] - 1.23e10) < 1e6 and rows[2][2] == 'float'
    assert rows[3][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_strings(client_context):
    """Bulk copy string types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantStrings"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, "Hello, World!"),
        (2, "Unicode: 你好"),
        (3, ""),
        (4, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4

    assert rows[0][1] == "Hello, World!" and rows[0][2] == 'nvarchar'
    assert rows[1][1] == "Unicode: 你好" and rows[1][2] == 'nvarchar'
    assert rows[2][1] == "" and rows[2][2] == 'nvarchar'
    assert rows[3][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_datetime(client_context):
    """Bulk copy datetime types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantDateTime"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    dt = datetime(2024, 12, 31, 23, 59, 59, 123456)
    d = date(2024, 6, 15)
    t = time(14, 30, 45, 123456)

    data = [
        (1, dt),
        (2, d),
        (3, t),
        (4, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4

    # Note: datetime might be stored as datetime2
    assert rows[0][2] in ('datetime', 'datetime2')
    assert rows[1][2] == 'date'
    assert rows[2][2] == 'time'
    assert rows[3][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_decimal(client_context):
    """Bulk copy Decimal types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantDecimal"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, Decimal("123.45")),
        (2, Decimal("-999.999")),
        (3, Decimal("0.001")),
        (4, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4

    assert rows[0][2] == 'decimal'
    assert rows[1][2] == 'decimal'
    assert rows[2][2] == 'decimal'
    assert rows[3][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_bool(client_context):
    """Bulk copy boolean types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantBool"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, True),
        (2, False),
        (3, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3

    assert rows[0][1] == True and rows[0][2] == 'bit'
    assert rows[1][1] == False and rows[1][2] == 'bit'
    assert rows[2][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_binary(client_context):
    """Bulk copy binary types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantBinary"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, b'\x00\x01\x02\x03'),
        (2, b'binary data'),
        (3, b''),
        (4, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4

    assert rows[0][2] == 'varbinary'
    assert rows[1][2] == 'varbinary'
    assert rows[2][2] == 'varbinary'
    assert rows[3][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_uuid(client_context):
    """Bulk copy UUID types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantUuid"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    uuid1 = UUID('12345678-1234-5678-1234-567812345678')
    uuid2 = UUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')

    data = [
        (1, uuid1),
        (2, uuid2),
        (3, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT id, data, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3

    assert rows[0][2] == 'uniqueidentifier'
    assert rows[1][2] == 'uniqueidentifier'
    assert rows[2][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_variant_mixed_types(client_context):
    """Bulk copy mixed types into SQL_VARIANT column."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVariantMixed"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, data SQL_VARIANT NULL)")

    data = [
        (1, 42),
        (2, "Hello"),
        (3, 3.14),
        (4, True),
        (5, b'\x00\x01\x02'),
        (6, Decimal("123.45")),
        (7, date(2024, 1, 1)),
        (8, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result is not None
    assert result["rows_copied"] == 8

    cursor.execute(f"SELECT id, SQL_VARIANT_PROPERTY(data, 'BaseType') FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 8

    assert rows[0][1] == 'int'
    assert rows[1][1] == 'nvarchar'
    assert rows[2][1] == 'float'
    assert rows[3][1] == 'bit'
    assert rows[4][1] == 'varbinary'
    assert rows[5][1] == 'decimal'
    assert rows[6][1] == 'date'
    assert rows[7][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
