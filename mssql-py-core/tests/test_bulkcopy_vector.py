# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for VECTOR data type using Python end-to-end path.

These tests validate PyCoreCursor.bulkcopy() when Python provides vectors
as array.array('f', [...]) objects, ensuring correct mapping to SQL Server
VECTOR columns and on-wire serialization via the Rust core.

Note: Requires SQL Server 2025+ with VECTOR enabled.
"""

import pytest
import mssql_py_core
from array import array
from datetime import datetime
from decimal import Decimal


@pytest.mark.integration
def test_cursor_bulkcopy_vector_basic(client_context):
    """Bulk copy into a table with VECTOR(3), including a NULL, and verify roundtrip."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorBasic"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(3) NULL)")

    # Python provides vectors as plain list[float]
    data = [
        (1, [1.0, 2.0, 3.0]),
        (2, [4.0, 5.0, 6.0]),
        (3, [7.0, 8.0, 9.0]),
        (4, None),  # NULL vector
    ]

    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[(0, "id"), (1, "embedding")],
    )

    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1

    cursor.execute(f"SELECT id, embedding FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and list(rows[0][1]) == [1.0, 2.0, 3.0]
    assert rows[1][0] == 2 and list(rows[1][1]) == [4.0, 5.0, 6.0]
    assert rows[2][0] == 3 and list(rows[2][1]) == [7.0, 8.0, 9.0]
    assert rows[3][0] == 4 and rows[3][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_vector_max_dimensions(client_context):
    """Bulk copy VECTOR(1998) and verify values at max supported dimension."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorMaxDims"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(1998) NULL)")

    vals1 = [float(i) for i in range(1998)]
    vals2 = [i * 0.5 for i in range(1998)]
    data = [
        (1, vals1),
        (2, vals2),
    ]

    result = cursor.bulkcopy(table_name, iter(data))

    assert result is not None
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, embedding FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2

    # Row 1
    assert rows[0][0] == 1
    emb1 = list(rows[0][1])
    assert len(emb1) == 1998 and emb1[0] == 0.0 and emb1[50] == 50.0 and emb1[1997] == 1997.0

    # Row 2
    assert rows[1][0] == 2
    emb2 = list(rows[1][1])
    assert len(emb2) == 1998
    for i, v in enumerate(emb2):
        assert abs(v - (i * 0.5)) < 1e-3

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()

@pytest.mark.integration
def test_cursor_bulkcopy_vector_via_generator(client_context):
    """Bulk copy VECTOR(1013) via Python generator and verify values at max supported dimension."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorGenerator"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(1013) NULL)")

    vals = [float(i) for i in range(1013)]

    # Generator that yields (1, vals) n times
    def gen_rows(n: int, vals):
        for i in range(n):
            yield (i, vals)

    total_rows = 10
    result = cursor.bulkcopy(table_name, gen_rows(total_rows, vals), timeout=10000)

    assert result is not None
    assert result["rows_copied"] == total_rows

    # Validate count without fetching all rows into memory
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count_row = cursor.fetchone()
    assert count_row is not None
    assert count_row[0] == total_rows

    # Validate a sample row's embedding matches vals1
    cursor.execute(f"SELECT TOP 1 id, embedding FROM {table_name}")
    sample = cursor.fetchone()
    assert sample is not None
    emb = list(sample[1])
    assert len(emb) == 1013 and emb[0] == 0.0 and emb[50] == 50.0 and emb[1012] == 1012.0

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()

@pytest.mark.integration
def test_cursor_bulkcopy_vector_exceeds_max_dimensions(client_context):
    """Attempt to bulk copy a vector with >1998 dimensions; expect an error."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorTooLarge"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(1998) NULL)")

    # Build a vector that exceeds the max supported dimension
    too_many = [float(i) for i in range(1999)]
    data = [
        (1, too_many),
    ]

    # Expect client/server to reject >1998 dims
    error_raised = False
    try:
        result = cursor.bulkcopy(table_name, iter(data))
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
def test_cursor_bulkcopy_vector_wrong_dimension(client_context):
    """Sending a vector with wrong dimension for the destination column should fail."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorWrongDim"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(3) NULL)")

    # Wrong dimension: 2 elements but destination expects VECTOR(3)
    data = [
        (1, [1.0, 2.0]),
    ]

    # Expect client/server to reject wrong dimensions
    error_raised = False
    try:
        result = cursor.bulkcopy(table_name, iter(data))
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
def test_cursor_bulkcopy_vector_wrong_element_type(client_context):
    """Sending non-float elements (e.g., datetimes) should raise an error."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorWrongType"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(3) NULL)")

    # Wrong element type: provide non-float elements (datetimes)
    bad = [datetime.now(), datetime.now(), datetime.now()]
    data = [
        (1, bad),
    ]

    # Expect client/server to reject wrong element types
    error_raised = False
    try:
        result = cursor.bulkcopy(table_name, iter(data))
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
def test_cursor_bulkcopy_vector_wrong_python_type_decimal(client_context):
    """Providing a Decimal for a VECTOR column should raise an error (expects array('f'))."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorWrongPyTypeDecimal"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(3) NULL)")

    # Wrong Python type: a Decimal instead of array('f')
    data = [
        (1, Decimal("1.23")),
    ]

    # Expect client/server to reject wrong Python type
    error_raised = False
    try:
        result = cursor.bulkcopy(table_name, iter(data))
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
def test_cursor_bulkcopy_vector_from_json_string(client_context):
    """Bulk copy a JSON float array string into a VECTOR column and verify roundtrip."""
    import json
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorFromJson"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(4) NULL)")

    # JSON string representing a float array
    float_list = [1.1, 2.2, 3.3, 4.4]
    json_str = json.dumps(float_list)
    data = [
        (1, json_str),
        (2, None),  # NULL vector
    ]

    # The Rust/Python core should accept a JSON string and convert to VECTOR
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        batch_size=1000,
        timeout=30,
        column_mappings=[(0, "id"), (1, "embedding")],
    )

    assert result is not None
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT id, embedding FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 1 and [round(x, 2) for x in list(rows[0][1])] == [1.1, 2.2, 3.3, 4.4]
    assert rows[1][0] == 2 and rows[1][1] is None

    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_vector_from_bad_json_string_1(client_context):
    """Bulk copy with an ill-formatted JSON string (missing closing bracket) into VECTOR column should fail."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorFromBadJson1"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(4) NULL)")

    # Ill-formatted JSON string (missing closing bracket)
    bad_json_str = "[1.1, 2.2, 3.3, 4.4"
    data = [
        (1, bad_json_str),
    ]

    error_raised = False
    try:
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "embedding")],
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
def test_cursor_bulkcopy_vector_from_bad_json_string_2(client_context):
    """Bulk copy with an ill-formatted JSON string (non-numeric element) into VECTOR column should fail."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorFromBadJson2"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(4) NULL)")

    # Ill-formatted JSON string (contains non-numeric string element)
    bad_json_str = "[9.9, 0, testStr, -1]"
    data = [
        (1, bad_json_str),
    ]

    error_raised = False
    try:
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "embedding")],
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
def test_cursor_bulkcopy_vector_from_bad_json_string_3(client_context):
    """Bulk copy with JSON string having wrong dimensions into VECTOR column should fail."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyVectorFromBadJson3"
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, embedding VECTOR(4) NULL)")

    # JSON string with wrong dimension (3 elements but table expects VECTOR(4))
    bad_json_str = "[1.1, 2.2, 3.3]"
    data = [
        (1, bad_json_str),
    ]

    error_raised = False
    try:
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "embedding")],
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