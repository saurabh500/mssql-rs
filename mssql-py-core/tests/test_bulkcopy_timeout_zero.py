# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for bulk copy with timeout=0 (infinite timeout)."""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_bulkcopy_timeout_zero_completes(client_context):
    """Verify that timeout=0 means infinite timeout, not instant expiry."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "BulkCopyTimeoutZeroTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id BIGINT, value BIGINT)")

    data = [(i, i * 10) for i in range(100)]

    try:
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=50,
            timeout=0,
            column_mappings=[(0, "id"), (1, "value")],
        )

        assert result is not None
        assert result["rows_copied"] == 100
        assert result["batch_count"] == 2

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 100
    finally:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        conn.close()
