# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for PyCoreConnection functionality."""
import time

import pytest
import mssql_py_core


def test_module_import():
    """Test that the mssql_py_core module can be imported."""
    assert mssql_py_core is not None


@pytest.mark.integration
def test_connection_close_terminates_server_session(client_context):
    """Verify close() terminates the server-side SPID."""
    # Open a monitoring connection first (gets its own SPID)
    monitor = mssql_py_core.PyCoreConnection(client_context)
    mon_cursor = monitor.cursor()

    # Open the connection under test
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()
    cursor.execute("SELECT @@SPID")
    spid = cursor.fetchone()[0]

    # Confirm SPID is alive
    mon_cursor.execute(
        f"SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {spid}"
    )
    assert mon_cursor.fetchone()[0] == 1, "SPID should be alive before close"

    # Close and verify SPID is gone
    conn.close()
    time.sleep(0.5)

    mon_cursor.execute(
        f"SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {spid}"
    )
    assert mon_cursor.fetchone()[0] == 0, "SPID should be gone after close"

    monitor.close()


@pytest.mark.integration
def test_connection_close_rejects_new_cursor(client_context):
    """Verify cursor() raises after close()."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    conn.close()

    with pytest.raises(RuntimeError, match="closed"):
        conn.cursor()


@pytest.mark.integration
def test_connection_close_rejects_query_on_existing_cursor(client_context):
    """Verify execute() on an existing cursor fails after connection close."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchone()

    conn.close()

    with pytest.raises(RuntimeError):
        cursor.execute("SELECT 1")


@pytest.mark.integration
def test_connection_close_after_bulkcopy(client_context):
    """Verify close() works correctly after a bulk copy operation."""
    monitor = mssql_py_core.PyCoreConnection(client_context)
    mon_cursor = monitor.cursor()

    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()
    cursor.execute("SELECT @@SPID")
    spid = cursor.fetchone()[0]

    table_name = "TestConnectionCloseBCP"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, val NVARCHAR(5))")

    rows = [(i, f"r{i}") for i in range(10)]
    cursor.bulkcopy(table_name, iter(rows), batch_size=100, timeout=30, table_lock=True)

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert cursor.fetchone()[0] == 10

    conn.close()
    time.sleep(0.5)

    # SPID should be gone
    mon_cursor.execute(
        f"SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = {spid}"
    )
    assert mon_cursor.fetchone()[0] == 0, "SPID should be gone after close"

    # Data should persist (committed before close)
    mon_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    assert mon_cursor.fetchone()[0] == 10

    mon_cursor.execute(f"DROP TABLE {table_name}")
    monitor.close()


@pytest.mark.integration
def test_connection_double_close(client_context):
    """Verify calling close() twice does not error."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    conn.close()
    conn.close()  # should be a no-op
