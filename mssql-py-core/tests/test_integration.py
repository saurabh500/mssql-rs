# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Integration tests for PyConnection with real database."""
import pytest


@pytest.mark.integration
def test_connection_real_database(client_context):
    """Test connection to a real database."""
    import mssql_py_core

    conn = mssql_py_core.PyCoreConnection(client_context)
    assert conn is not None
    assert conn.is_connected()
    conn.close()
    assert not conn.is_connected()
