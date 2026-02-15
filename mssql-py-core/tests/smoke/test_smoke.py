# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""PyCore smoke tests — validates basic TDS connectivity and operations.

Uses the root conftest.py fixtures (connection, client_context) which
respect SMOKE_AUTH_MODE for auth switching between sql_auth, access_token,
and managed_identity.
"""

import mssql_py_core
import pytest

pytestmark = pytest.mark.smoke


class TestConnectivity:

    def test_connect(self, connection):
        assert connection.is_connected()

    def test_select_one(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT 1 AS val")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    def test_server_version(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        assert row is not None
        assert "Microsoft SQL" in str(row[0])

    def test_session_info(self, connection):
        cursor = connection.cursor()
        cursor.execute(
            "SELECT @@SERVERNAME, DB_NAME(), SUSER_NAME(), "
            "CONNECTIONPROPERTY('net_transport')"
        )
        row = cursor.fetchone()
        assert row is not None
        assert len(row) >= 4


class TestQueries:

    def test_multiple_columns(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT 1 AS a, 'hello' AS b, CAST(3.14 AS FLOAT) AS c")
        row = cursor.fetchone()
        assert row[0] == 1
        assert row[1] == "hello"
        assert abs(row[2] - 3.14) < 0.001

    def test_null_handling(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT NULL, CAST(NULL AS INT), CAST(NULL AS VARCHAR(10))")
        row = cursor.fetchone()
        assert row[0] is None
        assert row[1] is None
        assert row[2] is None

    def test_temp_table_roundtrip(self, connection):
        cursor = connection.cursor()
        cursor.execute(
            "CREATE TABLE #smoke_test (id INT, name NVARCHAR(50)); "
            "INSERT INTO #smoke_test VALUES (1, N'smoke'); "
            "SELECT id, name FROM #smoke_test"
        )
        row = cursor.fetchone()
        assert row == (1, "smoke")

    def test_multiple_batches(self, connection):
        cursor = connection.cursor()
        cursor.execute("SELECT 'batch1'")
        r1 = cursor.fetchone()
        cursor.execute("SELECT 'batch2'")
        r2 = cursor.fetchone()
        assert r1[0] == "batch1"
        assert r2[0] == "batch2"


class TestReconnection:

    def test_fresh_connection(self, client_context):
        conn = mssql_py_core.PyCoreConnection(client_context)
        assert conn.is_connected()
        cursor = conn.cursor()
        cursor.execute("SELECT 42")
        assert cursor.fetchone()[0] == 42
        conn.close()
        assert not conn.is_connected()
