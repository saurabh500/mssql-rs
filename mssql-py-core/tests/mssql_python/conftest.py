# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Fixtures for mssql_python driver tests."""
import os
from pathlib import Path
import pytest
from dotenv import load_dotenv
from mssql_python import connect

# Load .env from mssql-tds root (3 levels up from this file)
_env_file = Path(__file__).parent.parent.parent.parent / ".env"
if _env_file.exists():
    load_dotenv(_env_file)


def get_connection_string():
    """Build connection string from environment variables."""
    conn_str = os.environ.get("DB_CONNECTION_STRING")
    if conn_str:
        return conn_str
    
    server = os.environ.get("DB_HOST", os.environ.get("SQL_SERVER", "localhost"))
    database = os.environ.get("SQL_DATABASE", "master")
    username = os.environ.get("DB_USERNAME", "sa")
    
    # Try SQL_PASSWORD env var first, then /tmp/password file (same as main conftest.py)
    password = os.environ.get("SQL_PASSWORD")
    if not password:
        try:
            with open("/tmp/password", "r") as f:
                password = f.read().strip()
        except FileNotFoundError:
            pytest.skip("SQL_PASSWORD not set and /tmp/password not found")
    
    return f"Server={server};Database={database};UID={username};PWD={password};TrustServerCertificate=Yes"


@pytest.fixture
def conn_str():
    """Connection string fixture."""
    return get_connection_string()


@pytest.fixture
def connection(conn_str):
    """Provides a connected mssql_python connection."""
    conn = connect(conn_str, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture
def cursor(connection):
    """Provides a cursor from the connection."""
    cur = connection.cursor()
    yield cur
    cur.close()
