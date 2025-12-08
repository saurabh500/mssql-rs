# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Integration tests for PyConnection with real database."""
import os
import pytest
from pathlib import Path
from dotenv import load_dotenv


def get_db_credentials():
    """Get database credentials from environment variables or /tmp/password."""
    # Load .env file from project root
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    
    username = os.environ.get("DB_USERNAME", "sa")
    
    # Try SQL_PASSWORD env var first, then /tmp/password file
    password = os.environ.get("SQL_PASSWORD")
    if not password:
        try:
            with open("/tmp/password", "r") as f:
                password = f.read().strip()
        except FileNotFoundError:
            pytest.skip("SQL_PASSWORD not set and /tmp/password not found")
    
    return username, password


def get_server_address():
    """Get server address from environment variable."""
    return os.environ.get("SQL_SERVER", "localhost")


def get_database_name():
    """Get database name from environment variable."""
    return os.environ.get("SQL_DATABASE", "master")


def trust_server_certificate():
    """Get trust server certificate setting from environment."""
    return os.environ.get("TRUST_SERVER_CERTIFICATE", "false").lower() == "true"


@pytest.mark.integration
def test_connection_real_database():
    """Test connection to a real database."""
    import mssql_py_core
    
    username, password = get_db_credentials()
    server = get_server_address()
    database = get_database_name()
    
    client_context = {
        "server": server,
        "user_name": username,
        "password": password,
        "database": database,
        "trust_server_certificate": trust_server_certificate(),
        "encryption": "Optional"
    }
    
    conn = mssql_py_core.PyCoreConnection(client_context)
    assert conn is not None
    assert conn.is_connected()
    conn.close()
    assert not conn.is_connected()
