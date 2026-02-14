# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Shared test fixtures and utilities."""
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


def get_client_context():
    """Get client context dictionary for connecting to the database."""
    username, password = get_db_credentials()
    server = get_server_address()
    database = get_database_name()
    trust_cert = trust_server_certificate()

    return {
        "server": server,
        "user_name": username,
        "password": password,
        "database": database,
        "trust_server_certificate": trust_cert,
        "encryption": "Optional",
    }


@pytest.fixture
def client_context():
    """Pytest fixture that provides client context for database connections."""
    return get_client_context()


@pytest.fixture
def connection():
    """Pytest fixture that provides a connected PyCoreConnection instance."""
    import mssql_py_core

    context = get_client_context()
    conn = mssql_py_core.PyCoreConnection(context)
    yield conn
    if conn.is_connected():
        conn.close()
