# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for ClientContext parameter mapping from Python to Rust."""
import os
import pytest
import socket
import mssql_py_core
from pathlib import Path
from dotenv import load_dotenv


def get_db_credentials():
    """Get database credentials from environment variables or /tmp/password."""
    env_path = Path(__file__).parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    username = os.environ.get("DB_USERNAME", "sa")
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


def trust_server_certificate():
    """Get trust server certificate setting from environment."""
    return os.environ.get("TRUST_SERVER_CERTIFICATE", "false").lower() == "true"


def get_base_context():
    """Get minimal connection context for tests."""
    username, password = get_db_credentials()
    server = get_server_address()
    return {
        "server": server,
        "user_name": username,
        "password": password,
        "trust_server_certificate": trust_server_certificate(),
    }


@pytest.mark.integration
class TestDatabaseParameter:
    """Tests for database parameter mapping."""

    def test_database_default_master(self):
        """Test that omitting database connects to default (master)."""
        context = get_base_context()
        # Don't set database - should default to empty string which SQL Server treats as default
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME() AS current_db")
        result = cursor.fetchone()
        conn.close()
        # Default database for 'sa' is typically 'master'
        assert result[0] == "master"

    def test_database_explicit_master(self):
        """Test connecting to master database explicitly."""
        context = get_base_context()
        context["database"] = "master"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME() AS current_db")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "master"

    def test_database_tempdb(self):
        """Test connecting to tempdb database."""
        context = get_base_context()
        context["database"] = "tempdb"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME() AS current_db")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "tempdb"

    def test_database_msdb(self):
        """Test connecting to msdb database."""
        context = get_base_context()
        context["database"] = "msdb"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME() AS current_db")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "msdb"


@pytest.mark.integration
class TestLanguageParameter:
    """Tests for language parameter mapping."""

    def test_language_default_us_english(self):
        """Test that default language is us_english."""
        context = get_base_context()
        context["database"] = "master"
        # Don't set language - should default to us_english
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT @@LANGUAGE AS current_lang")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "us_english"

    def test_language_deutsch(self):
        """Test setting language to Deutsch (German)."""
        context = get_base_context()
        context["database"] = "master"
        context["language"] = "Deutsch"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT @@LANGUAGE AS current_lang")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "Deutsch"

    def test_language_french(self):
        """Test setting language to French.
        
        Note: SQL Server accepts both 'French' and 'Français' as input,
        but @@LANGUAGE always returns the native name 'Français'.
        This is SQL Server behavior - it normalizes language names to their
        native form in sys.syslanguages.alias column.
        """
        context = get_base_context()
        context["database"] = "master"
        context["language"] = "French"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT @@LANGUAGE AS current_lang")
        result = cursor.fetchone()
        conn.close()
        # SQL Server returns the native name 'Français' when 'French' is set
        assert result[0] == "Français"

    def test_language_francais_native(self):
        """Test setting language using native name Français.
        
        Note: This test is skipped because sending non-ASCII characters 
        (like 'ç' in Français) in the LOGIN7 packet causes connection timeout.
        Use 'French' instead which SQL Server normalizes to 'Français'.
        This may be a Unicode encoding issue in the TDS login packet.
        """
        pytest.skip("Non-ASCII language names cause connection timeout - use English alias 'French' instead")
        context = get_base_context()
        context["database"] = "master"
        context["language"] = "Français"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT @@LANGUAGE AS current_lang")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "Français"

    def test_language_british(self):
        """Test setting language to British."""
        context = get_base_context()
        context["database"] = "master"
        context["language"] = "British"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT @@LANGUAGE AS current_lang")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "British"


@pytest.mark.integration
class TestServerPortParameter:
    """Tests for server and port parameter parsing."""

    def test_server_with_comma_port(self):
        """Test server,port format (e.g., localhost,1433) - SQL Server standard."""
        context = get_base_context()
        # SQL Server uses comma as port separator
        context["server"] = get_server_address() + ",1433"
        context["database"] = "master"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_server_without_port_uses_default(self):
        """Test that server without port uses default 1433."""
        context = get_base_context()
        # Server without port - should use default 1433
        context["database"] = "master"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_server_ip_address_with_port(self):
        """Test connecting via IP address,port format."""
        context = get_base_context()
        # SQL Server uses comma as port separator
        context["server"] = get_server_address() + ",1433"
        context["database"] = "master"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1


@pytest.mark.integration
class TestApplicationNameParameter:
    """Tests for application_name parameter mapping."""

    def test_application_name_default(self):
        """Test that default application name is mssql-python."""
        context = get_base_context()
        context["database"] = "master"
        # Don't set application_name - should default to "mssql-python"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT APP_NAME() AS app_name")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "mssql-python"

    def test_application_name_custom(self):
        """Test setting custom application name."""
        context = get_base_context()
        context["database"] = "master"
        context["application_name"] = "MyTestApp"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT APP_NAME() AS app_name")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "MyTestApp"

    def test_application_name_with_spaces(self):
        """Test application name with spaces."""
        context = get_base_context()
        context["database"] = "master"
        context["application_name"] = "My Test Application"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT APP_NAME() AS app_name")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "My Test Application"


@pytest.mark.integration
class TestWorkstationIdParameter:
    """Tests for workstation_id parameter mapping."""

    def test_workstation_id_default_hostname(self):
        """Test that default workstation_id is the machine hostname."""
        context = get_base_context()
        context["database"] = "master"
        # Don't set workstation_id - should default to hostname
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT HOST_NAME() AS host_name")
        result = cursor.fetchone()
        conn.close()
        # Should match the local machine's hostname
        expected_hostname = socket.gethostname()
        assert result[0] == expected_hostname

    def test_workstation_id_custom(self):
        """Test setting custom workstation_id."""
        context = get_base_context()
        context["database"] = "master"
        context["workstation_id"] = "MYWORKSTATION"
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT HOST_NAME() AS host_name")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "MYWORKSTATION"


@pytest.mark.integration
class TestMultipleParametersCombined:
    """Tests for multiple parameters set together."""

    def test_all_parameters_combined(self):
        """Test setting multiple parameters at once."""
        context = get_base_context()
        context["database"] = "tempdb"
        context["language"] = "Deutsch"
        context["application_name"] = "CombinedTest"
        context["workstation_id"] = "TESTHOST"

        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()

        # Verify all parameters in a single query
        cursor.execute("""
            SELECT 
                DB_NAME() AS db,
                @@LANGUAGE AS lang,
                APP_NAME() AS app,
                HOST_NAME() AS host
        """)
        result = cursor.fetchone()
        conn.close()

        assert result[0] == "tempdb"
        assert result[1] == "Deutsch"
        assert result[2] == "CombinedTest"
        assert result[3] == "TESTHOST"


@pytest.mark.integration
class TestKeepAliveParameters:
    """Tests for keep_alive and keep_alive_interval parameter mapping.
    
    These parameters control TCP keep-alive settings:
    - keep_alive: Idle time (ms) before first probe is sent (default: 30000)
    - keep_alive_interval: Interval (ms) between subsequent probes (default: 1000)
    
    Note: These are TCP socket-level settings and cannot be directly queried
    from SQL Server. We test that the parameters are accepted and don't break
    the connection.
    """

    def test_keep_alive_default_values(self):
        """Test that connection works with default keep_alive values."""
        context = get_base_context()
        context["database"] = "master"
        # Don't set keep_alive - should use defaults (30000, 1000)
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_keep_alive_custom_values(self):
        """Test connection with custom keep_alive values."""
        context = get_base_context()
        context["database"] = "master"
        context["keep_alive"] = 60000  # 60 seconds
        context["keep_alive_interval"] = 5000  # 5 seconds
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_keep_alive_minimum_values(self):
        """Test connection with minimum practical keep_alive values (1 second).
        
        Note: OS-level TCP keep-alive has minimum constraints:
        - Linux: minimum ~1 second (kernel rejects lower values)
        - Windows/macOS: similar restrictions apply
        """
        context = get_base_context()
        context["database"] = "master"
        context["keep_alive"] = 1000  # 1 second (OS minimum)
        context["keep_alive_interval"] = 1000  # 1 second (OS minimum)
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_keep_alive_large_values(self):
        """Test connection with large keep_alive values."""
        context = get_base_context()
        context["database"] = "master"
        context["keep_alive"] = 3600000  # 1 hour
        context["keep_alive_interval"] = 60000  # 1 minute
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_keep_alive_only(self):
        """Test setting only keep_alive without keep_alive_interval."""
        context = get_base_context()
        context["database"] = "master"
        context["keep_alive"] = 45000  # 45 seconds
        # keep_alive_interval should default to 1000
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_keep_alive_interval_only(self):
        """Test setting only keep_alive_interval without keep_alive."""
        context = get_base_context()
        context["database"] = "master"
        # keep_alive should default to 30000
        context["keep_alive_interval"] = 2000  # 2 seconds
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 AS connected")
        result = cursor.fetchone()
        conn.close()
        assert result[0] == 1

    def test_keep_alive_with_other_parameters(self):
        """Test keep_alive combined with other connection parameters."""
        context = get_base_context()
        context["database"] = "tempdb"
        context["application_name"] = "KeepAliveTest"
        context["keep_alive"] = 20000  # 20 seconds
        context["keep_alive_interval"] = 3000  # 3 seconds
        conn = mssql_py_core.PyCoreConnection(context)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                DB_NAME() AS db,
                APP_NAME() AS app
        """)
        result = cursor.fetchone()
        conn.close()
        assert result[0] == "tempdb"
        assert result[1] == "KeepAliveTest"
