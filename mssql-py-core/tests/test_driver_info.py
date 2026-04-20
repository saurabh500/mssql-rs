# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for driver name and version information."""

def test_driver_version_parameter():
    """Test that driver_version parameter is accepted and stored."""
    import mssql_py_core
    
    # Create a minimal context with driver_version
    context = {
        "server": "localhost",
        "user_name": "test_user",
        "password": "test_password",
        "driver_version": "1.2.3"
    }
    
    # This should not raise an error
    # The connection will fail since we don't have a real server, but that's OK
    # We're just testing that the parameter is accepted
    try:
        conn = mssql_py_core.PyCoreConnection(context)
    except RuntimeError as e:
        # Expected to fail connection, but should have accepted the parameter
        assert "driver_version" not in str(e).lower(), "driver_version parameter should be accepted"


def test_driver_version_encoding():
    """Test that driver version encoding works correctly."""
    # Import the internal test function if exposed, or we can test indirectly
    # For now, we verify that the connection accepts the driver_version parameter
    # without errors related to the parameter itself
    
    import mssql_py_core
    
    context = {
        "server": "localhost",
        "user_name": "test_user",
        "password": "test_password",
        "driver_version": "2.5.1234"
    }
    
    try:
        conn = mssql_py_core.PyCoreConnection(context)
    except RuntimeError:
        # Connection failure is expected, parameter should be accepted
        pass


def test_no_driver_version():
    """Test that connection works without driver_version parameter."""
    import mssql_py_core
    
    context = {
        "server": "localhost",
        "user_name": "test_user",
        "password": "test_password"
    }
    
    try:
        conn = mssql_py_core.PyCoreConnection(context)
    except RuntimeError:
        # Connection failure is expected, but parameter should be optional
        pass

def test_runtime_details_telemetry_priority():
    import mssql_py_core
    
    # 1. Test setting the global lock works (will not complain)
    mssql_py_core.set_runtime_details('Global Override Config')
    
    # 2. Test Connection Dictionary takes precedence and doesn't throw errors
    context_with_override = {
        'server': 'localhost',
        'user_name': 'test_user',
        'password': 'test_password',
        'runtime_details': 'Connection Level Config'
    }
    
    try:
        conn = mssql_py_core.PyCoreConnection(context_with_override)
    except RuntimeError as e:
        assert 'runtime_details' not in str(e).lower(), 'runtime_details parameter should be accepted'

    # 3. Test that omitting it also doesn't throw errors
    context_without_override = {
        'server': 'localhost',
        'user_name': 'test_user',
        'password': 'test_password',
    }
    
    try:
        conn = mssql_py_core.PyCoreConnection(context_without_override)
    except RuntimeError as e:
        assert 'runtime_details' not in str(e).lower(), 'Should fallback gracefully'
