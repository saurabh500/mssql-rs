# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Basic integration tests using the full mssql_python driver."""
import pytest


def test_connection_and_cursor(cursor):
    """Test that connection and cursor work correctly."""
    cursor.execute("SELECT 1 AS connected")
    result = cursor.fetchone()
    assert result[0] == 1


def test_insert_and_fetch(cursor):
    """Test basic insert and fetch operations."""
    table_name = "mssql_python_test_basic"
    
    # Create table
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NVARCHAR(50))")
    
    # Insert data
    cursor.execute(f"INSERT INTO {table_name} (id, name) VALUES (?, ?)", (1, "Alice"))
    cursor.execute(f"INSERT INTO {table_name} (id, name) VALUES (?, ?)", (2, "Bob"))
    
    # Fetch and verify
    cursor.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    
    assert len(rows) == 2
    assert rows[0][0] == 1 and rows[0][1] == "Alice"
    assert rows[1][0] == 2 and rows[1][1] == "Bob"
    
    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
