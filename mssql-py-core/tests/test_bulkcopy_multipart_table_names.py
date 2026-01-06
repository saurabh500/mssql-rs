# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for multipart table names (database.schema.table)."""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_one_part_table_name(client_context):
    """Test cursor bulkcopy with simple one-part table name."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with simple name
    table_name = "TestTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Execute bulk copy with simple table name
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == (1, 100)
    assert rows[1] == (2, 200)
    assert rows[2] == (3, 300)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_two_part_table_name(client_context):
    """Test cursor bulkcopy with two-part table name (schema.table)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two-part name
    table_name = "dbo.TestTable2Part"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (10, 1000),
        (20, 2000),
        (30, 3000),
    ]

    # Execute bulk copy with two-part name
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == (10, 1000)
    assert rows[1] == (20, 2000)
    assert rows[2] == (30, 3000)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_three_part_table_name(client_context):
    """Test cursor bulkcopy with three-part table name (database.schema.table)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Get the current database name
    cursor.execute("SELECT DB_NAME()")
    current_db = cursor.fetchone()[0]
    conn.close()
    
    # Create new connection for the actual test
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with three-part name
    table_name = f"{current_db}.dbo.TestTable3Part"
    simple_name = "dbo.TestTable3Part"
    
    cursor.execute(
        f"IF OBJECT_ID('{simple_name}', 'U') IS NOT NULL DROP TABLE {simple_name}"
    )
    cursor.execute(f"CREATE TABLE {simple_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (100, 10000),
        (200, 20000),
        (300, 30000),
    ]

    # Execute bulk copy with three-part name
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {simple_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == (100, 10000)
    assert rows[1] == (200, 20000)
    assert rows[2] == (300, 30000)

    # Cleanup
    cursor.execute(f"DROP TABLE {simple_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_quoted_table_name(client_context):
    """Test cursor bulkcopy with quoted identifiers (special characters in name)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with special characters in name
    table_name = "[Test Table]"  # Space in name requires quotes
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (1, 111),
        (2, 222),
    ]

    # Execute bulk copy with quoted name
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1, 111)
    assert rows[1] == (2, 222)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_quoted_two_part_name(client_context):
    """Test cursor bulkcopy with quoted two-part name [schema].[table]."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with quoted two-part name
    table_name = "[dbo].[Test Table 2Part]"
    simple_name = "dbo.[Test Table 2Part]"
    
    cursor.execute(
        f"IF OBJECT_ID('{simple_name}', 'U') IS NOT NULL DROP TABLE {simple_name}"
    )
    cursor.execute(f"CREATE TABLE {simple_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (5, 555),
        (6, 666),
    ]

    # Execute bulk copy with quoted two-part name
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {simple_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (5, 555)
    assert rows[1] == (6, 666)

    # Cleanup
    cursor.execute(f"DROP TABLE {simple_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_escaped_brackets_in_name(client_context):
    """Test cursor bulkcopy with escaped brackets in table name [Test]]Table]."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with brackets in name (escaped as ]])
    # The actual table name is "Test]Table" but in SQL it's [Test]]Table]
    table_name = "[Test]]Table]"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (7, 777),
        (8, 888),
    ]

    # Execute bulk copy with escaped brackets
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (7, 777)
    assert rows[1] == (8, 888)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_temp_table(client_context):
    """Test cursor bulkcopy with temp table (should use tempdb catalog automatically)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table
    table_name = "#TempTest"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (9, 999),
        (10, 1010),
    ]

    # Execute bulk copy with temp table
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0] == (9, 999)
    assert rows[1] == (10, 1010)

    # Temp table automatically cleaned up on connection close
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_sql_injection_protection(client_context):
    """Test cursor bulkcopy SQL injection protection via proper escaping."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a legitimate table
    table_name = "LegitTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Attempt SQL injection via table name
    # This should fail safely without executing the injection
    malicious_name = "LegitTable'; DROP TABLE LegitTable; --"

    # Prepare test data
    data = [
        (1, 123),
    ]

    # Execute bulk copy with malicious name
    error_raised = False
    try:
        result = cursor.bulkcopy(
            malicious_name,
            iter(data),
            kwargs={"batch_size": 1000, "timeout": 30},
        )
    except Exception as e:
        error_raised = True
        print(f"Expected error caught: {e}")

    # Verify that an error was raised
    assert error_raised, "SQL injection attempt should fail"

    # Verify the table still exists (try to select from it)
    # Create a new connection to ensure clean state
    conn2 = mssql_py_core.PyCoreConnection(client_context)
    cursor2 = conn2.cursor()
    try:
        cursor2.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_result = cursor2.fetchone()
        # Consume all results before next query
        cursor2.fetchall()
        assert count_result is not None, "Table should still exist after injection attempt"
        # Cleanup
        cursor2.execute(f"DROP TABLE {table_name}")
        conn2.close()
    except Exception as e:
        conn2.close()
        # If we can't query the table, that's also a problem
        raise AssertionError(f"Table was unexpectedly dropped or damaged: {e}")
    
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_invalid_multipart_names(client_context):
    """Test cursor bulkcopy with invalid multipart table names."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Prepare test data
    data = [
        (1, "Test"),
    ]

    # Test too many parts (5 parts)
    error_raised = False
    try:
        result = cursor.bulkcopy(
            "Server.DB.Schema.Table.Extra",
            iter(data),
            kwargs={"batch_size": 1000, "timeout": 30},
        )
    except Exception as e:
        error_raised = True
        print(f"Expected error for 5-part name: {e}")
    
    assert error_raised, "5-part name should fail"

    # Test empty identifier
    error_raised = False
    try:
        result = cursor.bulkcopy(
            "",
            iter(data),
            kwargs={"batch_size": 1000, "timeout": 30},
        )
    except Exception as e:
        error_raised = True
        print(f"Expected error for empty name: {e}")
    
    assert error_raised, "Empty name should fail"

    # Test unclosed quote
    error_raised = False
    try:
        result = cursor.bulkcopy(
            "[UnclosedTable",
            iter(data),
            kwargs={"batch_size": 1000, "timeout": 30},
        )
    except Exception as e:
        error_raised = True
        print(f"Expected error for unclosed quote: {e}")
    
    assert error_raised, "Unclosed quote should fail"

    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_multipart_with_different_schemas(client_context):
    """Test cursor bulkcopy with different schema names."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a custom schema if it doesn't exist
    cursor.execute(
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'TestSchema') "
        "EXEC('CREATE SCHEMA TestSchema')"
    )

    # Create a test table in custom schema
    table_name = "TestSchema.MultipartTest"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

    # Prepare test data
    data = [
        (11, 1111),
        (12, 1212),
        (13, 1313),
    ]

    # Execute bulk copy with custom schema
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={"batch_size": 1000, "timeout": 30},
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0] == (11, 1111)
    assert rows[1] == (12, 1212)
    assert rows[2] == (13, 1313)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    # Don't drop the schema as it might be used by other tests
    conn.close()
