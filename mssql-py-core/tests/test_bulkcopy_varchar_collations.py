# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for VARCHAR data type with different collations.

Tests VARCHAR columns with various collations to ensure proper handling of:
- Case-sensitive vs case-insensitive collations
- Binary collations
- Different language-specific collations
- UTF-8 collations
- Variable-length string handling
"""

import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_latin1_general_ci_as(client_context):
    """Test cursor bulkcopy with VARCHAR column using Latin1_General_CI_AS collation.
    
    Tests the default SQL Server collation (case-insensitive, accent-sensitive).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using Latin1_General_CI_AS collation
    table_name = "BulkCopyTestVarcharLatin1"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE Latin1_General_CI_AS,
            description VARCHAR(200) COLLATE Latin1_General_CI_AS
        )"""
    )

    # Prepare test data - variable length strings
    data = [
        (1, "John Doe", "First employee"),
        (2, "Jane Smith", "Second employee"),
        (3, "Bob Johnson", "Third employee"),
    ]

    # Execute bulk copy with explicit column mappings
    result = cursor.bulkcopy(
        table_name,
        iter(data),
        kwargs={
            "batch_size": 1000,
            "timeout": 30,
            "column_mappings": [
                (0, "id"),
                (1, "name"),
                (2, "description"),
            ],
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly (VARCHAR is variable-length, no padding)
    cursor.execute(f"SELECT id, name, description FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "John Doe" and rows[0][2] == "First employee"
    assert rows[1][0] == 2 and rows[1][1] == "Jane Smith"
    assert rows[2][0] == 3 and rows[2][1] == "Bob Johnson" and rows[2][2] == "Third employee"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_latin1_general_cs_as(client_context):
    """Test cursor bulkcopy with VARCHAR column using Latin1_General_CS_AS collation.
    
    Tests case-sensitive collation to ensure case is preserved.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using Latin1_General_CS_AS collation (case-sensitive)
    table_name = "BulkCopyTestVarcharCS"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE Latin1_General_CS_AS,
            code VARCHAR(20) COLLATE Latin1_General_CS_AS
        )"""
    )

    # Prepare test data with mixed case values
    data = [
        (1, "JohnDoe", "ABC123"),
        (2, "JANEDOE", "XYZ789"),
        (3, "bobsmith", "def456"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted with case preserved
    cursor.execute(f"SELECT id, name, code FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "JohnDoe" and rows[0][2] == "ABC123"
    assert rows[1][0] == 2 and rows[1][1] == "JANEDOE" and rows[1][2] == "XYZ789"
    assert rows[2][0] == 3 and rows[2][1] == "bobsmith" and rows[2][2] == "def456"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_latin1_general_bin(client_context):
    """Test cursor bulkcopy with VARCHAR column using Latin1_General_BIN collation.
    
    Tests binary collation where comparisons are based on binary representation.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using Latin1_General_BIN collation (binary)
    table_name = "BulkCopyTestVarcharBinary"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            value VARCHAR(30) COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data with values that differ in case
    data = [
        (1, "Apple"),
        (2, "APPLE"),
        (3, "apple"),
        (4, "aPpLe"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1

    # Verify data was inserted with exact binary values preserved
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == "Apple"
    assert rows[1][0] == 2 and rows[1][1] == "APPLE"
    assert rows[2][0] == 3 and rows[2][1] == "apple"
    assert rows[3][0] == 4 and rows[3][1] == "aPpLe"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_latin1_general_bin2(client_context):
    """Test cursor bulkcopy with VARCHAR column using Latin1_General_BIN2 collation.
    
    Tests BIN2 binary collation which uses code point comparison.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using Latin1_General_BIN2 collation
    table_name = "BulkCopyTestVarcharBinary2"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            value VARCHAR(30) COLLATE Latin1_General_BIN2
        )"""
    )

    # Prepare test data
    data = [
        (1, "Banana"),
        (2, "BANANA"),
        (3, "banana"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Banana"
    assert rows[1][0] == 2 and rows[1][1] == "BANANA"
    assert rows[2][0] == 3 and rows[2][1] == "banana"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_sql_latin1_general_cp1_ci_as(client_context):
    """Test cursor bulkcopy with VARCHAR column using SQL_Latin1_General_CP1_CI_AS collation.
    
    Tests legacy SQL Server collation for backward compatibility.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using SQL_Latin1_General_CP1_CI_AS collation
    table_name = "BulkCopyTestVarcharSQLLatin1"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS,
            description VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS
        )"""
    )

    # Prepare test data
    data = [
        (1, "Alice", "Employee with legacy collation"),
        (2, "Bob", "Another legacy employee"),
        (3, "Charlie", "Third legacy employee"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, name, description FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Alice"
    assert rows[1][0] == 2 and rows[1][1] == "Bob"
    assert rows[2][0] == 3 and rows[2][1] == "Charlie"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_chinese_prc_ci_as(client_context):
    """Test cursor bulkcopy with VARCHAR column using Chinese_PRC_CI_AS collation.
    
    Tests Chinese simplified collation support.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using Chinese_PRC_CI_AS collation
    table_name = "BulkCopyTestVarcharChinese"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE Chinese_PRC_CI_AS
        )"""
    )

    # Prepare test data with ASCII characters (VARCHAR doesn't support Unicode)
    data = [
        (1, "Zhang Wei"),
        (2, "Li Ming"),
        (3, "Wang Fang"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Zhang Wei"
    assert rows[1][0] == 2 and rows[1][1] == "Li Ming"
    assert rows[2][0] == 3 and rows[2][1] == "Wang Fang"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_japanese_ci_as(client_context):
    """Test cursor bulkcopy with VARCHAR column using Japanese_CI_AS collation.
    
    Tests Japanese collation support.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns using Japanese_CI_AS collation
    table_name = "BulkCopyTestVarcharJapanese"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE Japanese_CI_AS
        )"""
    )

    # Prepare test data with ASCII characters
    data = [
        (1, "Tanaka"),
        (2, "Suzuki"),
        (3, "Sato"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Tanaka"
    assert rows[1][0] == 2 and rows[1][1] == "Suzuki"
    assert rows[2][0] == 3 and rows[2][1] == "Sato"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_mixed_collations_null_values(client_context):
    """Test cursor bulkcopy with VARCHAR columns having different collations and NULL values.
    
    Tests handling of NULL values and different collations in the same table.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with mixed collations
    table_name = "BulkCopyTestVarcharMixed"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE Latin1_General_CI_AS,
            code VARCHAR(20) COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data with NULL values
    data = [
        (1, "Alice", "CODE1"),
        (2, None, "CODE2"),
        (3, "Charlie", None),
        (4, None, None),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, name, code FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == "Alice" and rows[0][2] == "CODE1"
    assert rows[1][0] == 2 and rows[1][1] is None and rows[1][2] == "CODE2"
    assert rows[2][0] == 3 and rows[2][1] == "Charlie" and rows[2][2] is None
    assert rows[3][0] == 4 and rows[3][1] is None and rows[3][2] is None

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_numbers_to_varchar_collations(client_context):
    """Test cursor bulkcopy with numeric values to VARCHAR columns with different collations.
    
    Tests that numbers are properly converted to strings for VARCHAR columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR columns
    table_name = "BulkCopyTestVarcharNumbers"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id VARCHAR(20) COLLATE Latin1_General_CI_AS, 
            value VARCHAR(30) COLLATE Latin1_General_BIN,
            decimal_val VARCHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AS
        )"""
    )

    # Prepare test data with numeric values
    data = [
        (1, 100, 99.99),
        (2, 200, 199.99),
        (3, 300, 299.99),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly as strings
    cursor.execute(f"SELECT id, value, decimal_val FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == "1" and rows[0][1] == "100"
    assert rows[1][0] == "2" and rows[1][1] == "200"
    assert rows[2][0] == "3" and rows[2][1] == "300"
    # Verify decimal values were converted to strings
    assert "99.99" in rows[0][2]
    assert "199.99" in rows[1][2]

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_varying_lengths_collations(client_context):
    """Test cursor bulkcopy with VARCHAR columns of different lengths and collations.
    
    Tests that varying length strings are handled properly with different collations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with different VARCHAR lengths
    table_name = "BulkCopyTestVarcharLengths"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            short_text VARCHAR(10) COLLATE Latin1_General_CI_AS,
            medium_text VARCHAR(50) COLLATE Latin1_General_BIN,
            long_text VARCHAR(500) COLLATE SQL_Latin1_General_CP1_CI_AS
        )"""
    )

    # Prepare test data with varying lengths
    data = [
        ("Short", "Medium length text here", "Long text " * 20),
        ("Tiny", "Another medium", "Another long text " * 15),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT short_text, medium_text, long_text FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == "Short"
    assert rows[0][1] == "Medium length text here"
    assert len(rows[0][2]) > 100  # Verify long text was stored

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_max_collations(client_context):
    """Test cursor bulkcopy with VARCHAR(MAX) column with different collations.
    
    Tests VARCHAR(MAX) for large text data with collations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with VARCHAR(MAX) column
    table_name = "BulkCopyTestVarcharMax"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            content VARCHAR(MAX) COLLATE Latin1_General_CI_AS
        )"""
    )

    # Prepare test data - including a large string
    large_text = "A" * 10000  # 10,000 characters
    data = [
        (1, "Short text"),
        (2, large_text),
        (3, "Another short text"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, content FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Short text"
    assert rows[1][0] == 2 and rows[1][1] == large_text
    assert rows[2][0] == 3 and rows[2][1] == "Another short text"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_varchar_empty_strings_collations(client_context):
    """Test cursor bulkcopy with empty strings in VARCHAR columns with collations.
    
    Tests that empty strings are handled properly with different collations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table
    table_name = "BulkCopyTestVarcharEmpty"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name VARCHAR(50) COLLATE Latin1_General_CI_AS,
            code VARCHAR(20) COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data with empty strings
    data = [
        (1, "", "CODE1"),
        (2, "Alice", ""),
        (3, "", ""),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, name, code FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "" and rows[0][2] == "CODE1"
    assert rows[1][0] == 2 and rows[1][1] == "Alice" and rows[1][2] == ""
    assert rows[2][0] == 3 and rows[2][1] == "" and rows[2][2] == ""

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
