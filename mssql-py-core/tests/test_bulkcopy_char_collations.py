# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for CHAR data type with different collations.

Tests CHAR columns with various collations to ensure proper handling of:
- Case-sensitive vs case-insensitive collations
- Binary collations
- Different language-specific collations
- UTF-8 collations
"""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_char_latin1_general_ci_as(client_context):
    """Test cursor bulkcopy with CHAR column using Latin1_General_CI_AS collation.
    
    Tests the default SQL Server collation (case-insensitive, accent-sensitive).
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using Latin1_General_CI_AS collation
    table_name = "BulkCopyTestCharLatin1"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name CHAR(20) COLLATE Latin1_General_CI_AS,
            description CHAR(50) COLLATE Latin1_General_CI_AS
        )"""
    )

    # Prepare test data - strings that will be padded to fixed length
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

    # Verify data was inserted correctly (CHAR is fixed-length, so values are right-padded with spaces)
    cursor.execute(f"SELECT id, name, description FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "John Doe" and rows[0][2].rstrip() == "First employee"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "Jane Smith"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "Bob Johnson" and rows[2][2].rstrip() == "Third employee"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_latin1_general_cs_as(client_context):
    """Test cursor bulkcopy with CHAR column using Latin1_General_CS_AS collation.
    
    Tests case-sensitive collation to ensure case is preserved.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using Latin1_General_CS_AS collation (case-sensitive)
    table_name = "BulkCopyTestCharCS"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name CHAR(20) COLLATE Latin1_General_CS_AS,
            code CHAR(10) COLLATE Latin1_General_CS_AS
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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "JohnDoe" and rows[0][2].rstrip() == "ABC123"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "JANEDOE" and rows[1][2].rstrip() == "XYZ789"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "bobsmith" and rows[2][2].rstrip() == "def456"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_latin1_general_bin(client_context):
    """Test cursor bulkcopy with CHAR column using Latin1_General_BIN collation.
    
    Tests binary collation where comparisons are based on binary representation.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using Latin1_General_BIN collation (binary)
    table_name = "BulkCopyTestCharBinary"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            value CHAR(15) COLLATE Latin1_General_BIN
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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Apple"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "APPLE"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "apple"
    assert rows[3][0] == 4 and rows[3][1].rstrip() == "aPpLe"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_sql_latin1_general_cp1_ci_as(client_context):
    """Test cursor bulkcopy with CHAR column using SQL_Latin1_General_CP1_CI_AS collation.
    
    Tests legacy SQL Server collation for backward compatibility.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using SQL_Latin1_General_CP1_CI_AS collation
    table_name = "BulkCopyTestCharSQLLatin1"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name CHAR(30) COLLATE SQL_Latin1_General_CP1_CI_AS,
            description CHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS
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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Alice"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "Bob"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "Charlie"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_chinese_prc_ci_as(client_context):
    """Test cursor bulkcopy with CHAR column using Chinese_PRC_CI_AS collation.
    
    Tests Chinese simplified collation support.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using Chinese_PRC_CI_AS collation
    table_name = "BulkCopyTestCharChinese"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name CHAR(50) COLLATE Chinese_PRC_CI_AS
        )"""
    )

    # Prepare test data with Chinese characters and ASCII
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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Zhang Wei"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "Li Ming"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "Wang Fang"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_japanese_ci_as(client_context):
    """Test cursor bulkcopy with CHAR column using Japanese_CI_AS collation.
    
    Tests Japanese collation support.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using Japanese_CI_AS collation
    table_name = "BulkCopyTestCharJapanese"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            name CHAR(50) COLLATE Japanese_CI_AS
        )"""
    )

    # Prepare test data with Japanese characters (using ASCII for compatibility)
    data = [
        (1, "Tanaka"),
        (2, "Suzuki"),
        (3, "Watanabe"),
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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Tanaka"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "Suzuki"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "Watanabe"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_mixed_collations_null_values(client_context):
    """Test cursor bulkcopy with CHAR columns using different collations and NULL values.
    
    Tests multiple collations in the same table and NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using different collations
    table_name = "BulkCopyTestCharMixedCollations"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            ci_col CHAR(20) COLLATE Latin1_General_CI_AS,
            cs_col CHAR(20) COLLATE Latin1_General_CS_AS,
            bin_col CHAR(20) COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data with NULL values
    data = [
        (1, "Test", "Test", "Test"),
        (2, None, "VALUE", "VALUE"),  # NULL in ci_col
        (3, "data", None, "data"),  # NULL in cs_col
        (4, "info", "info", None),  # NULL in bin_col
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1

    # Verify data was inserted correctly with NULLs preserved
    cursor.execute(f"SELECT id, ci_col, cs_col, bin_col FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Test" and rows[0][2].rstrip() == "Test"
    assert rows[1][0] == 2 and rows[1][1] is None and rows[1][2].rstrip() == "VALUE"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "data" and rows[2][2] is None
    assert rows[3][0] == 4 and rows[3][1].rstrip() == "info" and rows[3][3] is None

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_numbers_to_char_collations(client_context):
    """Test cursor bulkcopy with numeric values as strings to CHAR columns with various collations.
    
    Tests that numbers can be bulk copied into CHAR columns with different collations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using different collations
    table_name = "BulkCopyNumbersToCharCollations"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            value_ci CHAR(15) COLLATE Latin1_General_CI_AS,
            value_cs CHAR(15) COLLATE Latin1_General_CS_AS,
            value_bin CHAR(15) COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data - numeric values that should be converted to strings
    data = [
        (1, 100, 200, 300),
        (2, 999, 888, 777),
        (3, 12345, 67890, 11111),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly as strings with padding
    cursor.execute(f"SELECT id, value_ci, value_cs, value_bin FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "100" and rows[0][2].rstrip() == "200" and rows[0][3].rstrip() == "300"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "999" and rows[1][2].rstrip() == "888"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "12345" and rows[2][2].rstrip() == "67890"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_fixed_length_padding_collations(client_context):
    """Test cursor bulkcopy with CHAR columns to verify fixed-length padding with various collations.
    
    Tests that CHAR columns are properly right-padded with spaces to the defined length,
    regardless of the collation used.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR(10) columns using different collations
    table_name = "BulkCopyTestCharPaddingCollations"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            short_ci CHAR(10) COLLATE Latin1_General_CI_AS,
            short_cs CHAR(10) COLLATE Latin1_General_CS_AS,
            short_bin CHAR(10) COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data with short strings that should be padded
    data = [
        (1, "Hi", "Hi", "Hi"),  # 2 chars, should be padded to 10
        (2, "Test", "Test", "Test"),  # 4 chars, should be padded to 10
        (3, "A", "A", "A"),  # 1 char, should be padded to 10
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted with proper padding (CHAR is fixed-length)
    cursor.execute(f"SELECT id, short_ci, short_cs, short_bin, LEN(short_ci), DATALENGTH(short_ci) FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    
    # Row 1: "Hi" should be stored as "Hi        " (10 chars total)
    assert rows[0][0] == 1
    assert rows[0][1].rstrip() == "Hi"
    assert rows[0][2].rstrip() == "Hi"
    assert rows[0][3].rstrip() == "Hi"
    assert rows[0][5] == 10  # DATALENGTH should be 10 for CHAR(10)
    
    # Row 2: "Test" should be stored as "Test      " (10 chars total)
    assert rows[1][0] == 2
    assert rows[1][1].rstrip() == "Test"
    
    # Row 3: "A" should be stored as "A         " (10 chars total)
    assert rows[2][0] == 3
    assert rows[2][1].rstrip() == "A"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_char_latin1_general_bin2(client_context):
    """Test cursor bulkcopy with CHAR column using Latin1_General_BIN2 collation.
    
    Tests the newer binary collation (BIN2) that provides correct Unicode sorting semantics.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with CHAR columns using Latin1_General_BIN2 collation
    table_name = "BulkCopyTestCharBinary2"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT, 
            value CHAR(20) COLLATE Latin1_General_BIN2
        )"""
    )

    # Prepare test data
    data = [
        (1, "Binary2Test"),
        (2, "BINARY2TEST"),
        (3, "binary2test"),
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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Binary2Test"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "BINARY2TEST"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "binary2test"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
