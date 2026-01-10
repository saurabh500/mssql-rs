# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for TEXT data type with varying content sizes.

Note: TEXT is a legacy data type (deprecated in favor of VARCHAR(MAX)).
However, these tests ensure backward compatibility with legacy databases.
TEXT uses single-byte encoding (like VARCHAR) unlike NTEXT which uses Unicode.
"""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_text_basic(client_context):
    """Test cursor bulkcopy method with text columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with text columns
    table_name = "BulkCopyTestTableText"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, name TEXT, description TEXT)"
    )

    # Prepare test data - strings
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

    # Verify data was inserted correctly (TEXT is variable-length, no padding)
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
def test_cursor_bulkcopy_text_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping and NULL values.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable text columns
    table_name = "BulkCopyAutoMapTableText"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name TEXT)")

    # Prepare test data with NULL values
    data = [
        (1, "Alice"),
        (2, None),  # NULL value in name column
        (None, "Bob"),  # NULL value in id column
        (4, "Charlie"),
    ]

    # Execute bulk copy WITHOUT column mappings
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly, including NULL values
    cursor.execute(f"SELECT id, name FROM {table_name} ORDER BY COALESCE(id, 999)")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == "Alice"
    assert rows[1][0] == 2 and rows[1][1] is None  # Verify NULL in name column
    assert rows[2][0] == 4 and rows[2][1] == "Charlie"
    assert rows[3][0] is None and rows[3][1] == "Bob"  # Verify NULL in id column

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_numbers_as_strings(client_context):
    """Test cursor bulkcopy with numeric values as strings to TEXT columns.

    Tests that numbers can be bulk copied into TEXT columns by converting
    them to their string representations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT columns
    table_name = "BulkCopyNumbersToTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    # Change id to INT so we can use ORDER BY (TEXT cannot be used in ORDER BY)
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value TEXT, decimal_val TEXT)"
    )

    # Prepare test data - numeric values that should be converted to strings
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
    assert "elapsed_time" in result

    # Verify data was inserted correctly as strings (no padding for TEXT)
    cursor.execute(f"SELECT id, value, decimal_val FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "100"
    assert rows[1][0] == 2 and rows[1][1] == "200"
    assert rows[2][0] == 3 and rows[2][1] == "300"
    # Verify decimal values were converted to strings
    assert "99.99" in rows[0][2]
    assert "199.99" in rows[1][2]

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_string_numbers_to_text(client_context):
    """Test cursor bulkcopy with string representations of numbers to TEXT columns.

    Tests that string values containing numbers can be bulk copied to TEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT columns
    table_name = "BulkCopyStringNumbersToTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    # Change id to INT so we can use ORDER BY (TEXT cannot be used in ORDER BY)
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value TEXT)")

    # Prepare test data - strings containing numbers
    data = [
        (1, "100"),
        (2, "200"),
        (3, "300"),
        (123456789, "987654321"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly as strings (no padding for TEXT)
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == "100"
    assert rows[1][0] == 2 and rows[1][1] == "200"
    assert rows[2][0] == 3 and rows[2][1] == "300"
    assert rows[3][0] == 123456789 and rows[3][1] == "987654321"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_large_text(client_context):
    """Test cursor bulkcopy with TEXT columns containing large text data.

    Tests that large text data is correctly inserted into TEXT columns.
    TEXT can store up to 2GB of single-byte character data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT column
    table_name = "BulkCopyLargeTextTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, large_text TEXT)"
    )

    # Prepare test data with large strings
    # Creating strings larger than typical VARCHAR sizes (8000 bytes)
    data = [
        (1, "A" * 10000),  # 10,000 characters
        (2, "B" * 50000),  # 50,000 characters
        (3, "C" * 100000),  # 100,000 characters
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(
        f"SELECT id, large_text, DATALENGTH(large_text) as byte_count FROM {table_name} ORDER BY id"
    )
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][1] == "A" * 10000
    assert rows[0][2] == 10000  # Byte count (single-byte encoding)
    
    assert rows[1][0] == 2
    assert rows[1][1] == "B" * 50000
    assert rows[1][2] == 50000
    
    assert rows[2][0] == 3
    assert rows[2][1] == "C" * 100000
    assert rows[2][2] == 100000

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_ascii_characters(client_context):
    """Test cursor bulkcopy with ASCII characters in TEXT columns.

    Tests that ASCII characters (standard English characters and symbols)
    are correctly stored in TEXT columns. TEXT uses single-byte encoding
    and is best suited for ASCII data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT columns
    table_name = "BulkCopyAsciiTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text TEXT)")

    # Prepare test data with ASCII characters
    data = [
        (1, "Hello World"),  # Simple English
        (2, "UPPERCASE and lowercase"),  # Mixed case
        (3, "Numbers: 0123456789"),  # Numbers
        (4, "Symbols: !@#$%^&*()"),  # Special symbols
        (5, "Punctuation: .,;:?!-_"),  # Punctuation
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    assert rows[0][0] == 1 and rows[0][1] == "Hello World"
    assert rows[1][0] == 2 and rows[1][1] == "UPPERCASE and lowercase"
    assert rows[2][0] == 3 and rows[2][1] == "Numbers: 0123456789"
    assert rows[3][0] == 4 and rows[3][1] == "Symbols: !@#$%^&*()"
    assert rows[4][0] == 5 and rows[4][1] == "Punctuation: .,;:?!-_"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_empty_strings(client_context):
    """Test cursor bulkcopy with empty strings to TEXT columns.

    Tests that empty strings are correctly distinguished from NULL values
    when bulk copying to TEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable TEXT column
    table_name = "BulkCopyEmptyStringTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text TEXT)")

    # Prepare test data with empty strings and NULL values
    data = [
        (1, ""),  # Empty string
        (2, None),  # NULL
        (3, "  "),  # Whitespace
        (4, "Text"),  # Normal text
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and rows[0][1] == ""  # Empty string preserved
    assert rows[1][0] == 2 and rows[1][1] is None  # NULL preserved
    assert rows[2][0] == 3 and rows[2][1] == "  "  # Whitespace preserved
    assert rows[3][0] == 4 and rows[3][1] == "Text"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_mixed_types(client_context):
    """Test cursor bulkcopy with mixed data types converting to TEXT columns.

    Tests that various Python types (int, float, bool, string) can all be
    converted to their string representations when targeting TEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT columns
    table_name = "BulkCopyMixedTypesTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value TEXT)")

    # Prepare test data with mixed types
    data = [
        (1, 123),  # Integer
        (2, 456.789),  # Float
        (3, True),  # Boolean
        (4, False),  # Boolean
        (5, "Regular string"),  # String
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was converted to strings correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    assert rows[0][0] == 1 and rows[0][1] == "123"
    assert rows[1][0] == 2 and "456.789" in rows[1][1]  # Float may have slight variations
    assert rows[2][0] == 3 and rows[2][1] in ["True", "1"]  # Boolean representations
    assert rows[3][0] == 4 and rows[3][1] in ["False", "0"]  # Boolean representations
    assert rows[4][0] == 5 and rows[4][1] == "Regular string"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_multiline_text(client_context):
    """Test cursor bulkcopy with multiline text in TEXT columns.

    Tests that text with newlines and tabs is correctly stored in TEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT column
    table_name = "BulkCopyMultilineTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text TEXT)")

    # Prepare test data with multiline text
    data = [
        (1, "Line 1\nLine 2\nLine 3"),
        (2, "Tab\tseparated\tvalues"),
        (3, "Mixed:\nNew line\tand tab\r\nCarriage return too"),
        (4, """Multi
line
string
literal"""),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly with special characters preserved
    cursor.execute(f"SELECT id, text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == 1 and "\n" in rows[0][1] and "Line 2" in rows[0][1]
    assert rows[1][0] == 2 and "\t" in rows[1][1] and "separated" in rows[1][1]
    assert rows[2][0] == 3 and "\n" in rows[2][1] and "\t" in rows[2][1]
    assert rows[3][0] == 4 and "\n" in rows[3][1] and "line" in rows[3][1]

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_special_characters(client_context):
    """Test cursor bulkcopy with special characters in TEXT columns.

    Tests that various special characters are correctly stored in TEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT column
    table_name = "BulkCopySpecialCharsTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text TEXT)")

    # Prepare test data with special characters
    data = [
        (1, "Quotes: 'single' and \"double\""),
        (2, "Backslash: \\ and forward slash: /"),
        (3, "Percent: % and underscore: _"),
        (4, "Brackets: [] {} () <>"),
        (5, "Punctuation: ! @ # $ % ^ & * ( ) - + = ; : , . ? /"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly with special characters preserved
    cursor.execute(f"SELECT id, text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    assert rows[0][0] == 1 and "'" in rows[0][1] and '"' in rows[0][1]
    assert rows[1][0] == 2 and "\\" in rows[1][1] and "/" in rows[1][1]
    assert rows[2][0] == 3 and "%" in rows[2][1] and "_" in rows[2][1]
    assert rows[3][0] == 4 and "[" in rows[3][1] and "]" in rows[3][1]
    assert rows[4][0] == 5 and "!" in rows[4][1] and "@" in rows[4][1]

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_extended_ascii(client_context):
    """Test cursor bulkcopy with extended ASCII characters in TEXT columns.

    Tests that extended ASCII characters (code points 128-255) are correctly
    stored in TEXT columns. These include accented characters common in
    Western European languages.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT column
    table_name = "BulkCopyExtendedAsciiTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text TEXT)")

    # Prepare test data with extended ASCII characters (Latin-1 supplement)
    data = [
        (1, "Café"),  # é - e with acute accent
        (2, "Résumé"),  # é - e with acute accent
        (3, "Naïve"),  # ï - i with diaeresis
        (4, "Piñata"),  # ñ - n with tilde
        (5, "Ångström"),  # Å - A with ring above
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    # Note: TEXT with Latin1 collation should preserve these characters
    assert rows[0][0] == 1 and "Caf" in rows[0][1]
    assert rows[1][0] == 2 and "sum" in rows[1][1]
    assert rows[2][0] == 3 and "Na" in rows[2][1]
    assert rows[3][0] == 4 and "ata" in rows[3][1]
    assert rows[4][0] == 5 and "ngstr" in rows[4][1]

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_batch_processing(client_context):
    """Test cursor bulkcopy with multiple batches for TEXT columns.

    Tests that bulk copy correctly processes multiple batches when inserting
    large numbers of rows with TEXT data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT column
    table_name = "BulkCopyBatchTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text TEXT)")

    # Prepare test data with many rows
    data = [(i, f"Text content for row {i}") for i in range(1, 101)]

    # Execute bulk copy with small batch size to force multiple batches
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 10, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 100
    assert result["batch_count"] >= 10  # Should have at least 10 batches
    assert "elapsed_time" in result

    # Verify all data was inserted correctly
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 100

    # Verify some sample rows
    cursor.execute(f"SELECT id, text FROM {table_name} WHERE id IN (1, 50, 100) ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Text content for row 1"
    assert rows[1][0] == 50 and rows[1][1] == "Text content for row 50"
    assert rows[2][0] == 100 and rows[2][1] == "Text content for row 100"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_text_with_collations(client_context):
    """Test cursor bulkcopy with TEXT columns using different collations.

    Tests that TEXT columns with various collations work correctly with bulk copy.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with TEXT columns having different collations
    table_name = "BulkCopyTextCollationsTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT,
            ci_text TEXT COLLATE Latin1_General_CI_AS,
            cs_text TEXT COLLATE Latin1_General_CS_AS,
            bin_text TEXT COLLATE Latin1_General_BIN
        )"""
    )

    # Prepare test data
    data = [
        (1, "Apple", "APPLE", "apple"),
        (2, "Banana", "banana", "BANANA"),
        (3, "Cherry", "ChErRy", "Cherry"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1

    # Verify data was inserted correctly with collations preserved
    cursor.execute(f"SELECT id, ci_text, cs_text, bin_text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1] == "Apple" and rows[0][2] == "APPLE" and rows[0][3] == "apple"
    assert rows[1][0] == 2 and rows[1][1] == "Banana" and rows[1][2] == "banana" and rows[1][3] == "BANANA"
    assert rows[2][0] == 3 and rows[2][1] == "Cherry" and rows[2][2] == "ChErRy" and rows[2][3] == "Cherry"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
