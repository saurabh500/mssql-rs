# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for NTEXT data type with varying content sizes.

Note: NTEXT is a legacy data type (deprecated in favor of NVARCHAR(MAX)).
However, these tests ensure backward compatibility with legacy databases.
"""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_ntext_basic(client_context):
    """Test cursor bulkcopy method with ntext columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with ntext columns
    table_name = "BulkCopyTestTableNText"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, name NTEXT, description NTEXT)"
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
        batch_size=1000,
        timeout=30,
        column_mappings=[
            (0, "id"),
            (1, "name"),
            (2, "description"),
        ],
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly (NTEXT is variable-length, no padding)
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
def test_cursor_bulkcopy_ntext_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping and NULL values.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable ntext columns
    table_name = "BulkCopyAutoMapTableNText"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NTEXT)")

    # Prepare test data with NULL values
    data = [
        (1, "Alice"),
        (2, None),  # NULL value in name column
        (None, "Bob"),  # NULL value in id column
        (4, "Charlie"),
    ]

    # Execute bulk copy WITHOUT column mappings
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_ntext_numbers_as_strings(client_context):
    """Test cursor bulkcopy with numeric values as strings to NTEXT columns.

    Tests that numbers can be bulk copied into NTEXT columns by converting
    them to their string representations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT columns
    table_name = "BulkCopyNumbersToNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    # Change id to INT so we can use ORDER BY (NTEXT cannot be used in ORDER BY)
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value NTEXT, decimal_val NTEXT)"
    )

    # Prepare test data - numeric values that should be converted to strings
    data = [
        (1, 100, 99.99),
        (2, 200, 199.99),
        (3, 300, 299.99),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly as strings (no padding for NTEXT)
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
def test_cursor_bulkcopy_ntext_string_numbers_to_ntext(client_context):
    """Test cursor bulkcopy with string representations of numbers to NTEXT columns.

    Tests that string values containing numbers can be bulk copied to NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT columns
    table_name = "BulkCopyStringNumbersToNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    # Change id to INT so we can use ORDER BY (NTEXT cannot be used in ORDER BY)
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value NTEXT)")

    # Prepare test data - strings containing numbers
    data = [
        (1, "100"),
        (2, "200"),
        (3, "300"),
        (123456789, "987654321"),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly as strings (no padding for NTEXT)
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
def test_cursor_bulkcopy_ntext_large_text(client_context):
    """Test cursor bulkcopy with NTEXT columns containing large text data.

    Tests that large text data is correctly inserted into NTEXT columns.
    NTEXT can store up to 2GB of Unicode text data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT column
    table_name = "BulkCopyLargeTextNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, large_text NTEXT)"
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
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(
        f"SELECT id, large_text, DATALENGTH(large_text)/2 as char_count FROM {table_name} ORDER BY id"
    )
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][1] == "A" * 10000
    assert rows[0][2] == 10000  # Character count
    
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
def test_cursor_bulkcopy_ntext_unicode_characters(client_context):
    """Test cursor bulkcopy with Unicode characters in NTEXT columns.

    Tests that Unicode characters (emoji, special characters, non-Latin scripts)
    are correctly stored in NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT columns
    table_name = "BulkCopyUnicodeNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NTEXT)")

    # Prepare test data with various Unicode characters
    data = [
        (1, "Hello 世界"),  # Chinese characters
        (2, "Привет мир"),  # Cyrillic characters
        (3, "مرحبا بالعالم"),  # Arabic characters
        (4, "Hello 🌍🌎🌏"),  # Emoji
        (5, "Special: ©®™€£¥"),  # Special symbols
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 5
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly with Unicode preserved
    cursor.execute(f"SELECT id, text FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    assert rows[0][0] == 1 and rows[0][1] == "Hello 世界"
    assert rows[1][0] == 2 and rows[1][1] == "Привет мир"
    assert rows[2][0] == 3 and rows[2][1] == "مرحبا بالعالم"
    assert rows[3][0] == 4 and rows[3][1] == "Hello 🌍🌎🌏"
    assert rows[4][0] == 5 and rows[4][1] == "Special: ©®™€£¥"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_ntext_empty_strings(client_context):
    """Test cursor bulkcopy with empty strings to NTEXT columns.

    Tests that empty strings are correctly distinguished from NULL values
    when bulk copying to NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable NTEXT column
    table_name = "BulkCopyEmptyStringNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NTEXT)")

    # Prepare test data with empty strings and NULL values
    data = [
        (1, ""),  # Empty string
        (2, None),  # NULL
        (3, "  "),  # Whitespace
        (4, "Text"),  # Normal text
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_ntext_mixed_types(client_context):
    """Test cursor bulkcopy with mixed data types converting to NTEXT columns.

    Tests that various Python types (int, float, bool, string) can all be
    converted to their string representations when targeting NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT columns
    table_name = "BulkCopyMixedTypesNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value NTEXT)")

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
        table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_ntext_multiline_text(client_context):
    """Test cursor bulkcopy with multiline text in NTEXT columns.

    Tests that text with newlines and tabs is correctly stored in NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT column
    table_name = "BulkCopyMultilineNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NTEXT)")

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
        table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_ntext_special_characters(client_context):
    """Test cursor bulkcopy with special characters in NTEXT columns.

    Tests that various special characters are correctly stored in NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT column
    table_name = "BulkCopySpecialCharsNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NTEXT)")

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
        table_name, iter(data), batch_size=1000, timeout=30
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
def test_cursor_bulkcopy_ntext_long_unicode_text(client_context):
    """Test cursor bulkcopy with long Unicode text in NTEXT columns.

    Tests that large amounts of Unicode text are correctly stored in NTEXT columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT column
    table_name = "BulkCopyLongUnicodeNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NTEXT)")

    # Prepare test data with long Unicode strings
    unicode_pattern = "世界 🌍 Мир "  # Mix of Chinese, emoji, Cyrillic
    data = [
        (1, unicode_pattern * 1000),  # ~14,000 characters
        (2, "Привет мир! " * 2000),  # ~24,000 characters
        (3, "Hello 🌍🌎🌏 " * 3000),  # ~36,000 characters
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=1000, timeout=30
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, DATALENGTH(text)/2 as char_count FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    # Verify approximate character counts (emoji are multi-codepoint in UTF-16)
    # "世界 🌍 Мир " is 10 chars but emoji takes 2 UTF-16 code units
    assert rows[0][1] >= 10000  # Should be at least 10,000 UTF-16 code units
    assert rows[1][1] >= 23000  # Second string should be around 24,000 chars
    assert rows[2][1] >= 35000  # Third string should be around 36,000 chars

    # Verify actual content for first row
    cursor.execute(f"SELECT text FROM {table_name} WHERE id = 1")
    row = cursor.fetchone()
    assert "世界" in row[0]
    assert "🌍" in row[0]
    assert "Мир" in row[0]

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_ntext_batch_processing(client_context):
    """Test cursor bulkcopy with multiple batches for NTEXT columns.

    Tests that bulk copy correctly processes multiple batches when inserting
    large numbers of rows with NTEXT data.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NTEXT column
    table_name = "BulkCopyBatchNTextTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NTEXT)")

    # Prepare test data with many rows
    data = [(i, f"Text content for row {i}") for i in range(1, 101)]

    # Execute bulk copy with small batch size to force multiple batches
    result = cursor.bulkcopy(
        table_name, iter(data), batch_size=10, timeout=30
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
