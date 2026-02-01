# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for NCHAR data type with varying lengths."""
import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_basic(client_context):
    """Test cursor bulkcopy method with nchar columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nchar columns of different lengths
    table_name = "BulkCopyTestTableNChar"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, name NCHAR(50), description NCHAR(200))"
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

    # Verify data was inserted correctly (NCHAR pads with spaces)
    cursor.execute(f"SELECT id, name, description FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "John Doe" and rows[0][2].rstrip() == "First employee"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "Jane Smith"
    # Verify NCHAR padding behavior
    assert len(rows[0][1]) == 50  # NCHAR(50) is fixed length
    assert len(rows[0][2]) == 200  # NCHAR(200) is fixed length

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping and NULL values.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable nchar columns
    table_name = "BulkCopyAutoMapTableNChar"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NCHAR(100))")

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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Alice"
    assert rows[1][0] == 2 and rows[1][1] is None  # Verify NULL in name column
    assert rows[2][0] == 4 and rows[2][1].rstrip() == "Charlie"
    assert rows[3][0] is None and rows[3][1].rstrip() == "Bob"  # Verify NULL in id column

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_numbers_as_strings(client_context):
    """Test cursor bulkcopy with numeric values as strings to NCHAR columns.

    Tests that numbers can be bulk copied into NCHAR columns by converting
    them to their string representations.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NCHAR columns
    table_name = "BulkCopyNumbersToNCharTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"CREATE TABLE {table_name} (id NCHAR(20), value NCHAR(50), decimal_val NCHAR(50))"
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

    # Verify data was inserted correctly as strings (with padding)
    cursor.execute(f"SELECT id, value, decimal_val FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0].rstrip() == "1" and rows[0][1].rstrip() == "100"
    assert rows[1][0].rstrip() == "2" and rows[1][1].rstrip() == "200"
    assert rows[2][0].rstrip() == "3" and rows[2][1].rstrip() == "300"
    # Verify decimal values were converted to strings
    assert "99.99" in rows[0][2]
    assert "199.99" in rows[1][2]
    # Verify NCHAR fixed length
    assert len(rows[0][0]) == 20
    assert len(rows[0][1]) == 50

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_string_numbers_to_nchar(client_context):
    """Test cursor bulkcopy with string representations of numbers to NCHAR columns.

    Tests that string values containing numbers can be bulk copied to NCHAR columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NCHAR columns
    table_name = "BulkCopyStringNumbersToNCharTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NCHAR(20), value NCHAR(50))")

    # Prepare test data - strings containing numbers
    data = [
        ("1", "100"),
        ("2", "200"),
        ("3", "300"),
        ("123456789", "987654321"),
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

    # Verify data was inserted correctly as strings (with padding)
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0].rstrip() == "1" and rows[0][1].rstrip() == "100"
    assert rows[1][0].rstrip() == "123456789" and rows[1][1].rstrip() == "987654321"
    assert rows[2][0].rstrip() == "2" and rows[2][1].rstrip() == "200"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_varying_lengths(client_context):
    """Test cursor bulkcopy with NCHAR columns of different lengths.

    Tests that data is correctly inserted into NCHAR columns with
    varying length specifications: small, medium, and large.
    Note: NCHAR uses 2 bytes per character, and SQL Server has an 8060 byte row limit.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NCHAR columns of varying lengths
    # Total: 10 + 100 + 3000 = 3110 chars = 6220 bytes (within 8060 limit)
    table_name = "BulkCopyVaryingLengthNCharTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(
        f"""CREATE TABLE {table_name} (
            id INT,
            small_text NCHAR(10),
            medium_text NCHAR(100),
            large_text NCHAR(3000)
        )"""
    )

    # Prepare test data with varying string lengths
    data = [
        (1, "Short", "Medium length text here", "A" * 1000),
        (2, "Test", "Another medium text", "C" * 2000),
        (3, "End", "Final text", "E" * 500),
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

    # Verify data was inserted correctly with fixed-length padding
    cursor.execute(
        f"SELECT id, small_text, medium_text, large_text FROM {table_name} ORDER BY id"
    )
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == 1
    assert rows[0][1].rstrip() == "Short"
    assert rows[0][2].rstrip() == "Medium length text here"
    # Verify NCHAR fixed length
    assert len(rows[0][1]) == 10
    assert len(rows[0][2]) == 100
    assert len(rows[0][3]) == 3000
    # Verify content
    assert rows[0][3].rstrip() == "A" * 1000
    
    assert rows[1][0] == 2
    assert rows[1][3].rstrip() == "C" * 2000

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable NCHAR column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a non-nullable NCHAR column
    table_name = "#BulkCopyNonNullableNCharTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NCHAR(100) NOT NULL)")

    # Prepare test data with a null value
    data = [
        (1, "Alice"),
        (2, None),  # This should trigger a conversion error
        (3, "Bob"),
    ]

    # Execute bulk copy and expect a ValueError
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), batch_size=1000, timeout=30
        )
        # If we get here, no error was raised
        print(f"No error raised. Result: {result}")
    except ValueError as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Expected ValueError caught: {e}")

    # Verify that an error was raised with appropriate message
    assert (
        error_raised
    ), "Expected a ValueError to be raised for null value in non-nullable column"
    assert (
        "conversion" in error_message or "null" in error_message
    ), f"Expected conversion error, got: {error_message}"
    assert (
        "non-nullable" in error_message
    ), f"Expected 'non-nullable' in error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_exceeds_length(client_context):
    """Test cursor bulkcopy with string exceeding NCHAR column length.

    Tests that strings longer than the column's defined length are properly handled.
    This should either truncate or raise an error depending on implementation.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a short NCHAR column
    table_name = "#BulkCopyNCharLengthExceedTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, name NCHAR(10))")

    # Prepare test data with string exceeding column length
    data = [
        (1, "Short"),
        (2, "ThisStringIsWayTooLongForTheColumn"),  # Exceeds NCHAR(10)
        (3, "Test"),
    ]

    # Execute bulk copy and expect an error or truncation
    # The exact behavior depends on implementation
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), batch_size=1000, timeout=30
        )
        # If we get here, the operation succeeded (possibly with truncation)
        print(f"Operation completed. Result: {result}")
        
        # Check if data was truncated
        cursor.execute(f"SELECT id, name FROM {table_name} WHERE id = 2")
        row = cursor.fetchone()
        if row:
            print(f"Row 2 value: '{row[1]}' (length: {len(row[1])})")
            # Verify truncation occurred - NCHAR is always fixed length
            assert len(row[1]) == 10, "NCHAR should be fixed at column length"
    except (ValueError, RuntimeError) as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Error caught: {e}")
        # If error is raised, verify it mentions length or truncation
        assert (
            "length" in error_message or "truncat" in error_message or "exceed" in error_message
        ), f"Expected length-related error, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_unicode_characters(client_context):
    """Test cursor bulkcopy with Unicode characters in NCHAR columns.

    Tests that Unicode characters (emoji, special characters, non-Latin scripts)
    are correctly stored in NCHAR columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NCHAR columns
    table_name = "BulkCopyUnicodeNCharTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NCHAR(200))")

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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "Hello 世界"
    assert rows[1][0] == 2 and rows[1][1].rstrip() == "Привет мир"
    assert rows[2][0] == 3 and rows[2][1].rstrip() == "مرحبا بالعالم"
    assert rows[3][0] == 4 and rows[3][1].rstrip() == "Hello 🌍🌎🌏"
    assert rows[4][0] == 5 and rows[4][1].rstrip() == "Special: ©®™€£¥"
    # Note: NCHAR fixed length verification skipped as driver may auto-trim on retrieval

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_empty_strings(client_context):
    """Test cursor bulkcopy with empty strings to NCHAR columns.

    Tests that empty strings are correctly distinguished from NULL values
    when bulk copying to NCHAR columns. Empty strings will be padded to column length.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable NCHAR column
    table_name = "BulkCopyEmptyStringNCharTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NCHAR(100))")

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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == ""  # Empty string preserved (padded)
    assert rows[1][0] == 2 and rows[1][1] is None  # NULL preserved
    # For whitespace, we check that it starts with spaces (not using rstrip which would remove them)
    assert rows[2][0] == 3 and rows[2][1].startswith("  ")  # Whitespace preserved (padded)
    assert rows[3][0] == 4 and rows[3][1].rstrip() == "Text"
    # Note: Fixed length verification may not work if driver auto-trims NCHAR on retrieval

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_mixed_types(client_context):
    """Test cursor bulkcopy with mixed data types converting to NCHAR columns.

    Tests that various Python types (int, float, bool, string) can all be
    converted to their string representations when targeting NCHAR columns.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NCHAR columns
    table_name = "BulkCopyMixedTypesNCharTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value NCHAR(100))")

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
    assert rows[0][0] == 1 and rows[0][1].rstrip() == "123"
    assert rows[1][0] == 2 and "456.789" in rows[1][1]  # Float may have slight variations
    assert rows[2][0] == 3 and rows[2][1].rstrip() in ["True", "1"]  # Boolean representations
    assert rows[3][0] == 4 and rows[3][1].rstrip() in ["False", "0"]  # Boolean representations
    assert rows[4][0] == 5 and rows[4][1].rstrip() == "Regular string"
    # Verify NCHAR fixed length
    assert all(len(row[1]) == 100 for row in rows)

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_nchar_padding_verification(client_context):
    """Test cursor bulkcopy verifies NCHAR padding behavior.

    Tests that NCHAR columns are always fixed-length and padded with spaces,
    unlike NVARCHAR which is variable-length.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with NCHAR column
    table_name = "BulkCopyNCharPaddingTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id INT, text NCHAR(20))")

    # Prepare test data with strings of different lengths
    data = [
        (1, "A"),  # 1 character
        (2, "AB"),  # 2 characters
        (3, "ABCDE"),  # 5 characters
        (4, "ABCDEFGHIJ"),  # 10 characters
        (5, "ABCDEFGHIJKLMNOPQRST"),  # 20 characters (full length)
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

    # Verify NCHAR padding - all strings should be exactly 20 characters
    cursor.execute(f"SELECT id, text, LEN(text) as text_len, DATALENGTH(text)/2 as char_count FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 5
    
    # All NCHAR values should have data length of 20 characters (40 bytes for Unicode)
    for row in rows:
        assert row[3] == 20, f"NCHAR(20) should always be 20 characters, got {row[3]} for id={row[0]}"
    
    # LEN() trims trailing spaces, so it will show actual content length
    assert rows[0][2] == 1  # "A" + 19 spaces
    assert rows[1][2] == 2  # "AB" + 18 spaces
    assert rows[2][2] == 5  # "ABCDE" + 15 spaces
    assert rows[3][2] == 10  # "ABCDEFGHIJ" + 10 spaces
    assert rows[4][2] == 20  # "ABCDEFGHIJKLMNOPQRST" (no padding needed)
    
    # Verify actual content when trimmed
    assert rows[0][1].rstrip() == "A"
    assert rows[1][1].rstrip() == "AB"
    assert rows[2][1].rstrip() == "ABCDE"

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
