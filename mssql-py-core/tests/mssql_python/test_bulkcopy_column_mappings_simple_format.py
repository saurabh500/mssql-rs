# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for simple List[str] column mapping format.

Tests the column_mappings parameter using the simple format:
- column_mappings: List[str] - maps source columns by position to named columns
- Example: ['col1', 'col2', 'col3'] maps index 0→col1, 1→col2, 2→col3
"""

import time
import pytest


def unique_table_name(prefix: str) -> str:
    """Generate a unique table name with timestamp suffix."""
    return f"{prefix}_{int(time.time() * 1000)}"


def test_bulkcopy_simple_list_format_basic(cursor):
    """Test column mappings using simple List[str] format."""
    table_name = unique_table_name("BulkCopySimpleList")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50),
                value INT
            )
        """)

        # Data matches table column order
        data = [
            (1, "Alice", 100),
            (2, "Bob", 200),
            (3, "Charlie", 300),
        ]

        # Simple format: position in list = source ordinal
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=['id', 'name', 'value'],
        )

        assert result is not None
        assert result["rows_copied"] == 3

        cursor.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1 and rows[0][1] == "Alice" and rows[0][2] == 100
        assert rows[1][0] == 2 and rows[1][1] == "Bob" and rows[1][2] == 200
        assert rows[2][0] == 3 and rows[2][1] == "Charlie" and rows[2][2] == 300

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_reordering(cursor):
    """Test simple List[str] format handles column reordering.
    
    Table columns: [first_name, last_name, id]
    Source data: (id, first_name, last_name)
    Mapping: ['id', 'first_name', 'last_name'] → maps source[0]→id, [1]→first_name, [2]→last_name
    """
    table_name = unique_table_name("BulkCopySimpleReorder")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                first_name NVARCHAR(50),
                last_name NVARCHAR(50),
                id INT
            )
        """)

        # Source data: (id, first_name, last_name) - different order than table
        data = [
            (1, "John", "Doe"),
            (2, "Jane", "Smith"),
        ]

        # Map source positions to target columns
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=['id', 'first_name', 'last_name'],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT id, first_name, last_name FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1 and rows[0][1] == "John" and rows[0][2] == "Doe"
        assert rows[1][0] == 2 and rows[1][1] == "Jane" and rows[1][2] == "Smith"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_subset_columns(cursor):
    """Test simple List[str] format with fewer columns than table has."""
    table_name = unique_table_name("BulkCopySimpleSubset")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50),
                optional_col NVARCHAR(100) NULL DEFAULT 'default',
                timestamp DATETIME DEFAULT GETDATE()
            )
        """)

        # Source only has 2 columns
        data = [
            (1, "Product A"),
            (2, "Product B"),
        ]

        # Map only the columns we have
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=['id', 'name'],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT id, name, optional_col FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1 and rows[0][1] == "Product A"
        assert rows[0][2] == "default"  # Default value applied

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_many_columns(cursor):
    """Test simple List[str] format with many columns."""
    table_name = unique_table_name("BulkCopySimpleMany")

    num_columns = 20

    try:
        # Create table with many columns
        columns_def = ", ".join([f"col{i} INT" for i in range(num_columns)])
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} ({columns_def})")

        # Generate data
        data = [tuple(row * 100 + col for col in range(num_columns)) for row in range(5)]

        # Simple mappings
        column_mappings = [f"col{i}" for i in range(num_columns)]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=column_mappings,
        )

        assert result is not None
        assert result["rows_copied"] == 5

        cursor.execute(f"SELECT col0, col{num_columns-1} FROM {table_name} WHERE col0 = 0")
        rows = cursor.fetchall()
        assert rows[0][0] == 0 and rows[0][1] == num_columns - 1

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_with_identity(cursor):
    """Test simple List[str] format with IDENTITY column and keep_identity."""
    table_name = unique_table_name("BulkCopySimpleIdentity")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1),
                name NVARCHAR(50),
                value INT
            )
        """)

        # Source includes explicit identity values
        data = [
            (100, "Record 100", 1000),
            (200, "Record 200", 2000),
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=['id', 'name', 'value'],
            keep_identity=True,
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT id, name, value FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        # Identity values should be preserved
        assert rows[0][0] == 100 and rows[0][1] == "Record 100" and rows[0][2] == 1000
        assert rows[1][0] == 200 and rows[1][1] == "Record 200" and rows[1][2] == 2000

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_mixed_types_error(cursor):
    """Test that mixing strings and tuples in column_mappings raises an error."""
    table_name = unique_table_name("BulkCopyMixedError")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50)
            )
        """)

        data = [(1, "Test")]

        # Mixed format should fail
        with pytest.raises(TypeError, match="Not all items in column mapping are the same type"):
            cursor.bulkcopy(
                table_name,
                iter(data),
                column_mappings=['id', (1, 'name')],  # Mixed!
            )

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_case_sensitivity(cursor):
    """Test simple List[str] format with exact case matching."""
    table_name = unique_table_name("BulkCopySimpleCase")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                ProductID INT,
                ProductName NVARCHAR(100),
                UnitPrice DECIMAL(10, 2)
            )
        """)

        data = [
            (1, "Widget", 9.99),
            (2, "Gadget", 19.99),
        ]

        # Use exact case
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=['ProductID', 'ProductName', 'UnitPrice'],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT ProductID, ProductName, UnitPrice FROM {table_name} ORDER BY ProductID")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1 and rows[0][1] == "Widget"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_simple_list_format_empty_list(cursor):
    """Test that empty column_mappings list triggers auto-mapping."""
    table_name = unique_table_name("BulkCopyEmptyList")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                value INT
            )
        """)

        data = [(1, 100), (2, 200)]

        # Empty list should use auto-mapping
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            column_mappings=[],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        assert rows[0][0] == 1 and rows[0][1] == 100

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
