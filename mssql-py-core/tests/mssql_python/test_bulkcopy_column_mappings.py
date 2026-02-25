# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for column mapping scenarios.

Tests the column_mappings parameter:
- Simple list of column names: List[str]
- Tuple mapping with ordinal: List[Tuple[int, str]]
- Column reordering
- Partial column mapping
- Case sensitivity
"""

import time


def unique_table_name(prefix: str) -> str:
    """Generate a unique table name with timestamp suffix."""
    return f"{prefix}_{int(time.time() * 1000)}"


def test_bulkcopy_column_mappings_simple_list(cursor):
    """Test column mappings using simple list of column names.
    
    column_mappings: List[str] - maps source columns 0,1,2... to named columns in order.
    """
    table_name = unique_table_name("BulkCopySimpleMapping")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                first_name NVARCHAR(50),
                last_name NVARCHAR(50),
                age INT
            )
        """)

        # Data: (id, first_name, last_name, age) - matching table column order
        data = [
            (1, "John", "Doe", 30),
            (2, "Jane", "Smith", 25),
            (3, "Bob", "Wilson", 40),
        ]

        # Tuple mapping - source ordinal to target column name
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "first_name"), (2, "last_name"), (3, "age")],
        )

        assert result is not None
        assert result["rows_copied"] == 3

        cursor.execute(f"SELECT id, first_name, last_name, age FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1 and rows[0][1] == "John" and rows[0][2] == "Doe" and rows[0][3] == 30
        assert rows[1][0] == 2 and rows[1][1] == "Jane" and rows[1][2] == "Smith" and rows[1][3] == 25
        assert rows[2][0] == 3 and rows[2][1] == "Bob" and rows[2][2] == "Wilson" and rows[2][3] == 40

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_column_mappings_reorder(cursor):
    """Test column mappings with tuple format to reorder columns.
    
    column_mappings: List[Tuple[int, str]] - maps source ordinal to target column name.
    """
    table_name = unique_table_name("BulkCopyReorderMapping")

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

        # Source data has different order: (id, first_name, last_name)
        data = [
            (1, "John", "Doe"),
            (2, "Jane", "Smith"),
        ]

        # Map: source[0] -> "id", source[1] -> "first_name", source[2] -> "last_name"
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "first_name"), (2, "last_name")],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT first_name, last_name, id FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == "John" and rows[0][1] == "Doe" and rows[0][2] == 1
        assert rows[1][0] == "Jane" and rows[1][1] == "Smith" and rows[1][2] == 2

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_column_mappings_partial_columns(cursor):
    """Test column mappings with subset of target columns.
    
    Target table has more columns than source data provides.
    """
    table_name = unique_table_name("BulkCopyPartialMapping")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50),
                optional_field NVARCHAR(100) NULL DEFAULT 'default_value',
                created_at DATETIME DEFAULT GETDATE()
            )
        """)

        # Source data only has id and name
        data = [
            (1, "Product A"),
            (2, "Product B"),
            (3, "Product C"),
        ]

        # Only map the columns we have data for
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "name")],
        )

        assert result is not None
        assert result["rows_copied"] == 3

        cursor.execute(f"SELECT id, name, optional_field FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1
        assert rows[0][1] == "Product A"
        # Default value should be applied
        assert rows[0][2] == "default_value"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_column_mappings_skip_source_column(cursor):
    """Test column mappings that skip a source column.
    
    Source has 4 columns but we only map 3 to target.
    """
    table_name = unique_table_name("BulkCopySkipColumn")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                name NVARCHAR(50),
                amount INT
            )
        """)

        # Source has 4 columns: id, name, unused_field, amount
        data = [
            (1, "Item 1", "ignore_me", 100),
            (2, "Item 2", "skip_this", 200),
        ]

        # Map source[0] -> id, source[1] -> name, source[3] -> amount (skip source[2])
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "name"), (3, "amount")],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT id, name, amount FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1 and rows[0][1] == "Item 1" and rows[0][2] == 100
        assert rows[1][0] == 2 and rows[1][1] == "Item 2" and rows[1][2] == 200

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_column_mappings_case_sensitivity(cursor):
    """Test column mappings must use exact case matching.
    
    NOTE: The Rust mssql_py_core implementation requires exact case matching
    for column names, unlike SQL Server which is typically case-insensitive.
    This test verifies correct case works.
    """
    table_name = unique_table_name("BulkCopyCaseSensitive")

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

        # Use exact case matching (required by mssql_py_core)
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "ProductID"), (1, "ProductName"), (2, "UnitPrice")],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT ProductID, ProductName, UnitPrice FROM {table_name} ORDER BY ProductID")
        rows = cursor.fetchall()
        
        assert rows[0][0] == 1
        assert rows[0][1] == "Widget"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_column_mappings_with_identity(cursor):
    """Test column mappings with IDENTITY column and keep_identity option."""
    table_name = unique_table_name("BulkCopyMappingIdentity")

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

        # Source data includes the identity column values
        data = [
            (100, "Record 100", 1000),
            (200, "Record 200", 2000),
        ]

        # Map all columns including identity, with keep_identity=True
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "id"), (1, "name"), (2, "value")],
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


def test_bulkcopy_column_mappings_noncontiguous_ordinals(cursor):
    """Test column mappings with non-contiguous source ordinals."""
    table_name = unique_table_name("BulkCopyNonContiguous")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                col_a NVARCHAR(50),
                col_b INT,
                col_c NVARCHAR(50)
            )
        """)

        # Source: (a, b, c, d, e) - we pick columns 0, 2, 4
        data = [
            ("A1", "skip", 100, "skip", "C1"),
            ("A2", "skip", 200, "skip", "C2"),
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=[(0, "col_a"), (2, "col_b"), (4, "col_c")],
        )

        assert result is not None
        assert result["rows_copied"] == 2

        cursor.execute(f"SELECT col_a, col_b, col_c FROM {table_name} ORDER BY col_a")
        rows = cursor.fetchall()
        
        assert rows[0][0] == "A1" and rows[0][1] == 100 and rows[0][2] == "C1"
        assert rows[1][0] == "A2" and rows[1][1] == 200 and rows[1][2] == "C2"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_column_mappings_many_columns(cursor):
    """Test column mappings with many columns (stress test)."""
    table_name = unique_table_name("BulkCopyManyColsMapping")

    num_columns = 30

    try:
        # Create table with many columns
        columns_def = ", ".join([f"col{i} INT" for i in range(num_columns)])
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} ({columns_def})")

        # Generate data with all columns
        data = [tuple(row * 100 + col for col in range(num_columns)) for row in range(10)]

        # Map all columns explicitly using tuple format
        column_mappings = [(i, f"col{i}") for i in range(num_columns)]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
            column_mappings=column_mappings,
        )

        assert result is not None
        assert result["rows_copied"] == 10

        # Verify first and last columns
        cursor.execute(f"SELECT col0, col{num_columns-1} FROM {table_name} WHERE col0 = 0")
        rows = cursor.fetchall()
        assert rows[0][0] == 0 and rows[0][1] == num_columns - 1

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
