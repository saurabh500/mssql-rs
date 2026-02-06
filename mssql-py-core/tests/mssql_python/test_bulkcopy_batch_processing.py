# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for batch processing behavior.

Tests the batch_size option and multi-batch scenarios:
- Small batch sizes with large datasets
- Large batch sizes
- Batch boundaries and row counting
- Generator exhaustion with batching
"""

import time
import pytest


def unique_table_name(prefix: str) -> str:
    """Generate a unique table name with timestamp suffix."""
    return f"{prefix}_{int(time.time() * 1000)}"


def test_bulkcopy_multiple_batches(cursor):
    """Test bulk copy with multiple batches.
    
    Insert 1000 rows with batch_size=100, expecting 10 batches.
    """
    table_name = unique_table_name("BulkCopyMultiBatch")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

        # Generate 1000 rows
        data = [(i, i * 10) for i in range(1, 1001)]

        result = cursor._bulkcopy(
            table_name,
            iter(data),
            batch_size=100,
            timeout=60,
        )

        assert result is not None
        assert result["rows_copied"] == 1000
        assert result["batch_count"] == 10, f"Expected 10 batches, got {result['batch_count']}"

        # Verify data integrity
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 1000

        # Spot check some values
        cursor.execute(f"SELECT value FROM {table_name} WHERE id = 500")
        rows = cursor.fetchall()
        assert rows[0][0] == 5000

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_partial_final_batch(cursor):
    """Test bulk copy where final batch is smaller than batch_size.
    
    Insert 250 rows with batch_size=100, expecting 3 batches (100, 100, 50).
    """
    table_name = unique_table_name("BulkCopyPartialBatch")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value NVARCHAR(50))")

        # Generate 250 rows - not evenly divisible by batch_size
        data = [(i, f"Value {i}") for i in range(1, 251)]

        result = cursor._bulkcopy(
            table_name,
            iter(data),
            batch_size=100,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 250
        assert result["batch_count"] == 3, f"Expected 3 batches, got {result['batch_count']}"

        # Verify all rows inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 250

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_single_row_batches(cursor):
    """Test bulk copy with batch_size=1 (one row per batch).
    
    Extreme case to verify batch boundary handling.
    """
    table_name = unique_table_name("BulkCopySingleRowBatch")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, name NVARCHAR(100))")

        # 10 rows with batch_size=1 means 10 batches
        data = [(i, f"Row {i}") for i in range(1, 11)]

        result = cursor._bulkcopy(
            table_name,
            iter(data),
            batch_size=1,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 10
        assert result["batch_count"] == 10

        # Verify data
        cursor.execute(f"SELECT id, name FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        assert len(rows) == 10
        assert rows[4][0] == 5 and rows[4][1] == "Row 5"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_large_batch_size(cursor):
    """Test bulk copy with batch_size larger than data count.
    
    Should result in single batch.
    """
    table_name = unique_table_name("BulkCopyLargeBatch")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

        # 50 rows with batch_size=10000
        data = [(i, i * 2) for i in range(1, 51)]

        result = cursor._bulkcopy(
            table_name,
            iter(data),
            batch_size=10000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 50
        assert result["batch_count"] == 1, "Should be single batch"

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 50

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_generator_with_batching(cursor):
    """Test bulk copy using a generator function with batching.
    
    Verifies that generator is properly consumed across batches.
    """
    table_name = unique_table_name("BulkCopyGenerator")

    def data_generator(count):
        """Generator that yields rows one at a time."""
        for i in range(1, count + 1):
            yield (i, f"Generated row {i}", i * 100)

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, description NVARCHAR(100), amount INT)")

        result = cursor._bulkcopy(
            table_name,
            data_generator(500),  # Generator, not list
            batch_size=75,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 500
        # 500 / 75 = 6.67, so 7 batches
        assert result["batch_count"] == 7, f"Expected 7 batches, got {result['batch_count']}"

        # Verify data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 500

        cursor.execute(f"SELECT amount FROM {table_name} WHERE id = 123")
        rows = cursor.fetchall()
        assert rows[0][0] == 12300

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_10000_rows(cursor):
    """Test bulk copy with 10,000 rows to verify large datasets work correctly."""
    table_name = unique_table_name("BulkCopy10K")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT,
                col1 INT,
                col2 INT,
                col3 NVARCHAR(50)
            )
        """)

        # Generate 10,000 rows
        data = [(i, i % 100, i * 2, f"Row {i}") for i in range(1, 10001)]

        result = cursor._bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=120,
        )

        assert result is not None
        assert result["rows_copied"] == 10000
        assert result["batch_count"] == 10

        # Verify data integrity
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        rows = cursor.fetchall()
        assert rows[0][0] == 10000

        # Verify random sample
        cursor.execute(f"SELECT col3 FROM {table_name} WHERE id = 7777")
        rows = cursor.fetchall()
        assert rows[0][0] == "Row 7777"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_batch_with_mixed_types(cursor):
    """Test batch processing with various data types in each row.
    
    Ensures type handling is consistent across batch boundaries.
    """
    table_name = unique_table_name("BulkCopyMixedTypes")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                int_col INT,
                bigint_col BIGINT,
                float_col FLOAT,
                nvarchar_col NVARCHAR(100),
                bit_col BIT
            )
        """)

        # Generate 150 rows with mixed types (3 batches of 50)
        data = [
            (i, i * 1000000000, i * 1.5, f"Text {i}", i % 2)
            for i in range(1, 151)
        ]

        result = cursor._bulkcopy(
            table_name,
            iter(data),
            batch_size=50,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 150
        assert result["batch_count"] == 3

        # Verify data across batch boundaries
        cursor.execute(f"SELECT * FROM {table_name} WHERE int_col = 50")
        rows = cursor.fetchall()
        assert rows[0][0] == 50
        assert rows[0][1] == 50000000000
        assert abs(rows[0][2] - 75.0) < 0.001
        assert rows[0][3] == "Text 50"

        # Row at batch boundary
        cursor.execute(f"SELECT * FROM {table_name} WHERE int_col = 51")
        rows = cursor.fetchall()
        assert rows[0][0] == 51

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
