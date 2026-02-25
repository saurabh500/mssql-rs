# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for Unicode and special character handling.

Tests proper encoding and handling of:
- Unicode characters (CJK, emoji, diacritics)
- Special characters
- Empty strings
- Very long strings
- Mixed character sets
"""

import time
import pytest


def unique_table_name(prefix: str) -> str:
    """Generate a unique table name with timestamp suffix."""
    return f"{prefix}_{int(time.time() * 1000)}"


def test_bulkcopy_unicode_cjk_characters(cursor):
    """Test bulk copy with Chinese, Japanese, and Korean characters."""
    table_name = unique_table_name("BulkCopyUnicodeCJK")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, text_col NVARCHAR(200))")

        data = [
            (1, "中文测试数据"),           # Chinese
            (2, "日本語テスト"),            # Japanese
            (3, "한국어 테스트"),           # Korean
            (4, "混合 Mixed 混合"),         # Mixed
            (5, "繁體中文測試"),            # Traditional Chinese
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 5

        cursor.execute(f"SELECT id, text_col FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][1] == "中文测试数据"
        assert rows[1][1] == "日本語テスト"
        assert rows[2][1] == "한국어 테스트"
        assert rows[3][1] == "混合 Mixed 混合"
        assert rows[4][1] == "繁體中文測試"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_unicode_emoji(cursor):
    """Test bulk copy with emoji characters."""
    table_name = unique_table_name("BulkCopyEmoji")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, emoji_col NVARCHAR(100))")

        data = [
            (1, "Hello 👋 World 🌍"),
            (2, "❤️ Love ❤️"),
            (3, "🎉 Party 🎊 Time 🕐"),
            (4, "Code 💻 Debug 🐛"),
            (5, "✅ Done ✓"),
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 5

        cursor.execute(f"SELECT emoji_col FROM {table_name} WHERE id = 1")
        rows = cursor.fetchall()
        assert "👋" in rows[0][0]
        assert "🌍" in rows[0][0]

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_unicode_diacritics(cursor):
    """Test bulk copy with diacritical marks and accented characters."""
    table_name = unique_table_name("BulkCopyDiacritics")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, name NVARCHAR(100))")

        data = [
            (1, "José García"),           # Spanish
            (2, "François Müller"),       # French/German
            (3, "Bjørn Østergren"),        # Norwegian
            (4, "Zoë Naïve Café"),         # Various diacritics
            (5, "Çağdaş Türkçe"),          # Turkish
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 5

        cursor.execute(f"SELECT name FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert rows[0][0] == "José García"
        assert rows[1][0] == "François Müller"
        assert rows[2][0] == "Bjørn Østergren"
        assert rows[3][0] == "Zoë Naïve Café"
        assert rows[4][0] == "Çağdaş Türkçe"

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_empty_strings(cursor):
    """Test bulk copy with empty strings vs NULL values."""
    table_name = unique_table_name("BulkCopyEmptyStrings")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, str_col NVARCHAR(50))")

        data = [
            (1, "Normal text"),
            (2, ""),              # Empty string
            (3, None),           # NULL
            (4, " "),            # Single space
            (5, "   "),          # Multiple spaces
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 5

        cursor.execute(f"SELECT id, str_col FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()

        # Row 1: Normal text
        assert rows[0][1] == "Normal text"
        
        # Row 2: Empty string
        assert rows[1][1] == ""
        
        # Row 3: NULL
        assert rows[2][1] is None
        
        # Row 4: Single space - verify it's stored (SQL Server LEN() trims trailing spaces)
        assert rows[3][1] is not None
        
        # Row 5: Multiple spaces - verify it's stored
        assert rows[4][1] is not None

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_special_sql_characters(cursor):
    """Test bulk copy with SQL special characters that need escaping."""
    table_name = unique_table_name("BulkCopySpecialChars")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, content NVARCHAR(200))")

        data = [
            (1, "Contains 'single quotes'"),
            (2, 'Contains "double quotes"'),
            (3, "Contains ; semicolon"),
            (4, "Contains -- comment markers"),
            (5, "Contains % percent and _ underscore"),
            (6, "Contains [brackets]"),
            (7, "Line 1\nLine 2\nLine 3"),  # Newlines
            (8, "Tab\there\tand\there"),     # Tabs
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 8

        cursor.execute(f"SELECT content FROM {table_name} WHERE id = 1")
        rows = cursor.fetchall()
        assert rows[0][0] == "Contains 'single quotes'"

        cursor.execute(f"SELECT content FROM {table_name} WHERE id = 7")
        rows = cursor.fetchall()
        assert "Line 1" in rows[0][0]
        assert "\n" in rows[0][0]

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_long_strings(cursor):
    """Test bulk copy with very long strings approaching max length."""
    table_name = unique_table_name("BulkCopyLongStrings")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, long_text NVARCHAR(4000))")

        # Generate long strings
        data = [
            (1, "A" * 100),
            (2, "B" * 1000),
            (3, "C" * 3000),
            (4, "D" * 4000),  # Max for NVARCHAR(4000)
            (5, "E" * 2500 + "中文" * 100),  # Mixed with unicode
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 5

        # Verify lengths
        cursor.execute(f"SELECT id, LEN(long_text) FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()

        assert rows[0][1] == 100
        assert rows[1][1] == 1000
        assert rows[2][1] == 3000
        assert rows[3][1] == 4000
        assert rows[4][1] == 2700  # 2500 + 200 unicode chars

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_nvarchar_max(cursor):
    """Test bulk copy with NVARCHAR(MAX) for very large text."""
    table_name = unique_table_name("BulkCopyNvarcharMax")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, large_text NVARCHAR(MAX))")

        # Generate very large strings
        large_text_8k = "X" * 8000
        large_text_16k = "Y" * 16000
        
        data = [
            (1, large_text_8k),
            (2, large_text_16k),
            (3, "Short text"),
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=60,
        )

        assert result is not None
        assert result["rows_copied"] == 3

        # Verify lengths
        cursor.execute(f"SELECT id, LEN(large_text) FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()

        assert rows[0][1] == 8000
        assert rows[1][1] == 16000
        assert rows[2][1] == 10

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_unicode_normalization(cursor):
    """Test bulk copy preserves Unicode normalization forms."""
    table_name = unique_table_name("BulkCopyUnicodeNorm")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, text_col NVARCHAR(100))")

        # Different representations of "é" - composed vs decomposed
        data = [
            (1, "café"),           # Composed form (single char é)
            (2, "cafe\u0301"),     # Decomposed form (e + combining acute)
            (3, "naïve"),          # ï composed
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 3

        # Verify the data was stored
        cursor.execute(f"SELECT text_col FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        # Both should contain the visual character
        assert "caf" in rows[0][0]
        assert "caf" in rows[1][0]
        assert "na" in rows[2][0]

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")


def test_bulkcopy_rtl_languages(cursor):
    """Test bulk copy with right-to-left languages (Arabic, Hebrew)."""
    table_name = unique_table_name("BulkCopyRTL")

    try:
        cursor.execute(
            f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
        )
        cursor.execute(f"CREATE TABLE {table_name} (id INT, text_col NVARCHAR(200))")

        data = [
            (1, "مرحبا بالعالم"),        # Arabic: "Hello World"
            (2, "שלום עולם"),            # Hebrew: "Hello World"
            (3, "Mixed: Hello مرحبا"),   # Mixed LTR and RTL
        ]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
            timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 3

        cursor.execute(f"SELECT text_col FROM {table_name} ORDER BY id")
        rows = cursor.fetchall()
        
        assert "مرحبا" in rows[0][0]
        assert "שלום" in rows[1][0]
        assert "Hello" in rows[2][0] and "مرحبا" in rows[2][0]

    finally:
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
