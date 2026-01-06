"""Test cursor bulkcopy with FLOAT datatype.

This module contains tests for bulk copy operations with SQL Server FLOAT datatype,
which includes both REAL (4-byte) and FLOAT (8-byte) floating-point numbers.

SQL Server FLOAT details:
- FLOAT(n) where n is 1-53 bits of precision (default 53)
- FLOAT(1-24) stored as REAL (4 bytes, ~7 digits precision)
- FLOAT(25-53) stored as FLOAT (8 bytes, ~15 digits precision)
- Python float maps to SQL Server FLOAT
"""

import pytest
import mssql_py_core


@pytest.mark.integration
def test_cursor_bulkcopy_float_basic(client_context):
    """Test cursor bulkcopy with FLOAT datatype - basic insertion."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatBasic"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    data = [
        (1, 3.14159),
        (2, 2.71828),
        (3, 1.41421),
        (4, -9.81),
        (5, 0.0),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 5

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 5
    assert rows[0][0] == 1
    assert abs(rows[0][1] - 3.14159) < 1e-5
    assert rows[4][0] == 5
    assert rows[4][1] == 0.0


@pytest.mark.integration
def test_cursor_bulkcopy_float_auto_mapping(client_context):
    """Test cursor bulkcopy with FLOAT using auto column mapping."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatAutoMap"
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, measurement FLOAT)"
    )

    data = [
        (1, 98.6),
        (2, 37.0),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 2

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    assert count == 2
    cursor.fetchall()


@pytest.mark.integration
def test_cursor_bulkcopy_real_precision_24(client_context):
    """Test cursor bulkcopy with REAL (FLOAT(24)) - 4-byte precision."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestReal"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col REAL)")

    data = [
        (1, 3.14159),
        (2, 1234.567),
        (3, -9876.54),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3
    # REAL has lower precision (~7 digits)
    assert abs(rows[0][1] - 3.14159) < 1e-4
    assert abs(rows[1][1] - 1234.567) < 0.01


@pytest.mark.integration
def test_cursor_bulkcopy_float_precision_53(client_context):
    """Test cursor bulkcopy with FLOAT(53) - 8-byte precision (default)."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloat53"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT(53))")

    # High precision values
    data = [
        (1, 3.141592653589793),
        (2, 2.718281828459045),
        (3, 1.414213562373095),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 3

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 3
    # FLOAT(53) has high precision (~15 digits)
    assert abs(rows[0][1] - 3.141592653589793) < 1e-14
    assert abs(rows[1][1] - 2.718281828459045) < 1e-14


@pytest.mark.integration
def test_cursor_bulkcopy_float_scientific_notation(client_context):
    """Test cursor bulkcopy with FLOAT using scientific notation values."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatScientific"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    data = [
        (1, 1.23e10),      # 12300000000.0
        (2, 4.56e-8),      # 0.0000000456
        (3, -7.89e15),     # -7890000000000000.0
        (4, 9.99e-100),    # Very small positive
        (5, 1.11e100),     # Very large positive
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 5

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 5
    assert abs(rows[0][1] - 1.23e10) < 1e5
    assert abs(rows[1][1] - 4.56e-8) < 1e-15
    assert abs(rows[4][1] - 1.11e100) < 1e85


@pytest.mark.integration
def test_cursor_bulkcopy_float_null_handling(client_context):
    """Test cursor bulkcopy with FLOAT NULL values."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatNull"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT NULL)")

    data = [
        (1, 3.14),
        (2, None),
        (3, 2.71),
        (4, None),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 4
    assert abs(rows[0][1] - 3.14) < 0.01
    assert rows[1][1] is None
    assert abs(rows[2][1] - 2.71) < 0.01
    assert rows[3][1] is None


@pytest.mark.integration
def test_cursor_bulkcopy_float_null_to_non_nullable_column(client_context):
    """Test that NULL values are rejected for non-nullable FLOAT columns."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatNonNullable"
    cursor.execute(
        f"CREATE TABLE {table_name} (id INT, value_col FLOAT NOT NULL)"
    )

    data = [
        (1, 3.14),
        (2, None),  # This should cause an error
    ]

    with pytest.raises(ValueError, match="Cannot insert NULL value into non-nullable column"):
        cursor.bulkcopy(table_name, iter(data))


@pytest.mark.integration
def test_cursor_bulkcopy_float_string_conversion(client_context):
    """Test cursor bulkcopy with string to FLOAT conversion."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatStringConversion"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    # Strings that should convert to float
    data = [
        (1, "3.14159"),
        (2, "2.71828"),
        (3, "-9.81"),
        (4, "1.23e10"),
        (5, "0.0"),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 5

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 5
    assert abs(rows[0][1] - 3.14159) < 1e-5
    assert abs(rows[3][1] - 1.23e10) < 1e5


@pytest.mark.integration
def test_cursor_bulkcopy_float_invalid_string_conversion(client_context):
    """Test that invalid float strings are rejected when converting to FLOAT."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatInvalidString"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    # Invalid float string
    data = [
        (1, "not-a-number"),
    ]

    with pytest.raises(Exception):  # Should raise an error
        cursor.bulkcopy(table_name, iter(data))


@pytest.mark.integration
def test_cursor_bulkcopy_float_boundary_values(client_context):
    """Test cursor bulkcopy with FLOAT boundary values."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatBoundary"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    # SQL Server FLOAT ranges approximately:
    # -1.79E+308 to -2.23E-308, 0, and 2.23E-308 to 1.79E+308
    data = [
        (1, 1.79e308),      # Near max positive
        (2, -1.79e308),     # Near max negative
        (3, 2.23e-308),     # Near min positive
        (4, -2.23e-308),    # Near min negative
        (5, 0.0),           # Zero
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 5

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 5
    # Use relative tolerance for boundary values (0.1% tolerance)
    assert abs(rows[0][1] - 1.79e308) / 1.79e308 < 0.001
    assert abs(rows[1][1] - (-1.79e308)) / 1.79e308 < 0.001
    assert rows[4][1] == 0.0


@pytest.mark.integration
def test_cursor_bulkcopy_float_integer_conversion(client_context):
    """Test cursor bulkcopy with integer to FLOAT conversion."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatIntConversion"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    # Integers should convert to float
    data = [
        (1, 42),
        (2, -100),
        (3, 0),
        (4, 1234567890),
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 4

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 4
    assert rows[0][1] == 42.0
    assert rows[1][1] == -100.0
    assert rows[3][1] == 1234567890.0


@pytest.mark.integration
def test_cursor_bulkcopy_float_mixed_types(client_context):
    """Test cursor bulkcopy with mixed numeric types to FLOAT."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name = "#BulkCopyTestFloatMixedTypes"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value_col FLOAT)")

    # Mix of int, float, and string representations
    data = [
        (1, 42),           # int
        (2, 3.14159),      # float
        (3, "2.71828"),    # string
        (4, -100),         # negative int
        (5, 1.23e10),      # scientific notation float
    ]

    result = cursor.bulkcopy(table_name, iter(data))
    assert result["rows_copied"] == 5

    cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()

    assert len(rows) == 5
    assert rows[0][1] == 42.0
    assert abs(rows[1][1] - 3.14159) < 1e-5
    assert abs(rows[2][1] - 2.71828) < 1e-5
    assert abs(rows[4][1] - 1.23e10) < 1e5


@pytest.mark.integration
def test_cursor_bulkcopy_real_vs_float_precision(client_context):
    """Test precision differences between REAL and FLOAT."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    table_name_real = "#BulkCopyTestRealPrecision"
    table_name_float = "#BulkCopyTestFloatPrecision"

    # Create REAL table
    cursor.execute(f"CREATE TABLE {table_name_real} (id INT, value_col REAL)")

    # Create FLOAT table
    cursor.execute(f"CREATE TABLE {table_name_float} (id INT, value_col FLOAT)")

    # High precision value
    high_precision_value = 3.141592653589793

    data_real = [(1, high_precision_value)]
    data_float = [(1, high_precision_value)]

    # Insert into REAL
    result_real = cursor.bulkcopy(table_name_real, iter(data_real))
    assert result_real["rows_copied"] == 1

    # Insert into FLOAT
    result_float = cursor.bulkcopy(table_name_float, iter(data_float))
    assert result_float["rows_copied"] == 1

    # Check REAL value (lower precision)
    cursor.execute(f"SELECT value_col FROM {table_name_real}")
    real_value = cursor.fetchone()[0]
    cursor.fetchall()

    # Check FLOAT value (higher precision)
    cursor.execute(f"SELECT value_col FROM {table_name_float}")
    float_value = cursor.fetchone()[0]
    cursor.fetchall()

    # REAL should have less precision than FLOAT
    real_error = abs(real_value - high_precision_value)
    float_error = abs(float_value - high_precision_value)

    # REAL typically has ~7 digits precision (relative error ~1e-7)
    # FLOAT has ~15 digits precision (relative error ~1e-15)
    # Check that REAL has more error than FLOAT
    assert real_error > float_error, f"REAL error ({real_error}) should be greater than FLOAT error ({float_error})"
    
    # Also verify that FLOAT maintains high precision
    assert float_error < 1e-14  # FLOAT maintains precision
