# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for NUMERIC data type."""
import pytest
import mssql_py_core
from decimal import Decimal


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_basic(client_context):
    """Test cursor bulkcopy method with two numeric columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two numeric columns
    table_name = "BulkCopyTestTableNumeric"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data - two columns, both numeric
    data = [
        (Decimal("1.00"), Decimal("100.5000")),
        (Decimal("2.50"), Decimal("200.7500")),
        (Decimal("3.99"), Decimal("300.9999")),
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
                (1, "value"),
            ],  # Map tuple positions to columns
        },
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1.00") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2.50") and rows[1][1] == Decimal("200.7500")


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two nullable numeric columns
    table_name = "BulkCopyAutoMapTableNumeric"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data - two columns, both numeric, with NULL values
    data = [
        (Decimal("1.00"), Decimal("100.5000")),
        (Decimal("2.50"), None),  # NULL value in second column
        (None, Decimal("300.9999")),  # NULL value in first column
        (Decimal("4.75"), Decimal("400.1234")),
    ]

    # Execute bulk copy WITHOUT column mappings - should auto-generate
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 4
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly, including NULL values
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY COALESCE(id, 999)")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == Decimal("1.00") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2.50") and rows[1][1] is None  # Verify NULL in value column
    assert rows[2][0] == Decimal("4.75") and rows[2][1] == Decimal("400.1234")
    assert rows[3][0] is None and rows[3][1] == Decimal("300.9999")  # Verify NULL in id column

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_string_to_numeric_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to numeric columns.

    Tests type coercion when source data contains numeric strings but
    destination columns are NUMERIC type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two numeric columns
    table_name = "BulkCopyStringToNumericTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data - strings containing valid numeric numbers
    data = [
        ("1.00", "100.5000"),
        ("2.50", "200.7500"),
        ("3.99", "300.9999"),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly and converted to numeric
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1.00") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2.50") and rows[1][1] == Decimal("200.7500")
    assert rows[2][0] == Decimal("3.99") and rows[2][1] == Decimal("300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_integer_to_numeric_conversion(client_context):
    """Test cursor bulkcopy with integer values that should convert to numeric columns.

    Tests type coercion when source data contains integers but
    destination columns are NUMERIC type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two numeric columns
    table_name = "BulkCopyIntToNumericTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data - integers that should convert to numeric
    data = [
        (1, 100),
        (2, 200),
        (3, 300),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly and converted to numeric
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1.00") and rows[0][1] == Decimal("100.0000")
    assert rows[1][0] == Decimal("2.00") and rows[1][1] == Decimal("200.0000")
    assert rows[2][0] == Decimal("3.00") and rows[2][1] == Decimal("300.0000")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_float_to_numeric_conversion(client_context):
    """Test cursor bulkcopy with float values that should convert to numeric columns.

    Tests type coercion when source data contains floats but
    destination columns are NUMERIC type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with two numeric columns
    table_name = "BulkCopyFloatToNumericTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data - floats that should convert to numeric
    data = [
        (1.25, 100.5),
        (2.50, 200.75),
        (3.99, 300.9999),
    ]

    # Execute bulk copy without explicit mappings
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 3
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly and converted to numeric
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1.25") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2.50") and rows[1][1] == Decimal("200.7500")
    assert rows[2][0] == Decimal("3.99") and rows[2][1] == Decimal("300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable numeric column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable numeric column
    table_name = "#BulkCopyNonNullableTableNumeric"
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2) NOT NULL)")

    # Prepare test data with a null value
    data = [
        (Decimal("1.00"),),
        (None,),  # This should trigger a conversion error
        (Decimal("3.00"),),
    ]

    # Execute bulk copy and expect a ValueError
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
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
def test_cursor_bulkcopy_numeric_invalid_string_to_numeric_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to numeric.

    Tests that client-side type coercion properly validates string-to-numeric conversion
    and fails with an appropriate error when the string is not a valid numeric number.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with numeric columns
    table_name = "#BulkCopyInvalidStringTableNumeric"
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data with invalid string that cannot be parsed as numeric
    data = [
        ("1.00", "100.5000"),
        ("not_a_number", "200.7500"),  # This should trigger a conversion error
        ("3.99", "300.9999"),
    ]

    # Execute bulk copy and expect a client-side ValueError
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
        )
        # If we get here, no error was raised
        print(f"No error raised. Result: {result}")
    except ValueError as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Client-side ValueError caught: {e}")

    # Verify that an error was raised with appropriate message about conversion failure
    assert (
        error_raised
    ), "Expected a ValueError to be raised for invalid string-to-numeric conversion"
    assert (
        "failed to parse decimal" in error_message
    ), f"Expected 'failed to parse decimal' message, got: {error_message}"
    # Verify that the original parse error message is preserved
    assert (
        "invalid" in error_message and "not_a_number" in error_message
    ), f"Expected original parse error details to be preserved, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_precision_overflow(client_context):
    """Test cursor bulkcopy with numeric value exceeding precision.

    When a numeric value has more digits than the column precision allows,
    it should be rejected during type coercion.

    Expected behavior:
    - Numeric value with precision <= column precision converts successfully
    - Numeric value with precision > column precision triggers validation error
    - Error raised: "Numeric value exceeds precision for NUMERIC column"
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with NUMERIC(10, 2) column (max 10 total digits, 2 after decimal)
    table_name = "#BulkCopyNumericPrecisionOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(10, 2))")

    # Prepare test data with numeric value exceeding precision
    # NUMERIC(10, 2) allows max: 99999999.99 (8 digits before, 2 after decimal)
    data = [
        (Decimal("1.00"), Decimal("100.50")),
        (Decimal("2.00"), Decimal("999999999.99")),  # 9 digits before decimal, exceeds NUMERIC(10, 2)
        (Decimal("3.00"), Decimal("300.99")),
    ]

    # Execute bulk copy and expect precision validation error
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
        )
        # If we get here, no error was raised (unexpected)
        print(f"No error raised. Result: {result}")
    except (ValueError, RuntimeError) as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Expected error caught: {e}")

    # Verify that an error was raised about precision overflow
    assert (
        error_raised
    ), "Expected an error for numeric value exceeding column precision"
    assert (
        "precision" in error_message or "exceeds" in error_message or "overflow" in error_message
    ), f"Expected precision error, got: {error_message}"
    assert (
        "numeric" in error_message
    ), f"Expected NUMERIC column type in error, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_scale_overflow(client_context):
    """Test cursor bulkcopy with numeric value exceeding scale.

    When a numeric value has more digits after the decimal point than the column scale allows,
    it should be rejected or rounded during type coercion (depending on implementation).

    Expected behavior:
    - Numeric value with scale <= column scale converts successfully
    - Numeric value with scale > column scale may trigger validation error or be rounded
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with NUMERIC(10, 2) column (2 digits after decimal point)
    table_name = "#BulkCopyNumericScaleOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(10, 2))")

    # Prepare test data with numeric value exceeding scale
    # NUMERIC(10, 2) allows max 2 digits after decimal point
    data = [
        (Decimal("1.00"), Decimal("100.50")),
        (Decimal("2.00"), Decimal("200.999")),  # 3 digits after decimal, exceeds scale of 2
        (Decimal("3.00"), Decimal("300.99")),
    ]

    # Execute bulk copy and expect scale validation error or rounding
    error_raised = False
    error_message = ""
    try:
        result = cursor.bulkcopy(
            table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
        )
        # If we get here, check if rounding occurred
        cursor.execute(f"SELECT id, value FROM {table_name} WHERE id = 2")
        rows = cursor.fetchall()
        if len(rows) > 0:
            # Rounding occurred - verify it was rounded correctly
            assert rows[0][1] == Decimal("201.00"), f"Expected rounding to 201.00, got: {rows[0][1]}"
            print(f"Numeric was rounded to fit scale: {rows[0][1]}")
    except (ValueError, RuntimeError) as e:
        error_raised = True
        error_message = str(e).lower()
        print(f"Expected error caught: {e}")

    # If error was raised, verify it's about scale overflow
    if error_raised:
        assert (
            "scale" in error_message or "exceeds" in error_message
        ), f"Expected scale error, got: {error_message}"
        assert (
            "numeric" in error_message
        ), f"Expected NUMERIC column type in error, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_negative_values(client_context):
    """Test cursor bulkcopy with negative numeric values.

    Tests that negative numeric values are correctly handled during bulk copy.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with numeric columns
    table_name = "BulkCopyNegativeNumericTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data with negative numeric values
    data = [
        (Decimal("-1.00"), Decimal("-100.5000")),
        (Decimal("-2.50"), Decimal("-200.7500")),
        (Decimal("3.99"), Decimal("-300.9999")),
        (Decimal("-4.75"), Decimal("400.1234")),
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
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == Decimal("-4.75") and rows[0][1] == Decimal("400.1234")
    assert rows[1][0] == Decimal("-2.50") and rows[1][1] == Decimal("-200.7500")
    assert rows[2][0] == Decimal("-1.00") and rows[2][1] == Decimal("-100.5000")
    assert rows[3][0] == Decimal("3.99") and rows[3][1] == Decimal("-300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_zero_values(client_context):
    """Test cursor bulkcopy with zero numeric values.

    Tests that zero numeric values with different representations are correctly handled.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with numeric columns
    table_name = "BulkCopyZeroNumericTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(10, 2), value NUMERIC(18, 4))")

    # Prepare test data with zero numeric values
    data = [
        (Decimal("0.00"), Decimal("0.0000")),
        (Decimal("0"), Decimal("0")),
        (0, 0.0),
        ("0.00", "0.0000"),
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

    # Verify data was inserted correctly - all should be zero
    cursor.execute(f"SELECT id, value FROM {table_name}")
    rows = cursor.fetchall()
    assert len(rows) == 4
    for row in rows:
        assert row[0] == Decimal("0.00")
        assert row[1] == Decimal("0.0000")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_numeric_large_precision_values(client_context):
    """Test cursor bulkcopy with large precision numeric values.

    Tests that numeric values with maximum precision are correctly handled.
    SQL Server NUMERIC supports up to 38 digits of precision.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with high precision numeric columns
    table_name = "BulkCopyLargePrecisionNumericTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id NUMERIC(38, 0), value NUMERIC(38, 10))")

    # Prepare test data with large precision values
    data = [
        (Decimal("12345678901234567890123456789012345678"), Decimal("1234567890123456789012345678.1234567890")),
        (Decimal("98765432109876543210987654321098765432"), Decimal("9876543210987654321098765432.9876543210")),
    ]

    # Execute bulk copy
    result = cursor.bulkcopy(
        table_name, iter(data), kwargs={"batch_size": 1000, "timeout": 30}
    )

    # Verify results
    assert result is not None
    assert result["rows_copied"] == 2
    assert result["batch_count"] == 1
    assert "elapsed_time" in result

    # Verify data was inserted correctly
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == Decimal("12345678901234567890123456789012345678")
    assert rows[0][1] == Decimal("1234567890123456789012345678.1234567890")
    assert rows[1][0] == Decimal("98765432109876543210987654321098765432")
    assert rows[1][1] == Decimal("9876543210987654321098765432.9876543210")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
