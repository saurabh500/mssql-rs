# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for MONEY and SMALLMONEY data types."""
import pytest
import mssql_py_core
from decimal import Decimal


@pytest.mark.integration
def test_cursor_bulkcopy_money_basic(client_context):
    """Test cursor bulkcopy method with MONEY and SMALLMONEY columns and explicit mappings."""
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyTestTableMoney"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data - MONEY has 4 decimal places, SMALLMONEY has 4 decimal places
    data = [
        (Decimal("1000.5000"), Decimal("100.5000")),
        (Decimal("2000.7500"), Decimal("200.7500")),
        (Decimal("3000.9999"), Decimal("300.9999")),
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
            ],
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
    assert rows[0][0] == Decimal("1000.5000") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2000.7500") and rows[1][1] == Decimal("200.7500")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_auto_mapping(client_context):
    """Test cursor bulkcopy with automatic column mapping for MONEY types.

    Tests bulkcopy when no mappings are specified, including NULL value handling.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with nullable MONEY and SMALLMONEY columns
    table_name = "BulkCopyAutoMapTableMoney"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data with NULL values
    data = [
        (Decimal("1000.5000"), Decimal("100.5000")),
        (Decimal("2000.7500"), None),  # NULL value in second column
        (None, Decimal("300.9999")),  # NULL value in first column
        (Decimal("4000.1234"), Decimal("400.1234")),
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
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY COALESCE(id, 999999)")
    rows = cursor.fetchall()
    assert len(rows) == 4
    assert rows[0][0] == Decimal("1000.5000") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2000.7500") and rows[1][1] is None
    assert rows[2][0] == Decimal("4000.1234") and rows[2][1] == Decimal("400.1234")
    assert rows[3][0] is None and rows[3][1] == Decimal("300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_string_to_money_conversion(client_context):
    """Test cursor bulkcopy with string values that should convert to MONEY columns.

    Tests type coercion when source data contains numeric strings but
    destination columns are MONEY/SMALLMONEY type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyStringToMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data - strings containing valid money values
    data = [
        ("1000.5000", "100.5000"),
        ("2000.7500", "200.7500"),
        ("3000.9999", "300.9999"),
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

    # Verify data was inserted correctly and converted to money
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1000.5000") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2000.7500") and rows[1][1] == Decimal("200.7500")
    assert rows[2][0] == Decimal("3000.9999") and rows[2][1] == Decimal("300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_integer_to_money_conversion(client_context):
    """Test cursor bulkcopy with integer values that should convert to MONEY columns.

    Tests type coercion when source data contains integers but
    destination columns are MONEY/SMALLMONEY type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyIntToMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data - integers that should convert to money
    data = [
        (1000, 100),
        (2000, 200),
        (3000, 300),
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

    # Verify data was inserted correctly and converted to money
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1000.0000") and rows[0][1] == Decimal("100.0000")
    assert rows[1][0] == Decimal("2000.0000") and rows[1][1] == Decimal("200.0000")
    assert rows[2][0] == Decimal("3000.0000") and rows[2][1] == Decimal("300.0000")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_float_to_money_conversion(client_context):
    """Test cursor bulkcopy with float values that should convert to MONEY columns.

    Tests type coercion when source data contains floats but
    destination columns are MONEY/SMALLMONEY type.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyFloatToMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data - floats that should convert to money
    data = [
        (1000.25, 100.5),
        (2000.50, 200.75),
        (3000.99, 300.9999),
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

    # Verify data was inserted correctly and converted to money
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1000.2500") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2000.5000") and rows[1][1] == Decimal("200.7500")
    assert rows[2][0] == Decimal("3000.9900") and rows[2][1] == Decimal("300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_decimal_to_money_conversion(client_context):
    """Test cursor bulkcopy with Decimal values for MONEY columns.

    Tests that Python Decimal objects are properly converted to MONEY/SMALLMONEY.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyDecimalToMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data - Decimal values with various precisions
    data = [
        (Decimal("1000.5"), Decimal("100.5")),
        (Decimal("2000.75"), Decimal("200.75")),
        (Decimal("3000.9999"), Decimal("300.9999")),
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
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("1000.5000") and rows[0][1] == Decimal("100.5000")
    assert rows[1][0] == Decimal("2000.7500") and rows[1][1] == Decimal("200.7500")
    assert rows[2][0] == Decimal("3000.9999") and rows[2][1] == Decimal("300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_null_to_non_nullable_column(client_context):
    """Test cursor bulkcopy with null value for non-nullable MONEY column.

    Tests that the client-side metadata validation catches attempts to insert
    null into a non-nullable column and raises an appropriate conversion error.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with a single non-nullable MONEY column
    table_name = "#BulkCopyNonNullableTableMoney"
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY NOT NULL)")

    # Prepare test data with a null value
    data = [
        (Decimal("1000.00"),),
        (None,),  # This should trigger a conversion error
        (Decimal("3000.00"),),
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
def test_cursor_bulkcopy_money_invalid_string_to_money_conversion(client_context):
    """Test cursor bulkcopy with invalid string that cannot be converted to MONEY.

    Tests that client-side type coercion properly validates string-to-money conversion
    and fails with an appropriate error when the string is not a valid money value.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with MONEY columns
    table_name = "#BulkCopyInvalidStringTableMoney"
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data with invalid string that cannot be parsed as money
    data = [
        ("1000.00", "100.5000"),
        ("not_a_number", "200.7500"),  # This should trigger a conversion error
        ("3000.99", "300.9999"),
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
    ), "Expected a ValueError to be raised for invalid string-to-money conversion"
    assert (
        "failed to parse" in error_message or "conversion" in error_message
    ), f"Expected parse/conversion error message, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_smallmoney_range_overflow(client_context):
    """Test cursor bulkcopy with SMALLMONEY value exceeding range.

    SMALLMONEY has a range from -214,748.3648 to 214,748.3647.
    Values outside this range should be rejected during type coercion.

    Expected behavior:
    - Value within SMALLMONEY range converts successfully
    - Value exceeding SMALLMONEY range triggers validation error
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with SMALLMONEY column
    table_name = "#BulkCopySmallMoneyRangeOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value SMALLMONEY)")

    # Prepare test data with value exceeding SMALLMONEY range
    # SMALLMONEY max: 214,748.3647
    data = [
        (1, Decimal("100.50")),
        (2, Decimal("300000.00")),  # Exceeds SMALLMONEY max range
        (3, Decimal("300.99")),
    ]

    # Execute bulk copy and expect range validation error
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

    # Verify that an error was raised about range overflow
    assert (
        error_raised
    ), "Expected an error for SMALLMONEY value exceeding range"
    assert (
        "range" in error_message or "exceeds" in error_message or "overflow" in error_message
    ), f"Expected range error, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_range_overflow(client_context):
    """Test cursor bulkcopy with MONEY value exceeding range.

    MONEY has a range from -922,337,203,685,477.5808 to 922,337,203,685,477.5807.
    Values outside this range should be rejected during type coercion.

    Expected behavior:
    - Value within MONEY range converts successfully
    - Value exceeding MONEY range triggers validation error
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a temp table with MONEY column
    table_name = "#BulkCopyMoneyRangeOverflowTable"
    cursor.execute(f"CREATE TABLE {table_name} (id INT, value MONEY)")

    # Prepare test data with value exceeding MONEY range
    # MONEY max: 922,337,203,685,477.5807
    data = [
        (1, Decimal("1000000.50")),
        (2, Decimal("999999999999999.00")),  # Exceeds MONEY max range
        (3, Decimal("3000.99")),
    ]

    # Execute bulk copy and expect range validation error
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

    # Verify that an error was raised about range overflow
    assert (
        error_raised
    ), "Expected an error for MONEY value exceeding range"
    assert (
        "range" in error_message or "exceeds" in error_message or "overflow" in error_message
    ), f"Expected range error, got: {error_message}"

    # Close connection - temp table will be automatically dropped
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_negative_values(client_context):
    """Test cursor bulkcopy with negative MONEY values.

    Tests that negative money values are correctly handled during bulk copy.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyNegativeMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data with negative money values
    data = [
        (Decimal("-1000.5000"), Decimal("-100.5000")),
        (Decimal("-2000.7500"), Decimal("-200.7500")),
        (Decimal("3000.9999"), Decimal("-300.9999")),
        (Decimal("-4000.1234"), Decimal("400.1234")),
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
    assert rows[0][0] == Decimal("-4000.1234") and rows[0][1] == Decimal("400.1234")
    assert rows[1][0] == Decimal("-2000.7500") and rows[1][1] == Decimal("-200.7500")
    assert rows[2][0] == Decimal("-1000.5000") and rows[2][1] == Decimal("-100.5000")
    assert rows[3][0] == Decimal("3000.9999") and rows[3][1] == Decimal("-300.9999")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_zero_values(client_context):
    """Test cursor bulkcopy with zero MONEY values.

    Tests that zero money values with different representations are correctly handled.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyZeroMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data with zero money values
    data = [
        (Decimal("0.0000"), Decimal("0.0000")),
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
        assert row[0] == Decimal("0.0000")
        assert row[1] == Decimal("0.0000")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_precision_handling(client_context):
    """Test cursor bulkcopy with MONEY values having more than 4 decimal places.

    MONEY and SMALLMONEY types have fixed 4 decimal places. Values with more
    decimal places should be rounded or handled according to SQL Server behavior.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyMoneyPrecisionTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data with more than 4 decimal places
    data = [
        (Decimal("1000.12345"), Decimal("100.56789")),
        (Decimal("2000.999999"), Decimal("200.111111")),
        (Decimal("3000.00001"), Decimal("300.99999")),
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

    # Verify data was inserted with rounding to 4 decimal places
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    # SQL Server uses banker's rounding for MONEY types
    assert rows[0][0] == Decimal("1000.1235")  # Rounded from 1000.12345
    assert rows[0][1] == Decimal("100.5679")   # Rounded from 100.56789
    assert rows[1][0] == Decimal("2001.0000")  # Rounded from 2000.999999
    assert rows[1][1] == Decimal("200.1111")   # Rounded from 200.111111

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()


@pytest.mark.integration
def test_cursor_bulkcopy_money_large_values(client_context):
    """Test cursor bulkcopy with large MONEY values within valid range.

    Tests that large valid MONEY values are correctly handled.
    """
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a test table with MONEY and SMALLMONEY columns
    table_name = "BulkCopyLargeMoneyTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    cursor.execute(f"CREATE TABLE {table_name} (id MONEY, value SMALLMONEY)")

    # Prepare test data with large values within valid ranges
    # MONEY: -922,337,203,685,477.5808 to 922,337,203,685,477.5807
    # SMALLMONEY: -214,748.3648 to 214,748.3647
    data = [
        (Decimal("922337203685477.5807"), Decimal("214748.3647")),  # Max values
        (Decimal("-922337203685477.5808"), Decimal("-214748.3648")),  # Min values
        (Decimal("1000000000000.0000"), Decimal("1000.5000")),  # Large positive
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
    cursor.execute(f"SELECT id, value FROM {table_name} ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 3
    assert rows[0][0] == Decimal("-922337203685477.5808")
    assert rows[0][1] == Decimal("-214748.3648")
    assert rows[1][0] == Decimal("1000000000000.0000")
    assert rows[1][1] == Decimal("1000.5000")
    assert rows[2][0] == Decimal("922337203685477.5807")
    assert rows[2][1] == Decimal("214748.3647")

    # Cleanup
    cursor.execute(f"DROP TABLE {table_name}")
    conn.close()
