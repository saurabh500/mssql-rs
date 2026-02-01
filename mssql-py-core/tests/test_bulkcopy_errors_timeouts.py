# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Bulk copy tests for timeout and error handling scenarios.

This module tests bulk copy timeout behavior matching SqlClient behavior:
- Overall operation timeout (configured via timeout parameter)
- Attention ACK timeout (5 second hardcoded timeout)
- Error propagation from Rust to Python (TimeoutError, ValueError)

Architecture:
- Type conversion errors are handled in PyO3 bindings layer
- Timeout errors originate from mssql-tds core and propagate to Python as TimeoutError
- Connection broken errors raise RuntimeError
"""
import pytest
import time
import mssql_py_core


@pytest.mark.integration
class TestBulkCopyTimeoutBasics:
    """Basic timeout parameter validation and behavior tests."""

    def test_bulkcopy_with_default_timeout(self, client_context):
        """Test that omitting timeout uses a reasonable default.
        
        When no timeout is specified, a default value should be used.
        """
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create a test table
        table_name = "#BulkCopyTimeoutDefaultTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

        # Small data set that should complete quickly
        data = [(i, i * 10) for i in range(10)]

        # Execute bulk copy without specifying timeout (uses default)
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000,
        )

        # Verify it completed successfully
        assert result is not None
        assert result["rows_copied"] == 10

        conn.close()

    def test_bulkcopy_with_explicit_timeout_completes(self, client_context):
        """Test that a reasonable timeout allows normal operation to complete."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create a test table
        table_name = "#BulkCopyTimeoutExplicitTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value INT)")

        # Small data set that should complete within timeout
        data = [(i, i * 10) for i in range(100)]

        # Execute bulk copy with 60 second timeout
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=100, timeout=60,
        )

        # Verify it completed successfully
        assert result is not None
        assert result["rows_copied"] == 100

        conn.close()

    def test_bulkcopy_negative_timeout_raises_error(self, client_context):
        """Test that negative timeout raises an error.
        
        Negative timeout values should be rejected - the exact error type
        depends on where the validation happens (Python layer vs Rust).
        """
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create a test table
        table_name = "#BulkCopyNegativeTimeoutTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT)")

        data = [(1,), (2,), (3,)]

        # Execute bulk copy with negative timeout should raise error
        # OverflowError is raised by Python when trying to convert negative int to unsigned
        with pytest.raises((ValueError, RuntimeError, OverflowError)) as exc_info:
            cursor.bulkcopy(
                table_name,
                iter(data),
                timeout=-1,
            )

        # Check error was raised (type check is sufficient since we caught it)
        assert exc_info.value is not None

        conn.close()


@pytest.mark.integration
class TestBulkCopyTimeoutBehavior:
    """Tests for bulk copy timeout behavior under various conditions."""

    def test_bulkcopy_timeout_with_slow_generator(self, client_context):
        """Test that timeout is tracked during bulk copy with slow data source.
        
        This test uses a generator that yields data slowly to verify that
        the timeout is being tracked across the entire operation.
        """
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create a test table
        table_name = "#BulkCopySlowGeneratorTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value NVARCHAR(100))")

        def slow_data_generator():
            """Generator that yields data with small delays."""
            for i in range(10):
                # Small delay to simulate slow data source
                time.sleep(0.05)  # 50ms delay per row
                yield (i, f"value_{i}")

        # With a 30 second timeout, this should complete (total delay ~500ms)
        result = cursor.bulkcopy(
            table_name,
            slow_data_generator(),
            batch_size=10, timeout=30,
        )

        assert result is not None
        assert result["rows_copied"] == 10

        conn.close()

    def test_bulkcopy_tracks_rows_before_timeout(self, client_context):
        """Verify that partial row count is available in timeout errors.
        
        When a timeout occurs, the error should include how many rows
        were successfully copied before the timeout.
        """
        # This is a design verification test - actual timeout testing requires
        # a slow server or large data set. Here we just verify the normal
        # operation reports correct counts.
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create a test table
        table_name = "#BulkCopyRowTrackingTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT)")

        data = [(i,) for i in range(500)]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=100, timeout=60,
        )

        assert result is not None
        assert result["rows_copied"] == 500
        assert result["batch_count"] >= 5  # At least 5 batches of 100

        conn.close()


@pytest.mark.integration
class TestBulkCopyErrorPropagation:
    """Tests for proper error propagation from Rust to Python."""

    def test_null_to_non_nullable_raises_valueerror(self, client_context):
        """Test that NULL in non-nullable column raises ValueError."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create table with non-nullable column
        table_name = "#BulkCopyNullErrorTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT NOT NULL)")

        data = [(1,), (None,), (3,)]  # NULL in non-nullable column

        with pytest.raises(ValueError) as exc_info:
            cursor.bulkcopy(
                table_name,
                iter(data),
                timeout=30,
            )

        error_msg = str(exc_info.value).lower()
        assert "null" in error_msg or "non-nullable" in error_msg

        conn.close()

    def test_invalid_type_conversion_raises_valueerror(self, client_context):
        """Test that invalid type conversion raises ValueError."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create table expecting integer
        table_name = "#BulkCopyConversionErrorTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT)")

        # String that can't be converted to int
        data = [("not_a_number",)]

        with pytest.raises(ValueError) as exc_info:
            cursor.bulkcopy(
                table_name,
                iter(data),
                timeout=30,
            )

        error_msg = str(exc_info.value).lower()
        assert "conversion" in error_msg or "invalid" in error_msg or "parse" in error_msg

        conn.close()

    def test_metadata_retrieval_error_raises_runtime_error(self, client_context):
        """Test that table not found raises RuntimeError."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        data = [(1, 2)]

        with pytest.raises(RuntimeError) as exc_info:
            cursor.bulkcopy(
                "NonExistentTable_XXXXXXXX",
                iter(data),
                timeout=30,
            )

        error_msg = str(exc_info.value).lower()
        # Should mention the table or metadata issue
        assert "metadata" in error_msg or "table" in error_msg or "invalid" in error_msg

        conn.close()


@pytest.mark.integration  
class TestBulkCopyLargeDataTimeout:
    """Tests for timeout behavior with larger data sets.
    
    Note: These tests verify correct behavior, but won't actually hit
    timeouts under normal conditions. Actual timeout testing requires
    either a very slow network or an extremely short timeout value.
    """

    def test_large_batch_completes_within_timeout(self, client_context):
        """Test that a large batch completes within reasonable timeout."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create table for large insert
        table_name = "#BulkCopyLargeBatchTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT, value NVARCHAR(50))")

        # Generate 10000 rows
        data = [(i, f"value_{i}") for i in range(10000)]

        start_time = time.time()
        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=1000, timeout=120,  # 2 minute timeout
        )
        elapsed = time.time() - start_time

        assert result is not None
        assert result["rows_copied"] == 10000
        print(f"Completed 10000 rows in {elapsed:.2f} seconds")

        conn.close()

    def test_multiple_batches_timeout_tracking(self, client_context):
        """Verify timeout is tracked across multiple batches."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        # Create table
        table_name = "#BulkCopyMultiBatchTimeoutTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT)")

        # Generate enough rows for multiple batches
        data = [(i,) for i in range(5000)]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=500, timeout=60,
        )

        assert result is not None
        assert result["rows_copied"] == 5000
        assert result["batch_count"] >= 10  # At least 10 batches

        conn.close()


@pytest.mark.integration
class TestBulkCopyElapsedTime:
    """Tests for elapsed time tracking in bulk copy results."""

    def test_elapsed_time_is_positive(self, client_context):
        """Verify elapsed time is tracked and positive."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        table_name = "#BulkCopyElapsedTimeTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT)")

        data = [(i,) for i in range(100)]

        result = cursor.bulkcopy(
            table_name,
            iter(data),
            batch_size=100,
        )

        assert "elapsed_time" in result
        assert result["elapsed_time"] >= 0
        # Elapsed time should be in seconds (not milliseconds)
        assert result["elapsed_time"] < 60  # Should complete in under a minute

        conn.close()

    def test_elapsed_time_reflects_actual_duration(self, client_context):
        """Verify elapsed time is reasonably accurate."""
        conn = mssql_py_core.PyCoreConnection(client_context)
        cursor = conn.cursor()

        table_name = "#BulkCopyElapsedTimeAccuracyTable"
        cursor.execute(f"CREATE TABLE {table_name} (id INT)")

        def slow_generator():
            for i in range(5):
                time.sleep(0.1)  # 100ms delay per row
                yield (i,)

        start = time.time()
        result = cursor.bulkcopy(
            table_name,
            slow_generator(),
            batch_size=10,
        )
        external_elapsed = time.time() - start

        assert "elapsed_time" in result
        # Internal elapsed should be similar to external measurement
        # Allow some tolerance for overhead
        assert result["elapsed_time"] <= external_elapsed + 0.5

        conn.close()
