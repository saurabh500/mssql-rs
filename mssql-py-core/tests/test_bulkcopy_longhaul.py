# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Long-haul bulk copy test for continuous stress testing."""
import pytest
import mssql_py_core
import os
import time
from decimal import Decimal
from datetime import datetime, date, timedelta


def generate_wide_table_data(duration_seconds=1800, batch_size=1000):
    """
    Generator that produces rows of data for a wide table with various SQL Server datatypes.
    
    Args:
        duration_seconds: How long to generate data (default 30 minutes = 1800 seconds)
        batch_size: Number of rows to generate per batch
    
    Yields:
        Tuples with values for all columns in the wide table
    """
    start_time = time.time()
    row_id = 0
    
    while time.time() - start_time < duration_seconds:
        # Generate a batch of rows
        for _ in range(batch_size):
            row_id += 1
            
            # Generate data for each column with different SQL Server datatypes
            # SMALLMONEY range is -214748.3648 to 214748.3647, so we use modulo to stay in range
            smallmoney_val = (row_id % 214748) + (row_id % 100) / 100.0
            yield (
                row_id,                                          # INT (id column)
                row_id * 1000000000,                            # BIGINT
                row_id % 2 == 0,                                # BIT
                f"char_value_{row_id}",                         # VARCHAR(100)
                f"nchar_value_{row_id}",                        # NVARCHAR(100)
                Decimal(f"{row_id}.99"),                        # DECIMAL(18, 2)
                float(row_id) * 1.5,                            # FLOAT
                row_id * 0.25,                                  # REAL
                Decimal(f"{row_id}.50"),                        # MONEY
                Decimal(f"{smallmoney_val:.2f}"),               # SMALLMONEY (constrained to valid range)
                date.today() + timedelta(days=row_id % 365),   # DATE
                datetime.now() + timedelta(seconds=row_id),     # DATETIME
                datetime.now() + timedelta(seconds=row_id),     # DATETIME2
                datetime.now() + timedelta(seconds=row_id),     # DATETIMEOFFSET
                f"{row_id % 24:02d}:{row_id % 60:02d}:00",     # TIME (as string HH:MM:SS)
                bytes([row_id % 256] * 10),                     # VARBINARY(100)
                f'{{"id": {row_id}, "value": "test"}}',        # NVARCHAR for JSON data
            )


@pytest.mark.longhaul
@pytest.mark.integration
def test_cursor_bulkcopy_longhaul_wide_table(client_context):
    """
    Long-haul test for bulk copy with a wide table containing multiple SQL Server datatypes.
    
    This test runs for a configurable duration (default 30 minutes) and continuously
    bulk copies data to a wide table. It's designed for stress testing and verifying
    stability over extended periods.
    
    Duration can be configured via LONGHAUL_DURATION_SECONDS environment variable.
    """
    # Get test duration from environment variable or use default (30 minutes)
    duration_seconds = int(os.environ.get("LONGHAUL_DURATION_SECONDS", "1800"))
    batch_size = int(os.environ.get("LONGHAUL_BATCH_SIZE", "1000"))
    
    print(f"\nStarting long-haul BCP test for {duration_seconds} seconds ({duration_seconds/60:.1f} minutes)")
    print(f"Batch size: {batch_size} rows per batch")
    
    conn = mssql_py_core.PyCoreConnection(client_context)
    cursor = conn.cursor()

    # Create a wide test table with multiple SQL Server datatypes
    table_name = "BulkCopyLongHaulWideTable"
    cursor.execute(
        f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"
    )
    
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        id INT PRIMARY KEY,
        bigint_col BIGINT,
        bit_col BIT,
        varchar_col VARCHAR(100),
        nvarchar_col NVARCHAR(100),
        decimal_col DECIMAL(18, 2),
        float_col FLOAT,
        real_col REAL,
        money_col MONEY,
        smallmoney_col SMALLMONEY,
        date_col DATE,
        datetime_col DATETIME,
        datetime2_col DATETIME2,
        datetimeoffset_col DATETIMEOFFSET,
        time_col TIME,
        varbinary_col VARBINARY(100),
        json_col NVARCHAR(MAX)
    )
    """
    cursor.execute(create_table_sql)
    print(f"Created table {table_name} with 17 columns")

    # Execute bulk copy with the data generator
    start_time = time.time()
    data_generator = generate_wide_table_data(duration_seconds=duration_seconds, batch_size=batch_size)
    
    try:
        result = cursor.bulkcopy(
            table_name,
            data_generator,
            batch_size=batch_size,
            timeout=duration_seconds + 60,  # Add buffer to timeout
        )
        
        elapsed_time = time.time() - start_time
        
        # Verify results
        assert result is not None
        assert "rows_copied" in result
        assert "batch_count" in result
        assert "elapsed_time" in result
        
        rows_copied = result["rows_copied"]
        batch_count = result["batch_count"]
        
        print(f"\nLong-haul test completed successfully!")
        print(f"Duration: {elapsed_time:.2f} seconds ({elapsed_time/60:.1f} minutes)")
        print(f"Rows copied: {rows_copied:,}")
        print(f"Batches: {batch_count:,}")
        print(f"Throughput: {rows_copied/elapsed_time:.2f} rows/second")
        
        # Verify some data was actually inserted
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_result = cursor.fetchone()
        assert count_result[0] == rows_copied
        print(f"Verified row count in database: {count_result[0]:,}")
        
        # Spot check a few rows to ensure data integrity
        cursor.execute(f"SELECT TOP 5 id, varchar_col, decimal_col FROM {table_name} ORDER BY id")
        sample_rows = cursor.fetchall()
        print(f"\nSample rows from table:")
        for row in sample_rows:
            print(f"  ID: {row[0]}, VARCHAR: {row[1]}, DECIMAL: {row[2]}")
        
    finally:
        # Cleanup
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.close()
        print(f"\nCleaned up table {table_name}")
