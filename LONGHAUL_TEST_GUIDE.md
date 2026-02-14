# Long-Haul BCP Test - Implementation Guide

## What Was Implemented

This implementation adds a 30-minute long-haul stress test for SQL Server bulk copy operations, addressing work item #42220.

## Changes Made

### 1. New Test File: `mssql-py-core/tests/test_bulkcopy_longhaul.py`

**Purpose**: Long-running stress test for BCP operations with a wide table

**Key Features**:
- Generator function `generate_wide_table_data()` that produces test data on-demand
- Wide table with 17 columns covering diverse SQL Server datatypes
- Configurable test duration via `LONGHAUL_DURATION_SECONDS` (default: 1800 seconds = 30 minutes)
- Configurable batch size via `LONGHAUL_BATCH_SIZE` (default: 1000 rows)
- Comprehensive validation and performance metrics

**Datatypes Tested**:
- Integers: INT, BIGINT
- Boolean: BIT
- Strings: VARCHAR(100), NVARCHAR(100), NVARCHAR(MAX)
- Decimals: DECIMAL(18,2), FLOAT, REAL, MONEY, SMALLMONEY
- Date/Time: DATE, DATETIME, DATETIME2, DATETIMEOFFSET, TIME
- Binary: VARBINARY(100)

**Test Markers**: `@pytest.mark.longhaul` and `@pytest.mark.integration`

### 2. New Pipeline Template: `.pipeline/templates/test-longhaul-template.yml`

**Purpose**: Azure DevOps pipeline template for running long-haul tests

**What It Does**:
1. Sets up Docker and pulls Ubuntu 22.04 build image
2. Starts SQL Server 2025 in a Docker container
3. Builds mssql-py-core from source inside Ubuntu container
4. Runs only tests marked with `@pytest.mark.longhaul`
5. Publishes JUnit test results
6. Cleans up Docker resources

**Parameters**:
- `durationSeconds` (default: 1800) - Test duration
- `batchSize` (default: 1000) - Rows per batch

### 3. Updated: `mssql-py-core/pytest.ini`

**Changes**:
- Added `longhaul` marker registration
- Allows tests to be marked with `@pytest.mark.longhaul`

### 4. Updated: `dev/test-python.sh`

**Changes**:
- Normal runs now exclude longhaul tests using `-m "not longhaul"`
- Both unit-only mode and full test mode exclude longhaul tests
- Prevents long-running tests from blocking regular CI/CD

### 5. Updated: `.pipeline/validation-pipeline.yml`

**Changes**:
- Added `RunLongHaul` parameter (boolean, default: false)
- Added `LongHaul` stage that:
  - Runs independently (no dependencies on build stage)
  - Only executes when `RunLongHaul=true`
  - Uses Ubuntu container on RUST-1ES-POOL-WUS3 pool
- Updated all existing stage conditions to exclude when `RunLongHaul=true`
  - Prevents running build + longhaul simultaneously

## How to Use

### Running Locally

1. Start a SQL Server instance
2. Set environment variables:
   ```bash
   export SQL_PASSWORD="<your-password-here>"
   export SQL_SERVER="localhost"
   export LONGHAUL_DURATION_SECONDS=60  # 1 minute for testing
   export LONGHAUL_BATCH_SIZE=1000
   ```
3. Run the test:
   ```bash
   cd mssql-py-core
   source ../.venv-pycore/bin/activate
   pytest tests/test_bulkcopy_longhaul.py -v
   ```

### Running in Azure Pipeline

1. Navigate to the validation pipeline in Azure DevOps
2. Click "Run pipeline"
3. Set `RunLongHaul` parameter to `true`
4. Set other parameters as needed (buildType can be left as default)
5. Run the pipeline
6. The LongHaul stage will run for 30 minutes

### Excluding from Normal Runs

Longhaul tests are automatically excluded from:
- Regular `./dev/test-python.sh` runs
- PR validation builds (unless RunLongHaul=true)
- Merge validation builds (unless RunLongHaul=true)

## Test Architecture

### Data Generation Flow
```
generate_wide_table_data()
  ↓
Yields tuples with 17 values
  ↓
cursor.bulkcopy() consumes generator
  ↓
Batches sent to SQL Server
  ↓
Continues until duration elapsed
```

### Generator Efficiency
- Data generated on-demand (lazy evaluation)
- No memory pre-allocation for all rows
- Efficient for long-running tests
- Can generate millions of rows without memory issues

### Performance Metrics
The test outputs:
- Total duration
- Rows copied
- Batch count
- Throughput (rows/second)
- Sample data verification

## Design Decisions

1. **Why a generator?**
   - Memory efficient for long-running tests
   - Can produce unlimited data without pre-allocation
   - Natural fit for streaming BCP operations

2. **Why 17 columns?**
   - Covers most common SQL Server datatypes
   - Wide enough to stress-test the BCP protocol
   - Representative of real-world scenarios

3. **Why separate pipeline stage?**
   - Long-running tests shouldn't block normal CI/CD
   - Can be run on-demand or scheduled separately
   - Uses same infrastructure as other tests

4. **Why exclude from normal runs?**
   - 30 minutes is too long for PR validation
   - Developers need fast feedback
   - Longhaul tests are for stress/stability, not correctness

## Troubleshooting

### Test times out
- Check `LONGHAUL_DURATION_SECONDS` is set correctly
- Verify timeout parameter in `cursor.bulkcopy()` is sufficient
- Default timeout is `duration_seconds + 60` for buffer

### Generator produces wrong data types
- Verify SQL Server column types match generator output
- Check decimal precision/scale matches
- Ensure datetime objects are properly formatted

### Pipeline fails to start
- Verify `RunLongHaul` parameter is set correctly
- Check Docker and SQL Server container logs
- Ensure build image is accessible

## Future Enhancements

Possible improvements:
- Add more datatypes (XML, GEOGRAPHY, HIERARCHYID)
- Support for NULL value distribution
- Configurable column count
- Performance regression detection
- Multiple table sizes (narrow, medium, wide, ultra-wide)
- Parallel BCP operations

## Related Files

- Test implementation: `mssql-py-core/tests/test_bulkcopy_longhaul.py`
- Pipeline template: `.pipeline/templates/test-longhaul-template.yml`
- Pipeline config: `.pipeline/validation-pipeline.yml`
- Test configuration: `mssql-py-core/pytest.ini`
- Test runner: `dev/test-python.sh`
