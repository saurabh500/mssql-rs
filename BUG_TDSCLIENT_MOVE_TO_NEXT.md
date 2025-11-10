# TdsClient Bug: move_to_next() Doesn't Properly Advance Through Multiple Result Sets

## Status
**OPEN** - Discovered during QueryResult to TdsClient API migration (November 10, 2025)

## Summary
When iterating through multiple result sets returned by a batch query, `TdsClient::move_to_next()` does not properly advance to the next result set after the first result set has been consumed. Instead, it either:
1. Returns `false` prematurely (indicating no more result sets)
2. Returns the same result set metadata/rows repeatedly

## Impact
- **Severity**: HIGH
- **Affected API**: TdsClient cursor-based API (ResultSet/ResultSetClient traits)
- **Workaround**: Full consumption of all rows in each result set before calling `move_to_next()`
- **Tests Affected**: 1 test ignored (`test_incomplete_result_set_iteration` in `mssql-tds/tests/query_results.rs`)

## Reproduction

### Test Case
File: `mssql-tds/tests/query_results.rs`
Test: `test_incomplete_result_set_iteration` (currently marked `#[ignore]`)

### Minimal Reproduction Code

```rust
use mssql_tds::connection::TdsClient;
use mssql_tds::connection::client::{ResultSet, ResultSetClient};

// Execute a batch query that returns multiple result sets
let query = "
    CREATE TABLE #temp1 (col INT);
    INSERT INTO #temp1 VALUES (1), (2), (3);
    
    SELECT col AS IntColumn FROM #temp1;
    
    CREATE TABLE #temp2 (col BIGINT);
    INSERT INTO #temp2 VALUES (100), (200);
    
    SELECT col AS BigIntColumn FROM #temp2;
    
    DROP TABLE #temp1;
    DROP TABLE #temp2;
";

let mut connection = TdsClient::connect(...).await?;
connection.execute(query).await?;

// First result set: SELECT col AS IntColumn FROM #temp1
// Expected: 3 rows with values 1, 2, 3
let has_result = connection.move_to_next().await?;
assert!(has_result); // ✅ PASSES

let metadata = connection.get_metadata();
assert_eq!(metadata[0].column_name, "IntColumn"); // ✅ PASSES

// Consume only PARTIAL rows (e.g., 2 out of 3)
let row1 = connection.next_row().await?.unwrap();
let row2 = connection.next_row().await?.unwrap();
// Skip row3

// Second result set: SELECT col AS BigIntColumn FROM #temp2
// Expected: 2 rows with values 100, 200
let has_result = connection.move_to_next().await?;
assert!(has_result); // ❌ FAILS - Returns false OR...

let metadata = connection.get_metadata();
assert_eq!(metadata[0].column_name, "BigIntColumn"); // ❌ FAILS - Still shows "IntColumn"

// If it doesn't fail above, row iteration shows same rows repeated
let row1 = connection.next_row().await?.unwrap();
// Expected: BigInt(100)
// Actual: Int(1) - same as first result set!
```

### Actual Behavior
1. **Scenario 1**: `move_to_next()` returns `false` after first partial consumption, claiming no more result sets exist
2. **Scenario 2**: `move_to_next()` returns `true` but `get_metadata()` returns the same metadata as the previous result set
3. **Scenario 3**: Rows from `next_row()` repeat the same data from the first result set instead of advancing

### Expected Behavior
After calling `move_to_next()`:
1. Should advance to the next ColMetadata token in the token stream
2. Should update internal state to point to the new result set
3. `get_metadata()` should return the new result set's column metadata
4. `next_row()` should return rows from the new result set
5. Partial consumption of the previous result set should not affect advancement

## Technical Details

### Affected Methods
- `TdsClient::move_to_next()` - Primary method that advances between result sets
- `TdsClient::move_to_column_metadata()` - Internal method called by `move_to_next()`
- `TdsClient::drain_rows()` - Called to skip remaining rows before advancing

### Code Location
File: `mssql-tds/src/connection/tds_client.rs`

#### Key Method (lines ~475-520)
```rust
pub async fn move_to_next(&mut self) -> Result<bool> {
    // ... validation checks ...
    
    // Drain remaining rows from current result set
    self.drain_rows().await?;
    
    // Attempt to advance to next ColMetadata token
    self.move_to_column_metadata().await
}
```

#### Suspected Issue
The `drain_rows()` → `move_to_column_metadata()` flow may not be correctly reading the token stream after partial consumption. Possible causes:

1. **Token Buffer State**: After partial row consumption, the internal token buffer may not be in the expected state when `drain_rows()` is called
2. **Done Token Handling**: The Done token that terminates a result set may not be properly consumed or checked
3. **State Machine**: The internal state machine tracking current position in the result set may not be reset correctly
4. **ColMetadata Search**: `move_to_column_metadata()` may not be reading enough tokens to find the next ColMetadata, or may be hitting an early return

### Related Code Patterns

#### Error Token Handling (FIXED in same migration)
Previously, `get_next_row()` had similar issues with unexpected tokens (Error tokens caused `unreachable!()`). This was fixed by adding proper Error token handling:

```rust
// In get_next_row() - lines 443-453
Tokens::Error(error_token) => {
    info!(?error_token);
    return Err(crate::error::Error::SqlServerError {
        message: error_token.message.clone(),
        state: error_token.state,
        class: error_token.severity as i32,
        number: error_token.number,
        server_name: Some(error_token.server_name.clone()),
        proc_name: Some(error_token.proc_name.clone()),
        line_number: Some(error_token.line_number as i32),
    });
}
```

Similar defensive coding may be needed in `move_to_column_metadata()`.

## Test Output

### Failing Test Output (before #[ignore])
```
Running test: test_incomplete_result_set_iteration
Expected metadata[0].column_name = "BigIntColumn"
Actual metadata[0].column_name = "IntColumn"

Expected rows: [Some(100), Some(200)]
Actual rows: [Some(1), Some(2), Some(3)]
```

### Successful Test Output (after workaround)
All other tests in `query_results.rs` pass when:
1. Full row consumption occurs before `move_to_next()`
2. No partial iteration happens
3. Auto-drain behavior of `move_to_next()` handles cleanup

```
Summary: 8 tests run: 8 passed, 1 skipped
```

## Investigation Steps

### Step 1: Add Debug Logging
Add trace logging to understand token stream state:

```rust
// In move_to_column_metadata()
tracing::debug!("Before drain_rows, state: {:?}", self.state);
self.drain_rows().await?;
tracing::debug!("After drain_rows, state: {:?}", self.state);

// In token reading loop
loop {
    let token = self.read_next_token().await?;
    tracing::debug!("Read token during move_to_column_metadata: {:?}", token);
    match token {
        // ... existing match arms
    }
}
```

### Step 2: Verify Token Stream
Create a minimal test that logs all tokens received for a multi-result batch:

```rust
// Log every token during execution
let query = "SELECT 1 AS First; SELECT 2 AS Second;";
connection.execute(query).await?;

// Enable trace logging and observe token sequence
// Expected: ColMetadata → Row → Done → ColMetadata → Row → Done
// Actual: ???
```

### Step 3: Check State Machine
Verify internal state variables:
- `current_result_set`: Is it being updated?
- `has_active_result_set`: Is it toggled correctly?
- Token buffer position: Is it advancing or stuck?

### Step 4: Test Different Scenarios
- Full consumption + move_to_next() ✅ WORKS
- Partial consumption (1 of 3 rows) + move_to_next() ❌ FAILS
- Partial consumption (2 of 3 rows) + move_to_next() ❌ FAILS
- Zero consumption (no next_row() calls) + move_to_next() ❓ TEST THIS
- Multiple result sets with DONE tokens only (no rows) ❓ TEST THIS

### Step 5: Compare with Working API
Look at how the deprecated `QueryResult` streaming API handled this:
- File: `mssql-tds/src/query/result.rs`
- The streaming implementation successfully handles multiple result sets
- Key differences in token stream handling?

## Workarounds

### Current Workaround
Fully consume all rows in a result set before calling `move_to_next()`:

```rust
// ✅ WORKS
let has_result = connection.move_to_next().await?;
while has_result {
    let metadata = connection.get_metadata();
    
    // Consume ALL rows
    while let Some(row) = connection.next_row().await? {
        // Process row
    }
    
    // Now safe to move to next result set
    has_result = connection.move_to_next().await?;
}
```

### Alternative Workaround
Use the deprecated `QueryResult` streaming API (not recommended as it's being removed):

```rust
let mut batch_result = connection.execute_query(query).await?;
while let Some(result_set) = batch_result.next_result_set().await? {
    // Works correctly with partial consumption
}
```

## Migration Status

### Tests Updated for Workaround
All tests migrated from QueryResult to TdsClient now use full consumption pattern:
- ✅ `tests/connectivity.rs`
- ✅ `tests/test_rpc_results.rs`
- ✅ `tests/timeout_and_cancel.rs`
- ✅ `tests/transaction.rs`
- ✅ `tests/test_client_transactions.rs`
- ✅ `tests/test_rpc_datatypes.rs`
- ✅ `tests/query_results.rs` (8 of 9 tests passing, 1 ignored for this bug)

### Integration Test Status
Full test suite: **172 tests passed, 8 skipped**
- All TdsClient-based tests work correctly with full consumption pattern
- Only 1 test (`test_incomplete_result_set_iteration`) exposes the partial consumption bug

## Priority & Next Steps

### Priority: HIGH
- Affects API usability
- Forces users to fully consume all rows (may not be desired behavior)
- Blocks complete migration from QueryResult to TdsClient

### Immediate Actions
1. ✅ Document bug with reproduction steps
2. ✅ Mark failing test with `#[ignore]` and TODO comments
3. ✅ Complete migration using full consumption workaround
4. 🔲 File GitHub issue with this documentation
5. 🔲 Add trace logging to affected methods
6. 🔲 Create minimal reproduction test case
7. 🔲 Debug `drain_rows()` → `move_to_column_metadata()` flow
8. 🔲 Fix root cause and validate with ignored test

### Fix Verification
Once fixed, validate by:
1. Un-ignore `test_incomplete_result_set_iteration`
2. Run: `cargo nextest run -E 'test(test_incomplete_result_set_iteration)'`
3. Expected: Test passes with partial row consumption
4. Run full test suite: `./dev/devtests.sh`
5. Expected: 173 tests passed, 7 skipped (one less than current)

## Related Work

### PR Context
- **Branch**: `dev/saurabh/deprecate-queryresult-api`
- **Base**: `development`
- **Previous PR**: #6410 (merged) - Fixed TdsClient timeout/cancel/EnvChange issues
- **Related Issues**: Migration from QueryResult streaming API to TdsClient cursor API

### Commit History
- Fixed TdsClient Error token handling (prevents unreachable!() panic during row iteration)
- Migrated all 8 test files to TdsClient API
- Added comprehensive error handling for SQL Server errors during result iteration
- Documented move_to_next() bug for future resolution

## Contact
For questions or updates on this bug, contact the author of the QueryResult → TdsClient migration work.

---
**Last Updated**: November 10, 2025
**Status**: Open, documented, workaround available
**Test File**: `mssql-tds/tests/query_results.rs:128-196` (test_incomplete_result_set_iteration)
