# Rust TdsClient API Improvement Proposal

## Date: November 10, 2025

## Problem Statement

The current Rust `TdsClient` API has a bug where `move_to_next()` fails when called after partial row consumption. However, the JavaScript API using the **exact same underlying TdsClient** works perfectly because it uses a safer access pattern.

### Current Rust Pattern (Unsafe):
```rust
// Direct cursor manipulation - UNSAFE with partial consumption
let resultset = client.get_current_resultset().unwrap();
while let Some(row) = resultset.next_row().await? {
    // Process rows
}
// User must manually call move_to_next()
client.move_to_next().await?;
```

### JavaScript Pattern (Safe):
```javascript
// Higher-level wrapper - SAFE by design
let next_row = await connection.nextRowInResultset();
if (!next_row) {
    // Automatically advances when rows exhausted
    await connection.nextResultSet();
}
```

## Root Cause Analysis

The JavaScript API is safe because:
1. **No direct cursor access** - Users never get a `ResultSet` reference
2. **Automatic advancement** - `nextResultSet()` is only called when `nextRowInResultset()` returns null
3. **Stateful wrapper** - The connection tracks position and advances automatically

The Rust API is unsafe because:
1. **Direct cursor exposure** - `get_current_resultset()` returns raw `ResultSet`
2. **Manual advancement** - Users must call `move_to_next()` themselves
3. **No safety checks** - Nothing prevents partial consumption + early move

## Proposed Solution: Add Safe Iterator API

### Option 1: Iterator-Based API (Recommended)

Add a safe, high-level API that matches JavaScript's behavior:

```rust
impl TdsClient {
    /// Returns an iterator over all result sets in the query.
    /// Automatically advances to next result set when current one is exhausted.
    pub fn result_sets(&mut self) -> ResultSetIterator<'_> {
        ResultSetIterator { client: self, done: false }
    }
    
    /// Returns an iterator over all rows across all result sets.
    /// Flattens multiple result sets into a single row stream.
    pub fn rows(&mut self) -> RowIterator<'_> {
        RowIterator { 
            client: self, 
            current_result: None,
            done: false 
        }
    }
}

pub struct ResultSetIterator<'a> {
    client: &'a mut TdsClient,
    done: bool,
}

impl<'a> ResultSetIterator<'a> {
    /// Get metadata for current result set
    pub fn metadata(&self) -> Option<&[ColumnMetadata]> {
        self.client.get_current_resultset()
            .map(|rs| rs.get_metadata())
    }
    
    /// Get next row in current result set
    /// Returns None when current result set is exhausted
    pub async fn next_row(&mut self) -> TdsResult<Option<Vec<ColumnValues>>> {
        if self.done {
            return Ok(None);
        }
        
        if let Some(resultset) = self.client.get_current_resultset() {
            return resultset.next_row().await;
        }
        
        Ok(None)
    }
    
    /// Advance to next result set
    /// Returns true if there is another result set
    pub async fn next_result_set(&mut self) -> TdsResult<bool> {
        if self.done {
            return Ok(false);
        }
        
        let has_next = self.client.move_to_next().await?;
        self.done = !has_next;
        Ok(has_next)
    }
}

pub struct RowIterator<'a> {
    client: &'a mut TdsClient,
    current_result: Option<Vec<ColumnMetadata>>,
    done: bool,
}

impl<'a> RowIterator<'a> {
    /// Get next row from any result set, automatically advancing between result sets
    pub async fn next(&mut self) -> TdsResult<Option<(Vec<ColumnMetadata>, Vec<ColumnValues>)>> {
        if self.done {
            return Ok(None);
        }
        
        loop {
            if let Some(resultset) = self.client.get_current_resultset() {
                // Update metadata if changed
                if self.current_result.is_none() {
                    self.current_result = Some(resultset.get_metadata().to_vec());
                }
                
                // Try to get next row in current result set
                if let Some(row) = resultset.next_row().await? {
                    return Ok(Some((self.current_result.clone().unwrap(), row)));
                }
            }
            
            // Current result set exhausted, try to advance
            if !self.client.move_to_next().await? {
                self.done = true;
                return Ok(None);
            }
            
            // Reset metadata for new result set
            self.current_result = None;
        }
    }
}
```

### Usage Examples:

#### Example 1: Iterate through result sets
```rust
let mut client = TdsClient::connect(context).await?;
client.execute("SELECT 1; SELECT 2; SELECT 3", None, None).await?;

let mut results = client.result_sets();

// First result set
while let Some(row) = results.next_row().await? {
    println!("Result 1: {:?}", row);
}

// Second result set
results.next_result_set().await?;
while let Some(row) = results.next_row().await? {
    println!("Result 2: {:?}", row);
}

// Third result set
results.next_result_set().await?;
while let Some(row) = results.next_row().await? {
    println!("Result 3: {:?}", row);
}

client.close_query().await?;
```

#### Example 2: Flatten all rows (JavaScript-like)
```rust
let mut client = TdsClient::connect(context).await?;
client.execute("SELECT 1; SELECT 2; SELECT 3", None, None).await?;

let mut rows = client.rows();
while let Some((metadata, row)) = rows.next().await? {
    println!("Metadata: {:?}", metadata);
    println!("Row: {:?}", row);
}

client.close_query().await?;
```

#### Example 3: Safe partial consumption (No bug!)
```rust
let mut client = TdsClient::connect(context).await?;
client.execute("SELECT 1, 2, 3; SELECT 'a', 'b'", None, None).await?;

let mut results = client.result_sets();

// Partially consume first result set (only 2 of 3 rows)
let row1 = results.next_row().await?;
let row2 = results.next_row().await?;
// Skip row3

// Advance to next result set - SAFE!
results.next_result_set().await?;  // Internally drains remaining rows

// Process second result set
while let Some(row) = results.next_row().await? {
    println!("Result 2: {:?}", row);
}

client.close_query().await?;
```

### Option 2: Fix move_to_next() to Auto-Drain (Alternative)

Instead of new APIs, fix `move_to_next()` to automatically drain remaining rows:

```rust
impl TdsClient {
    pub async fn move_to_next(&mut self) -> TdsResult<bool> {
        // Automatically drain any remaining rows in current result set
        if let Some(resultset) = self.get_current_resultset() {
            while resultset.next_row().await?.is_some() {
                // Drain remaining rows
            }
        }
        
        // Now safe to move to next result set
        self.move_to_column_metadata().await
    }
}
```

**Pros**: 
- Minimal API change
- Fixes the bug transparently
- All existing code works

**Cons**:
- Hidden behavior (auto-draining might be unexpected)
- Performance cost for users who want to skip large result sets
- Less explicit than iterator API

## Recommendation

**Use Option 1 (Iterator API) as the primary solution:**

1. ✅ **Explicit and clear** - Users know exactly what's happening
2. ✅ **Matches JavaScript API** - Consistent experience across languages
3. ✅ **Prevents bugs by design** - Can't misuse because cursor is wrapped
4. ✅ **Flexible** - Users can choose between result-set iteration or flat row iteration
5. ✅ **Backward compatible** - Keep existing low-level API for power users

**Also implement Option 2 as a safety fix:**
- Fix `move_to_next()` to auto-drain even for low-level API users
- Document that partial consumption is supported
- Warn in docs that auto-draining has performance implications

## Migration Path

### Phase 1: Add new safe APIs
```rust
// NEW: Safe iterator APIs
pub fn result_sets(&mut self) -> ResultSetIterator<'_>
pub fn rows(&mut self) -> RowIterator<'_>
```

### Phase 2: Update test helpers to use safe APIs
```rust
// In tests/common/mod.rs
pub async fn validate_results(
    client: &mut TdsClient,
    expected_results: &[ExpectedQueryResultType],
) -> TdsResult<()> {
    let mut results = client.result_sets();
    
    for expected in expected_results {
        match expected {
            ExpectedQueryResultType::Result(expected_row_count) => {
                let mut actual_rows = 0;
                while let Some(row) = results.next_row().await? {
                    actual_rows += 1;
                }
                assert_eq!(actual_rows, *expected_row_count);
            }
            ExpectedQueryResultType::Update(_) => {
                // Drain any rows
                while results.next_row().await?.is_some() {}
            }
        }
        results.next_result_set().await?;
    }
    
    Ok(())
}
```

### Phase 3: Fix low-level API
```rust
// Fix move_to_next() to auto-drain
impl TdsClient {
    pub async fn move_to_next(&mut self) -> TdsResult<bool> {
        // Auto-drain remaining rows for safety
        if let Some(resultset) = self.get_current_resultset() {
            while resultset.next_row().await?.is_some() {}
        }
        self.move_to_column_metadata().await
    }
}
```

### Phase 4: Update documentation
- Mark direct cursor API as "advanced usage"
- Recommend iterator APIs for most use cases
- Document auto-draining behavior

## Benefits

1. **Safety**: Bug impossible with new APIs
2. **Consistency**: Matches JavaScript API design
3. **Ergonomics**: Easier to use correctly than low-level cursors
4. **Performance**: No overhead - same underlying calls
5. **Backward compatibility**: Existing code continues to work

## Test Coverage

Update the ignored test to use new API:

```rust
#[tokio::test]
async fn test_incomplete_result_set_iteration() {
    let mut connection = begin_connection(create_context()).await;
    
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
    
    connection.execute(query, None, None).await.unwrap();
    
    let mut results = connection.result_sets();
    
    // First result set: partial consumption (2 of 3 rows)
    results.next_result_set().await.unwrap();
    assert_eq!(results.metadata().unwrap()[0].column_name, "IntColumn");
    
    let row1 = results.next_row().await.unwrap().unwrap();
    let row2 = results.next_row().await.unwrap().unwrap();
    // Skip row3 intentionally
    
    // Second result set: SAFE! Auto-drains row3
    results.next_result_set().await.unwrap();
    assert_eq!(results.metadata().unwrap()[0].column_name, "BigIntColumn");
    
    let row1 = results.next_row().await.unwrap().unwrap();
    assert_eq!(row1[0], ColumnValues::BigInt(100));
    
    let row2 = results.next_row().await.unwrap().unwrap();
    assert_eq!(row2[0], ColumnValues::BigInt(200));
    
    connection.close_query().await.unwrap();
}
```

## Conclusion

The Rust API should mirror the JavaScript API's safety by providing iterator-based abstractions that handle result set advancement automatically. This makes the bug **impossible** rather than just **fixable**.

The low-level cursor API should also be fixed to auto-drain, but users should be steered toward the safer iterator APIs through documentation and examples.

---

**Priority**: HIGH - Provides both immediate safety and long-term API improvement
**Effort**: MEDIUM - ~200 lines of new code, test updates, documentation
**Risk**: LOW - Backward compatible, additive changes
