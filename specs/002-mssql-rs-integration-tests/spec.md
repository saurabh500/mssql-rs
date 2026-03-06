# Feature Specification: mssql-rs Integration Tests

**Feature Branch**: `002-mssql-rs-integration-tests`  
**Created**: 2026-03-06  
**Status**: Draft  
**Input**: User description: "Create integration tests for the mssql-rs public API crate, covering connection, queries, streaming, type system, parameterized queries, prepared statements, and transactions. Based on the implementation plan from 001-mssql-rs-public-api."

## Clarifications

### Session 2026-03-06

- Q: Which backend should server-dependent integration tests connect to? → A: Live SQL Server only (env-gated, skip when no server available). The mock server cannot validate real SQL type coalescing, sp_executesql, transaction isolation, or sp_prepare/sp_unprepare.
- Q: Should pure-logic tests (connection string parsing, FromValue conversions) be inline unit tests or integration test files? → A: All tests in `mssql-rs/tests/` (both pure-logic and server-dependent). Pure-logic tests are placed alongside server-dependent tests for unified test organization.
- Q: What environment variable name should gate server-dependent tests? → A: `MSSQL_RS_TEST_CONNECTION_STRING`. If set, its value is the full connection string and server-dependent tests run. If absent, they are skipped.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Connection and Basic Query Tests (Priority: P1)

A contributor opens the `mssql-rs` crate, runs the test suite, and gets immediate confidence that the core connect-and-query path works. Tests validate connection string parsing (valid, malformed, unknown keys, brace-quoted values), successful connection to a SQL Server instance, and execution of a simple `SELECT` that returns the expected value. This is the foundation — if connection tests fail, nothing else matters.

**Why this priority**: Connection is the prerequisite for every other feature. A failing connection test immediately signals a broken driver. These tests also validate the ODBC connection string parser which is the sole entry point into the crate.

**Independent Test**: Can be fully tested by connecting to a test SQL Server (or mock), running `SELECT 1 AS val` via `query_collect`, and asserting the result is `[[Value::Int(1)]]`. Connection string parser tests can run without any server.

**Acceptance Scenarios**:

1. **Given** a valid ODBC connection string and a running SQL Server, **When** the test calls `Client::connect()` and then `query_collect("SELECT 1 AS val")`, **Then** the result is a single row containing `Value::Int(1)`.
2. **Given** a connection string missing the `Server` key, **When** the test calls `Client::connect()`, **Then** the call returns `Error::ConnectionStringInvalid`.
3. **Given** a connection string with an unknown key (e.g., `Bogus=true`), **When** the test calls `Client::connect()`, **Then** the call returns `Error::ConnectionStringInvalid` identifying the unknown key.
4. **Given** a connection string with a brace-quoted password containing semicolons and equals signs (e.g., `Password={my;pass=word}`), **When** the test calls `Client::connect()`, **Then** the connection succeeds (parser handles brace quoting correctly).
5. **Given** a connection string pointing to an unreachable server, **When** the test calls `Client::connect()`, **Then** the call returns an error within a bounded time (respecting `Connection Timeout`).
6. **Given** a successful connection, **When** the test calls `Client::close()`, **Then** the call completes without error and subsequent operations on a new client are unaffected.
7. **Given** a DDL statement (e.g., `CREATE TABLE #temp (id INT)`), **When** the test executes it via `query_collect`, **Then** the result is an empty `Vec` (zero rows, no error).

---

### User Story 2 - Row Streaming and Multiple Result Sets (Priority: P2)

A contributor verifies that the `ResultSet` streaming interface correctly yields rows one at a time and supports advancing through multiple result sets. Tests confirm that `futures::Stream` iteration works, that `next_result_set()` correctly transitions between result sets, and that dropping a `ResultSet` mid-iteration does not corrupt the connection.

**Why this priority**: Streaming is the performance-critical path. If row-by-row iteration is broken, the driver is unusable for production workloads. Multi-result-set support is critical for stored procedure results and batched queries.

**Independent Test**: Can be tested by executing a query returning a known number of rows, consuming them via `StreamExt::next()`, and counting. Multi-result tested with `SELECT 1; SELECT 2` batch.

**Acceptance Scenarios**:

1. **Given** a query returning 100 rows, **When** the test iterates via `StreamExt::next()`, **Then** exactly 100 rows are yielded, each with the expected values.
2. **Given** a batch query `SELECT 1 AS a; SELECT 2 AS b`, **When** the test consumes the first result set and calls `next_result_set()`, **Then** the second result set is returned with a row containing `Value::Int(2)`.
3. **Given** a batch returning 3 result sets, **When** the test calls `next_result_set()` after the third set, **Then** `None` is returned.
4. **Given** a streaming iteration with rows remaining, **When** the test drops the `ResultSet`, **Then** the client remains usable for subsequent queries (connection is not corrupted).
5. **Given** a query returning zero rows, **When** the test calls `StreamExt::next()`, **Then** `None` is returned immediately.
6. **Given** a `ResultSet`, **When** the test calls `collect_rows()`, **Then** all rows are drained into a `Vec<Vec<Value>>` matching the expected data.

---

### User Story 3 - Type System and Value Conversion (Priority: P3)

A contributor verifies that SQL Server data types are correctly coalesced into `Value` variants and that the `FromValue` trait correctly extracts typed values from rows. Tests cover all 13 `Value` variants, `NULL` handling via `Option<T>`, type mismatch errors, and narrowing conversion overflow errors.

**Why this priority**: The type system is the developer-facing contract. Incorrect coalescing or conversion silently corrupts application data. Every `row.get::<T>(index)` call depends on this being correct.

**Independent Test**: Can be tested by executing queries that return columns of each SQL type, extracting them via `row.get::<T>()`, and asserting correctness. NULL handling tested with `SELECT NULL AS x`.

**Acceptance Scenarios**:

1. **Given** a query returning an `INT` column with value `42`, **When** the test calls `row.get::<i64>(0)`, **Then** the result is `42i64`.
2. **Given** a query returning a `FLOAT` column with value `3.14`, **When** the test calls `row.get::<f64>(0)`, **Then** the result is approximately `3.14`.
3. **Given** a query returning an `NVARCHAR` column with value `'hello'`, **When** the test calls `row.get::<String>(0)`, **Then** the result is `"hello"`.
4. **Given** a query returning a `VARBINARY` column, **When** the test calls `row.get::<Vec<u8>>(0)`, **Then** the result contains the expected bytes.
5. **Given** a query returning a `UNIQUEIDENTIFIER` column, **When** the test calls `row.get::<uuid::Uuid>(0)`, **Then** the result matches the expected UUID.
6. **Given** a query returning a `DECIMAL` column, **When** the test calls `row.get::<BigDecimal>(0)`, **Then** the result preserves the correct precision and scale.
7. **Given** a query returning a `DATETIME2` column, **When** the test calls `row.get::<DateTime>(0)`, **Then** the year, month, day, hour, minute, second, and nanoseconds fields are populated correctly.
8. **Given** a query returning a `BIT` column with value `1`, **When** the test calls `row.get::<bool>(0)`, **Then** the result is `true`.
9. **Given** a query returning a `NULL` column, **When** the test calls `row.get::<Option<String>>(0)`, **Then** the result is `None`.
10. **Given** a query returning a `NULL` column, **When** the test calls `row.get::<String>(0)` (non-optional), **Then** the result is `Error::TypeConversion`.
11. **Given** a query returning a `VARCHAR` column, **When** the test calls `row.get::<i64>(0)`, **Then** the result is `Error::TypeConversion` describing the mismatch.
12. **Given** a query returning `BIGINT` with value `i64::MAX`, **When** the test calls `row.get::<i32>(0)`, **Then** the result is `Error::TypeConversion` describing the range overflow.
13. **Given** a query returning columns of mixed types in a single row, **When** the test calls `row.value(index)` for each column, **Then** each returns the correct `Value` variant.

---

### User Story 4 - Column Metadata and Row Access Patterns (Priority: P4)

A contributor verifies that column metadata (name, type, nullability) is correctly exposed, and that both random-access (`get`, `get_by_name`, `value`) and sequential (`next_column`) row access modes work correctly.

**Why this priority**: Metadata drives schema-aware applications. The dual access mode is a core design decision — both paths must work correctly and the mutual-exclusivity contract must hold.

**Independent Test**: Can be tested by executing a query with known column definitions, inspecting `ResultSet::metadata()`, and accessing columns via both index and name.

**Acceptance Scenarios**:

1. **Given** a query `SELECT 1 AS id, 'alice' AS name`, **When** the test inspects `result_set.metadata()`, **Then** it contains 2 entries: column `id` with `DataType::Int` and column `name` with a string `DataType`.
2. **Given** a row with columns `[1, 'hello']`, **When** the test calls `row.get_by_name::<i64>("id")`, **Then** the result is `1i64` (case-insensitive name lookup).
3. **Given** a row with columns `[1, 'hello']`, **When** the test calls `row.get_by_name::<i64>("ID")` (different case), **Then** the result is `1i64`.
4. **Given** a row, **When** the test calls `row.get_by_name::<i64>("nonexistent")`, **Then** the result is an error.
5. **Given** a row with 3 columns, **When** the test calls `row.get::<i64>(5)` (out of bounds), **Then** the result is an error.
6. **Given** a row with 3 columns, **When** the test calls `next_column()` four times, **Then** the first three return `Some(Value)` and the fourth returns `None`.
7. **Given** a row, **When** the test calls `into_values()`, **Then** the result is a `Vec<Value>` containing all column values.
8. **Given** a row, **When** the test calls `len()` and `is_empty()`, **Then** the results match the column count.

---

### User Story 5 - Parameterized Queries (Priority: P5)

A contributor verifies that parameterized queries correctly transmit parameters via sp_executesql, preventing SQL injection and supporting all `Value` types as parameters. Tests confirm named parameter binding, `NULL` parameters, and server-side error reporting for mismatched parameter counts.

**Why this priority**: Parameterized queries are essential for production safety (injection prevention) and performance (plan cache reuse). They extend the basic query path with parameter binding.

**Independent Test**: Can be tested by executing `SELECT @p1 + @p2` with integer parameters and verifying the result equals the sum.

**Acceptance Scenarios**:

1. **Given** a parameterized query `SELECT @p1 + @p2 AS result` with `@p1 = 10` and `@p2 = 20`, **When** the test calls `query_collect_with_params`, **Then** the result is `[[Value::Int(30)]]`.
2. **Given** a parameterized query with a string parameter containing SQL injection patterns (e.g., `'; DROP TABLE --`), **When** the test executes the query, **Then** the string is returned verbatim as data (not interpreted as SQL).
3. **Given** a parameterized query with a `Value::Null` parameter, **When** the test executes the query, **Then** the `NULL` is correctly transmitted and the query succeeds.
4. **Given** a parameterized query with parameters of different types (Int, String, Float, Bool, Decimal, Uuid, DateTime, Binary), **When** the test executes the query, **Then** each parameter is correctly transmitted and the returned values match.
5. **Given** a parameterized query expecting 2 parameters but only 1 provided, **When** the test executes the query, **Then** the server returns an error surfaced as `Error::QueryFailed`.
6. **Given** a parameterized query, **When** the test calls `query_with_params` (streaming variant), **Then** the result can be iterated via `StreamExt::next()` yielding the correct rows.

---

### User Story 6 - Prepared Statement Lifecycle (Priority: P6)

A contributor verifies the full prepare/execute/close lifecycle. Tests confirm that a statement can be prepared once and executed multiple times with different parameters, that closing releases server resources, and that the deferred unprepare mechanism works when a `PreparedStatement` is dropped without explicit `close()`.

**Why this priority**: Prepared statements are a performance optimization for repeated queries. The deferred cleanup pattern (Drop → pending_unprepare) is a non-trivial design decision that must be validated.

**Independent Test**: Can be tested by preparing `SELECT @p1 * @p2`, executing three times with different parameters, verifying each result, and then closing.

**Acceptance Scenarios**:

1. **Given** a prepared statement for `SELECT @p1 * @p2`, **When** the test executes it with `[Value::Int(3), Value::Int(7)]`, **Then** the result is `[[Value::Int(21)]]`.
2. **Given** a prepared statement, **When** the test executes it three times with different parameters, **Then** each execution returns the correct result.
3. **Given** a prepared statement, **When** the test calls `close()`, **Then** the call completes without error.
4. **Given** a prepared statement that is dropped without calling `close()`, **When** the test performs a subsequent query on the same client, **Then** the query succeeds (deferred unprepare is executed transparently).
5. **Given** a prepared statement, **When** the test executes it with streaming (`StreamExt::next()`), **Then** rows are yielded correctly.

---

### User Story 7 - Transaction Control (Priority: P7)

A contributor verifies that transactions correctly scope data modifications — committed changes persist, rolled-back changes do not. Tests cover begin/commit, begin/rollback, isolation level setting, query and parameterized query execution within a transaction, and the deferred rollback on drop behavior.

**Why this priority**: Transactions are essential for data integrity. The deferred rollback on Drop is a safety net that must work correctly to prevent data corruption from leaked transactions.

**Independent Test**: Can be tested by beginning a transaction, inserting a row into a temp table, rolling back, and verifying the row is absent. Then repeating with commit and verifying persistence.

**Acceptance Scenarios**:

1. **Given** an active transaction, **When** the test inserts a row and commits, **Then** the row is visible to subsequent queries outside the transaction.
2. **Given** an active transaction, **When** the test inserts a row and rolls back, **Then** the row is not visible to subsequent queries.
3. **Given** a transaction begun with `IsolationLevel::ReadUncommitted`, **When** the test queries within that transaction, **Then** the transaction operates at the specified isolation level.
4. **Given** a transaction begun with `IsolationLevel::Snapshot`, **When** the test queries within that transaction, **Then** the transaction operates at snapshot isolation (requires database-level snapshot support).
5. **Given** a `Transaction` that is dropped without explicit commit or rollback, **When** the test performs a subsequent query on the same client, **Then** the pending rollback is executed transparently and the client remains usable.
6. **Given** an active transaction, **When** the test executes a parameterized query within it via `txn.query_with_params()`, **Then** the query succeeds and participates in the transaction scope.
7. **Given** an active transaction, **When** the test prepares and executes a statement within it via `txn.prepare()`, **Then** the prepared statement participates in the transaction scope.

---

### Edge Cases

- What happens when a test executes a query on a client whose connection was dropped by the server? The next operation returns `Error::Io` or `Error::ConnectionFailed`.
- What happens when a test attempts to use a client after an I/O error? Subsequent operations fail; a new `Client` must be constructed.
- What happens when a query returns a zero-column result set (e.g., `SET NOCOUNT ON` followed by a statement)? The `ResultSet` has empty metadata and yields zero rows.
- What happens when deferred cleanup (pending rollback or pending unprepare) fails on the next operation? The cleanup error is returned from that operation; the intended operation does not execute.
- What happens when `ResultSet::next_result_set()` is called without consuming the current result set? Remaining rows are drained and the next set is returned.
- What happens when a query returns very wide rows (e.g., 100 columns of mixed types)? All columns are accessible by index and by name, with correct metadata.
- What happens when Unicode strings with supplementary characters (emoji, CJK) are round-tripped through parameters and results? The string is preserved exactly.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The test suite MUST validate all 7 user stories from the mssql-rs public API specification (001-mssql-rs-public-api) with at least one test per acceptance scenario.
- **FR-002**: Tests requiring a live SQL Server MUST be gated by the `MSSQL_RS_TEST_CONNECTION_STRING` environment variable. When set, its value is used as the full ODBC connection string; when absent, server-dependent tests are skipped. Tests that only exercise pure logic (e.g., connection string parsing, FromValue conversions) MUST run without any server. Both pure-logic and server-dependent tests reside in `mssql-rs/tests/`.
- **FR-003**: The test suite MUST use the project's existing test runner (`cargo nextest` via `cargo btest`) and produce results compatible with CI (JUnit XML, 5-retry profile).
- **FR-004**: The test suite MUST place all test files (both pure-logic and server-dependent) in the `mssql-rs/tests/` directory. This keeps all feature-level test coverage in a single location for unified organization and discovery.
- **FR-005**: Each test file MUST correspond to a logical test group aligned with the user stories: connection, query and collection, streaming, type system, parameterized queries, prepared statements, and transactions.
- **FR-006**: Tests MUST assert specific error variants (e.g., `Error::ConnectionStringInvalid`, `Error::TypeConversion`) rather than generic "is error" checks, to verify the error classification contract.
- **FR-007**: Tests MUST cover the `FromValue` conversion trait for all 16 built-in implementations: `bool`, `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `f32`, `f64`, `String`, `Vec<u8>`, `BigDecimal`, `uuid::Uuid`, `DateTime`, and `Option<T>`.
- **FR-008**: Tests MUST verify that the `Value` enum correctly coalesces TDS wire types into the 13 logical variants (Null, Bool, Int, Float, Decimal, String, Binary, DateTime, Uuid, Xml, Json, Vector).
- **FR-009**: Tests MUST verify parameterized query injection safety by passing SQL injection patterns as parameter values and confirming they are treated as literal data.
- **FR-010**: Tests MUST verify the deferred cleanup mechanisms: (a) `PreparedStatement` Drop triggers pending unprepare on next Client operation, and (b) `Transaction` Drop triggers pending rollback on next Client operation.
- **FR-011**: Tests MUST verify multi-result-set navigation via `ResultSet::next_result_set()` with batched queries returning 2+ result sets.
- **FR-012**: Tests MUST verify column metadata correctness: column names, data types, nullability, and the `DataType` enum variants match the query schema.
- **FR-013**: Tests MUST verify that `Client::cancel()` interrupts an in-flight query.
- **FR-014**: Tests MUST verify that Connection Timeout and Query Timeout configurations from the connection string are respected.
- **FR-015**: Tests MUST compile and pass with zero warnings under `cargo clippy --all-features --all-targets -- -D warnings`.

### Key Entities

- **Test Group**: A file in `mssql-rs/tests/` that exercises one user story's acceptance scenarios. Each group is independently runnable.
- **Test Helper**: Shared utilities (connection setup, environment loading) used across test groups to avoid duplication.
- **Environment Gate**: An environment variable check that skips server-dependent tests when no database is available, allowing pure-logic tests to always run.

## Assumptions

- A live SQL Server is required for server-dependent integration tests. The connection string is provided via the `MSSQL_RS_TEST_CONNECTION_STRING` environment variable (or loaded from `.env` via `dotenv`). Tests are skipped when this variable is absent.
- The `mssql-mock-tds` crate is not used for integration tests. Real SQL Server behavior (type coalescing, sp_executesql parameter transmission, transaction isolation semantics, sp_prepare/sp_unprepare) cannot be validated against a mock.
- Tests do not require a specific SQL Server version; they use standard T-SQL that works on SQL Server 2016+.
- Snapshot isolation tests require the target database to have `ALLOW_SNAPSHOT_ISOLATION ON` enabled. If not available, those specific tests should be skippable.
- Tests use temporary tables (`#temp`) or transactions with rollback for data isolation, ensuring tests do not leave persistent state in the database.
- The test suite targets the same platforms as `mssql-rs`: Linux, Windows, and macOS.
- Test execution time for the full suite should remain under 60 seconds when run against a local SQL Server instance.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Every acceptance scenario from all 7 user stories in the original specification has at least one corresponding test.
- **SC-002**: The test suite achieves 80%+ code coverage of the `mssql-rs` crate (measured via `cargo-llvm-cov`), excluding unreachable error paths in the `mssql-tds` layer.
- **SC-003**: All tests pass with zero warnings under `cargo btest` and `cargo bclippy`.
- **SC-004**: Pure-logic tests (connection string parsing, Value coalescing, FromValue conversions) can run without any SQL Server, enabling execution in any CI environment.
- **SC-005**: Server-dependent tests complete within 30 seconds when run against a local SQL Server instance.
- **SC-006**: No test leaves persistent state — all data modifications are contained in temporary tables or rolled-back transactions.
