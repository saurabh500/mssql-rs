# Feature Specification: mssql-rs Public API Crate

**Feature Branch**: `001-mssql-rs-public-api`  
**Created**: 2026-03-04  
**Status**: Draft  
**Input**: User description: "Expose a crate called mssql-rs which can take a connection string, run queries, has a type system which is extensible, can support high performance row and column streaming, but for ease of use expose a data structure which exposes all rows and columns as potentially a Vec<Vec<>> type."

## Clarifications

### Session 2026-03-04

- Q: What connection string format should the crate accept? → A: ODBC-style semicolon-delimited key=value pairs (e.g., `Server=localhost,1433;Database=mydb;User Id=sa;Password=...`). No `Driver=` keyword required.
- Q: What should happen when a Transaction is dropped without explicit commit/rollback? → A: Silent rollback with a `tracing::debug!` event (no panic, no warn).
- Q: Should the crate define its own error type or re-export mssql-tds errors? → A: Own error enum wrapping mssql-tds errors (decoupled semver).
- Q: Should the Value enum mirror every TDS wire type or use simplified logical groups? → A: Simplified enum — coalesce integer sizes, float sizes, date/time variants into logical groups.
- Q: What Rust API pattern should the streaming row reader use? → A: `Stream<Item = Result<Row>>` via `futures::Stream` trait (async iterator).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Connect and Execute a Simple Query (Priority: P1)

A developer adds `mssql-rs` as a dependency, provides a connection string, and executes a SQL query to retrieve all rows in a single convenient collection. The developer does not need to understand the TDS protocol internals, stream management, or token parsing.

**Why this priority**: Connection and simple query execution is the foundational capability. Without it, no other feature is useful.

**Independent Test**: Can be fully tested by connecting to a test SQL Server instance (or mock), running `SELECT 1 AS val`, and verifying the returned rows contain the expected value. Delivers: end-to-end proof that the crate works.

**Acceptance Scenarios**:

1. **Given** a valid connection string and a running SQL Server, **When** the developer calls the connect function with the connection string and then executes `SELECT 1 AS val`, **Then** the call returns a collection of rows where the first row's first column contains the integer value `1`.
2. **Given** a malformed connection string, **When** the developer calls the connect function, **Then** the call returns a descriptive error indicating which part of the connection string is invalid.
3. **Given** a valid connection string but an unreachable server, **When** the developer calls the connect function, **Then** the call returns a connection error within the configured timeout period.

---

### User Story 2 - High-Performance Row Streaming (Priority: P2)

A developer needs to process a large result set (millions of rows) without loading everything into memory. They iterate over rows one at a time using a streaming interface. Within each row, they iterate over columns one at a time without materializing the full row in memory. For large binary or text columns, they can additionally stream the column's raw data in chunks.

**Why this priority**: Streaming is the performance-critical path that distinguishes this driver from a toy implementation. Large result sets and large column values are common in production workloads.

**Independent Test**: Can be tested by executing a query that returns a known number of rows and consuming them one-by-one via the streaming interface without memory growing proportionally to row count. Column-by-column access can be tested by reading columns sequentially within each row. Binary streaming can be tested with a `varbinary(max)` column.

**Acceptance Scenarios**:

1. **Given** a query returning 100,000 rows, **When** the developer iterates using the streaming row reader, **Then** each row is yielded individually and peak memory usage stays constant (does not scale with total row count).
2. **Given** a query returning multiple result sets, **When** the developer finishes iterating one result set, **Then** they can advance to the next result set and iterate its rows.
3. **Given** a streaming iteration in progress, **When** the developer stops consuming rows early, **Then** the remaining rows are properly drained or the connection is left in a valid state for reuse.
4. **Given** a row with 5 columns, **When** the developer reads columns one at a time via the column iterator, **Then** each column value is yielded individually without loading all 5 column values into memory simultaneously.
5. **Given** a row containing a `varbinary(max)` column with 10 MB of data, **When** the developer opens a binary stream on that column, **Then** the data is yielded in chunks. (Note: The initial implementation MAY buffer the column internally and yield synthetic chunks; true zero-copy PLP streaming is deferred per FR-017.)

---

### User Story 3 - Extensible Type System (Priority: P3)

A developer wants to convert SQL column values into their own application types. They implement a conversion trait on their types so that row values can be extracted directly as the target type, avoiding manual matching against a value enum.

**Why this priority**: Type-safe extraction reduces boilerplate and eliminates runtime type-mismatch bugs. It builds on the query functionality from P1/P2.

**Independent Test**: Can be tested by defining a custom struct, implementing the conversion trait, and extracting a row's columns directly into that struct from a query result.

**Acceptance Scenarios**:

1. **Given** a query returning an integer column, **When** the developer extracts the value using the conversion trait for the standard integer type, **Then** the extraction succeeds and returns the correct value.
2. **Given** a query returning a nullable column with a NULL value, **When** the developer extracts the value into an `Option<T>`, **Then** the extraction returns `None`.
3. **Given** a query returning a string column, **When** the developer attempts to extract the value as an integer type, **Then** the extraction returns a descriptive type-mismatch error.
4. **Given** a user-defined struct, **When** the developer implements the conversion trait for that struct, **Then** rows can be extracted directly into instances of that struct.

---

### User Story 4 - Collect All Results as Vec<Vec<Value>> (Priority: P4)

A developer wants a quick, convenient way to get all rows and columns as a two-dimensional collection without streaming. This is the "just give me the data" convenience API for small-to-medium result sets.

**Why this priority**: Provides the simplest possible API surface for common use cases. Depends on the streaming infrastructure from P2 internally but exposes a simpler interface.

**Independent Test**: Can be tested by executing a query with known data and verifying the returned `Vec<Vec<Value>>` matches expected dimensions and contents.

**Acceptance Scenarios**:

1. **Given** a query returning 3 rows of 2 columns each, **When** the developer calls the collect-all-rows function, **Then** the return value is a collection with 3 entries, each containing 2 column values.
2. **Given** a query returning zero rows, **When** the developer calls the collect-all-rows function, **Then** the return value is an empty collection.
3. **Given** a query returning columns of mixed types (integer, string, NULL), **When** the developer calls the collect-all-rows function, **Then** each cell contains the correct value variant.

---

### User Story 5 - Parameterized Queries (Priority: P5)

A developer wants to execute queries with parameters to prevent injection and improve plan cache reuse on the server. They pass parameter values alongside the query text.

**Why this priority**: Parameterized queries are essential for any production workload. Ranked after core query and streaming because the execute path must be established first.

**Independent Test**: Can be tested by executing a parameterized `SELECT @p1 + @p2` query and verifying the result matches the sum of the supplied parameter values.

**Acceptance Scenarios**:

1. **Given** a query with named parameters and corresponding values, **When** the developer executes the query, **Then** the server receives the parameters via sp_executesql and returns the correct result.
2. **Given** a parameter value that contains SQL injection patterns (e.g., `'; DROP TABLE --`), **When** the developer executes the parameterized query, **Then** the value is safely transmitted as a parameter (not interpolated into the query text).

---

### User Story 6 - Prepared Statements (Priority: P6)

A developer wants to prepare a SQL statement once and execute it multiple times with different parameter values. This avoids repeated query parsing and plan compilation on the server, improving throughput for repeated operations (e.g., batch inserts, repeated lookups).

**Why this priority**: Prepared statements are a performance optimization that builds on parameterized queries (P5). The execute path and parameter binding must be established first.

**Independent Test**: Can be tested by preparing a `SELECT @p1 * @p2` statement, executing it three times with different parameter pairs, verifying each result is correct, and then unpreparing the statement.

**Acceptance Scenarios**:

1. **Given** a SQL statement with parameters, **When** the developer prepares it, **Then** a handle is returned that can be used for subsequent executions without re-sending the query text.
2. **Given** a prepared statement handle, **When** the developer executes it with different parameter values multiple times, **Then** each execution returns the correct result.
3. **Given** a prepared statement handle, **When** the developer unprepares it (or drops the handle), **Then** the server-side prepared statement resources are released.
4. **Given** a prepared statement handle, **When** the developer attempts to execute it after unpreparing, **Then** a clear error is returned.

---

### User Story 7 - Transaction Control (Priority: P7)

A developer wants explicit control over transaction boundaries — begin, commit, and rollback — to group multiple statements into an atomic unit of work. The developer may also need to set the isolation level for the transaction.

**Why this priority**: Transactions are essential for data integrity in any non-trivial application. Ranked after prepared statements because the query and parameter path must be fully established first.

**Independent Test**: Can be tested by beginning a transaction, inserting a row, rolling back, and verifying the row is not present. Then repeating with commit and verifying the row persists.

**Acceptance Scenarios**:

1. **Given** an open connection, **When** the developer begins a transaction, executes an INSERT, and commits, **Then** the inserted data is visible to subsequent queries.
2. **Given** an open transaction with a pending INSERT, **When** the developer rolls back, **Then** the inserted data is not persisted.
3. **Given** an open connection, **When** the developer begins a transaction with a specific isolation level (e.g., Snapshot, ReadCommitted), **Then** the transaction uses that isolation level on the server.
4. **Given** an open transaction, **When** the transaction handle is dropped without commit/rollback, **Then** the transaction is silently rolled back and a `tracing::debug!` event is emitted (no panic, no warning).

---

### Edge Cases

- What happens when the connection drops mid-stream while iterating rows? The streaming interface must return an I/O error and leave the client in a state where the developer can reconnect.
- What happens when a query returns more columns than expected metadata? The driver must surface a protocol error rather than panicking.
- How does the system handle encoding of non-UTF-8 string data from the server (e.g., columns with non-Unicode collations)? The type system must represent the server's collation-aware strings faithfully.
- What happens when a conversion trait extraction is attempted on a column index that is out of bounds? A clear error indicating the invalid index must be returned.
- How does the collect-all-rows convenience API behave when the result set is very large (e.g., millions of rows)? It should succeed (memory permitting) but the streaming API should be recommended in documentation for such cases.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The crate MUST expose a public function to establish a connection using an ODBC-style semicolon-delimited connection string (key=value pairs). Standard keys include `Server`, `Database`, `User Id`, `Password`, `Encrypt`, `TrustServerCertificate`, `Connection Timeout`, `Query Timeout`, `Application Name`, `Packet Size`, `Authentication`, `Access Token`. No `Driver=` keyword is required or accepted. Unknown keys MUST produce an error. Values containing semicolons or equals signs MUST be wrapped in braces per the ODBC specification (e.g., `Password={my;pass=word}`).
- **FR-002**: The crate MUST support executing ad-hoc SQL queries on an established connection and returning results. For DDL statements (e.g., `CREATE TABLE`, `ALTER`, `DROP`) and other non-result-set-producing commands, the query method returns an empty `ResultSet` (zero rows, no metadata).
- **FR-003**: The crate MUST provide two levels of streaming: (a) a row-level streaming reader that implements `futures::Stream<Item = Result<Row>>`, yielding rows one at a time without buffering the entire result set; and (b) a column-level iterator within each row that yields column values one at a time without materializing all columns simultaneously. This enables idiomatic composition with `StreamExt` combinators.
- **FR-004**: The crate MUST provide a convenience function that collects all rows from a query result into a `Vec<Vec<Value>>` (or equivalent two-dimensional collection).
- **FR-005**: The crate MUST define a simplified value enum that coalesces TDS wire-level types into logical groups (e.g., all integer sizes → `Int(i64)`, all float sizes → `Float(f64)`, date/time variants → a single `DateTime` struct). The enum MUST cover integers, floats, strings, binary, date/time, decimal, UUID, XML, JSON, vector, bit/bool, and NULL.
- **FR-006**: The crate MUST expose a conversion trait that allows users to extract column values into standard Rust types (`i32`, `i64`, `f64`, `String`, `bool`, `Vec<u8>`, etc.).
- **FR-007**: The conversion trait MUST be implementable by users on their own types to enable custom deserialization from column values.
- **FR-008**: The crate MUST support `Option<T>` extraction from nullable columns, returning `None` for SQL NULL.
- **FR-009**: The crate MUST support parameterized queries where parameter values are sent separately from the query text.
- **FR-010**: The crate MUST support iterating over multiple result sets from a single query batch.
- **FR-011**: The crate MUST expose column metadata (name, data type, nullability) alongside result set data.
- **FR-012**: The crate MUST define its own public error enum that wraps `mssql-tds` errors, providing a decoupled semver boundary. Error variants MUST be typed and descriptive (not opaque strings). The `mssql-tds` error types MUST NOT appear in the public API surface.
- **FR-013**: The crate MUST support connection timeout and query timeout configuration via connection string keys (`Connection Timeout` and `Query Timeout`, both in seconds). Connection timeout governs TCP connect and TDS login handshake. Query timeout governs individual query execution; when exceeded, the query is cancelled and `Error::Timeout` is returned.
- **FR-014**: The crate MUST support cancellation of in-flight queries.
- **FR-015**: The crate MUST support preparing a SQL statement, executing it multiple times with different parameters, and unpreparing it. The prepare/execute/unprepare lifecycle MUST map to sp_prepare/sp_execute/sp_unprepare on the server.
- **FR-016**: The crate MUST support explicit transaction control: begin, commit, and rollback. The begin operation MUST accept an optional isolation level (ReadUncommitted, ReadCommitted, RepeatableRead, Serializable, Snapshot).
- **FR-018**: Within an active transaction, the crate MUST support executing queries (`query`, `query_with_params`) and preparing statements (`prepare`) that participate in the transaction scope.
- **FR-017**: For large data types (`varchar(max)`, `nvarchar(max)`, `varbinary(max)`, `xml`), the crate SHOULD support opening a binary stream on an individual column that yields data in chunks via `futures::Stream<Item = Result<Bytes>>` (or equivalent) without buffering the entire column value in memory. The initial implementation MAY buffer internally; true zero-copy streaming requires `mssql-tds` PLP decoder changes and is deferred (see research decision R4).

### Key Entities

- **Client**: The primary handle to a SQL Server connection. Created from a connection string. Owns the underlying TDS transport and exposes query execution methods.
- **Value**: A simplified enum representing SQL Server column values with coalesced logical groups (e.g., `Int(i64)`, `Float(f64)`, `String(String)`, `Binary(Vec<u8>)`, `DateTime(DateTime)`, `Decimal(BigDecimal)`, `Uuid(Uuid)`, `Xml(String)`, `Json(String)`, `Vector(Vec<f32>)`, `Bool(bool)`, `Null`). Maps multiple TDS wire-level types into ergonomic variants. This is the unit cell in the `Vec<Vec<Value>>` convenience API.
- **Row**: A streaming cursor over the columns in a single result row. Yields column values one at a time without loading the full row into memory. Provides indexed and trait-based access to column values. For large columns (`max` types, `xml`), provides a binary stream reader that yields data in chunks.
- **ColumnMetadata**: Description of a result column — name, data type, nullability, collation — available before row data is read.
- **ResultSet**: A handle to one result set within a query batch. Provides streaming row access and metadata. Supports advancing to the next result set.
- **FromValue trait**: A conversion trait that extracts a `Value` into a concrete Rust type. Implementable by users for custom types.
- **PreparedStatement**: A server-side prepared statement handle returned by the prepare operation. Holds the handle ID and parameter metadata. Can be executed multiple times and must be unprepared (or auto-unprepared on drop) to release server resources.
- **Transaction**: A handle representing an active transaction. Provides commit and rollback methods. Created via the Client with an optional isolation level. If dropped without explicit commit or rollback, the transaction is silently rolled back with a `tracing::debug!` event emitted.

## Assumptions

- The crate delegates all protocol-level work to `mssql-tds`; it is a thin, ergonomic wrapper — not a reimplementation.
- Authentication methods (SQL auth, integrated/Kerberos, AAD) are inherited from the underlying `mssql-tds` connection provider; the public API does not add new auth mechanisms.
- The crate targets async-first usage; a synchronous blocking wrapper is out of scope for this specification.
- Connection pooling is out of scope; the crate provides single-connection semantics.
- The `Vec<Vec<Value>>` convenience API loads all data into memory and is not intended for unbounded result sets.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer can go from `cargo add mssql-rs` to executing a query and reading results in under 10 lines of application code (excluding imports and error handling).
- **SC-002**: Streaming 1 million rows through the row reader completes with constant memory overhead (memory usage does not grow proportionally to row count).
- **SC-003**: The `Vec<Vec<Value>>` convenience API returns correct results for queries spanning all supported SQL Server data types (int, bigint, varchar, nvarchar, datetime2, decimal, uniqueidentifier, varbinary, xml, json, bit, null, etc.).
- **SC-004**: User-implemented conversion traits successfully extract column values into custom Rust types in tests.
- **SC-005**: All public API types and functions have doc comments with usage examples that compile and pass as doc-tests.
- **SC-006**: The crate adds no measurable overhead (< 5% latency regression) compared to using `mssql-tds` directly for equivalent operations.
