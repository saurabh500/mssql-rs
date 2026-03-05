# Research: mssql-rs Public API Crate

**Date**: 2026-03-05  
**Spec**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md)

## R1: Connection String Parsing

**Decision**: Implement an ODBC-style `key=value;` parser in `mssql-rs` that populates `ClientContext` fields and passes the `Server` value to `TdsConnectionProvider::create_client()` as the `datasource` argument.

**Rationale**: `mssql-tds` has no built-in ODBC connection string parser. The JS layer receives pre-parsed fields from Node.js; the Python layer extracts individual keys from a `PyDict`. Both layers manually map individual values onto `ClientContext` fields. The `mssql-rs` crate must own its own parser to fulfill FR-001.

**Alternatives considered**:
- *Re-use Python's `dict_to_client_context` logic* — rejected; it's Python-specific (PyDict), tightly coupled to PyO3, and handles auth transform inline. The Rust crate needs a standalone parser.
- *Add a shared parser to `mssql-tds`* — viable long-term but out of scope. The current FFI layers don't share parsing code. Adding a parser to `mssql-tds` would require updating all consumers. Better to start in `mssql-rs` and optionally refactor later.

**Key mapping (ODBC key → ClientContext field)**:

| Connection String Key | ClientContext Field | Notes |
|---|---|---|
| `Server` | `datasource` param to `create_client()` | Parsed by `ParsedDataSource::parse()` for protocol, server, port, instance |
| `Database` | `database` | |
| `User Id` / `UID` | `user_name` | |
| `Password` / `PWD` | `password` | |
| `Encrypt` | `encryption_options.mode` | yes/no/strict → Required/Optional/Strict |
| `TrustServerCertificate` | `encryption_options.trust_server_certificate` | yes/no → bool |
| `Connection Timeout` | `connect_timeout` | seconds |
| `Application Name` | `application_name` | |
| `Packet Size` | `packet_size` | |
| `Application Intent` | `application_intent` | ReadWrite/ReadOnly |
| `MultiSubnetFailover` | `multi_subnet_failover` | yes/no → bool |
| `Authentication` | `tds_authentication_method` | Use `validate_auth()` and `transform_auth()` from `mssql-tds` |
| `Access Token` | `access_token` | |

**Implementation notes**:
- Keys are case-insensitive, values preserve case.
- Semicolons separate pairs; values containing semicolons/equals must be wrapped in `{braces}` (ODBC spec).
- Unknown keys produce a descriptive error (not silently ignored).
- Auth validation reuses `mssql_tds::connection::odbc_authentication_validator::validate_auth()` and `odbc_authentication_transformer::transform_auth()`.

## R2: Value Enum Coalescing

**Decision**: Map the 25 `ColumnValues` variants into ~11 `Value` variants by coalescing integer sizes, float sizes, and date/time types.

**Rationale**: The spec requires a simplified enum (clarification Q4). Users don't care whether the wire sent `TinyInt(u8)` vs `BigInt(i64)` — they want a single integer variant with the widest representation.

**Mapping**:

| ColumnValues variant(s) | Value variant | Representation |
|---|---|---|
| `TinyInt(u8)`, `SmallInt(i16)`, `Int(i32)`, `BigInt(i64)` | `Int(i64)` | Widen to i64 |
| `Real(f32)`, `Float(f64)` | `Float(f64)` | Widen to f64 |
| `Bit(bool)` | `Bool(bool)` | Direct |
| `String(SqlString)` | `String(String)` | Decode SqlString to UTF-8 String |
| `Bytes(Vec<u8>)` | `Binary(Vec<u8>)` | Direct (rename for clarity) |
| `Decimal(DecimalParts)`, `Numeric(DecimalParts)` | `Decimal(BigDecimal)` | Convert via `bigdecimal` crate |
| `DateTime`, `SmallDateTime`, `DateTime2`, `Date`, `Time`, `DateTimeOffset` | `DateTime(DateTime)` | Custom struct holding date+time+offset+scale, constructed from any temporal variant |
| `SmallMoney(SqlSmallMoney)`, `Money(SqlMoney)` | `Decimal(BigDecimal)` | Money is a fixed-point decimal; coalesce into Decimal |
| `Uuid(Uuid)` | `Uuid(Uuid)` | Direct (re-export `uuid::Uuid`) |
| `Xml(SqlXml)` | `Xml(String)` | Extract XML text |
| `Json(SqlJson)` | `Json(String)` | Extract JSON text |
| `Vector(SqlVector)` | `Vector(Vec<f32>)` | Extract vector data |
| `Null` | `Null` | Direct |

**Alternatives considered**:
- *Keep all 25 variants* — rejected per spec clarification. Too noisy for consumers.
- *Use `Other(ColumnValues)` escape hatch* — rejected; `ColumnValues` is `pub(crate)` in `mssql-tds` and exposing it breaks the decoupled semver goal.

## R3: Row Streaming — Column-by-Column

**Decision**: Implement `Row` as a streaming column cursor using the existing `RowWriter` trait for column-by-column decode.

**Rationale**: The token stream decoder already calls `RowWriter` methods column-by-column during wire decode (`receive_row_into_internal()` at `token_stream.rs:167-192`). By implementing a custom `RowWriter` that yields values via a channel or buffer one at a time, the `Row` type can offer column-level iteration without loading all columns into memory.

**Design**:
- `mssql-rs` implements `RowWriter` with a single-column buffer: each `write_*(col, val)` call stores the value, and `Row::next_column()` awaits the next write. This avoids materializing all columns simultaneously.
- For convenience, `Row` also provides `Row::get<T: FromValue>(index)` for random access, which internally buffers remaining columns on first random-access call.
- The streaming path and the random-access path are mutually exclusive on a given `Row` instance to avoid confusion.

**Alternatives considered**:
- *Always buffer full row, expose indexed access only* — rejected; violates the two-level streaming requirement.
- *Expose raw `RowWriter` to consumers* — rejected; it's a sink trait designed for decoders, not a consumer-facing API.

## R4: Binary Chunk Streaming for Large Columns (FR-017)

**Decision**: Defer binary chunk streaming (FR-017) to a follow-up iteration. The initial implementation will fully materialize PLP columns as per existing `mssql-tds` behavior.

**Rationale**: The `mssql-tds` decoder (`read_plp_bytes()` at `decoder.rs:612-711`) fully buffers PLP data into `Vec<u8>` before delivery. Adding true chunk streaming requires:
1. New chunked methods on `RowWriter` (`begin_bytes` / `write_bytes_chunk` / `end_bytes`)
2. A parallel `read_plp_streaming` path in the decoder
3. Careful synchronization with the token stream's packet-boundary handling

This is a significant change to `mssql-tds` internals, not just a wrapper concern. The `mssql-rs` API contract (FR-017) will be defined now but the initial implementation will buffer internally. The streaming behavior will be added when `mssql-tds` gains chunked PLP support.

**Alternatives considered**:
- *Implement in `mssql-rs` by wrapping buffered data in a Stream* — this is a lie; it doesn't actually stream from the wire. Would give false ergonomic benefit without the memory savings.
- *Block the entire crate on PLP streaming* — rejected; the feature is valuable without it, and the API surface can be designed now for future compatibility.

## R5: Error Type Design

**Decision**: Define `mssql_rs::Error` as a `#[derive(thiserror::Error)]` enum with variants that map to user-facing error categories. Wrap `mssql_tds::Error` in an opaque `Protocol` variant.

**Rationale**: The spec (clarification Q3) requires a decoupled error type. The `mssql-tds` `Error` enum has 18+ variants including internal concerns like `Redirection`, `BulkCopyError`, and certificate errors that shouldn't leak into the public API.

**Variants**:

```rust
pub enum Error {
    ConnectionFailed(String),
    ConnectionStringInvalid(String),
    QueryFailed(String),
    SqlServer { message: String, state: u8, number: u32 },
    Timeout(String),
    Cancelled,
    TypeConversion(String),
    Protocol(Box<dyn std::error::Error + Send + Sync>), // wraps mssql_tds::Error
    Io(std::io::Error),
}
```

**Alternatives considered**:
- *Re-export `mssql_tds::Error`* — rejected per spec clarification.
- *1:1 variant mapping* — rejected; exposes internal error categories (redirects, bulk copy, certificate details) that `mssql-rs` consumers don't need.

## R6: Stream Trait Implementation

**Decision**: Implement `futures::Stream<Item = Result<Row, Error>>` on `ResultSet` using `async-stream` or manual `Poll`-based implementation.

**Rationale**: The spec (clarification Q5) requires the `futures::Stream` trait, enabling `StreamExt` combinators. The underlying `TdsClient::get_next_row()` is an async method that returns `Option<Vec<ColumnValues>>`. This maps directly to a `Stream::poll_next()` implementation via `async-stream` (generates state machine) or `Pin<Box<dyn Stream>>`.

**Design**:
- Prefer manual `poll_next` implementation to avoid the `async-stream` proc-macro dependency and to maintain zero-overhead (Constitution Principle VI).
- `ResultSet` holds a mutable reference to the `Client`'s inner `TdsClient` and delegates to `get_next_row()` / `get_next_row_into()`.
- `ResultSet::next_result_set()` consumes the current stream and returns a new `ResultSet` for the next result set (maps to `move_to_next()` on `ResultSetClient`).

**Alternatives considered**:
- *`async-stream` crate* — simpler to write but adds a proc-macro dependency and may generate suboptimal state machines.
- *`tokio_stream::wrappers`* — doesn't fit; no existing stream to wrap.

## R7: Transaction Drop Behavior

**Decision**: `Transaction::drop()` sends a rollback via a Tokio `spawn_blocking`-free mechanism and emits `tracing::debug!`.

**Rationale**: Per spec clarification Q2. The challenge is that `Drop` cannot be async. Options:
- Use `tokio::task::block_in_place` in Drop — blocks the current thread; acceptable for the rare case of forgotten transactions.
- Store a `tokio::runtime::Handle` and use `handle.block_on()` — same trade-off.
- Use a synchronous flag that the next `Client` operation detects and rolls back before proceeding.

**Design**: Use the deferred-rollback approach — `Transaction::drop()` sets a flag on the inner client state. The next `Client` operation (query, prepare, etc.) checks the flag and issues the rollback before proceeding. This avoids blocking in Drop and is consistent with how connection cleanup works in practice.

**Alternatives considered**:
- *`block_in_place` in Drop* — works but risks panicking if called from a non-Tokio context.
- *Spawn a background task* — requires `Arc<Mutex<TdsClient>>` which conflicts with the borrow model for the streaming API.

## R8: Prepared Statement Handle Lifecycle

**Decision**: `PreparedStatement` holds the `i32` handle from `sp_prepare` and a back-reference to the `Client`. `Drop` calls `sp_unprepare` using the same deferred-cleanup approach as transactions.

**Rationale**: The TDS protocol returns an `i32` handle from `sp_prepare`. The handle is used with `sp_execute` and released with `sp_unprepare`. The `mssql-tds` methods (`execute_sp_prepare`, `execute_sp_execute`, `execute_sp_unprepare`) already implement this lifecycle.

**Design**:
- `Client::prepare(sql, params) -> Result<PreparedStatement>` calls `execute_sp_prepare`, returns handle.
- `PreparedStatement::execute(params) -> Result<ResultSet>` calls `execute_sp_execute` with the stored handle.
- `PreparedStatement::close()` explicitly calls `execute_sp_unprepare`.
- `PreparedStatement::drop()` sets a deferred-unprepare flag if not already closed.

**Alternatives considered**:
- *Auto-unprepare on every Drop via `block_in_place`* — same risks as transaction Drop.
- *No auto-cleanup; require explicit close* — rejected; resource leaks in error paths are too easy.
