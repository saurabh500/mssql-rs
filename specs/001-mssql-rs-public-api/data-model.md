# Data Model: mssql-rs Public API Crate

**Spec**: [spec.md](spec.md) | **Research**: [research.md](research.md)

## Entities

### Client

Primary connection handle. Wraps `mssql_tds::TdsClient` behind a private field.

| Field | Type | Description |
|---|---|---|
| `inner` | `TdsClient` | Underlying TDS client (private) |
| `cancel_handle` | `CancelHandle` | Cancellation token for in-flight operations (private) |
| `pending_rollback` | `bool` | Deferred rollback flag set by Transaction::drop (private) |
| `pending_unprepare` | `Vec<i32>` | Deferred sp_unprepare handles from PreparedStatement::drop (private) |

**Construction**: `Client::connect(connection_string: &str) -> Result<Client>`

**Invariants**:
- At most one active `ResultSet` borrow at a time (enforced by Rust lifetimes).
- Before any operation, check and drain `pending_rollback` / `pending_unprepare`.

**State transitions**: `Connected` → `Querying` (has active ResultSet) → `Connected` (ResultSet dropped) → `Closed` (connection dropped).

---

### Value

Simplified column value enum. Coalesces 25 `ColumnValues` variants into 13 user-facing variants.

```rust
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(BigDecimal),
    String(String),
    Binary(Vec<u8>),
    DateTime(DateTime),
    Uuid(uuid::Uuid),
    Xml(String),
    Json(String),
    Vector(Vec<f32>),
}
```

**Coalescing rules** (from research R2):
- `TinyInt | SmallInt | Int | BigInt` → `Int(i64)` — widening
- `Real | Float` → `Float(f64)` — widening
- `Bit` → `Bool(bool)`
- `String(SqlString)` → `String(String)` — UTF-8 decode
- `Bytes` → `Binary(Vec<u8>)`
- `Decimal | Numeric | SmallMoney | Money` → `Decimal(BigDecimal)`
- `DateTime | SmallDateTime | DateTime2 | Date | Time | DateTimeOffset` → `DateTime(DateTime)`
- `Uuid` → `Uuid(uuid::Uuid)`
- `Xml(SqlXml)` → `Xml(String)`
- `Json(SqlJson)` → `Json(String)`
- `Vector(SqlVector)` → `Vector(Vec<f32>)`
- `Null` → `Null`

---

### DateTime

Custom temporal struct holding the union of all TDS date/time components.

| Field | Type | Description |
|---|---|---|
| `year` | `Option<i32>` | Year component (None for TIME-only values) |
| `month` | `Option<u8>` | Month 1-12 |
| `day` | `Option<u8>` | Day 1-31 |
| `hour` | `Option<u8>` | Hour 0-23 |
| `minute` | `Option<u8>` | Minute 0-59 |
| `second` | `Option<u8>` | Second 0-59 |
| `nanoseconds` | `Option<u32>` | Sub-second precision in nanoseconds |
| `offset_minutes` | `Option<i16>` | UTC offset in minutes (None = no timezone) |

**Construction**: Via `From<SqlDateTime>`, `From<SqlDateTime2>`, `From<SqlDate>`, `From<SqlTime>`, `From<SqlDateTimeOffset>`, `From<SqlSmallDateTime>` — each fills the fields it has and leaves others `None`.

---

### Row

Streaming cursor over columns within a decoded row. Two access modes (mutually exclusive per instance):

**Sequential mode** (column streaming):
- `next_column() -> Result<Option<Value>>` — yields the next column value.
- Columns must be read in order; skipping is not supported.

**Random-access mode** (indexed):
- `get<T: FromValue>(index: usize) -> Result<T>` — extracts column by zero-based index.
- `get_by_name<T: FromValue>(name: &str) -> Result<T>` — extracts column by name.
- On first random-access call, remaining unread columns are buffered.

| Field | Type | Description |
|---|---|---|
| `columns` | `Vec<Option<Value>>` | Column buffer (populated lazily or eagerly depending on mode) (private) |
| `metadata` | `Arc<Vec<ColumnMetadata>>` | Shared metadata for the result set (private) |
| `position` | `usize` | Current sequential read position (private) |

**Invariants**:
- After first `get()` or `get_by_name()`, `next_column()` returns an error.
- After first `next_column()`, `get()` / `get_by_name()` buffer remaining columns then serve the request.

---

### ColumnMetadata

Read-only description of a result column.

| Field | Type | Description |
|---|---|---|
| `name` | `String` | Column name from the server |
| `data_type` | `DataType` | Logical data type enum |
| `nullable` | `bool` | Whether the column allows NULLs |
| `collation` | `Option<String>` | Collation name for string columns |

`DataType` enum:

```rust
pub enum DataType {
    Bool,
    Int,
    Float,
    Decimal { precision: u8, scale: u8 },
    String { max_length: Option<u32> },
    Binary { max_length: Option<u32> },
    DateTime { scale: u8 },
    Uuid,
    Xml,
    Json,
    Vector { dimensions: Option<u32> },
}
```

---

### ResultSet

Stream handle for one result set. Implements `futures::Stream<Item = Result<Row>>`.

| Field | Type | Description |
|---|---|---|
| `client` | `&mut Client` | Mutable borrow of the parent client (private) |
| `metadata` | `Arc<Vec<ColumnMetadata>>` | Column metadata for this result set (private) |
| `exhausted` | `bool` | Whether all rows have been yielded (private) |

**Methods**:
- `metadata() -> &[ColumnMetadata]` — column metadata available before any rows are read.
- `next_result_set(self) -> Result<Option<ResultSet>>` — consumes this set, advances to the next. Returns `None` if no more result sets.
- `collect_rows(self) -> Result<Vec<Vec<Value>>>` — convenience drain into 2D collection.

**Stream contract**: Yields `Row` instances one at a time. After the last row, `poll_next` returns `None`. The `ResultSet` must be fully consumed or dropped before the `Client` can execute another query.

---

### FromValue Trait

Conversion trait for extracting `Value` into Rust types.

```rust
pub trait FromValue: Sized {
    fn from_value(value: Value) -> Result<Self>;
}
```

**Built-in implementations**:

| Rust Type | Accepted Value Variants |
|---|---|
| `bool` | `Bool` |
| `i8`, `i16`, `i32`, `i64` | `Int` (with range check for narrowing) |
| `f32`, `f64` | `Float` |
| `String` | `String`, `Xml`, `Json` |
| `Vec<u8>` | `Binary` |
| `BigDecimal` | `Decimal` |
| `uuid::Uuid` | `Uuid` |
| `DateTime` | `DateTime` |
| `Option<T: FromValue>` | Any (returns `None` for `Null`) |

**Error on mismatch**: Returns `Error::TypeConversion` with a message describing the source variant and target type.

---

### PreparedStatement

Server-side prepared statement handle.

| Field | Type | Description |
|---|---|---|
| `handle` | `i32` | sp_prepare handle returned by the server (private) |
| `client` | `&mut Client` | Mutable borrow of the parent client (private) |
| `closed` | `bool` | Whether sp_unprepare has been called (private) |

**Methods**:
- `execute(params: &[Value]) -> Result<ResultSet>` — calls sp_execute with the stored handle.
- `close(self) -> Result<()>` — calls sp_unprepare, consumes self.

**Drop behavior**: If not closed, sets `pending_unprepare` flag on the `Client` for deferred cleanup (research R8).

**Invariants**:
- `execute()` after `close()` is impossible (self consumed).
- Active `ResultSet` must be dropped before next `execute()`.

---

### Transaction

Active transaction handle.

| Field | Type | Description |
|---|---|---|
| `client` | `&mut Client` | Mutable borrow of the parent client (private) |
| `committed` | `bool` | Whether commit was called (private) |
| `rolled_back` | `bool` | Whether rollback was called (private) |

**Methods**:
- `commit(self) -> Result<()>` — calls `commit_transaction()`, consumes self.
- `rollback(self) -> Result<()>` — calls `rollback_transaction()`, consumes self.
- `execute(query, params) -> Result<ResultSet>` — execute within the transaction.
- `prepare(sql, params) -> Result<PreparedStatement>` — prepare within the transaction.

**Drop behavior**: If neither committed nor rolled back, sets `pending_rollback` flag on `Client`, emits `tracing::debug!("transaction dropped without commit/rollback, deferred rollback scheduled")` (research R7).

---

### IsolationLevel

Transaction isolation level enum.

```rust
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}
```

Maps 1:1 to `mssql_tds::TransactionIsolationLevel`.

---

### Error

Public error enum.

```rust
pub enum Error {
    ConnectionFailed(String),
    ConnectionStringInvalid(String),
    QueryFailed(String),
    SqlServer { message: String, state: u8, number: u32 },
    Timeout(String),
    Cancelled,
    TypeConversion(String),
    Protocol(Box<dyn std::error::Error + Send + Sync>),
    Io(std::io::Error),
}
```

`Result<T>` type alias: `pub type Result<T> = std::result::Result<T, Error>;`

Maps from `mssql_tds::Error` via `From` impl:
- `mssql_tds::Error::Io(_)` → `Error::Io`
- `mssql_tds::Error::ConnectionError(_)` → `Error::ConnectionFailed`
- `mssql_tds::Error::TimeoutError(_)` → `Error::Timeout`
- `mssql_tds::Error::SqlServerError { message, state, number, .. }` → `Error::SqlServer`
- `mssql_tds::Error::TypeConversionError(_)` → `Error::TypeConversion`
- All other variants → `Error::Protocol(Box::new(e))`

## Relationships

```
Client ─────────┬──── executes ───→ ResultSet (borrows &mut Client)
                │                      │
                │                      ├── yields ──→ Row (owns column data)
                │                      │                 │
                │                      │                 └── contains ──→ Value
                │                      │
                │                      └── exposes ──→ ColumnMetadata
                │
                ├──── prepares ───→ PreparedStatement (borrows &mut Client)
                │                      │
                │                      └── executes ──→ ResultSet
                │
                └──── begins ─────→ Transaction (borrows &mut Client)
                                       │
                                       ├── executes ──→ ResultSet
                                       └── prepares ──→ PreparedStatement
```

**Lifetime constraints**:
- `ResultSet<'a>` borrows `&'a mut Client` — at most one active result set.
- `PreparedStatement<'a>` borrows `&'a mut Client` — at most one active prepared statement (or it must be closed before another is created).
- `Transaction<'a>` borrows `&'a mut Client` — at most one active transaction.
- `Row` is owned (no lifetime constraint on Client) — extracted from the stream.
