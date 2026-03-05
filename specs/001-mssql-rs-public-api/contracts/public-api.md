# Public API Contract: mssql-rs

**Spec**: [../spec.md](../spec.md) | **Data Model**: [../data-model.md](../data-model.md)

This document defines the complete public API surface of the `mssql-rs` crate. Every `pub` item listed here is part of the semver contract. Items not listed are `pub(crate)` or private.

## Re-exports (lib.rs)

```rust
pub use client::Client;
pub use value::Value;
pub use row::Row;
pub use result_set::ResultSet;
pub use metadata::{ColumnMetadata, DataType};
pub use from_value::FromValue;
pub use prepared::PreparedStatement;
pub use transaction::{Transaction, IsolationLevel};
pub use error::{Error, Result};
pub use datetime::DateTime;
```

## Client

```rust
impl Client {
    /// Connect to SQL Server using an ODBC-style connection string.
    ///
    /// Keys are case-insensitive. Standard keys: Server, Database, User Id,
    /// Password, Encrypt, TrustServerCertificate, Connection Timeout,
    /// Application Name, Packet Size, Authentication, Access Token.
    pub async fn connect(connection_string: &str) -> Result<Client>;

    /// Execute a SQL query, returning the first result set.
    pub async fn query(&mut self, sql: &str) -> Result<ResultSet<'_>>;

    /// Execute a parameterized query (sp_executesql).
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<ResultSet<'_>>;

    /// Execute a query and collect all rows into a 2D collection.
    pub async fn query_collect(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<Value>>>;

    /// Execute a parameterized query and collect all rows.
    pub async fn query_collect_with_params(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<Vec<Vec<Value>>>;

    /// Prepare a SQL statement for repeated execution.
    pub async fn prepare(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<PreparedStatement<'_>>;

    /// Begin a transaction with default isolation level (ReadCommitted).
    pub async fn begin_transaction(&mut self) -> Result<Transaction<'_>>;

    /// Begin a transaction with a specific isolation level.
    pub async fn begin_transaction_with_isolation(
        &mut self,
        level: IsolationLevel,
    ) -> Result<Transaction<'_>>;

    /// Cancel any in-flight query.
    pub fn cancel(&self);

    /// Close the connection.
    pub async fn close(self) -> Result<()>;
}
```

## ResultSet

```rust
impl<'a> ResultSet<'a> {
    /// Column metadata for this result set.
    pub fn metadata(&self) -> &[ColumnMetadata];

    /// Advance to the next result set. Returns None if no more sets.
    pub async fn next_result_set(self) -> Result<Option<ResultSet<'a>>>;

    /// Drain all remaining rows into a Vec<Vec<Value>>.
    pub async fn collect_rows(self) -> Result<Vec<Vec<Value>>>;
}

impl<'a> futures::Stream for ResultSet<'a> {
    type Item = Result<Row>;
    // Yields rows one at a time.
}
```

## Row

```rust
impl Row {
    // --- Random-access mode (requires all columns materialized) ---

    /// Get a typed value by zero-based column index.
    pub fn get<T: FromValue>(&self, index: usize) -> Result<T>;

    /// Get a typed value by column name (case-insensitive lookup).
    pub fn get_by_name<T: FromValue>(&self, name: &str) -> Result<T>;

    /// Get the raw Value by index without conversion.
    pub fn value(&self, index: usize) -> Result<&Value>;

    /// Number of columns in this row.
    pub fn len(&self) -> usize;

    /// Whether the row has zero columns.
    pub fn is_empty(&self) -> bool;

    /// Column metadata for this row's result set.
    pub fn metadata(&self) -> &[ColumnMetadata];

    /// Convert the row into a Vec<Value>, consuming it.
    pub fn into_values(self) -> Vec<Value>;

    // --- Sequential column-streaming mode ---

    /// Yield the next column value in column order.
    /// Returns `None` after the last column. Mutually exclusive
    /// with random-access methods within the same Row instance.
    pub fn next_column(&mut self) -> Result<Option<Value>>;
}
```

## Value

```rust
#[derive(Debug, Clone, PartialEq)]
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

impl Value {
    /// Returns true if this value is Null.
    pub fn is_null(&self) -> bool;
}
```

## DateTime

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct DateTime {
    pub year: Option<i32>,
    pub month: Option<u8>,
    pub day: Option<u8>,
    pub hour: Option<u8>,
    pub minute: Option<u8>,
    pub second: Option<u8>,
    pub nanoseconds: Option<u32>,
    pub offset_minutes: Option<i16>,
}
```

## ColumnMetadata

```rust
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
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

## FromValue Trait

```rust
pub trait FromValue: Sized {
    fn from_value(value: Value) -> Result<Self>;
}
```

**Built-in impls**: `bool`, `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `f32`, `f64`, `String`, `Vec<u8>`, `BigDecimal`, `uuid::Uuid`, `DateTime`, `Option<T: FromValue>`.

## PreparedStatement

```rust
impl<'a> PreparedStatement<'a> {
    /// Execute the prepared statement with the given parameter values.
    pub async fn execute(
        &mut self,
        params: &[Value],
    ) -> Result<ResultSet<'_>>;

    /// Release the server-side prepared statement handle.
    pub async fn close(self) -> Result<()>;
}
```

Drop: deferred sp_unprepare via `Client::pending_unprepare`.

## Transaction

```rust
impl<'a> Transaction<'a> {
    /// Commit the transaction.
    pub async fn commit(self) -> Result<()>;

    /// Roll back the transaction.
    pub async fn rollback(self) -> Result<()>;

    /// Execute a query within this transaction.
    pub async fn query(&mut self, sql: &str) -> Result<ResultSet<'_>>;

    /// Execute a parameterized query within this transaction.
    pub async fn query_with_params(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<ResultSet<'_>>;

    /// Prepare a statement within this transaction.
    pub async fn prepare(
        &mut self,
        sql: &str,
        params: &[(&str, Value)],
    ) -> Result<PreparedStatement<'_>>;
}
```

Drop: deferred rollback via `Client::pending_rollback` + `tracing::debug!`.

## IsolationLevel

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}
```

## Error

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("invalid connection string: {0}")]
    ConnectionStringInvalid(String),

    #[error("query failed: {0}")]
    QueryFailed(String),

    #[error("SQL Server error {number} (state {state}): {message}")]
    SqlServer { message: String, state: u8, number: u32 },

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("query cancelled")]
    Cancelled,

    #[error("type conversion: {0}")]
    TypeConversion(String),

    #[error("protocol error: {0}")]
    Protocol(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
```

## Non-Public Items

The following are explicitly **not** part of the public API:
- `mssql_tds::TdsClient` — wrapped, never exposed
- `mssql_tds::ColumnValues` — coalesced into `Value`, never exposed
- `mssql_tds::Error` — wrapped in `Error::Protocol`, never exposed
- `mssql_tds::ClientContext` — constructed internally from connection string
- Connection string parser internals
- `RowWriter` implementation
- Deferred cleanup state (`pending_rollback`, `pending_unprepare`)
