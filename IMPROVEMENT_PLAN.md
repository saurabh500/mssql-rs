# mssql-tds Improvement Plan

**Date:** 2026-02-16
**Scope:** All work happens within the existing `mssql-tds`, `mssql-js`, and `mssql-py-core` crates. No new crates except where explicitly noted (MCP server, C FFI).

---

## Phase 1: RowWriter Foundation (3-4 weeks)

Everything in the high-throughput decode path depends on a pluggable decode sink. This is the critical-path work.

### 1.1 Define the `RowWriter` Trait in `mssql-tds`

**Where:** New module `mssql-tds/src/datatypes/row_writer.rs`

Create a trait with typed write methods the decoder can call directly during wire decoding:

```rust
pub trait RowWriter {
    fn write_null(&mut self, col: usize);
    fn write_bool(&mut self, col: usize, val: bool);
    fn write_i16(&mut self, col: usize, val: i16);
    fn write_i32(&mut self, col: usize, val: i32);
    fn write_i64(&mut self, col: usize, val: i64);
    fn write_f32(&mut self, col: usize, val: f32);
    fn write_f64(&mut self, col: usize, val: f64);
    fn write_str(&mut self, col: usize, val: &str);
    fn write_bytes(&mut self, col: usize, val: &[u8]);
    fn write_decimal(&mut self, col: usize, parts: &DecimalParts);
    fn write_date(&mut self, col: usize, days_from_epoch: i32);
    fn write_time(&mut self, col: usize, micros_from_midnight: i64, scale: u8);
    fn write_datetime2(&mut self, col: usize, days_from_epoch: i32, micros_from_midnight: i64, scale: u8);
    fn write_datetimeoffset(&mut self, col: usize, days_from_epoch: i32, micros_from_midnight: i64, scale: u8, offset_minutes: i16);
    fn write_uuid(&mut self, col: usize, val: &uuid::Uuid);
    fn end_row(&mut self);
}
```

Provide a `DefaultRowWriter` that assembles `Vec<ColumnValues>` (the current behavior), so existing code paths are unchanged.

**Deliverables:**
- `row_writer.rs` with trait + `DefaultRowWriter`
- Unit tests confirming `DefaultRowWriter` produces identical output to current decode

### 1.2 Refactor the Decoder to Accept a `RowWriter`

**Where:** `mssql-tds/src/datatypes/decoder.rs` (2,384 lines)

Add a parallel method on `GenericDecoder` and `StringDecoder`:

```rust
pub async fn decode_into<W: RowWriter>(
    reader: &mut TdsPacketReader,
    metadata: &ColumnMetadata,
    col: usize,
    writer: &mut W,
) -> TdsResult<()>;
```

The existing `decode()` → `ColumnValues` path stays intact. The `decode_into` path calls writer methods directly for the hot types (int, float, string, binary, datetime). Rare types (XML, Vector) fall through to the old path and call `writer.write_*` after decoding.

**Deliverables:**
- `decode_into()` methods on `GenericDecoder` and `StringDecoder`
- Direct decode for: bit, tinyint, smallint, int, bigint, real, float, string, binary, date, time, datetime2, datetimeoffset, decimal, uuid
- Fallback path for: XML, JSON, Vector, encrypted columns
- Integration tests via `mssql-mock-tds` proving equivalence

### 1.3 Wire `RowWriter` into the Token Stream / ResultSet Path

**Where:** `mssql-tds/src/io/token_stream/`, `mssql-tds/src/connection/tds_client.rs`

Add a `next_row_into<W: RowWriter>(&mut self, writer: &mut W)` method on `TdsClient` (or on the `ResultSet` trait) alongside the existing `next_row()`. The existing `next_row()` internally uses `DefaultRowWriter`.

**Deliverables:**
- `next_row_into()` on `ResultSet` trait and `TdsClient`
- `next_row()` reimplemented as a thin wrapper over `next_row_into(DefaultRowWriter)`
- All existing tests pass unchanged

---

## Phase 2: Performance — Direct Decode & Binary Query (2-3 weeks)

### 2.1 Optimize the Direct Decode Hot Path

**Where:** `mssql-tds/src/datatypes/decoder.rs`

After 1.2, profile the `decode_into` path. Eliminate any remaining allocations for the common types: no `String` allocation if the writer can accept `&str`, no `Vec<u8>` allocation if the writer can accept `&[u8]`. The `RowWriter` methods take references for this reason.

**Deliverables:**
- Benchmark (criterion) comparing `next_row()` vs `next_row_into()` with `DefaultRowWriter`
- No regressions; target ≥10% allocation reduction for typical query workloads

### 2.2 Binary-Encoded `query_raw()` in `mssql-js`

**Where:** `mssql-js/src/connection.rs`, new file `mssql-js/js/decode.js`

Implement a `RowWriter` in `mssql-js` that writes results into a compact binary buffer:

**Binary format:**
```
Header:  col_count(u16) | row_count(u32) | string_table_len(u32) | rows_affected(i64)
Columns: [name_offset(u32), type_id(u8)] × col_count
StringTable: [utf8_bytes...] (deduplicated strings, null-separated)
Rows:    [tag(u8) value...]  per cell
         tag 0 = NULL, 1 = bool(u8), 2 = i32(le), 3 = f64(le),
         4 = string_ref(u32 index into string table),
         5 = buffer(u32 len + bytes), 6 = bigint(i64 le), ...
```

Key techniques:
- **String interning:** HashMap tracking unique strings → index in string table. Repeated values (status columns, enum-like data) stored once.
- **Single Buffer transfer:** One N-API `Buffer` allocation crosses the boundary.
- **JS decoder:** `decode.js` parses the buffer into row arrays. Shipped as part of the npm package.

**NAPI export:**
```rust
#[napi]
pub async fn query_raw(&self, query: String) -> napi::Result<Buffer> { ... }
```

**Deliverables:**
- `BinaryRowWriter` implementing `RowWriter` in mssql-js
- `query_raw()` NAPI method
- `decode.js` / `decode.ts` client-side decoder
- Benchmark comparing `query()` vs `query_raw()` on 10K+ row result sets
- TypeScript declarations

---

## Phase 3: Arrow Integration (2-3 weeks)

### 3.1 Arrow Feature Flag & `ArrowRowWriter`

**Where:** `mssql-tds/src/datatypes/arrow_writer.rs`, gated behind `arrow` feature in `mssql-tds/Cargo.toml`

```toml
[features]
arrow = ["arrow-array", "arrow-schema", "arrow-buffer"]
```

Implement `ArrowRowWriter` that appends directly to Arrow `ArrayBuilder`s during decode:

| TDS Type | Arrow Type |
|----------|-----------|
| Bit | Boolean |
| TinyInt | UInt8 |
| SmallInt | Int16 |
| Int | Int32 |
| BigInt | Int64 |
| Real | Float32 |
| Float | Float64 |
| Decimal/Numeric | Decimal128 |
| String/NVarChar | Utf8 |
| Binary/VarBinary | Binary |
| Date | Date32 |
| Time | Time64(Microsecond) |
| DateTime2 | Timestamp(Microsecond, None) |
| DateTimeOffset | Timestamp(Microsecond, Some(tz)) |
| UniqueIdentifier | FixedSizeBinary(16) |

**Public API:**
```rust
pub async fn query_arrow(client: &mut TdsClient, sql: &str) -> TdsResult<RecordBatch> { ... }
pub async fn bulk_write_arrow(client: &mut TdsClient, table: &str, batch: &RecordBatch) -> TdsResult<()> { ... }
```

**Deliverables:**
- `arrow_writer.rs` with `ArrowRowWriter` implementing `RowWriter`
- `query_arrow()` and `bulk_write_arrow()` convenience functions
- Full type mapping table above
- Integration tests with `mssql-mock-tds`

### 3.2 Arrow in `mssql-py-core` via PyArrow FFI

**Where:** `mssql-py-core/src/`, gated behind `arrow` feature

Expose `query_arrow()` to Python using Arrow C Data Interface (FFI):
- Call `query_arrow()` from mssql-tds
- Export the `RecordBatch` via `arrow::ffi` → raw pointers
- Python side wraps it with `pyarrow.RecordBatch._import_from_c()`

This gives Python users zero-copy access to query results as PyArrow tables.

**Deliverables:**
- `mssql-py-core` `arrow` feature flag
- `PyCoreCursor.fetch_arrow()` → PyArrow RecordBatch
- Integration tests

---

## Phase 4: Sync Client (2 weeks)

### 4.1 Add `sync` Feature Flag to `mssql-tds`

**Where:** `mssql-tds/Cargo.toml`, new module `mssql-tds/src/connection/sync_client.rs`

```toml
[features]
sync = []
```

Implement synchronous I/O wrappers:
- `SyncTdsClient` wrapping `std::net::TcpStream`
- Blocking read/write using the same packet framing and token parsing logic
- Reuse encoder/decoder (they operate on byte slices, not async streams) — the `RowWriter`-based decoder from Phase 1 makes this easier since decode logic is separated from I/O

The sync client shares all protocol logic (encoder, decoder, token parsers) with the async client. Only the I/O and connection layers differ.

**Deliverables:**
- `SyncTdsClient` with `execute()`, `query()`, `next_row()`, `close()`
- Sync `ResultSet` iteration
- Connection + login flow (sync TCP + TLS via `native-tls`)
- Integration tests against live SQL Server and `mssql-mock-tds`

### 4.2 Sync Python Driver (DB-API 2.0 Foundation)

**Where:** `mssql-py-core/src/`

With the sync feature, `mssql-py-core` can offer a simpler path that doesn't require a tokio runtime bridge:
- `PySyncConnection` using `SyncTdsClient` directly
- No `LazyLock<Bridge>`, no `tokio::sync::Mutex` — plain `parking_lot::Mutex`
- PEP 249 DB-API 2.0 methods: `connect()`, `cursor()`, `execute()`, `fetchone()`, `fetchmany()`, `fetchall()`, `close()`

**Deliverables:**
- `mssql-py-core` `sync` feature flag
- `PySyncConnection` + `PySyncCursor` classes
- DB-API 2.0 compliance for core operations

---

## Phase 5: C FFI / ODBC Layer (2-3 weeks)

### 5.1 C FFI Crate

**Where:** New crate `mssql-ffi/` (this is the one exception where a new crate is warranted — it must be `cdylib`)

Thin C API over `mssql-tds` sync client:
- `mssql_connect()`, `mssql_disconnect()`
- `mssql_exec_direct()`, `mssql_fetch()`, `mssql_get_data()`
- `mssql_prepare()`, `mssql_execute()`, `mssql_bind_param()`
- Handle-based API: connection handles, statement handles
- Thread-safe via `parking_lot::Mutex`

Enables:
- Go driver via CGO
- Ruby FFI
- .NET P/Invoke
- Any language with C FFI

**Deliverables:**
- `mssql-ffi` crate producing `libmssql.so` / `mssql.dll`
- C header file (`mssql.h`) generated or handwritten
- Basic integration test from C

---

## Phase 6: API Quality & Ergonomics (1-2 weeks)

### 6.1 Temporal Types Module

**Where:** New module `mssql-tds/src/datatypes/temporal.rs`

Extract temporal types from the monolithic decoder into standalone structs with clean constructors:

```rust
pub struct TdsDate { days_from_ce: i32 }
pub struct TdsTime { micros: i64, scale: u8 }
pub struct TdsDateTime2 { date: TdsDate, time: TdsTime }
pub struct TdsDateTimeOffset { dt: TdsDateTime2, offset_minutes: i16 }
```

Each type provides:
- `from_epoch_days()` / `to_epoch_days()` — for Arrow Date32
- `from_epoch_micros()` / `to_epoch_micros()` — for Arrow Timestamp
- `to_chrono()` / `from_chrono()` — for `chrono` interop (feature-gated)

These types are what `RowWriter::write_date()` etc. pass through, making the trait cleaner and enabling uniform conversion in Arrow/JS/Python writers.

**Deliverables:**
- `temporal.rs` module
- Migration of existing temporal decode logic to use these types
- chrono feature gate

### 6.2 Collation Completeness Audit

**Where:** `mssql-tds/src/datatypes/` (existing collation handling)

Audit the LCID-based encoding support against the full SQL Server collation table. Low priority, small effort.

**Deliverables:**
- Gap analysis document
- Any missing collation mappings added

### 6.3 Connection String Parsing in `mssql-js`

**Where:** `mssql-js/src/`

Support ADO.NET-style connection strings (`Server=host,port;Database=db;UID=user;PWD=pass;TrustServerCertificate=yes`) in addition to the current config-object approach. mssql-tds already has `datasource_parser.rs` (1,310 lines) — wire it through to the JS API.

**Deliverables:**
- `connect_with_string(conn_str)` NAPI export
- Mapping from ADO.NET keys to `JsClientContext` fields
- Tests

### 6.4 RowWriter-Based Decode in `mssql-js`

**Where:** `mssql-js/src/connection.rs`

With the RowWriter trait from Phase 1, implement a `NapiRowWriter` that writes directly to N-API values during decode. This is the per-cell optimization complementary to the binary `query_raw()` from Phase 2.

**Deliverables:**
- `NapiRowWriter` implementing `RowWriter`
- `next_row_in_resultset()` uses `NapiRowWriter` to skip the `ColumnValues` intermediate

### 6.5 RowWriter-Based Decode in `mssql-py-core`

**Where:** `mssql-py-core/src/`

Same pattern as 6.4 but for PyO3. A `PyRowWriter` that calls `PyList::append()` etc. directly.

**Deliverables:**
- `PyRowWriter` implementing `RowWriter`
- Integration into `PyCoreCursor.fetchone()` / `fetchmany()` / `fetchall()`

---

## Phase 7: Stretch Goals (as time permits)

### 7.1 MCP Server

**Where:** New crate `mssql-mcp/` (standalone binary, depends on `mssql-tds`)

MCP (Model Context Protocol) server exposing SQL Server operations as AI-agent-callable tools. JSON-RPC over stdio. Tools: query, schema inspect, etc.

### 7.2 TypeScript Type Quality

Audit `mssql-js` TypeScript definitions for completeness. Ensure generics for typed query results.

---

## Dependency Graph

```
Phase 1: RowWriter Foundation
    │
    ├──→ Phase 2: Direct Decode + Binary query_raw()
    │        │
    │        └──→ Phase 6.4: NapiRowWriter in mssql-js
    │        └──→ Phase 6.5: PyRowWriter in mssql-py-core
    │
    ├──→ Phase 3: Arrow Integration
    │        │
    │        └──→ Phase 3.2: PyArrow FFI in mssql-py-core
    │
    └──→ Phase 4: Sync Client
             │
             ├──→ Phase 4.2: Sync Python DB-API 2.0
             └──→ Phase 5: C FFI / ODBC

Phase 6.1 (Temporal), 6.2 (Collation), 6.3 (Conn String) — independent, can start anytime
Phase 7 — stretch goals, no hard dependencies
```

## Effort Summary

| Phase | Description | Estimate | Depends On |
|-------|-------------|----------|------------|
| 1 | RowWriter Foundation | 3-4 weeks | — |
| 2 | Direct Decode + Binary query_raw | 2-3 weeks | Phase 1 |
| 3 | Arrow Integration | 2-3 weeks | Phase 1 |
| 4 | Sync Client | 2 weeks | Phase 1 (partial) |
| 5 | C FFI / ODBC | 2-3 weeks | Phase 4 |
| 6 | API Quality & Ergonomics | 1-2 weeks | Phase 1 (partial) |
| 7 | Stretch Goals | 1-2 weeks | Various |

**Total estimated:** ~14-18 weeks for Phases 1-6, assuming serial execution and one engineer.

Phases 3, 4, and 6.1-6.3 can run in parallel with Phase 2 once Phase 1 is complete, reducing wall-clock time to ~10-12 weeks with parallelism.

---

## Principles

1. **No new core crate.** All wire protocol and client API work stays in `mssql-tds`. The only new crates are leaf bindings (`mssql-ffi` for C, `mssql-mcp` for MCP) that are pure consumers of `mssql-tds`.
2. **Backward compatible.** Every change preserves the existing API. New capabilities are additive (new methods, feature flags).
3. **Feature-gated dependencies.** Arrow, sync, chrono — all behind feature flags so default compile stays lean.
4. **Leverage existing strengths.** mssql-tds already has superior auth, bulk copy, named pipes, connection providers, certificate validation, and test infrastructure. The plan adds what's missing (pluggable decode, Arrow, sync) without disrupting what works.
5. **Language bindings benefit automatically.** `mssql-js` and `mssql-py-core` get performance and capability improvements by implementing `RowWriter` against the same trait.
