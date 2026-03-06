# Data Model: mssql-rs Integration Tests

**Feature**: 002-mssql-rs-integration-tests | **Date**: 2026-03-06

## Entities

### Test Helper Module (`common/mod.rs`)

| Function | Signature | Purpose |
|----------|-----------|---------|
| `connection_string` | `fn() -> Option<String>` | Load `MSSQL_RS_TEST_CONNECTION_STRING` from env/dotenv. Returns `None` when absent. |
| `connect` | `async fn() -> Option<Client>` | Connect using env connection string. Returns `None` when absent. |
| `require_connect` | `async fn() -> Client` | Connect or panic. For tests that must not skip. |
| `init_tracing` | `fn()` | Once-guarded tracing subscriber init (gated by `ENABLE_TEST_TRACE`). |

### Skip Macro

```rust
macro_rules! skip_if_no_server {
    () => {
        if common::connection_string().is_none() {
            eprintln!("MSSQL_RS_TEST_CONNECTION_STRING not set — skipping");
            return;
        }
    };
}
```

Used at the top of every server-dependent test function. Pure-logic tests do not use this macro.

## Test File Mapping

| File | User Story | Pure-Logic Tests | Server-Dependent Tests |
|------|-----------|-----------------|----------------------|
| `connection.rs` | US1 (P1) | Connection string parsing (valid, malformed, unknown keys, brace quoting) | Connect + basic query, unreachable server timeout, close, DDL execution |
| `streaming.rs` | US2 (P2) | — | 100-row iteration, multi-result-set, drop mid-iteration, zero rows, collect_rows |
| `type_system.rs` | US3 (P3) | FromValue: 16 impls, type mismatch errors, overflow errors | SQL type → Value coalescing (INT, FLOAT, NVARCHAR, VARBINARY, UNIQUEIDENTIFIER, DECIMAL, DATETIME2, BIT, NULL), mixed-type row |
| `metadata.rs` | US4 (P4) | — | Column metadata inspection, get_by_name (case-insensitive), out-of-bounds, next_column, into_values, len/is_empty |
| `parameterized.rs` | US5 (P5) | — | Arithmetic params, injection safety, NULL params, multi-type params, param count mismatch, streaming variant |
| `prepared.rs` | US6 (P6) | — | Prepare + execute, multiple executions, close, deferred unprepare on drop, streaming execute |
| `transactions.rs` | US7 (P7) | — | Commit persists, rollback reverts, ReadUncommitted, Snapshot (conditional skip), deferred rollback on drop, parameterized within txn, prepared within txn |

## FR Coverage Matrix

| FR | Covered By | Test Count |
|----|-----------|-----------|
| FR-001 | All files | 52 (one per acceptance scenario) |
| FR-002 | `common/mod.rs` + all files | Env gate + skip logic |
| FR-003 | Inherited | `cargo btest` compatibility |
| FR-004 | All files | All in `mssql-rs/tests/` |
| FR-005 | File mapping above | 7 files, 1 per user story |
| FR-006 | `connection.rs`, `type_system.rs`, `parameterized.rs` | Variant-specific `matches!` assertions |
| FR-007 | `type_system.rs` | 16 `FromValue` impl tests |
| FR-008 | `type_system.rs` | 13 `Value` variant coalescing tests |
| FR-009 | `parameterized.rs` | SQL injection pattern test |
| FR-010 | `prepared.rs`, `transactions.rs` | Deferred unprepare + deferred rollback |
| FR-011 | `streaming.rs` | Multi-result-set with 2+ sets |
| FR-012 | `metadata.rs` | Column name, type, nullability |
| FR-013 | Server-dependent (any file) | `Client::cancel()` test |
| FR-014 | `connection.rs` | Connection Timeout, Query Timeout |
| FR-015 | Inherited | `cargo bclippy` with `-D warnings` |

## Dev-Dependencies Change

```toml
# mssql-rs/Cargo.toml [dev-dependencies] — additions
dotenv = "0.15"
tracing-subscriber = { version = "0.3", features = ["fmt"] }
futures = "0.3"
```
