# Implementation Plan: mssql-rs Public API Crate

**Branch**: `001-mssql-rs-public-api` | **Date**: 2026-03-05 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-mssql-rs-public-api/spec.md`

## Summary

New `mssql-rs` crate providing an ergonomic, async-first public API over `mssql-tds`. The crate wraps the internal TDS protocol library with: ODBC-style connection string parsing, two-level streaming (rows via `futures::Stream`, columns via an async iterator within each row), a simplified `Value` enum coalescing 25 wire-level `ColumnValues` variants into ~10 logical groups, an extensible `FromValue` conversion trait, `Vec<Vec<Value>>` convenience collection, parameterized queries, prepared statements, and transaction control. The crate owns its error type to decouple semver from `mssql-tds`.

## Technical Context

**Language/Version**: Rust 1.90, Edition 2024  
**Primary Dependencies**: `mssql-tds` (protocol core), `futures` (Stream trait), `bytes` (chunk streaming), `tokio` (async runtime), `thiserror` (error derive), `tracing` (diagnostics)  
**Storage**: N/A — thin wrapper over network protocol  
**Testing**: `cargo nextest` via `cargo btest`, `mssql-mock-tds` for unit tests, live SQL Server via `.env` for integration tests  
**Target Platform**: Cross-platform (Linux, Windows, macOS) — same as `mssql-tds`  
**Project Type**: Library (Rust crate)  
**Performance Goals**: < 5% latency overhead vs. direct `mssql-tds` usage; constant memory for streaming 1M+ rows  
**Constraints**: Zero-copy where feasible; no heap allocation on hot streaming path beyond what `mssql-tds` produces  
**Scale/Scope**: ~2,500–3,500 LOC for the wrapper crate; 8 public types, 17 functional requirements

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Layered Protocol Architecture | ✅ PASS | `mssql-rs` is a new layer above Client API; does not bypass Transport/IO/Token layers. Module organization follows `foo.rs` + `foo/` pattern. Uses `thiserror` and `TdsResult`-style alias. |
| II. Rust Core, Thin FFI Bindings | ✅ PASS | `mssql-rs` is a Rust consumer of `mssql-tds`, not an FFI binding. Consistent with principle — all protocol logic stays in `mssql-tds`. |
| III. Zero-Warning Discipline | ✅ PASS | Crate added to workspace `members`; `cargo bfmt`, `cargo bclippy`, `cargo btest` will cover it. `-D warnings` enforced. |
| IV. Test Infrastructure | ✅ PASS | Unit tests via `#[cfg(test)]` inline modules; integration tests in `mssql-rs/tests/`; uses `mssql-mock-tds` for mock server tests. |
| V. Code Quality — No AI Slop | ✅ PASS | Doc comments explain *why*; no filler. Copyright header on every `.rs` file. |
| VI. High Performance | ✅ PASS | Two-level streaming (row + column) avoids buffering. `RowWriter` trait enables zero-copy column decode. `Value` coalescing trades minor loss of wire-type fidelity for ergonomics (justified by spec). |

**Gate Result**: ALL PASS — proceed to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/001-mssql-rs-public-api/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   └── public-api.md    # Public API contract
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
mssql-rs/                        # New crate added to workspace
├── Cargo.toml                   # Dependencies: mssql-tds, futures, bytes, thiserror, tracing, tokio
├── src/
│   ├── lib.rs                   # Crate root — re-exports public API
│   ├── client.rs                # Client struct (connect, execute, prepare, transaction)
│   ├── client/
│   │   ├── connection.rs        # Connection string parsing → ClientContext
│   │   ├── query.rs             # Ad-hoc query execution
│   │   ├── parameterized.rs     # Parameterized query execution (sp_executesql)
│   │   ├── prepared.rs          # PreparedStatement lifecycle (sp_prepare/execute/unprepare)
│   │   └── transaction.rs       # Transaction (begin/commit/rollback, drop rollback)
│   ├── value.rs                 # Value enum (coalesced from ColumnValues)
│   ├── value/
│   │   └── from_column.rs       # ColumnValues → Value conversion
│   ├── row.rs                   # Row (streaming column cursor)
│   ├── result_set.rs            # ResultSet (Stream<Item=Result<Row>>, metadata, multi-result)
│   ├── column_stream.rs         # Binary chunk streaming for max-type columns
│   ├── from_value.rs            # FromValue trait + standard impls
│   ├── error.rs                 # Error enum wrapping mssql-tds Error
│   └── metadata.rs              # ColumnMetadata (public wrapper)
└── tests/
    ├── connection_test.rs       # Connection string parsing tests
    ├── query_test.rs            # Simple query + collect tests
    ├── streaming_test.rs        # Row/column streaming tests
    ├── type_system_test.rs      # Value conversion + FromValue tests
    ├── prepared_test.rs         # Prepared statement lifecycle tests
    └── transaction_test.rs      # Transaction begin/commit/rollback tests
```

**Structure Decision**: Single crate in the workspace following the existing `foo.rs` + `foo/` module pattern from Principle I. The crate is a thin wrapper — no sub-crates, no workspace restructuring.

## Phase 0: Research Summary

**Output**: [research.md](research.md)

| ID | Decision | Key Finding |
|----|----------|-------------|
| R1 | ODBC connection string parser | No parser exists in `mssql-tds` or FFI layers. `mssql-rs` must implement one. Python `dict_to_client_context` provides the key→field mapping reference. |
| R2 | Value enum coalescing | 25 `ColumnValues` → 13 `Value` variants via widening (int sizes → i64, float sizes → f64, money → Decimal, temporal variants → DateTime). |
| R3 | Row streaming via RowWriter | `RowWriter` trait is called column-by-column during decode. Custom `RowWriter` impl enables column-level streaming cursor. |
| R4 | Binary chunk streaming deferred | PLP columns (`max` types) are fully buffered in `read_plp_bytes()`. True chunk streaming requires `mssql-tds` changes (new RowWriter chunk methods + read_plp_streaming). API defined now; initial impl buffers internally. |
| R5 | Own error enum | `mssql_tds::Error` has 18+ variants. Public enum maps to 9 user-facing categories; internal variants boxed into `Protocol`. |
| R6 | Stream impl via manual poll_next | Avoids `async-stream` proc-macro dep. `ResultSet` delegates to `get_next_row()`. |
| R7 | Transaction drop → deferred rollback | Drop sets flag on Client; next operation issues rollback before proceeding. Avoids async-in-Drop problem. |
| R8 | PreparedStatement drop → deferred unprepare | Same deferred pattern as R7. Drop stores i32 handle in `Client::pending_unprepare`. |

## Phase 1: Design Summary

**Outputs**: [data-model.md](data-model.md) | [contracts/public-api.md](contracts/public-api.md) | [quickstart.md](quickstart.md)

### Key Design Decisions

1. **Client ownership model**: `ResultSet<'a>`, `PreparedStatement<'a>`, and `Transaction<'a>` each borrow `&'a mut Client`. This enforces single-active-operation at compile time.

2. **Row is owned, not borrowed**: `Row` owns its column data (extracted from the stream). No lifetime coupling to `Client`. This allows rows to outlive the result set iteration.

3. **Two access modes on Row**: Sequential (`next_column()` for streaming) and random-access (`get(index)` for convenience). Mutually exclusive per Row instance to avoid confusion.

4. **Deferred cleanup pattern**: Both `Transaction::drop` and `PreparedStatement::drop` set flags on `Client` for cleanup on next operation, avoiding async-in-Drop.

5. **FR-017 deferred**: Binary chunk streaming API is defined in the contract but initial implementation buffers internally. True streaming requires `mssql-tds` decoder changes.

## Constitution Check — Post-Design

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Layered Protocol Architecture | ✅ PASS | `mssql-rs` wraps `mssql-tds` Client API. No direct Transport/IO/Token access. Module layout uses `foo.rs` + `foo/` pattern. Error type uses `thiserror`. |
| II. Rust Core, Thin FFI Bindings | ✅ PASS | No FFI in `mssql-rs`. Pure Rust consumer of `mssql-tds`. |
| III. Zero-Warning Discipline | ✅ PASS | Crate joins workspace. All `cargo b*` commands will cover it. |
| IV. Test Infrastructure | ✅ PASS | Unit tests in `#[cfg(test)]` modules; integration tests in `mssql-rs/tests/`; `mssql-mock-tds` for mock tests; nextest runner. |
| V. Code Quality — No AI Slop | ✅ PASS | Data model and contracts are precise, not verbose. Doc comments planned for *why* not *what*. |
| VI. High Performance | ✅ PASS | Row streaming via `futures::Stream` with `poll_next`. Column streaming via `RowWriter` column cursor. Value coalescing is a one-time conversion per column, not a hot-path overhead. Deferred cleanup avoids async-in-Drop overhead. FR-017 deferred to avoid false streaming. |

**Gate Result**: ALL PASS — ready for Phase 2 (task generation via `/speckit.tasks`).
