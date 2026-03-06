# Implementation Plan: mssql-rs Integration Tests

**Branch**: `002-mssql-rs-integration-tests` | **Date**: 2026-03-06 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/002-mssql-rs-integration-tests/spec.md`

## Summary

Integration test suite for the `mssql-rs` public API crate, covering 7 user stories with 52 acceptance scenarios across 15 functional requirements. Tests are organized by feature area (connection, streaming, type system, metadata, parameterized queries, prepared statements, transactions) in `mssql-rs/tests/`. Pure-logic tests (connection string parsing, `FromValue` conversions) run without infrastructure. Server-dependent tests are gated by `MSSQL_RS_TEST_CONNECTION_STRING` — when set, its value is the full ODBC connection string; when absent, those tests are skipped. A shared helper module provides connection setup, environment loading, and tracing initialization. Data isolation uses temporary tables and transaction rollback.

## Technical Context

**Language/Version**: Rust 1.90, Edition 2024
**Primary Dependencies**: `mssql-rs` (crate under test), `tokio` (async runtime + test-util), `dotenv` (env loading), `tracing-subscriber` (test diagnostics), `futures` (StreamExt for streaming tests)
**Storage**: N/A — tests target a live SQL Server via connection string
**Testing**: `cargo nextest` via `cargo btest`; CI profile in `.config/nextest.toml` (5 retries, JUnit XML)
**Target Platform**: Cross-platform (Linux, Windows, macOS) — same as `mssql-rs`
**Project Type**: Test suite (integration tests for a library crate)
**Performance Goals**: Full suite < 60s against local SQL Server; pure-logic tests < 2s
**Constraints**: No persistent database state; all data modifications scoped to temp tables or rolled-back transactions
**Scale/Scope**: ~1,500–2,000 LOC across 8 test files + 1 helper module; 52 test functions covering 15 FRs

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Layered Protocol Architecture | ✅ PASS | Tests exercise only the public `mssql-rs` API. No direct access to Transport/IO/Token layers. |
| II. Rust Core, Thin FFI Bindings | ✅ PASS | Pure Rust tests. No FFI involvement. |
| III. Zero-Warning Discipline | ✅ PASS | Tests compile under `cargo bclippy` with `-D warnings`. `cargo bfmt` enforced. |
| IV. Test Infrastructure | ✅ PASS | All test files in `mssql-rs/tests/` — correct placement for public API tests. Uses `cargo nextest`. Server-dependent tests load connection string from env/`.env` via `dotenv`. See note below. |
| V. Code Quality — No AI Slop | ✅ PASS | Tests are concise assertions, no verbose comments. Copyright header on every `.rs` file. |
| VI. High Performance | ✅ PASS | Not directly applicable to test code. Tests avoid unnecessary allocations in hot loops (row streaming verification). |

**Principle IV note**: The constitution states "Unit tests live in `#[cfg(test)]` inline modules for pure logic." The spec places pure-logic tests (connection string parsing, `FromValue`) in `tests/` rather than inline. This is correct: these tests exercise the *public API surface* from outside the crate, which is Rust's definition of integration tests. The existing inline `#[cfg(test)]` modules in `mssql-rs/src/` test *private implementation details* (e.g., `parse_connection_string`). Both coexist without conflict.

**Gate Result**: ALL PASS — proceed to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/002-mssql-rs-integration-tests/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

*No `contracts/` directory — integration tests do not expose external interfaces.*

### Source Code (repository root)

```text
mssql-rs/
├── Cargo.toml                   # Updated: add dotenv, tracing-subscriber, futures to dev-dependencies
└── tests/
    ├── common/
    │   └── mod.rs               # Shared helpers: env gate, connection setup, tracing init
    ├── connection.rs            # US1: connection string parsing, connect, close, basic query
    ├── streaming.rs             # US2: row streaming, multi-result-set, collect_rows
    ├── type_system.rs           # US3: Value coalescing, FromValue for all 16 impls, NULL, errors
    ├── metadata.rs              # US4: column metadata, name lookup, row access patterns
    ├── parameterized.rs         # US5: sp_executesql params, injection safety, NULL params
    ├── prepared.rs              # US6: prepare/execute/close lifecycle, deferred unprepare
    └── transactions.rs          # US7: begin/commit/rollback, isolation levels, deferred rollback
```

**Structure Decision**: One test file per user story, plus a shared `common/` helper module. Follows the pattern established by `mssql-tds/tests/` (which uses `common/mod.rs` for connection setup). No sub-crates or workspace changes needed — `mssql-rs` is already a workspace member.

## Phase 0: Research Summary

**Output**: [research.md](research.md)

| ID | Decision | Key Finding |
|----|----------|-------------|
| R1 | Env gate via `MSSQL_RS_TEST_CONNECTION_STRING` | Single env var serves as both gate and connection string. Helper returns `Option<String>` — callers skip with early return when `None`. Fallback to `.env` via `dotenv`. |
| R2 | Dev-dependencies to add | `dotenv = "0.15"`, `tracing-subscriber` with `fmt` feature, `futures = "0.3"` for `StreamExt` in streaming tests. `tokio` already present with `test-util`. |
| R3 | Common module pattern | Follow `mssql-tds/tests/common/mod.rs`: `mod common;` declaration in each test file, `Once`-guarded tracing init, `create_client()` async helper. |
| R4 | Data isolation strategy | Temp tables (`#temp`) auto-drop on disconnect. Transaction tests use explicit rollback verification against temp tables. No persistent schema changes. |
| R5 | Snapshot isolation handling | Requires `ALLOW_SNAPSHOT_ISOLATION ON` at database level. Test should attempt and skip gracefully if the server returns an error (SQL error 3952). |
| R6 | Error variant assertions | Match on `Error` enum variants (e.g., `Error::ConnectionStringInvalid(_)`) using `matches!` macro. Avoids string-matching fragility. |
| R7 | Test naming convention | `#[tokio::test]` functions named `test_<feature>_<scenario>`. No `#[test]` wrapper modules — nextest discovers `#[tokio::test]` directly. |

## Phase 1: Design Summary

**Outputs**: [data-model.md](data-model.md) | [quickstart.md](quickstart.md)

### Key Design Decisions

1. **Helper module provides two connection functions**: `try_connect()` returns `Option<Client>` (returns `None` when env var absent, used by server-dependent tests) and `require_connect()` panics on missing env var (used in tests that must not be skipped).

2. **Skip macro for server-dependent tests**: A `skip_if_no_server!()` macro calls `try_connect()` and returns from the test function if `None`. Keeps test bodies clean.

3. **One file per user story**: Each file is independently compilable and runnable. No cross-file dependencies beyond `common/`.

4. **No mock server**: All server-dependent tests target a live SQL Server. The `mssql-mock-tds` dev-dependency is retained for future use but not used in these integration tests.

5. **Copyright header**: Every test file and the common module include the Microsoft copyright header per Principle V.

## Constitution Check — Post-Design

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Layered Protocol Architecture | ✅ PASS | Tests use only `mssql_rs::*` public API. No `mssql_tds` imports in test code. |
| II. Rust Core, Thin FFI Bindings | ✅ PASS | Pure Rust tests; no FFI. |
| III. Zero-Warning Discipline | ✅ PASS | `#[allow(dead_code)]` on common helpers (per `mssql-tds` pattern — each test binary only uses a subset). All other code warning-free. |
| IV. Test Infrastructure | ✅ PASS | Integration tests in `tests/`. `cargo nextest` runner. `dotenv` for env loading. Server tests gated by env var. |
| V. Code Quality — No AI Slop | ✅ PASS | Test assertions are direct. No explanatory comments on obvious steps. |
| VI. High Performance | ✅ PASS | N/A for test code. |

**Gate Result**: ALL PASS — ready for Phase 2 (task generation via `/speckit.tasks`).
