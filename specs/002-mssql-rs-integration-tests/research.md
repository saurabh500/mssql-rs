# Research: mssql-rs Integration Tests

**Feature**: 002-mssql-rs-integration-tests | **Date**: 2026-03-06

## R1: Environment Gate Pattern

**Decision**: Use `MSSQL_RS_TEST_CONNECTION_STRING` as both gate and value.

**Rationale**: A single env var eliminates the need for multiple variables (`DB_HOST`, `DB_PORT`, `DB_USERNAME`, `SQL_PASSWORD`) that the `mssql-tds` tests use. Since `mssql-rs` exposes `Client::connect(conn_str)` accepting an ODBC connection string, the env var value is passed directly. Fallback loading from `.env` via `dotenv` keeps local development convenient.

**Alternatives considered**:
- Separate `DB_HOST`/`DB_PORT`/`DB_USERNAME`/`SQL_PASSWORD` env vars (as in `mssql-tds/tests/common/mod.rs`): rejected because `mssql-rs` doesn't expose `ClientContext` — it only accepts a connection string.
- Feature flag gating (e.g., `--features live-tests`): rejected because env-var gating is simpler, doesn't require recompilation, and is the established pattern in this workspace (cf. `KERBEROS_TEST=1`).

**Implementation**: Helper function in `common/mod.rs`:
```rust
pub fn connection_string() -> Option<String> {
    dotenv().ok();
    std::env::var("MSSQL_RS_TEST_CONNECTION_STRING").ok()
}
```

## R2: Dev-Dependencies Required

**Decision**: Add `dotenv = "0.15"`, `tracing-subscriber` (with `fmt` feature), and `futures = "0.3"` to `mssql-rs/Cargo.toml` dev-dependencies.

**Rationale**:
- `dotenv`: loads `.env` file for `MSSQL_RS_TEST_CONNECTION_STRING`. Same version as `mssql-tds` dev-dependencies.
- `tracing-subscriber`: enables `ENABLE_TEST_TRACE=true` for debugging test failures. Same pattern as `mssql-tds/tests/common/mod.rs`.
- `futures`: provides `StreamExt` for `ResultSet` streaming tests (e.g., `result_set.next().await`). Already a regular dependency of `mssql-rs` but listed for completeness.

**Current `mssql-rs` dev-dependencies** (to preserve):
```toml
[dev-dependencies]
tokio = { version = "1.48.0", features = ["full", "test-util"] }
mssql-mock-tds = { path = "../mssql-mock-tds" }
```

**After**:
```toml
[dev-dependencies]
tokio = { version = "1.48.0", features = ["full", "test-util"] }
mssql-mock-tds = { path = "../mssql-mock-tds" }
dotenv = "0.15"
tracing-subscriber = { version = "0.3", features = ["fmt"] }
futures = "0.3"
```

## R3: Common Module Pattern

**Decision**: Follow `mssql-tds/tests/common/mod.rs` — each test file declares `mod common;`, the module lives at `tests/common/mod.rs`.

**Rationale**: This is the established pattern in the workspace. Rust's test harness compiles each file in `tests/` as a separate crate; the `mod common;` declaration makes helpers available without duplicating code across test binaries.

**Key differences from `mssql-tds` pattern**:
- `mssql-tds` helpers build a `ClientContext` from individual env vars. `mssql-rs` helpers return a connection string (or `None`).
- `mssql-tds` helpers use `TdsConnectionProvider` directly. `mssql-rs` helpers call `Client::connect()`.
- Both use `#[allow(dead_code)]` annotations because each test binary uses a different subset of helpers.

**Helper functions**:
| Function | Purpose |
|----------|---------|
| `connection_string()` | Returns `Option<String>` from env/dotenv |
| `connect().await` | Returns `Option<Client>` — `None` if no env var |
| `require_connect().await` | Returns `Client` or panics — for tests that must not be skipped |
| `init_tracing()` | Once-guarded tracing subscriber init (gated by `ENABLE_TEST_TRACE`) |

## R4: Data Isolation Strategy

**Decision**: Temp tables (`#temp`) for all data modifications. Transaction tests verify commit/rollback against temp tables.

**Rationale**:
- Temp tables are session-scoped — automatically dropped when the connection closes. No cleanup needed.
- Each test creates its own connection, so temp tables are inherently isolated between tests.
- No risk of test interference even under parallel nextest execution (each test gets a separate SQL session).

**Alternatives considered**:
- Persistent test database with setup/teardown scripts: rejected because it requires external infrastructure management and creates ordering dependencies.
- Shared connection with `SAVEPOINT`: rejected because nextest runs tests in parallel across processes; shared state would require synchronization.

**Pattern**:
```rust
client.query_collect("CREATE TABLE #test_data (id INT, name NVARCHAR(50))").await?;
client.query_collect("INSERT INTO #test_data VALUES (1, 'alice')").await?;
// ... assertions ...
// #test_data is auto-dropped when client is dropped
```

## R5: Snapshot Isolation Handling

**Decision**: Attempt `SET TRANSACTION ISOLATION LEVEL SNAPSHOT` and skip the test if the server returns SQL error 3952 ("Snapshot isolation is not supported").

**Rationale**: Snapshot isolation requires `ALTER DATABASE ... SET ALLOW_SNAPSHOT_ISOLATION ON` at the database level, which is a DBA configuration decision. Tests should not require specific database settings to pass.

**Implementation**: Check the error number in the `Error::SqlServer` variant:
```rust
match result {
    Err(Error::SqlServer { number: 3952, .. }) => return, // skip
    other => other.unwrap(),
}
```

## R6: Error Variant Assertions

**Decision**: Use `matches!` macro against `Error` enum variants.

**Rationale**: The `mssql-rs` error type is a structured enum with named variants. Matching on variants is more robust than string matching and survives message text changes.

**Pattern**:
```rust
let err = Client::connect("MissingEquals").await.unwrap_err();
assert!(matches!(err, Error::ConnectionStringInvalid(_)));
```

For `Error::SqlServer` with specific error numbers:
```rust
assert!(matches!(err, Error::SqlServer { number: 8144, .. }));
```

## R7: Test Naming Convention

**Decision**: Flat `#[tokio::test]` functions named `test_<feature>_<scenario>`. No nested `mod` blocks.

**Rationale**: `cargo nextest` discovers `#[tokio::test]` functions directly. Flat naming maximizes readability in `nextest` output and avoids unnecessary module nesting.

**Examples**:
- `test_connect_valid_connection_string`
- `test_connect_missing_server_key`
- `test_streaming_100_rows`
- `test_from_value_i64_overflow`
- `test_transaction_commit_persists`
