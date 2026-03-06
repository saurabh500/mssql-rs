# Quickstart: mssql-rs Integration Tests

**Feature**: 002-mssql-rs-integration-tests | **Date**: 2026-03-06

## Prerequisites

- Rust 1.90+ with `cargo-nextest` installed
- For server-dependent tests: a running SQL Server instance

## Running Tests

### Pure-logic tests only (no server required)

```bash
cargo nextest run -p mssql-rs
```

Connection string parsing and `FromValue` conversion tests run without any environment setup.

### Full test suite (with SQL Server)

```bash
# Option 1: Set env var directly
export MSSQL_RS_TEST_CONNECTION_STRING="Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword123;TrustServerCertificate=true"
cargo btest -p mssql-rs

# Option 2: Use .env file
echo 'MSSQL_RS_TEST_CONNECTION_STRING=Server=localhost,1433;Database=master;User Id=sa;Password=YourPassword123;TrustServerCertificate=true' > .env
cargo btest -p mssql-rs
```

### Run a specific test file

```bash
cargo nextest run -p mssql-rs --test connection
cargo nextest run -p mssql-rs --test transactions
```

### Enable tracing output for debugging

```bash
export ENABLE_TEST_TRACE=true
cargo nextest run -p mssql-rs --test type_system --success-output immediate
```

## Test Organization

| File | What it tests |
|------|--------------|
| `tests/common/mod.rs` | Shared helpers (not a test file) |
| `tests/connection.rs` | Connection string parsing, connect/close, basic queries |
| `tests/streaming.rs` | Row streaming, multi-result-set navigation |
| `tests/type_system.rs` | SQL type → `Value` coalescing, `FromValue` conversions |
| `tests/metadata.rs` | Column metadata, named/indexed access |
| `tests/parameterized.rs` | Parameterized queries, injection safety |
| `tests/prepared.rs` | Prepared statement lifecycle |
| `tests/transactions.rs` | Transaction commit/rollback, isolation levels |

## Writing a New Test

```rust
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use mssql_rs::{Client, Error, Value};

#[tokio::test]
async fn test_example_pure_logic() {
    // No server needed — test public API with in-memory data
    let err = Client::connect("InvalidString").await.unwrap_err();
    assert!(matches!(err, Error::ConnectionStringInvalid(_)));
}

#[tokio::test]
async fn test_example_server_dependent() {
    let Some(conn_str) = common::connection_string() else {
        eprintln!("MSSQL_RS_TEST_CONNECTION_STRING not set — skipping");
        return;
    };
    let mut client = Client::connect(&conn_str).await.unwrap();

    let rows = client.query_collect("SELECT 1 AS val").await.unwrap();
    assert_eq!(rows[0][0], Value::Int(1));

    client.close().await.unwrap();
}
```
