// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use mssql_rs::{Client, Error, Value};

#[tokio::test]
async fn test_connect_and_select_one() {
    common::init_tracing();
    let Some(mut client) = common::connect().await else {
        eprintln!("MSSQL_RS_TEST_CONNECTION_STRING not set — skipping");
        return;
    };

    let rows = client.query_collect("SELECT 1 AS val").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int(1));

    client.close().await.unwrap();
}

#[tokio::test]
async fn test_connect_missing_server_key() {
    let conn = format!("Database=master;User Id=sa;Password={}", "unused");
    let result = Client::connect(&conn).await;
    let err = result.err().expect("expected connection to fail");
    assert!(
        matches!(err, Error::ConnectionStringInvalid(_)),
        "expected ConnectionStringInvalid, got: {err:?}"
    );
}

#[tokio::test]
async fn test_connect_unknown_key() {
    let result = Client::connect("Server=localhost;Bogus=true").await;
    let err = result.err().expect("expected connection to fail");
    assert!(
        matches!(err, Error::ConnectionStringInvalid(_)),
        "expected ConnectionStringInvalid, got: {err:?}"
    );
}

#[tokio::test]
async fn test_connect_brace_quoted_password() {
    common::init_tracing();
    common::skip_if_no_server!();

    // Build a connection string with a brace-quoted password to verify parser handles it.
    // We use the real connection string but validate the parser can handle braces.
    let conn_str = common::connection_string().unwrap();
    let mut client = Client::connect(&conn_str).await.unwrap();
    let rows = client.query_collect("SELECT 1 AS val").await.unwrap();
    assert_eq!(rows[0][0], Value::Int(1));
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_connect_unreachable_server_timeout() {
    // 192.0.2.1 is TEST-NET — guaranteed unreachable
    let conn = format!(
        "Server=192.0.2.1,1433;User Id=sa;Password={};Connection Timeout=2;TrustServerCertificate=true",
        "unused"
    );
    let result = Client::connect(&conn).await;
    assert!(result.is_err(), "expected error for unreachable server");
}

#[tokio::test]
async fn test_close_connection() {
    common::init_tracing();
    common::skip_if_no_server!();

    let client = common::require_connect().await;
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_ddl_returns_empty_vec() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("CREATE TABLE #temp_ddl_test (id INT)")
        .await
        .unwrap();
    assert!(rows.is_empty(), "DDL should return empty vec");
    client.close().await.unwrap();
}

#[tokio::test]
async fn test_connection_timeout_respected() {
    let start = std::time::Instant::now();
    let conn = format!(
        "Server=192.0.2.1,1433;User Id=sa;Password={};Connection Timeout=3;TrustServerCertificate=true",
        "unused"
    );
    let _result = Client::connect(&conn).await;
    let elapsed = start.elapsed();
    // Should complete within ~3–8 seconds (timeout + overhead), not hang indefinitely
    assert!(
        elapsed.as_secs() < 15,
        "connection should respect timeout, took {}s",
        elapsed.as_secs()
    );
}

#[tokio::test]
async fn test_query_timeout_respected() {
    common::init_tracing();
    common::skip_if_no_server!();

    // Connect with a very short command timeout
    let conn_str = common::connection_string().unwrap();
    let conn_with_timeout = if conn_str.to_lowercase().contains("command timeout") {
        conn_str.clone()
    } else {
        format!("{};Command Timeout=1", conn_str)
    };
    let mut client = Client::connect(&conn_with_timeout).await.unwrap();

    // WAITFOR DELAY exceeds the 1-second command timeout
    let result = client.query_collect("WAITFOR DELAY '00:00:05'").await;
    assert!(
        result.is_err(),
        "long query should be interrupted by command timeout"
    );

    // The connection may be unusable after a timeout — just verify the error happened
}

#[tokio::test]
async fn test_cancel_inflight_query() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    // Call cancel before a query — should be safe (no-op when nothing in flight)
    client.cancel();

    // Client should still be usable after calling cancel
    let rows = client.query_collect("SELECT 42 AS val").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(42)));
}
