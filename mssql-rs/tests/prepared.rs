// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use futures::StreamExt;
use mssql_rs::Value;

#[tokio::test]
async fn test_prepare_execute() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut stmt = client
        .prepare(
            "SELECT @p1 * @p2 AS result",
            &[("@p1", Value::Int(0)), ("@p2", Value::Int(0))],
        )
        .await
        .unwrap();

    let rows = stmt.execute(&[Value::Int(3), Value::Int(7)]).await.unwrap();
    let collected = rows.collect_rows().await.unwrap();
    assert_eq!(collected.len(), 1);
    assert!(matches!(collected[0][0], Value::Int(21)));

    stmt.close().await.unwrap();
}

#[tokio::test]
async fn test_prepare_execute_multiple_times() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut stmt = client
        .prepare("SELECT @p1 + 100 AS result", &[("@p1", Value::Int(0))])
        .await
        .unwrap();

    for i in 1..=3i64 {
        let rows = stmt.execute(&[Value::Int(i)]).await.unwrap();
        let collected = rows.collect_rows().await.unwrap();
        assert_eq!(collected.len(), 1);
        assert!(
            matches!(collected[0][0], Value::Int(v) if v == i + 100),
            "iteration {i}: expected Value::Int({}), got: {:?}",
            i + 100,
            collected[0][0]
        );
    }

    stmt.close().await.unwrap();
}

#[tokio::test]
async fn test_prepare_close() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let stmt = client
        .prepare("SELECT @p1 AS val", &[("@p1", Value::Int(0))])
        .await
        .unwrap();

    stmt.close().await.unwrap();

    // Client still usable after close
    let rows = client.query_collect("SELECT 1 AS check_val").await.unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_prepare_drop_deferred_unprepare() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    {
        let mut stmt = client
            .prepare("SELECT @p1 AS val", &[("@p1", Value::Int(0))])
            .await
            .unwrap();

        let rows = stmt.execute(&[Value::Int(99)]).await.unwrap();
        let collected = rows.collect_rows().await.unwrap();
        assert!(matches!(collected[0][0], Value::Int(99)));

        // stmt dropped here without close → deferred unprepare queued
    }

    // Subsequent query triggers drain_pending which cleans up the handle
    let rows = client.query_collect("SELECT 42 AS val").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(42)));
}

#[tokio::test]
async fn test_prepare_execute_streaming() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut stmt = client
        .prepare(
            "SELECT @p1 AS val UNION ALL SELECT @p1 + 1 UNION ALL SELECT @p1 + 2",
            &[("@p1", Value::Int(0))],
        )
        .await
        .unwrap();

    let mut rs = stmt.execute(&[Value::Int(10)]).await.unwrap();
    let mut values = Vec::new();
    while let Some(row) = rs.next().await {
        let row = row.unwrap();
        let v: i64 = row.get(0).unwrap();
        values.push(v);
    }
    assert_eq!(values, vec![10, 11, 12]);

    drop(rs);
    stmt.close().await.unwrap();
}
