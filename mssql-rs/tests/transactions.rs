// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use mssql_rs::{Error, IsolationLevel, Value};

#[tokio::test]
async fn test_transaction_commit_persists() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    client
        .query_collect("CREATE TABLE #txn_commit_test (id INT)")
        .await
        .unwrap();

    let mut txn = client.begin_transaction().await.unwrap();
    let rs = txn
        .query("INSERT INTO #txn_commit_test VALUES (1)")
        .await
        .unwrap();
    rs.collect_rows().await.unwrap();
    txn.commit().await.unwrap();

    let rows = client
        .query_collect("SELECT id FROM #txn_commit_test")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(1)));
}

#[tokio::test]
async fn test_transaction_rollback_reverts() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    client
        .query_collect("CREATE TABLE #txn_rollback_test (id INT)")
        .await
        .unwrap();

    let mut txn = client.begin_transaction().await.unwrap();
    let rs = txn
        .query("INSERT INTO #txn_rollback_test VALUES (1)")
        .await
        .unwrap();
    rs.collect_rows().await.unwrap();
    txn.rollback().await.unwrap();

    let rows = client
        .query_collect("SELECT id FROM #txn_rollback_test")
        .await
        .unwrap();
    assert!(rows.is_empty(), "row should not be visible after rollback");
}

#[tokio::test]
async fn test_transaction_read_uncommitted() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut txn = client
        .begin_transaction_with_isolation(IsolationLevel::ReadUncommitted)
        .await
        .unwrap();

    let rs = txn.query("SELECT 1 AS val").await.unwrap();
    let rows = rs.collect_rows().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(1)));

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_transaction_snapshot() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let result = client
        .begin_transaction_with_isolation(IsolationLevel::Snapshot)
        .await;

    match result {
        Ok(txn) => {
            txn.commit().await.unwrap();
        }
        Err(Error::SqlServer { number: 3952, .. }) | Err(Error::QueryFailed(_)) => {
            // Snapshot isolation not enabled on this database — skip
        }
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

#[tokio::test]
async fn test_transaction_drop_deferred_rollback() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    client
        .query_collect("CREATE TABLE #txn_drop_test (id INT)")
        .await
        .unwrap();

    {
        let mut txn = client.begin_transaction().await.unwrap();
        let rs = txn
            .query("INSERT INTO #txn_drop_test VALUES (1)")
            .await
            .unwrap();
        rs.collect_rows().await.unwrap();
        // txn dropped without commit or rollback → deferred rollback
    }

    // drain_pending triggers the deferred rollback
    let rows = client
        .query_collect("SELECT id FROM #txn_drop_test")
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "row should be rolled back after drop, got {} rows",
        rows.len()
    );
}

#[tokio::test]
async fn test_transaction_parameterized_query() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut txn = client.begin_transaction().await.unwrap();

    let rs = txn
        .query_with_params(
            "SELECT @p1 + @p2 AS result",
            &[("@p1", Value::Int(10)), ("@p2", Value::Int(20))],
        )
        .await
        .unwrap();
    let rows = rs.collect_rows().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(30)));

    txn.commit().await.unwrap();
}

#[tokio::test]
async fn test_transaction_prepared_statement() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut txn = client.begin_transaction().await.unwrap();

    let mut stmt = txn
        .prepare("SELECT @p1 * 2 AS result", &[("@p1", Value::Int(0))])
        .await
        .unwrap();

    let rs = stmt.execute(&[Value::Int(5)]).await.unwrap();
    let rows = rs.collect_rows().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(10)));

    stmt.close().await.unwrap();
    txn.commit().await.unwrap();
}
