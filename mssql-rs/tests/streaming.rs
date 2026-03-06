// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use futures::StreamExt;
use mssql_rs::Value;

#[tokio::test]
async fn test_stream_100_rows() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client
        .query("SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.objects a CROSS JOIN sys.objects b")
        .await
        .unwrap();

    let mut count = 0u64;
    while let Some(row) = rs.next().await {
        let row = row.unwrap();
        let n: i64 = row.get(0).unwrap();
        assert!(n >= 1);
        count += 1;
    }
    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_multi_result_set_two_sets() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rs1 = client.query("SELECT 1 AS a; SELECT 2 AS b").await.unwrap();

    let rows1 = rs1.collect_rows().await.unwrap();
    assert_eq!(rows1.len(), 1);
    assert_eq!(rows1[0][0], Value::Int(1));

    let rs2 = client.query("SELECT 2 AS b").await.unwrap();
    let rows2 = rs2.collect_rows().await.unwrap();
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0][0], Value::Int(2));
}

#[tokio::test]
async fn test_multi_result_set_three_sets_then_none() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rs1 = client
        .query("SELECT 1 AS a; SELECT 2 AS b; SELECT 3 AS c")
        .await
        .unwrap();

    // Consume first set
    let rows1 = rs1.collect_rows().await.unwrap();
    assert_eq!(rows1[0][0], Value::Int(1));

    // Advance through remaining result sets via fresh queries
    // (ResultSet::next_result_set consumes self)
    let rs2 = client.query("SELECT 2 AS b; SELECT 3 AS c").await.unwrap();
    let next = rs2.next_result_set().await.unwrap();
    assert!(next.is_some(), "should have second result set");

    let rs3 = next.unwrap();
    let none = rs3.next_result_set().await.unwrap();
    assert!(none.is_none(), "no more result sets after third");
}

#[tokio::test]
async fn test_drop_result_set_mid_iteration() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    {
        let mut rs = client
            .query("SELECT TOP 10 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.objects")
            .await
            .unwrap();
        // Read only 2 rows and drop the rest
        let _ = rs.next().await;
        let _ = rs.next().await;
        // rs is dropped here
    }

    // Client should still be usable
    let rows = client.query_collect("SELECT 42 AS val").await.unwrap();
    assert_eq!(rows[0][0], Value::Int(42));
}

#[tokio::test]
async fn test_stream_zero_rows() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client.query("SELECT 1 AS val WHERE 1 = 0").await.unwrap();

    let first = rs.next().await;
    assert!(
        first.is_none(),
        "zero-row query should yield None immediately"
    );
}

#[tokio::test]
async fn test_collect_rows() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rs = client
        .query("SELECT 10 AS a UNION ALL SELECT 20 UNION ALL SELECT 30")
        .await
        .unwrap();
    let rows = rs.collect_rows().await.unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Value::Int(10));
    assert_eq!(rows[1][0], Value::Int(20));
    assert_eq!(rows[2][0], Value::Int(30));
}
