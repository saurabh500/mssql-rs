// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use futures::StreamExt;
use mssql_rs::{DataType, Error, Value};

#[tokio::test]
async fn test_metadata_column_names_and_types() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client
        .query("SELECT CAST(1 AS INT) AS id, N'alice' AS name")
        .await
        .unwrap();

    let meta = rs.metadata().to_vec();
    assert_eq!(meta.len(), 2);
    assert_eq!(meta[0].name, "id");
    assert!(matches!(meta[0].data_type, DataType::Int));
    assert_eq!(meta[1].name, "name");
    assert!(matches!(meta[1].data_type, DataType::String { .. }));

    // consume the stream to avoid partial-read issues
    while rs.next().await.is_some() {}
}

#[tokio::test]
async fn test_get_by_name_case_insensitive() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client.query("SELECT CAST(1 AS INT) AS id").await.unwrap();

    let row = rs.next().await.unwrap().unwrap();
    let lower: i64 = row.get_by_name("id").unwrap();
    let upper: i64 = row.get_by_name("ID").unwrap();
    let mixed: i64 = row.get_by_name("Id").unwrap();
    assert_eq!(lower, 1);
    assert_eq!(upper, 1);
    assert_eq!(mixed, 1);
}

#[tokio::test]
async fn test_get_by_name_nonexistent() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client.query("SELECT CAST(1 AS INT) AS id").await.unwrap();

    let row = rs.next().await.unwrap().unwrap();
    let result: Result<i64, _> = row.get_by_name("nonexistent");
    assert!(
        matches!(result, Err(Error::TypeConversion(_))),
        "expected error for nonexistent column, got: {result:?}"
    );
}

#[tokio::test]
async fn test_get_out_of_bounds() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client.query("SELECT 1 AS a, 2 AS b, 3 AS c").await.unwrap();

    let row = rs.next().await.unwrap().unwrap();
    assert_eq!(row.len(), 3);
    let result: Result<i64, _> = row.get(5);
    assert!(
        matches!(result, Err(Error::TypeConversion(_))),
        "expected out-of-bounds error, got: {result:?}"
    );
}

#[tokio::test]
async fn test_next_column_sequential() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client.query("SELECT 1 AS a, 2 AS b, 3 AS c").await.unwrap();

    let mut row = rs.next().await.unwrap().unwrap();
    let c1 = row.next_column().unwrap();
    assert!(c1.is_some());
    let c2 = row.next_column().unwrap();
    assert!(c2.is_some());
    let c3 = row.next_column().unwrap();
    assert!(c3.is_some());
    let c4 = row.next_column().unwrap();
    assert!(c4.is_none(), "expected None after 3 columns");
}

#[tokio::test]
async fn test_into_values() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client.query("SELECT 1 AS a, N'hello' AS b").await.unwrap();

    let row = rs.next().await.unwrap().unwrap();
    let vals = row.into_values();
    assert_eq!(vals.len(), 2);
    assert!(matches!(vals[0], Value::Int(_)));
    assert!(matches!(vals[1], Value::String(_)));
}

#[tokio::test]
async fn test_len_and_is_empty() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    // Non-empty row
    {
        let mut rs = client.query("SELECT 1 AS a, 2 AS b, 3 AS c").await.unwrap();
        let row = rs.next().await.unwrap().unwrap();
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());

        // Consume remaining rows before reusing the client
        while rs.next().await.is_some() {}
    }

    // Zero-column query → empty metadata → collect returns empty
    let rows = client.query_collect("IF 1=0 SELECT 1 AS a").await.unwrap();
    assert!(rows.is_empty());
}
