// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use bigdecimal::BigDecimal;
use futures::StreamExt;
use mssql_rs::Value;
use std::str::FromStr;
use uuid::Uuid;

#[tokio::test]
async fn test_params_arithmetic() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect_with_params(
            "SELECT @p1 + @p2 AS result",
            &[("@p1", Value::Int(10)), ("@p2", Value::Int(20))],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(30)));
}

#[tokio::test]
async fn test_params_injection_safety() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let malicious = "'; DROP TABLE users; --";
    let rows = client
        .query_collect_with_params(
            "SELECT @p1 AS val",
            &[("@p1", Value::String(malicious.to_string()))],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let val = match &rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected Value::String, got: {other:?}"),
    };
    assert_eq!(val, malicious, "injection string must be returned verbatim");
}

#[tokio::test]
async fn test_params_null() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect_with_params("SELECT @p1 AS val", &[("@p1", Value::Null)])
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(
        matches!(rows[0][0], Value::Null),
        "expected Value::Null, got: {:?}",
        rows[0][0]
    );
}

#[tokio::test]
async fn test_params_multi_type() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let test_uuid = Uuid::parse_str("6F9619FF-8B86-D011-B42D-00CF4FC964FF").unwrap();
    let test_decimal = BigDecimal::from_str("123.45").unwrap();

    let rows = client
        .query_collect_with_params(
            "SELECT @p_int AS a, @p_str AS b, @p_float AS c, @p_bool AS d, @p_dec AS e, @p_uuid AS f, @p_bin AS g",
            &[
                ("@p_int", Value::Int(42)),
                ("@p_str", Value::String("hello".into())),
                ("@p_float", Value::Float(1.23)),
                ("@p_bool", Value::Bool(true)),
                ("@p_dec", Value::Decimal(test_decimal.clone())),
                ("@p_uuid", Value::Uuid(test_uuid)),
                ("@p_bin", Value::Binary(vec![0xCA, 0xFE])),
            ],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(42)));
    assert!(matches!(&rows[0][1], Value::String(s) if s == "hello"));
    assert!(matches!(rows[0][2], Value::Float(_)));
    assert!(matches!(rows[0][3], Value::Bool(true)));
    assert!(matches!(rows[0][4], Value::Decimal(_)));
    assert!(matches!(rows[0][5], Value::Uuid(_)));
    assert!(matches!(rows[0][6], Value::Binary(_)));
}

#[tokio::test]
async fn test_params_count_mismatch() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let result = client
        .query_collect_with_params("SELECT @p1 + @p2 AS result", &[("@p1", Value::Int(10))])
        .await;

    assert!(
        result.is_err(),
        "expected error for parameter count mismatch, got: {result:?}"
    );
}

#[tokio::test]
async fn test_params_streaming() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let mut rs = client
        .query_with_params(
            "SELECT @p1 AS val UNION ALL SELECT @p2",
            &[("@p1", Value::Int(1)), ("@p2", Value::Int(2))],
        )
        .await
        .unwrap();

    let mut count = 0;
    while let Some(row) = rs.next().await {
        let row = row.unwrap();
        let v: i64 = row.get(0).unwrap();
        assert!(v == 1 || v == 2);
        count += 1;
    }
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_unicode_supplementary_roundtrip() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;

    // Emoji, CJK, and supplementary plane characters
    let input = "Hello 🌍🌎🌏 你好世界 𝕳𝖊𝖑𝖑𝖔";
    let rows = client
        .query_collect_with_params(
            "SELECT @p1 AS val",
            &[("@p1", Value::String(input.to_string()))],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let val = match &rows[0][0] {
        Value::String(s) => s.clone(),
        other => panic!("expected Value::String, got: {other:?}"),
    };
    assert_eq!(val, input, "unicode string must round-trip exactly");
}
