// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

mod common;

use bigdecimal::BigDecimal;
use mssql_rs::{DateTime, Error, FromValue, Value};
use std::str::FromStr;
use uuid::Uuid;

// ── SQL type → Value coalescing (server-dependent) ──────────────────

#[tokio::test]
async fn test_value_int() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(42 AS INT) AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::Int(_)));
    let v: i64 = rows[0][0].clone().try_into_typed().unwrap();
    assert_eq!(v, 42);
}

#[tokio::test]
async fn test_value_float() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(1.23 AS FLOAT) AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::Float(_)));
    let v: f64 = rows[0][0].clone().try_into_typed().unwrap();
    assert!((v - 1.23).abs() < 0.001);
}

#[tokio::test]
async fn test_value_string() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT N'hello' AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::String(_)));
    let v: String = rows[0][0].clone().try_into_typed().unwrap();
    assert_eq!(v, "hello");
}

#[tokio::test]
async fn test_value_binary() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(0xDEADBEEF AS VARBINARY(4)) AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::Binary(_)));
    let v: Vec<u8> = rows[0][0].clone().try_into_typed().unwrap();
    assert_eq!(v, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

#[tokio::test]
async fn test_value_uuid() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect(
            "SELECT CAST('6F9619FF-8B86-D011-B42D-00CF4FC964FF' AS UNIQUEIDENTIFIER) AS val",
        )
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::Uuid(_)));
    let v: Uuid = rows[0][0].clone().try_into_typed().unwrap();
    assert_eq!(
        v,
        Uuid::parse_str("6F9619FF-8B86-D011-B42D-00CF4FC964FF").unwrap()
    );
}

#[tokio::test]
async fn test_value_decimal() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(12345.6789 AS DECIMAL(18,4)) AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::Decimal(_)));
    let v: BigDecimal = rows[0][0].clone().try_into_typed().unwrap();
    let expected = BigDecimal::from_str("12345.6789").unwrap();
    assert_eq!(v, expected);
}

#[tokio::test]
async fn test_value_datetime() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST('2026-03-06T14:30:00' AS DATETIME2) AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::DateTime(_)));
    let v: DateTime = rows[0][0].clone().try_into_typed().unwrap();
    assert_eq!(v.year, Some(2026));
    assert_eq!(v.month, Some(3));
    assert_eq!(v.day, Some(6));
    assert_eq!(v.hour, Some(14));
    assert_eq!(v.minute, Some(30));
    assert_eq!(v.second, Some(0));
}

#[tokio::test]
async fn test_value_bool() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(1 AS BIT) AS val")
        .await
        .unwrap();
    assert!(matches!(rows[0][0], Value::Bool(_)));
    let v: bool = rows[0][0].clone().try_into_typed().unwrap();
    assert!(v);
}

// ── NULL handling ───────────────────────────────────────────────────

#[tokio::test]
async fn test_null_option() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(NULL AS NVARCHAR(50)) AS val")
        .await
        .unwrap();
    let v: Option<String> = rows[0][0].clone().try_into_typed().unwrap();
    assert!(v.is_none());
}

#[tokio::test]
async fn test_null_non_optional_error() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(NULL AS NVARCHAR(50)) AS val")
        .await
        .unwrap();
    let result: Result<String, _> = rows[0][0].clone().try_into_typed();
    assert!(
        matches!(result, Err(Error::TypeConversion(_))),
        "expected TypeConversion error for NULL → String, got: {result:?}"
    );
}

// ── Type mismatch & overflow errors ─────────────────────────────────

#[tokio::test]
async fn test_type_mismatch_error() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client.query_collect("SELECT 'hello' AS val").await.unwrap();
    let result: Result<i64, _> = rows[0][0].clone().try_into_typed();
    assert!(
        matches!(result, Err(Error::TypeConversion(_))),
        "expected TypeConversion error for String → i64, got: {result:?}"
    );
}

#[tokio::test]
async fn test_narrowing_overflow_error() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(9223372036854775807 AS BIGINT) AS val")
        .await
        .unwrap();
    let result: Result<i32, _> = rows[0][0].clone().try_into_typed();
    assert!(
        matches!(result, Err(Error::TypeConversion(_))),
        "expected TypeConversion for i64::MAX → i32, got: {result:?}"
    );
}

// ── Mixed-type row ──────────────────────────────────────────────────

#[tokio::test]
async fn test_mixed_type_row() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(1 AS INT) AS a, N'hello' AS b, CAST(1.23 AS FLOAT) AS c, CAST(1 AS BIT) AS d")
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Int(_)));
    assert!(matches!(rows[0][1], Value::String(_)));
    assert!(matches!(rows[0][2], Value::Float(_)));
    assert!(matches!(rows[0][3], Value::Bool(_)));
}

// ── FromValue coverage (FR-007) ─────────────────────────────────────

#[tokio::test]
async fn test_from_value_all_int_widths() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(42 AS INT) AS val")
        .await
        .unwrap();

    let val = rows[0][0].clone();
    assert_eq!(i8::from_value(val.clone()).unwrap(), 42i8);
    assert_eq!(i16::from_value(val.clone()).unwrap(), 42i16);
    assert_eq!(i32::from_value(val.clone()).unwrap(), 42i32);
    assert_eq!(i64::from_value(val.clone()).unwrap(), 42i64);
    assert_eq!(u8::from_value(val.clone()).unwrap(), 42u8);
    assert_eq!(u16::from_value(val.clone()).unwrap(), 42u16);
    assert_eq!(u32::from_value(val).unwrap(), 42u32);
}

#[tokio::test]
async fn test_from_value_floats() {
    common::init_tracing();
    common::skip_if_no_server!();

    let mut client = common::require_connect().await;
    let rows = client
        .query_collect("SELECT CAST(2.5 AS FLOAT) AS val")
        .await
        .unwrap();

    let val = rows[0][0].clone();
    let f32_val = f32::from_value(val.clone()).unwrap();
    assert!((f32_val - 2.5f32).abs() < 0.001);
    let f64_val = f64::from_value(val).unwrap();
    assert!((f64_val - 2.5f64).abs() < 0.001);
}

/// Helper trait to convert `Value` into typed results using `FromValue`.
trait TryIntoTyped<T> {
    fn try_into_typed(self) -> Result<T, Error>;
}

impl<T: FromValue> TryIntoTyped<T> for Value {
    fn try_into_typed(self) -> Result<T, Error> {
        T::from_value(self)
    }
}
