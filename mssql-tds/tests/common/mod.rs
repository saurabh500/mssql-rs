// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
use std::default::Default;
use std::env;
use std::sync::Once;

use dotenv::dotenv;
use futures::StreamExt;
use mssql_tds::connection::client_context::TransportContext;
use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::core::{EncryptionOptions, TdsResult};
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::query::metadata::ColumnMetadata;
use mssql_tds::{
    connection::{client_context::ClientContext, tds_connection::TdsConnection},
    connection_provider::tds_connection_provider::TdsConnectionProvider,
    core::EncryptionSetting,
    query::result::{BatchResult, QueryResultType},
};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
// This module will have a lot of allow(dead_code). This is because some of the capabilities are being used only in
// some tests. Each test leads to a new binary being created. So if a test is not using parts of Common module, the functions in common
// will end up being analyzed as dead code. So we will allow dead code in this module.

#[allow(dead_code)]
static INIT: Once = Once::new();

#[allow(dead_code)]
pub fn init_tracing() {
    dotenv().ok();
    let enable_trace = env::var("ENABLE_TRACE")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap();
    if enable_trace {
        INIT.call_once(|| {
            // Initialize the global tracing subscriber
            let subscriber = FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("Setting default subscriber failed");
        });
    }
}

#[allow(dead_code)]
pub(crate) enum ExpectedQueryResultType {
    Update(u64),
    Result(u64),
}

#[allow(clippy::assertions_on_constants)]
pub async fn assert_matches_expected(qrt: QueryResultType<'_>, expected: &ExpectedQueryResultType) {
    match (qrt, expected) {
        (QueryResultType::ResultSet(_), ExpectedQueryResultType::Update(_)) => {
            assert!(false)
        }
        (QueryResultType::DmlResult(_), ExpectedQueryResultType::Result(_)) => {
            assert!(false)
        }
        (
            QueryResultType::ResultSet(result_set),
            ExpectedQueryResultType::Result(expected_row_count),
        ) => {
            let mut actual_rows: u64 = 0;
            println!("Columns: {:?}", result_set.get_metadata());
            let mut row_stream = result_set.into_row_stream().unwrap();
            while let Some(row) = row_stream.next().await {
                let mut unwrapped_row = row.unwrap();
                print!("Row {actual_rows:?}: ");
                while let Some(cell) = unwrapped_row.next().await {
                    print!("{:?},", cell.unwrap());
                }
                println!();
                actual_rows += 1;
            }
            row_stream.close().await.unwrap();
            assert_eq!(actual_rows, *expected_row_count);
        }
        (
            QueryResultType::DmlResult(rows_affected),
            ExpectedQueryResultType::Update(expected_row_count),
        ) => {
            assert_eq!(rows_affected, *expected_row_count);
        }
    }
}

pub fn create_context() -> ClientContext {
    dotenv().ok();
    ClientContext {
        transport_context: TransportContext::Tcp {
            host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: env::var("DB_PORT")
                .ok()
                .map(|v| v.parse::<u16>().expect("DB_PORT must be a valid u16"))
                .unwrap_or(1433),
        },
        user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
        password: env::var("SQL_PASSWORD")
            .or_else(|_| {
                std::fs::read_to_string("/tmp/password")
                    .map(|s| s.trim().to_string())
                    .map_err(|_| std::env::VarError::NotPresent)
            })
            .expect(
                "SQL_PASSWORD environment variable not set and /tmp/password could not be read",
            ),
        database: "master".to_string(),
        encryption_options: EncryptionOptions {
            mode: EncryptionSetting::On,
            trust_server_certificate: trust_server_certificate(),
            host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
        },
        ..Default::default()
    }
}

pub async fn begin_connection(client_context: ClientContext) -> Box<TdsConnection> {
    create_connection(client_context).await.unwrap()
}

#[allow(dead_code)]
pub async fn create_client(client_context: ClientContext) -> TdsResult<TdsClient> {
    let provider = TdsConnectionProvider {};
    let client = provider.create_client(client_context, None).await?;
    Ok(client)
}

pub async fn create_connection(context: ClientContext) -> TdsResult<Box<TdsConnection>> {
    let provider = TdsConnectionProvider {};
    let connection_result = provider.create_connection(context, None).await?;
    Ok(Box::new(connection_result))
}

pub async fn validate_results(
    batch_result: BatchResult<'_>,
    expected_results: &[ExpectedQueryResultType],
) {
    let mut query_result_stream = batch_result.stream_results();
    let mut expected_index = 0;
    println!("Before looping.");
    while let Some(query_result_type) = query_result_stream.next().await {
        println!("Current index {expected_index:?}");
        assert!(expected_index < expected_results.len());
        let qrt = query_result_type.unwrap();
        assert_matches_expected(qrt, &expected_results[expected_index]).await;
        expected_index += 1;
    }
    query_result_stream.close().await.unwrap();
}

pub async fn run_query_and_check_results(
    connection: &mut TdsConnection,
    query: String,
    expected_results: &[ExpectedQueryResultType],
) {
    let results = connection.execute(query, None, None).await;
    validate_results(results.unwrap(), expected_results).await;
}

#[allow(dead_code)]
pub async fn connect_query_and_validate(
    query: String,
    expected_results: &[ExpectedQueryResultType],
) {
    let context: ClientContext = create_context();
    let mut connection = begin_connection(context).await;
    run_query_and_check_results(&mut connection, query, expected_results).await;
}

// Returns the first column of the first row of the result set, and drains the resultset.
#[allow(dead_code)]
pub async fn get_scalar_value<'a, 'n>(
    batch_result: BatchResult<'n>,
) -> TdsResult<Option<ColumnValues>>
where
    'n: 'a,
{
    let mut result = None;
    let mut query_result_stream = batch_result.stream_results();

    while let Some(query_result_type) = query_result_stream.next().await {
        let qrt = query_result_type.unwrap();
        match qrt {
            QueryResultType::DmlResult(_) => {
                // Do Nothing. Skip;
            }
            QueryResultType::ResultSet(rs) => {
                let mut rowstream = rs.into_row_stream().unwrap();
                while let Some(row) = rowstream.next().await {
                    let mut unwrapped_row = row.unwrap();

                    if let Some(cell) = unwrapped_row.next().await {
                        result = Some(cell.unwrap());
                    }
                    if result.is_some() {
                        break;
                    }
                }
                rowstream.close().await?;
            }
        }
        if result.is_some() {
            query_result_stream.close().await?;
            break;
        }
    }

    Ok(result)
}

// Returns the first row of the result set, and drains the resultset.
#[allow(dead_code)]
pub async fn get_first_row<'a, 'n>(
    batch_result: BatchResult<'n>,
) -> TdsResult<(Vec<ColumnMetadata>, Vec<ColumnValues>)>
where
    'n: 'a,
{
    let mut result: Vec<ColumnValues> = Vec::new();
    let mut metadata: Vec<ColumnMetadata> = Vec::new();
    let mut query_result_stream = batch_result.stream_results();

    while let Some(query_result_type) = query_result_stream.next().await {
        let qrt = query_result_type.unwrap();
        match qrt {
            QueryResultType::DmlResult(_) => {
                // Do Nothing. Skip;
            }
            QueryResultType::ResultSet(rs) => {
                metadata.append(&mut rs.get_metadata().clone());
                let mut rowstream = rs.into_row_stream().unwrap();
                while let Some(row) = rowstream.next().await {
                    let mut unwrapped_row = row.unwrap();
                    while let Some(cell) = unwrapped_row.next().await {
                        result.push(cell.unwrap());
                    }
                }
                rowstream.close().await?;
            }
        }
        if !result.is_empty() {
            query_result_stream.close().await?;
            break;
        }
    }

    Ok((metadata, result))
}

pub fn trust_server_certificate() -> bool {
    env::var("TRUST_SERVER_CERTIFICATE")
        .map(|v| v.parse().unwrap_or(false))
        .unwrap_or(false)
}
