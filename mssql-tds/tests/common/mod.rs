// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
use std::default::Default;
use std::env;
use std::sync::Once;

use dotenv::dotenv;
use mssql_tds::connection::client_context::TransportContext;
use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
use mssql_tds::core::{EncryptionOptions, TdsResult};
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::query::metadata::ColumnMetadata;
use mssql_tds::{
    connection::client_context::ClientContext,
    connection_provider::tds_connection_provider::TdsConnectionProvider, core::EncryptionSetting,
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

#[allow(dead_code)]
pub async fn create_client(client_context: ClientContext) -> TdsResult<TdsClient> {
    let provider = TdsConnectionProvider {};
    let client = provider.create_client(client_context, None).await?;
    Ok(client)
}

#[allow(dead_code)]
pub async fn begin_connection(client_context: ClientContext) -> TdsClient {
    create_client(client_context).await.unwrap()
}

pub async fn validate_results(
    client: &mut TdsClient,
    expected_results: &[ExpectedQueryResultType],
) -> TdsResult<()> {
    let mut expected_index = 0;
    println!("Before looping.");

    loop {
        if let Some(resultset) = client.get_current_resultset() {
            println!("Current index {expected_index:?}");
            assert!(expected_index < expected_results.len());

            let expected = &expected_results[expected_index];
            match expected {
                ExpectedQueryResultType::Result(expected_row_count) => {
                    let mut actual_rows: u64 = 0;
                    println!("Columns: {:?}", resultset.get_metadata());

                    while let Some(row) = resultset.next_row().await? {
                        print!("Row {actual_rows:?}: ");
                        for cell in row {
                            print!("{cell:?},");
                        }
                        println!();
                        actual_rows += 1;
                    }
                    assert_eq!(actual_rows, *expected_row_count);
                }
                ExpectedQueryResultType::Update(_expected_row_count) => {
                    // For DML statements, we just drain any rows if present
                    while resultset.next_row().await?.is_some() {}
                }
            }
            expected_index += 1;
        }

        if !client.move_to_next().await? {
            break;
        }
    }

    client.close_query().await?;
    Ok(())
}

pub async fn run_query_and_check_results(
    client: &mut TdsClient,
    query: String,
    expected_results: &[ExpectedQueryResultType],
) {
    client.execute(query, None, None).await.unwrap();
    validate_results(client, expected_results).await.unwrap();
}

#[allow(dead_code)]
pub async fn connect_query_and_validate(
    query: String,
    expected_results: &[ExpectedQueryResultType],
) {
    let context: ClientContext = create_context();
    let mut client = create_client(context).await.unwrap();
    run_query_and_check_results(&mut client, query, expected_results).await;
}

// Returns the first column of the first row of the result set, and drains the resultset.
#[allow(dead_code)]
pub async fn get_scalar_value(client: &mut TdsClient) -> TdsResult<Option<ColumnValues>> {
    let mut result = None;

    loop {
        if let Some(resultset) = client.get_current_resultset() {
            if let Some(row) = resultset.next_row().await? {
                if !row.is_empty() {
                    result = Some(row[0].clone());
                    break;
                }
            }
        }

        if !client.move_to_next().await? {
            break;
        }
    }

    client.close_query().await?;
    Ok(result)
}

// Returns the first row of the result set, and drains the resultset.
#[allow(dead_code)]
pub async fn get_first_row(
    client: &mut TdsClient,
) -> TdsResult<(Vec<ColumnMetadata>, Vec<ColumnValues>)> {
    let mut result: Vec<ColumnValues> = Vec::new();
    let mut metadata: Vec<ColumnMetadata> = Vec::new();

    loop {
        if let Some(resultset) = client.get_current_resultset() {
            if metadata.is_empty() {
                metadata = resultset.get_metadata().clone();
            }

            if let Some(row) = resultset.next_row().await? {
                result = row;
                break;
            }
        }

        if !client.move_to_next().await? {
            break;
        }
    }

    client.close_query().await?;
    Ok((metadata, result))
}

pub fn trust_server_certificate() -> bool {
    env::var("TRUST_SERVER_CERTIFICATE")
        .map(|v| v.parse().unwrap_or(false))
        .unwrap_or(false)
}
