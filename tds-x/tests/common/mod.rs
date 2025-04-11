#[cfg(test)]
use std::env;

use dotenv::dotenv;
use futures::StreamExt;
use tds_x::connection::client_context::TransportContext;
use tds_x::core::TdsResult;
use tds_x::{
    connection::{client_context::ClientContext, tds_connection::TdsConnection},
    connection_provider::tds_connection_provider::TdsConnectionProvider,
    core::EncryptionSetting,
    query::result::{BatchResult, QueryResultType},
};

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
        (QueryResultType::Update(_), ExpectedQueryResultType::Result(_)) => {
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
                print!("Row {:?}: ", actual_rows);
                while let Some(cell) = unwrapped_row.next().await {
                    print!("{:?},", cell.unwrap().get_value());
                }
                println!();
                actual_rows += 1;
            }
            assert_eq!(actual_rows, *expected_row_count);
        }
        (
            QueryResultType::Update(rows_affected),
            ExpectedQueryResultType::Update(expected_row_count),
        ) => {
            assert_eq!(rows_affected, *expected_row_count as i64);
        }
    }
}

pub fn create_context() -> ClientContext {
    dotenv().ok();
    ClientContext {
        transport_context: TransportContext::Tcp {
            host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: 1433,
        },
        user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
        password: env::var("SQL_PASSWORD").expect("SQL_PASSWORD environment variable not set"),
        database: "master".to_string(),
        encryption: EncryptionSetting::On,
        ..Default::default()
    }
}

pub async fn begin_connection(client_context: &ClientContext) -> Box<TdsConnection> {
    create_connection(client_context).await.unwrap()
}

pub async fn create_connection(context: &ClientContext) -> TdsResult<Box<TdsConnection>> {
    let provider = TdsConnectionProvider {};
    let connection_result = provider.create_connection(context).await?;
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
        println!("Current index {:?}", expected_index);
        assert!(expected_index < expected_results.len());
        let qrt = query_result_type.unwrap();
        assert_matches_expected(qrt, &expected_results[expected_index]).await;
        expected_index += 1;
    }
}

pub async fn run_query_and_check_results<'a, 'n>(
    connection: &'a mut TdsConnection<'n>,
    query: String,
    expected_results: &[ExpectedQueryResultType],
) where
    'n: 'a,
{
    let results = connection.execute(query).await;
    validate_results(results.unwrap(), expected_results).await;
}

#[allow(dead_code)]
pub async fn connect_query_and_validate(
    query: String,
    expected_results: &[ExpectedQueryResultType],
) {
    let context: ClientContext = create_context();
    let mut connection = begin_connection(&context).await;
    run_query_and_check_results(&mut connection, query, expected_results).await;
}
