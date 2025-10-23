// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Named Pipe connectivity tests for Windows

#[cfg(all(test, windows))]
mod tests {
    use dotenv::dotenv;
    use futures::StreamExt;
    use mssql_tds::connection::client_context::{ClientContext, TransportContext};
    use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
    use mssql_tds::core::{EncryptionOptions, EncryptionSetting, TdsResult};
    use mssql_tds::datatypes::column_values::ColumnValues;
    use mssql_tds::query::result::{BatchResult, QueryResultType};
    use std::env;

    fn init_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
    }

    fn create_named_pipe_context() -> ClientContext {
        dotenv().ok();
        init_tracing();

        let pipe_name = env::var("DB_NAMED_PIPE")
            .unwrap_or_else(|_| r"\\.\pipe\sql\query".to_string());
        let user_name = env::var("DB_USERNAME").unwrap_or_else(|_| "sa".to_string());
        let password = env::var("SQL_PASSWORD")
            .or_else(|_| std::fs::read_to_string("/tmp/password")
                .map(|s| s.trim().to_string())
                .map_err(|_| std::env::VarError::NotPresent))
            .expect("SQL_PASSWORD not set");

        ClientContext {
            transport_context: TransportContext::NamedPipe { pipe_name },
            database: "master".to_string(),
            user_name,
            password,
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: env::var("TRUST_SERVER_CERTIFICATE")
                    .unwrap_or_else(|_| "true".to_string())
                    .parse()
                    .unwrap_or(true),
                ..EncryptionOptions::default()
            },
            ..Default::default()
        }
    }

    async fn get_scalar_value(batch_result: BatchResult<'_>) -> TdsResult<Option<ColumnValues>> {
        let mut result = None;
        let mut stream = batch_result.stream_results();
        while let Some(qrt) = stream.next().await {
            match qrt? {
                QueryResultType::ResultSet(rs) => {
                    let mut row_stream = rs.into_row_stream()?;
                    if let Some(row) = row_stream.next().await {
                        let mut unwrapped_row = row?;
                        if let Some(cell) = unwrapped_row.next().await {
                            result = Some(cell?);
                        }
                    }
                    break;
                }
                _ => continue,
            }
        }
        Ok(result)
    }

    #[test]
    fn test_named_pipe_transport_context() {
        let pipe_name = r"\\.\pipe\sql\query".to_string();
        let context = TransportContext::NamedPipe {
            pipe_name: pipe_name.clone(),
        };
        assert_eq!(context.get_server_name(), &pipe_name);
    }

    #[tokio::test]
    async fn test_named_pipe_basic_connection() -> TdsResult<()> {
        let context = create_named_pipe_context();
        let provider = TdsConnectionProvider {};
        let connection = provider.create_connection(context, None).await;
        assert!(connection.is_ok(), "Failed to connect: {:?}", connection.err());
        Ok(())
    }

    #[tokio::test]
    async fn test_named_pipe_select_one() -> TdsResult<()> {
        let context = create_named_pipe_context();
        let provider = TdsConnectionProvider {};
        let mut connection = provider.create_connection(context, None).await?;

        let result = connection.execute("SELECT 1".to_string(), None, None).await?;
        let value = get_scalar_value(result).await?;

        match value.unwrap() {
            ColumnValues::Int(val) => assert_eq!(val, 1),
            other => panic!("Expected Int, got {:?}", other),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_named_pipe_multiple_queries() -> TdsResult<()> {
        let context = create_named_pipe_context();
        let provider = TdsConnectionProvider {};
        let mut connection = provider.create_connection(context, None).await?;

        for i in 1..=3 {
            let result = connection
                .execute(format!("SELECT {}", i), None, None)
                .await?;
            let value = get_scalar_value(result).await?;
            match value.unwrap() {
                ColumnValues::Int(val) => assert_eq!(val, i),
                _ => panic!("Expected Int"),
            }
        }
        Ok(())
    }
}

#[cfg(all(test, not(windows)))]
#[test]
fn named_pipes_windows_only() {
    println!("Named Pipes are Windows-only");
}
