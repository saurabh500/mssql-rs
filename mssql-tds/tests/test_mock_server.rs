// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Integration tests using the mock TDS server

#[cfg(test)]
mod mock_server_tests {
    use mssql_mock_tds::MockTdsServer;
    use mssql_tds::connection::client_context::{ClientContext, TransportContext};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
    use mssql_tds::core::{EncryptionOptions, EncryptionSetting};
    use tokio::sync::oneshot;
    use tracing_subscriber::FmtSubscriber;

    fn init_tracing() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
        let _ = subscriber; // Ignore if already initialized
    }

    /// Test basic connectivity to mock server
    #[tokio::test]
    async fn test_connect_to_mock_server() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();

        // Start mock server on a random port
        let server = MockTdsServer::new("127.0.0.1:0").await?;
        let server_addr = server.local_addr();

        // Run server in background
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle =
            tokio::spawn(async move { server.run_with_shutdown(shutdown_rx).await });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create client context pointing to mock server
        let context = ClientContext {
            transport_context: TransportContext::Tcp {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
            user_name: "sa".to_string(),
            password: "TestPassword123!".to_string(),
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::PreferOff,
                trust_server_certificate: true,
                host_name_in_cert: None,
            },
            ..Default::default()
        };

        // Connect to mock server
        let provider = TdsConnectionProvider {};
        let client = provider.create_client(context, None).await?;

        println!("Successfully connected to mock server at {}", server_addr);

        // Cleanup
        drop(client);
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;

        Ok(())
    }

    /// Test executing "SELECT 1" against mock server
    #[tokio::test]
    async fn test_execute_select_one() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();

        // Start mock server
        let server = MockTdsServer::new("127.0.0.1:0").await?;
        let server_addr = server.local_addr();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle =
            tokio::spawn(async move { server.run_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create and connect client
        let context = ClientContext {
            transport_context: TransportContext::Tcp {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
            user_name: "sa".to_string(),
            password: "TestPassword123!".to_string(),
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::PreferOff,
                trust_server_certificate: true,
                host_name_in_cert: None,
            },
            ..Default::default()
        };

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;

        // Execute query
        client.execute("SELECT 1".to_string(), None, None).await?;

        // Read results
        let mut row_count = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await? {
                row_count += 1;
                println!("Row: {:?}", row);

                // Verify we got the value 1
                assert_eq!(row.len(), 1);
                if let Some(value) = row.first() {
                    println!("Value: {:?}", value);
                }
            }
        }

        assert_eq!(row_count, 1, "Expected 1 row from SELECT 1");

        client.close_query().await?;
        client.close_connection().await?;

        // Cleanup
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;

        Ok(())
    }

    /// Test executing multiple queries
    #[tokio::test]
    async fn test_execute_multiple_queries() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();

        // Start mock server
        let server = MockTdsServer::new("127.0.0.1:0").await?;
        let server_addr = server.local_addr();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle =
            tokio::spawn(async move { server.run_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create and connect client
        let context = ClientContext {
            transport_context: TransportContext::Tcp {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
            user_name: "sa".to_string(),
            password: "TestPassword123!".to_string(),
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::PreferOff,
                trust_server_certificate: true,
                host_name_in_cert: None,
            },
            ..Default::default()
        };

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;

        // Execute first query
        client.execute("SELECT 1".to_string(), None, None).await?;
        client.close_query().await?;

        // Execute second query
        client.execute("SELECT 1".to_string(), None, None).await?;

        let mut row_count = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(_row) = resultset.next_row().await? {
                row_count += 1;
            }
        }

        assert_eq!(row_count, 1);
        client.close_query().await?;

        client.close_connection().await?;

        // Cleanup
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;

        Ok(())
    }

    /// Test connection reuse
    #[tokio::test]
    async fn test_connection_reuse() -> Result<(), Box<dyn std::error::Error>> {
        init_tracing();

        // Start mock server
        let server = MockTdsServer::new("127.0.0.1:0").await?;
        let server_addr = server.local_addr();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle =
            tokio::spawn(async move { server.run_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create context
        let context = ClientContext {
            transport_context: TransportContext::Tcp {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
            user_name: "sa".to_string(),
            password: "TestPassword123!".to_string(),
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::PreferOff,
                trust_server_certificate: true,
                host_name_in_cert: None,
            },
            ..Default::default()
        };

        let provider = TdsConnectionProvider {};

        // Create multiple clients (simulating connection reuse scenario)
        for i in 0..3 {
            println!("Connecting client {}", i);
            let mut client = provider.create_client(context.clone(), None).await?;

            client.execute("SELECT 1".to_string(), None, None).await?;
            client.close_query().await?;
            client.close_connection().await?;

            println!("Client {} completed successfully", i);
        }

        // Cleanup
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;

        Ok(())
    }

    /// Test custom query responses with multiple data types
    #[tokio::test]
    async fn test_custom_query_response() -> Result<(), Box<dyn std::error::Error>> {
        use mssql_mock_tds::{ColumnDefinition, ColumnValue, QueryResponse, Row, SqlDataType};

        init_tracing();

        // Start mock server
        let server = MockTdsServer::new("127.0.0.1:0").await?;
        let server_addr = server.local_addr();

        // Register a custom query response before starting the server
        let registry = server.query_registry();
        {
            let mut reg = registry.lock().await;
            reg.register(
                "SELECT CAST(1 AS BIGINT), 2, 3",
                QueryResponse::new(
                    vec![
                        ColumnDefinition::new("col1", SqlDataType::BigInt),
                        ColumnDefinition::new("col2", SqlDataType::Int),
                        ColumnDefinition::new("col3", SqlDataType::Int),
                    ],
                    vec![Row::new(vec![
                        ColumnValue::BigInt(1),
                        ColumnValue::Int(2),
                        ColumnValue::Int(3),
                    ])],
                ),
            );
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle =
            tokio::spawn(async move { server.run_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create and connect client
        let context = ClientContext {
            transport_context: TransportContext::Tcp {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
            user_name: "sa".to_string(),
            password: "TestPassword123!".to_string(),
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::PreferOff,
                trust_server_certificate: true,
                host_name_in_cert: None,
            },
            ..Default::default()
        };

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;

        // Execute the custom query
        client
            .execute("SELECT CAST(1 AS BIGINT), 2, 3".to_string(), None, None)
            .await?;

        // Read results
        let mut row_count = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await? {
                row_count += 1;
                println!("Row: {:?}", row);

                // Verify we got 3 columns
                assert_eq!(row.len(), 3, "Expected 3 columns");

                // Print values
                for (i, value) in row.iter().enumerate() {
                    println!("Column {}: {:?}", i, value);
                }
            }
        }

        assert_eq!(row_count, 1, "Expected 1 row");

        client.close_query().await?;
        client.close_connection().await?;

        // Cleanup
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;

        Ok(())
    }

    /// Test query response with NULL values
    #[tokio::test]
    async fn test_query_with_nulls() -> Result<(), Box<dyn std::error::Error>> {
        use mssql_mock_tds::{ColumnDefinition, ColumnValue, QueryResponse, Row, SqlDataType};

        init_tracing();

        // Start mock server
        let server = MockTdsServer::new("127.0.0.1:0").await?;
        let server_addr = server.local_addr();

        // Register a query with NULL values
        let registry = server.query_registry();
        {
            let mut reg = registry.lock().await;
            reg.register(
                "SELECT 1, NULL, 3",
                QueryResponse::new(
                    vec![
                        ColumnDefinition::new("", SqlDataType::Int),
                        ColumnDefinition::new("", SqlDataType::Int),
                        ColumnDefinition::new("", SqlDataType::Int),
                    ],
                    vec![Row::new(vec![
                        ColumnValue::Int(1),
                        ColumnValue::Null,
                        ColumnValue::Int(3),
                    ])],
                ),
            );
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle =
            tokio::spawn(async move { server.run_with_shutdown(shutdown_rx).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Create and connect client
        let context = ClientContext {
            transport_context: TransportContext::Tcp {
                host: server_addr.ip().to_string(),
                port: server_addr.port(),
            },
            user_name: "sa".to_string(),
            password: "TestPassword123!".to_string(),
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::PreferOff,
                trust_server_certificate: true,
                host_name_in_cert: None,
            },
            ..Default::default()
        };

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;

        // Execute query with NULL
        client
            .execute("SELECT 1, NULL, 3".to_string(), None, None)
            .await?;

        // Read results
        let mut row_count = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await? {
                row_count += 1;
                println!("Row with NULLs: {:?}", row);
                assert_eq!(row.len(), 3, "Expected 3 columns");
            }
        }

        assert_eq!(row_count, 1, "Expected 1 row");

        client.close_query().await?;
        client.close_connection().await?;

        // Cleanup
        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), server_handle).await;

        Ok(())
    }
}
