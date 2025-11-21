// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

#[cfg(test)]
mod transport_protocols {
    use dotenv::dotenv;
    use mssql_tds::connection::client_context::{ClientContext, TransportContext};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
    use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
    use mssql_tds::core::{EncryptionOptions, EncryptionSetting, TdsResult};
    use std::env;

    use crate::common::init_tracing;

    /// Helper function to get database credentials from environment
    fn get_db_credentials() -> (String, String) {
        dotenv().ok();
        let username = env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set");
        let password = env::var("SQL_PASSWORD")
            .or_else(|_| {
                std::fs::read_to_string("/tmp/password")
                    .map(|s| s.trim().to_string())
                    .map_err(|_| std::env::VarError::NotPresent)
            })
            .expect(
                "SQL_PASSWORD environment variable not set and /tmp/password could not be read",
            );
        (username, password)
    }

    /// Helper function to get trust server certificate setting
    fn trust_server_certificate() -> bool {
        dotenv().ok();
        env::var("TRUST_SERVER_CERTIFICATE")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap()
    }

    /// Helper function to get certificate hostname
    fn get_cert_hostname() -> Option<String> {
        dotenv().ok();
        env::var("CERT_HOST_NAME").ok()
    }

    /// Create a client with the specified transport context
    async fn create_client_with_transport(
        transport_context: TransportContext,
    ) -> TdsResult<TdsClient> {
        create_client_with_transport_and_encryption(transport_context, EncryptionSetting::Strict)
            .await
    }

    /// Create a client with the specified transport context and encryption mode
    async fn create_client_with_transport_and_encryption(
        transport_context: TransportContext,
        encryption_mode: EncryptionSetting,
    ) -> TdsResult<TdsClient> {
        let (username, password) = get_db_credentials();

        let client_context = ClientContext {
            transport_context,
            user_name: username,
            password,
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: encryption_mode,
                trust_server_certificate: trust_server_certificate(),
                host_name_in_cert: get_cert_hostname(),
            },
            ..Default::default()
        };

        let provider = TdsConnectionProvider {};
        provider.create_client(client_context, None).await
    }

    /// Execute a simple query and verify we get results
    async fn test_simple_query(client: &mut TdsClient) -> TdsResult<()> {
        let query = "SELECT @@VERSION AS version";
        client.execute(query.to_string(), None, None).await?;

        let mut has_results = false;
        loop {
            if let Some(resultset) = client.get_current_resultset() {
                has_results = true;
                while let Some(_row) = resultset.next_row().await? {
                    // We got at least one row, which is what we expect
                }
            }

            if !client.move_to_next().await? {
                break;
            }
        }

        client.close_query().await?;
        assert!(has_results, "Query should return results");
        Ok(())
    }

    // =========================================================================
    // Named Pipe Tests
    // =========================================================================

    #[tokio::test]
    #[cfg(windows)]
    async fn test_named_pipe_default_instance() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        let host = env::var("DB_HOST").expect("DB_HOST environment variable not set");

        // Test connecting to default instance using Named Pipe
        // Format: \\server\pipe\sql\query
        let pipe_name = format!(r"\\{host}\pipe\sql\query");

        let transport_context = TransportContext::NamedPipe { pipe_name };

        let mut client = create_client_with_transport(transport_context).await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg(windows)]
    async fn test_named_pipe_local_default_instance() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test connecting to local default instance using Named Pipe
        // Format: \\.\pipe\sql\query (local machine shorthand)
        let pipe_name = r"\\.\pipe\sql\query".to_string();

        let transport_context = TransportContext::NamedPipe { pipe_name };

        let mut client = create_client_with_transport(transport_context).await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg(windows)]
    #[ignore]
    async fn test_named_pipe_with_encryption_on() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test Named Pipe with Encryption=On (TDS 7.4, negotiated encryption)
        // This should use TLS wrapping within TDS packets
        let pipe_name = r"\\.\pipe\sql\query".to_string();

        let transport_context = TransportContext::NamedPipe { pipe_name };

        let mut client =
            create_client_with_transport_and_encryption(transport_context, EncryptionSetting::On)
                .await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    // =========================================================================
    // Shared Memory Tests
    // =========================================================================

    #[tokio::test]
    #[cfg(windows)]
    async fn test_shared_memory_default_instance() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test connecting to default instance using Shared Memory (lpc:)
        // This uses the default instance name (empty string)
        let transport_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };

        let mut client = create_client_with_transport(transport_context).await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg(windows)]
    async fn test_shared_memory_mssqlserver_instance() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test connecting using explicit MSSQLSERVER instance name
        // (which is the default instance)
        let transport_context = TransportContext::SharedMemory {
            instance_name: "MSSQLSERVER".to_string(),
        };

        let mut client = create_client_with_transport(transport_context).await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    #[tokio::test]
    #[cfg(windows)]
    #[ignore]
    async fn test_shared_memory_with_encryption_on() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test Shared Memory with Encryption=On (TDS 7.4, negotiated encryption)
        // This should use TLS wrapping within TDS packets
        let transport_context = TransportContext::SharedMemory {
            instance_name: "MSSQLSERVER".to_string(),
        };

        let mut client =
            create_client_with_transport_and_encryption(transport_context, EncryptionSetting::On)
                .await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    // =========================================================================
    // TCP Tests (for comparison and to ensure TCP still works)
    // =========================================================================

    #[tokio::test]
    async fn test_tcp_connection() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        let host = env::var("DB_HOST").expect("DB_HOST environment variable not set");
        let port = env::var("DB_PORT")
            .ok()
            .map(|v| v.parse::<u16>().expect("DB_PORT must be a valid u16"))
            .unwrap_or(1433);

        let transport_context = TransportContext::Tcp { host, port };

        let mut client = create_client_with_transport(transport_context).await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tcp_with_encryption_on() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test TCP with Encryption=On (TDS 7.4, negotiated encryption)
        // This should use TLS wrapping within TDS packets
        // If this test fails the same way as Named Pipes, it proves the issue
        // is in the TDS 7.4 SSL handling, not transport-specific
        let host = env::var("DB_HOST").expect("DB_HOST environment variable not set");
        let port = env::var("DB_PORT")
            .ok()
            .map(|v| v.parse::<u16>().expect("DB_PORT must be a valid u16"))
            .unwrap_or(1433);

        let transport_context = TransportContext::Tcp { host, port };

        let mut client =
            create_client_with_transport_and_encryption(transport_context, EncryptionSetting::On)
                .await?;
        test_simple_query(&mut client).await?;

        Ok(())
    }

    // =========================================================================
    // Concurrent Connection Tests
    // =========================================================================

    #[tokio::test]
    #[cfg(windows)]
    async fn test_concurrent_named_pipe_connections() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test opening 10 Named Pipe connections simultaneously
        // This verifies that our retry mechanism handles concurrent access correctly
        let pipe_name = r"\\.\pipe\sql\query".to_string();

        let mut handles = Vec::new();

        for i in 0..10 {
            let pipe_name_clone = pipe_name.clone();
            let handle = tokio::spawn(async move {
                let transport_context = TransportContext::NamedPipe {
                    pipe_name: pipe_name_clone,
                };

                let mut client = create_client_with_transport(transport_context)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to create client {i}"));

                test_simple_query(&mut client)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to execute query on client {i}"));

                println!("Client {i} completed successfully");
            });
            handles.push(handle);
        }

        // Wait for all connections to complete
        for handle in handles {
            handle.await.expect("Task panicked");
        }

        Ok(())
    }

    #[tokio::test]
    #[cfg(windows)]
    async fn test_concurrent_shared_memory_connections() -> TdsResult<()> {
        init_tracing();
        dotenv().ok();

        // Test opening 10 Shared Memory connections simultaneously
        // This verifies that our retry mechanism handles concurrent access correctly
        // and that SQLLocal pipes don't conflict

        let mut handles = Vec::new();

        for i in 0..10 {
            let handle = tokio::spawn(async move {
                let transport_context = TransportContext::SharedMemory {
                    instance_name: "MSSQLSERVER".to_string(),
                };

                let mut client = create_client_with_transport(transport_context)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to create client {i}"));

                test_simple_query(&mut client)
                    .await
                    .unwrap_or_else(|_| panic!("Failed to execute query on client {i}"));

                println!("Client {i} completed successfully");
            });
            handles.push(handle);
        }

        // Wait for all connections to complete
        for handle in handles {
            handle.await.expect("Task panicked");
        }

        Ok(())
    }
}
