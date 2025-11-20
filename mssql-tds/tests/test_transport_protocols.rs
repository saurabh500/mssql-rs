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
        let (username, password) = get_db_credentials();

        let client_context = ClientContext {
            transport_context,
            user_name: username,
            password,
            database: "master".to_string(),
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::Strict,
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
        let pipe_name = format!(r"\\{}\pipe\sql\query", host);

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

    // =========================================================================
    // Unit Tests for TransportContext
    // =========================================================================

    #[test]
    fn test_transport_context_get_server_name() {
        // TCP
        let tcp_context = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert_eq!(tcp_context.get_server_name(), "localhost");

        // Named Pipe
        let np_context = TransportContext::NamedPipe {
            pipe_name: r"\\server\pipe\sql\query".to_string(),
        };
        assert_eq!(np_context.get_server_name(), "server");

        let np_local_context = TransportContext::NamedPipe {
            pipe_name: r"\\.\pipe\sql\query".to_string(),
        };
        assert_eq!(np_local_context.get_server_name(), "localhost");

        // Shared Memory
        let sm_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };
        assert_eq!(sm_context.get_server_name(), "localhost");

        let sm_named_context = TransportContext::SharedMemory {
            instance_name: "SQLEXPRESS".to_string(),
        };
        assert_eq!(sm_named_context.get_server_name(), "localhost");
    }

    #[test]
    fn test_transport_context_is_local() {
        // TCP - not local
        let tcp_context = TransportContext::Tcp {
            host: "remote-server".to_string(),
            port: 1433,
        };
        assert!(!tcp_context.is_local());

        // TCP - localhost
        let tcp_localhost = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert!(tcp_localhost.is_local());

        // TCP - 127.0.0.1
        let tcp_loopback = TransportContext::Tcp {
            host: "127.0.0.1".to_string(),
            port: 1433,
        };
        assert!(tcp_loopback.is_local());

        // Named Pipe with . (local)
        let np_local = TransportContext::NamedPipe {
            pipe_name: r"\\.\pipe\sql\query".to_string(),
        };
        assert!(np_local.is_local());

        // Named Pipe with remote server
        let np_remote = TransportContext::NamedPipe {
            pipe_name: r"\\remote-server\pipe\sql\query".to_string(),
        };
        assert!(!np_remote.is_local());

        // Shared Memory - always local
        let sm_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };
        assert!(sm_context.is_local());
    }

    #[test]
    fn test_transport_context_get_protocol() {
        use mssql_tds::connection::client_context::Protocol;

        // TCP
        let tcp_context = TransportContext::Tcp {
            host: "localhost".to_string(),
            port: 1433,
        };
        assert!(matches!(tcp_context.get_protocol(), Protocol::Tcp));

        // Named Pipe
        let np_context = TransportContext::NamedPipe {
            pipe_name: r"\\.\pipe\sql\query".to_string(),
        };
        assert!(matches!(np_context.get_protocol(), Protocol::NamedPipe));

        // Shared Memory
        let sm_context = TransportContext::SharedMemory {
            instance_name: String::new(),
        };
        assert!(matches!(
            sm_context.get_protocol(),
            Protocol::SharedMemory
        ));
    }
}
