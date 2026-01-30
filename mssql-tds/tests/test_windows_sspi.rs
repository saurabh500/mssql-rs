// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Integration test for Windows Integrated Authentication (SSPI).
//!
//! This test requires:
//! - A local SQL Server instance listening on port 1433
//! - Windows Integrated Authentication enabled on SQL Server
//! - The current Windows user must have access to SQL Server
//!
//! Set SSPI_TEST=1 to enable this test.

#![cfg(windows)]

use std::env;

mod common;

use mssql_tds::connection::client_context::{ClientContext, TdsAuthenticationMethod};
use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::core::{EncryptionOptions, EncryptionSetting, TdsResult};

/// Test Windows SSPI token generation
#[test]
fn test_windows_sspi_context_creation() {
    use mssql_tds::security::{IntegratedAuthConfig, create_security_context};

    let config = IntegratedAuthConfig::new();
    let result = create_security_context(&config, "localhost", 1433);

    match result {
        Ok(ctx) => {
            assert_eq!(ctx.spn(), "MSSQLSvc/localhost:1433");
            assert!(!ctx.is_complete());
            println!(
                "✓ SSPI context created successfully with SPN: {}",
                ctx.spn()
            );
        }
        Err(e) => {
            panic!("Failed to create SSPI context: {:?}", e);
        }
    }
}

/// Test Windows SSPI token generation with Negotiate package
#[test]
fn test_windows_sspi_generate_initial_token() {
    use mssql_tds::security::{IntegratedAuthConfig, create_security_context};

    let config = IntegratedAuthConfig::new();
    let mut ctx =
        create_security_context(&config, "localhost", 1433).expect("Failed to create SSPI context");

    // Generate initial token
    let token = ctx.generate_token(None);

    match token {
        Ok(t) => {
            assert!(!t.data.is_empty(), "Token should not be empty");
            println!("✓ Generated SSPI token of {} bytes", t.data.len());
            println!("  First byte: 0x{:02x}", t.data[0]);
            println!("  Is complete: {}", t.is_complete);
        }
        Err(e) => {
            panic!("Failed to generate initial token: {:?}", e);
        }
    }
}

/// Full integration test: Connect to local SQL Server with Windows Integrated Auth
///
/// Set SSPI_TEST=1 to enable this test.
#[tokio::test]
#[ignore = "Requires local SQL Server with Windows Auth - set SSPI_TEST=1 to run"]
async fn test_windows_integrated_auth_connection() -> TdsResult<()> {
    if env::var("SSPI_TEST").is_err() {
        println!("Skipping SSPI integration test - set SSPI_TEST=1 to enable");
        return Ok(());
    }

    common::init_tracing();

    // Create client context for integrated authentication
    let mut context = ClientContext::default();
    context.database = "master".to_string();
    context.tds_authentication_method = TdsAuthenticationMethod::SSPI;
    context.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::On,
        trust_server_certificate: true,
        host_name_in_cert: None,
        server_certificate: None,
    };

    let provider = TdsConnectionProvider {};
    let datasource = "tcp:localhost,1433";

    println!(
        "Connecting to {} with Windows Integrated Auth...",
        datasource
    );

    let mut connection = provider.create_client(context, datasource, None).await?;
    println!("✓ Successfully connected using Windows Integrated Auth!");

    // Verify the authentication scheme
    let query = "SELECT auth_scheme FROM sys.dm_exec_connections WHERE session_id = @@SPID";
    connection.execute(query.to_string(), None, None).await?;

    if let Some(resultset) = connection.get_current_resultset()
        && let Some(row) = resultset.next_row().await?
    {
        let auth_scheme = format!("{:?}", row.first());
        println!("✓ Authentication scheme: {}", auth_scheme);
        // Should be NTLM or Kerberos
        assert!(
            auth_scheme.contains("NTLM") || auth_scheme.contains("Kerberos"),
            "Expected NTLM or Kerberos authentication scheme"
        );
    }

    connection.close_query().await?;
    println!("✓ Test completed successfully!");

    Ok(())
}

/// Full integration test: Connect to LocalDB with Windows Integrated Auth
///
/// LocalDB uses named pipes and only supports Windows Integrated Authentication.
/// Note: LocalDB typically does not support encryption, but we specify Strict here
/// to verify that the connection provider automatically overrides it to PreferOff
/// (matching ODBC behavior).
/// Set SSPI_TEST=1 to enable this test.
#[tokio::test]
#[ignore = "Requires LocalDB instance - set SSPI_TEST=1 to run"]
async fn test_localdb_integrated_auth_connection() -> TdsResult<()> {
    if env::var("SSPI_TEST").is_err() {
        println!("Skipping LocalDB integration test - set SSPI_TEST=1 to enable");
        return Ok(());
    }

    common::init_tracing();

    // Create client context for integrated authentication
    // Specify Strict encryption to verify automatic override to PreferOff for LocalDB
    let mut context = ClientContext::default();
    context.database = "master".to_string();
    context.tds_authentication_method = TdsAuthenticationMethod::SSPI;
    context.encryption_options = EncryptionOptions {
        mode: EncryptionSetting::Strict,
        trust_server_certificate: true,
        host_name_in_cert: None,
        server_certificate: None,
    };

    let provider = TdsConnectionProvider {};

    // LocalDB connection string using the special (localdb) syntax
    let datasource = "(localdb)\\MSSQLLocalDB";

    println!(
        "Connecting to {} with Windows Integrated Auth...",
        datasource
    );

    let mut connection = provider.create_client(context, datasource, None).await?;
    println!("✓ Successfully connected to LocalDB using Windows Integrated Auth!");

    // Verify the authentication scheme
    let query = "SELECT auth_scheme FROM sys.dm_exec_connections WHERE session_id = @@SPID";
    connection.execute(query.to_string(), None, None).await?;

    if let Some(resultset) = connection.get_current_resultset()
        && let Some(row) = resultset.next_row().await?
    {
        let auth_scheme = format!("{:?}", row.first());
        println!("✓ Authentication scheme: {}", auth_scheme);
        // LocalDB only supports Windows auth, should be NTLM
        assert!(
            auth_scheme.contains("NTLM") || auth_scheme.contains("Kerberos"),
            "Expected NTLM or Kerberos authentication scheme"
        );
    }
    connection.close_query().await?;

    // Also verify we're connected to LocalDB by checking the server name
    let query = "SELECT @@SERVERNAME, @@VERSION";
    connection.execute(query.to_string(), None, None).await?;

    if let Some(resultset) = connection.get_current_resultset()
        && let Some(row) = resultset.next_row().await?
    {
        let server_name = format!("{:?}", row.first());
        let version = format!("{:?}", row.get(1));
        println!("✓ Server name: {}", server_name);
        println!("✓ Version: {}", &version[..100.min(version.len())]);
    }

    connection.close_query().await?;
    println!("✓ LocalDB test completed successfully!");

    Ok(())
}
