// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// Each test binary uses a different subset of helpers.
#![allow(dead_code)]

use std::env;
use std::sync::Once;

use dotenv::dotenv;
use mssql_rs::Client;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

static INIT: Once = Once::new();

/// Load the connection string from `MSSQL_RS_TEST_CONNECTION_STRING` or `.env`.
/// Returns `None` when the env var is absent (server-dependent tests should skip).
pub fn connection_string() -> Option<String> {
    dotenv().ok();
    env::var("MSSQL_RS_TEST_CONNECTION_STRING").ok()
}

/// Connect to the test SQL Server. Returns `None` when the env var is absent.
pub async fn connect() -> Option<Client> {
    let conn_str = connection_string()?;
    Some(Client::connect(&conn_str).await.expect("connect failed"))
}

/// Connect to the test SQL Server or panic.
pub async fn require_connect() -> Client {
    let conn_str =
        connection_string().expect("MSSQL_RS_TEST_CONNECTION_STRING must be set for this test");
    Client::connect(&conn_str).await.expect("connect failed")
}

/// Initialize tracing (once) when `ENABLE_TEST_TRACE=true`.
pub fn init_tracing() {
    dotenv().ok();
    let enable = env::var("ENABLE_TEST_TRACE")
        .unwrap_or_else(|_| "false".to_string())
        .parse::<bool>()
        .unwrap_or(false);
    if enable {
        INIT.call_once(|| {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        });
    }
}

/// Skip the current test if the server connection string is not set.
macro_rules! skip_if_no_server {
    () => {
        if common::connection_string().is_none() {
            eprintln!("MSSQL_RS_TEST_CONNECTION_STRING not set — skipping");
            return;
        }
    };
}

pub(crate) use skip_if_no_server;
