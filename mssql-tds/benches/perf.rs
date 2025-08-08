// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::env;

use criterion::{Criterion, criterion_group, criterion_main};
use mssql_tds::{
    connection::{
        client_context::{ClientContext, TransportContext},
        tds_client::{ResultSet, ResultSetClient},
    },
    connection_provider::tds_connection_provider::TdsConnectionProvider,
    core::{EncryptionOptions, EncryptionSetting},
};

static QUERY_TO_BENCHMARK: &str = "SELECT * FROM sys.databases; select * from sys.columns";

static _QUERY_MANY_ROWS: &str = r#"SELECT TOP (10000000)
                                c1.*, c2.column_id AS c2_column_id, c3.column_id AS c3_column_id, c4.column_id AS c4_column_id
                                FROM sys.columns c1
                                CROSS JOIN sys.columns c2
                                CROSS JOIN sys.columns c3
                                CROSS JOIN sys.columns c4"#;

fn connect_fetch_multiple_packets(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("connect_fetch_multiple_packets", |b| {
        b.iter(|| {
            let context = create_context();
            rt.block_on(async move {
                let provider = TdsConnectionProvider {};
                let mut client = provider.create_client(context, None).await.unwrap();

                let mut _row_count = 0;
                client
                    .execute(QUERY_TO_BENCHMARK.to_string(), None, None)
                    .await
                    .unwrap();
                // let mut row_count = 0;
                loop {
                    while client.next_row().await.unwrap().is_some() {
                        _row_count += 1;
                    }

                    if !client.move_to_next().await.unwrap() {
                        break;
                    }
                }
            });
        })
    });
}

pub fn create_context() -> ClientContext {
    dotenv::dotenv().ok();
    ClientContext {
        transport_context: TransportContext::Tcp {
            host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: env::var("DB_PORT")
                .expect("DB_PORT environment variable not set")
                .parse::<u16>()
                .expect("DB_PORT must be a valid u16"),
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
        encryption_options: EncryptionOptions {
            mode: EncryptionSetting::On,
            trust_server_certificate: env::var("TRUST_SERVER_CERTIFICATE")
                .map(|v| v.parse().unwrap_or(false))
                .unwrap_or(false),
            host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
        },
        ..Default::default()
    }
}

criterion_group!(benches, connect_fetch_multiple_packets);
criterion_main!(benches);
