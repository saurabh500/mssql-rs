#[cfg(test)]
mod common;

mod timeout_tests {
    use crate::common::{begin_connection, create_context};
    use std::time::{Duration, Instant};
    use tds_x::connection::client_context::ClientContext;
    use tds_x::connection_provider::tds_connection_provider::TdsConnectionProvider;
    use tds_x::core::{EncryptionSetting, TdsResult};
    use tds_x::error::Error;
    use tokio::net::TcpListener;

    #[tokio::test]
    pub async fn timeout_test_no_retry() {
        timeout_test_with_retries(0).await;
    }

    #[tokio::test]
    pub async fn query_timeout_e2e() {
        let context = create_context();
        let mut connection = begin_connection(&context).await;

        let start_time = Instant::now();
        let batch_result = connection
            .execute("WAITFOR DELAY '00:00:05'".to_string(), Some(2))
            .await;

        // Timeout could happen during send.
        if batch_result.is_err() {
            verify_duration(batch_result, start_time, 1500, 3000).await;
            return;
        }

        // Or it can happen when getting results (more likely).
        let close_result = batch_result.unwrap().close().await;
        verify_duration(close_result, start_time, 1500, 3000).await;
    }

    async fn timeout_test_with_retries(retry_count: u8) {
        // Spawn a local TCP server with a random port
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();

        // Create a client context with a two-second timeout that points to localhost:1433.
        let client_context = ClientContext {
            transport_context: tds_x::connection::client_context::TransportContext::Tcp {
                host: "localhost".to_string(),
                port: listener.local_addr().unwrap().port(),
            },
            database: "master".to_string(),
            encryption: EncryptionSetting::PreferOff,
            connect_timeout: 2,
            connect_retry_count: retry_count as u32,
            ..Default::default()
        };

        // Try to connect.
        let provider = TdsConnectionProvider {};

        // Start a timer.
        let start_time = Instant::now();
        let connection_result = provider.create_connection(&client_context).await;
        verify_duration(
            connection_result,
            start_time,
            1500 * (1 + retry_count) as u64,
            3000 * (1 + retry_count) as u64,
        )
        .await;
    }

    async fn verify_duration<T>(
        result: TdsResult<T>,
        start_time: Instant,
        expected_duration_lower: u64,
        expected_duration_upper: u64,
    ) {
        match result {
            Ok(_) => {
                panic!("Operation should not have succeeded.")
            }
            Err(error) => match error {
                Error::TimeoutError(_) => {
                    let duration = start_time.elapsed();

                    assert!(
                        duration < Duration::from_millis(expected_duration_upper),
                        "Expected duration < {:?}ms, got {:?}",
                        expected_duration_upper,
                        duration
                    );
                    assert!(
                        duration > Duration::from_millis(expected_duration_lower),
                        "Expected duration > {:?}ms, got {:?}",
                        expected_duration_lower,
                        duration
                    );
                }
                _ => panic!("Expected timeoutError error, got {:?}", error),
            },
        };
    }
}
