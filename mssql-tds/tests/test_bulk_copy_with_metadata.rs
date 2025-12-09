// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_integration_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use mssql_tds::connection::bulk_copy::{BulkCopy, BulkLoadRow};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::core::TdsResult;
    use mssql_tds::datatypes::column_values::ColumnValues;
    use async_trait::async_trait;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    // Define a simple test data structure
    #[derive(Debug, Clone)]
    struct TestUser {
        id: i32,
        value1: i32,
        value2: i32,
        value3: i32,
    }

    #[async_trait]
    impl BulkLoadRow for TestUser {
        async fn write_to_packet(
            &self,
            writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
            column_index: &mut usize,
        ) -> TdsResult<()> {
            writer.write_column_value(*column_index, &ColumnValues::Int(self.id)).await?;
            *column_index += 1;
            writer.write_column_value(*column_index, &ColumnValues::Int(self.value1)).await?;
            *column_index += 1;
            writer.write_column_value(*column_index, &ColumnValues::Int(self.value2)).await?;
            *column_index += 1;
            writer.write_column_value(*column_index, &ColumnValues::Int(self.value3)).await?;
            *column_index += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl BulkLoadRow for &TestUser {
        async fn write_to_packet(
            &self,
            writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
            column_index: &mut usize,
        ) -> TdsResult<()> {
            writer.write_column_value(*column_index, &ColumnValues::Int(self.id)).await?;
            *column_index += 1;
            writer.write_column_value(*column_index, &ColumnValues::Int(self.value1)).await?;
            *column_index += 1;
            writer.write_column_value(*column_index, &ColumnValues::Int(self.value2)).await?;
            *column_index += 1;
            writer.write_column_value(*column_index, &ColumnValues::Int(self.value3)).await?;
            *column_index += 1;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_with_server_metadata() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table (automatically cleaned up)
        client
            .execute(
                "CREATE TABLE #BulkCopyMetadataTest (
                    id INT NOT NULL,
                    value1 INT NOT NULL,
                    value2 INT NOT NULL,
                    value3 INT NOT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        // Close the query to free up the connection
        client.close_query().await.expect("Failed to close query");

        // Prepare test data
        let test_data = vec![
            TestUser {
                id: 1,
                value1: 100,
                value2: 200,
                value3: 300,
            },
            TestUser {
                id: 2,
                value1: 101,
                value2: 201,
                value3: 301,
            },
            TestUser {
                id: 3,
                value1: 102,
                value2: 202,
                value3: 302,
            },
        ];

        // Execute bulk copy using public API (without explicit column mappings - should use ordinal mapping)
        let result = {
            let bulk_copy = BulkCopy::new(&mut client, "#BulkCopyMetadataTest");
            bulk_copy
                .batch_size(1000)
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy failed")
        };

        println!(
            "DEBUG: Bulk copy completed, rows_affected: {}",
            result.rows_affected
        );

        // Check actual row count in database before assertion
        client
            .execute(
                "SELECT COUNT(*) as cnt FROM #BulkCopyMetadataTest".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to count rows");

        if let Some(resultset) = client.get_current_resultset()
            && let Some(row) = resultset.next_row().await.expect("Failed to read count")
        {
            println!("DEBUG: Actual rows in database: {:?}", row[0]);
        }
        client
            .close_query()
            .await
            .expect("Failed to close count query");

        assert_eq!(result.rows_affected, 3, "Expected 3 rows to be inserted");
    }
}
