// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_integration_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use async_trait::async_trait;
    use mssql_tds::connection::bulk_copy::{BulkCopy, BulkLoadRow};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::core::TdsResult;
    use mssql_tds::datatypes::column_values::ColumnValues;

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
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.id))
                .await?;
            *column_index += 1;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.value1))
                .await?;
            *column_index += 1;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.value2))
                .await?;
            *column_index += 1;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.value3))
                .await?;
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
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.id))
                .await?;
            *column_index += 1;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.value1))
                .await?;
            *column_index += 1;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.value2))
                .await?;
            *column_index += 1;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.value3))
                .await?;
            *column_index += 1;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_simple_insert() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table (automatically cleaned up)
        client
            .execute(
                "CREATE TABLE #BulkCopyTest (
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

        // Execute bulk copy
        let result = {
            let bulk_copy = BulkCopy::new(&mut client, "#BulkCopyTest");
            bulk_copy
                .batch_size(1000)
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy failed")
        };

        println!("Bulk copy result: {result:?}");

        // Check actual row count in database before assertion
        client
            .execute(
                "SELECT COUNT(*) as cnt FROM #BulkCopyTest".to_string(),
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

        // Verify the data was inserted
        client
            .execute(
                "SELECT id, value1, value2, value3 FROM #BulkCopyTest ORDER BY id".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to select data");

        let mut row_count = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await.expect("Failed to read row") {
                row_count += 1;
                match row_count {
                    1 => {
                        assert_eq!(row[0], ColumnValues::Int(1));
                        assert_eq!(row[1], ColumnValues::Int(100));
                        assert_eq!(row[2], ColumnValues::Int(200));
                        assert_eq!(row[3], ColumnValues::Int(300));
                    }
                    2 => {
                        assert_eq!(row[0], ColumnValues::Int(2));
                        assert_eq!(row[1], ColumnValues::Int(101));
                        assert_eq!(row[2], ColumnValues::Int(201));
                        assert_eq!(row[3], ColumnValues::Int(301));
                    }
                    3 => {
                        assert_eq!(row[0], ColumnValues::Int(3));
                        assert_eq!(row[1], ColumnValues::Int(102));
                        assert_eq!(row[2], ColumnValues::Int(202));
                        assert_eq!(row[3], ColumnValues::Int(302));
                    }
                    _ => panic!("Unexpected row"),
                }
            }
        }

        assert_eq!(row_count, 3, "Expected 3 rows to be returned");

        // Temp table will be automatically dropped when connection closes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_large_batch() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create test table
        client
            .execute(
                "CREATE TABLE #BulkCopyLarge (
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

        client.close_query().await.expect("Failed to close query");

        // Generate 100 rows (reduced from 1000 for faster testing)
        let test_data: Vec<TestUser> = (1..=100)
            .map(|i| TestUser {
                id: i,
                value1: i * 10,
                value2: i * 20,
                value3: i * 30,
            })
            .collect();

        // Execute bulk copy - use default batch size (all in one batch)
        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyLarge");
            bulk_copy
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy failed")
        };

        println!("Bulk copy result: {result:?}");
        assert_eq!(
            result.rows_affected, 100,
            "Expected 100 rows to be inserted"
        );
        assert!(result.rows_per_second > 0.0, "Expected positive throughput");

        // Verify count
        client
            .execute(
                "SELECT COUNT(*) FROM #BulkCopyLarge".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to select count");

        if let Some(resultset) = client.get_current_resultset()
            && let Some(row) = resultset.next_row().await.expect("Failed to read row")
        {
            assert_eq!(row[0], ColumnValues::Int(100));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_with_nulls() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create test table with nullable columns
        client
            .execute(
                "CREATE TABLE #BulkCopyNulls (
                    id INT NOT NULL,
                    value1 INT NULL,
                    value2 INT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Define structure with nullable fields
        #[derive(Debug, Clone)]
        struct NullableUser {
            id: i32,
            value1: Option<i32>,
            value2: Option<i32>,
        }

        #[async_trait]
        impl BulkLoadRow for NullableUser {
            async fn write_to_packet(
                &self,
                writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
                column_index: &mut usize,
            ) -> TdsResult<()> {
                writer
                    .write_column_value(*column_index, &ColumnValues::Int(self.id))
                    .await?;
                *column_index += 1;
                let value1 = self
                    .value1
                    .map(ColumnValues::Int)
                    .unwrap_or(ColumnValues::Null);
                writer.write_column_value(*column_index, &value1).await?;
                *column_index += 1;
                let value2 = self
                    .value2
                    .map(ColumnValues::Int)
                    .unwrap_or(ColumnValues::Null);
                writer.write_column_value(*column_index, &value2).await?;
                *column_index += 1;
                Ok(())
            }
        }

        #[async_trait]
        impl BulkLoadRow for &NullableUser {
            async fn write_to_packet(
                &self,
                writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
                column_index: &mut usize,
            ) -> TdsResult<()> {
                writer
                    .write_column_value(*column_index, &ColumnValues::Int(self.id))
                    .await?;
                *column_index += 1;
                let value1 = self
                    .value1
                    .map(ColumnValues::Int)
                    .unwrap_or(ColumnValues::Null);
                writer.write_column_value(*column_index, &value1).await?;
                *column_index += 1;
                let value2 = self
                    .value2
                    .map(ColumnValues::Int)
                    .unwrap_or(ColumnValues::Null);
                writer.write_column_value(*column_index, &value2).await?;
                *column_index += 1;
                Ok(())
            }
        }

        let test_data = vec![
            NullableUser {
                id: 1,
                value1: Some(100),
                value2: Some(200),
            },
            NullableUser {
                id: 2,
                value1: None,
                value2: Some(201),
            },
            NullableUser {
                id: 3,
                value1: Some(102),
                value2: None,
            },
            NullableUser {
                id: 4,
                value1: None,
                value2: None,
            },
        ];

        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyNulls");
            bulk_copy
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy failed")
        };

        assert_eq!(result.rows_affected, 4);

        // Verify the data
        client
            .execute(
                "SELECT id, value1, value2 FROM #BulkCopyNulls ORDER BY id".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to select data");

        let mut row_count = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await.expect("Failed to read row") {
                row_count += 1;
                match row_count {
                    1 => {
                        assert_eq!(row[0], ColumnValues::Int(1));
                        assert_eq!(row[1], ColumnValues::Int(100));
                        assert_eq!(row[2], ColumnValues::Int(200));
                    }
                    2 => {
                        assert_eq!(row[0], ColumnValues::Int(2));
                        assert_eq!(row[1], ColumnValues::Null);
                        assert_eq!(row[2], ColumnValues::Int(201));
                    }
                    3 => {
                        assert_eq!(row[0], ColumnValues::Int(3));
                        assert_eq!(row[1], ColumnValues::Int(102));
                        assert_eq!(row[2], ColumnValues::Null);
                    }
                    4 => {
                        assert_eq!(row[0], ColumnValues::Int(4));
                        assert_eq!(row[1], ColumnValues::Null);
                        assert_eq!(row[2], ColumnValues::Null);
                    }
                    _ => panic!("Unexpected row"),
                }
            }
        }

        assert_eq!(row_count, 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_empty_dataset() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        client
            .execute(
                "CREATE TABLE #BulkCopyEmpty (
                    id INT NOT NULL,
                    value1 INT NOT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        let test_data: Vec<TestUser> = vec![];

        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyEmpty");
            bulk_copy
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy should handle empty dataset")
        };

        assert_eq!(result.rows_affected, 0);
    }
}
