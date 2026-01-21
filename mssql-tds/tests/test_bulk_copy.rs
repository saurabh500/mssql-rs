// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_integration_tests {
    use crate::common::{begin_connection, build_tcp_datasource, init_tracing};
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
        let mut client = begin_connection(&build_tcp_datasource()).await;

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
        let mut client = begin_connection(&build_tcp_datasource()).await;

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
        let mut client = begin_connection(&build_tcp_datasource()).await;

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
        let mut client = begin_connection(&build_tcp_datasource()).await;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_null_to_non_nullable_column() {
        let mut client = begin_connection(&build_tcp_datasource()).await;

        // Create test table where all columns are non-nullable
        client
            .execute(
                "CREATE TABLE #BulkCopyNonNullable (
                    id INT NOT NULL,
                    value1 INT NOT NULL,
                    value2 INT NOT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Define structure that can hold nullable values
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

        // Create test data with NULL in non-nullable column
        let test_data = vec![
            NullableUser {
                id: 1,
                value1: Some(100),
                value2: Some(200),
            },
            NullableUser {
                id: 2,
                value1: None, // NULL to non-nullable column - should fail on server
                value2: Some(201),
            },
        ];

        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyNonNullable");
            bulk_copy.write_to_server_zerocopy(&test_data).await
        };

        // Server should reject this with error 515 (cannot insert NULL into non-nullable column)
        assert!(
            result.is_err(),
            "Expected error when inserting NULL into non-nullable column"
        );

        let error = result.unwrap_err();
        let error_msg = format!("{:?}", error);

        // Verify it's the expected SQL Server error (error 515)
        assert!(
            error_msg.contains("515") || error_msg.contains("NULL") || error_msg.contains("null"),
            "Expected error about NULL constraint violation, got: {}",
            error_msg
        );
    }

    /// Test bulk copy with very large varchar(max) strings.
    /// This test verifies the fix for issue #41685 where 50MB+ strings
    /// caused a segfault due to deep recursion in write_async.
    ///
    /// The test uses a smaller size (1MB) to keep test runtime reasonable
    /// while still validating the iterative write_async approach works correctly.
    #[derive(Debug, Clone)]
    struct LargeStringRow {
        id: i32,
        large_value: String,
    }

    #[async_trait]
    impl BulkLoadRow for LargeStringRow {
        async fn write_to_packet(
            &self,
            writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
            column_index: &mut usize,
        ) -> TdsResult<()> {
            use mssql_tds::datatypes::sql_string::SqlString;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.id))
                .await?;
            *column_index += 1;
            // Use SqlString for varchar(max) column
            let sql_string = SqlString::from_utf8_string(self.large_value.clone());
            writer
                .write_column_value(*column_index, &ColumnValues::String(sql_string))
                .await?;
            *column_index += 1;
            Ok(())
        }
    }

    #[async_trait]
    impl BulkLoadRow for &LargeStringRow {
        async fn write_to_packet(
            &self,
            writer: &mut mssql_tds::message::bulk_load::StreamingBulkLoadWriter<'_>,
            column_index: &mut usize,
        ) -> TdsResult<()> {
            use mssql_tds::datatypes::sql_string::SqlString;
            writer
                .write_column_value(*column_index, &ColumnValues::Int(self.id))
                .await?;
            *column_index += 1;
            // Use SqlString for varchar(max) column
            let sql_string = SqlString::from_utf8_string(self.large_value.clone());
            writer
                .write_column_value(*column_index, &ColumnValues::String(sql_string))
                .await?;
            *column_index += 1;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_large_varchar_max_no_stack_overflow() {
        let datasource = build_tcp_datasource();
        let mut client = begin_connection(&datasource).await;

        // Create temp table with varchar(max)
        client
            .execute(
                "CREATE TABLE #BulkCopyLargeString (
                    id INT NOT NULL,
                    large_value VARCHAR(MAX)
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Create a 1MB string (use 'A' repeated)
        // This is enough to test the iterative write_async approach
        // A 50MB string would take too long for unit tests
        let str_size_mb = 1;
        let large_string = "A".repeat(str_size_mb * 1024 * 1024);

        let test_data = vec![LargeStringRow {
            id: 1,
            large_value: large_string.clone(),
        }];

        // Perform bulk copy - this should NOT cause a stack overflow
        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyLargeString");
            bulk_copy.write_to_server_zerocopy(&test_data).await
        };

        assert!(
            result.is_ok(),
            "Bulk copy failed: {:?}",
            result.unwrap_err()
        );

        let bulk_result = result.unwrap();
        assert_eq!(bulk_result.rows_affected, 1, "Expected 1 row copied");

        // Verify data was inserted correctly
        client
            .execute(
                "SELECT id, LEN(large_value) as len FROM #BulkCopyLargeString".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to select from table");

        let mut rows_returned = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await.expect("Failed to read row") {
                let id = match &row[0] {
                    ColumnValues::Int(v) => *v,
                    _ => panic!("Expected Int for id"),
                };
                // LEN() on VARCHAR(MAX) returns BIGINT, not INT
                let len = match &row[1] {
                    ColumnValues::BigInt(v) => *v,
                    _ => panic!("Expected BigInt for len"),
                };

                assert_eq!(id, 1);
                assert_eq!(len, (str_size_mb * 1024 * 1024) as i64);
                rows_returned += 1;
            }
        }

        assert_eq!(rows_returned, 1, "Expected 1 row returned from SELECT");

        client.close_query().await.expect("Failed to close query");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_with_table_lock() {
        let mut client = begin_connection(&build_tcp_datasource()).await;

        // Create temp table
        client
            .execute(
                "CREATE TABLE #BulkCopyTableLock (
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

        // Execute bulk copy WITH table_lock option enabled
        let result = {
            let bulk_copy = BulkCopy::new(&mut client, "#BulkCopyTableLock");
            bulk_copy
                .table_lock(true) // Enable TABLOCK hint
                .batch_size(1000)
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy with table_lock failed")
        };

        println!("Bulk copy with table_lock result: {result:?}");
        assert_eq!(result.rows_affected, 3, "Expected 3 rows to be inserted");

        // Verify the data was inserted correctly
        client
            .execute(
                "SELECT COUNT(*) as cnt FROM #BulkCopyTableLock".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to count rows");

        if let Some(resultset) = client.get_current_resultset()
            && let Some(row) = resultset.next_row().await.expect("Failed to read count")
        {
            assert_eq!(
                row[0],
                ColumnValues::Int(3),
                "Expected 3 rows in table after bulk copy with table_lock"
            );
            println!("Verified: 3 rows inserted with TABLOCK option");
        }
        client
            .close_query()
            .await
            .expect("Failed to close count query");

        // Verify data integrity
        client
            .execute(
                "SELECT id, value1, value2, value3 FROM #BulkCopyTableLock ORDER BY id".to_string(),
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
        println!("table_lock option test passed: TABLOCK hint accepted by SQL Server");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bulk_copy_table_lock_actual_locking_behavior() {
        let datasource = build_tcp_datasource();
        let mut client = begin_connection(&datasource).await;

        // Create a persistent table (not temp) so we can query locks across connections
        let table_name = format!("BulkCopyLockTest_{}", std::process::id());

        // Drop table if exists from previous failed test
        let drop_sql = format!(
            "IF OBJECT_ID('{}', 'U') IS NOT NULL DROP TABLE {}",
            table_name, table_name
        );
        client
            .execute(drop_sql, None, None)
            .await
            .expect("Failed to drop test table");
        client.close_query().await.ok();

        // Create persistent table
        let create_sql = format!(
            "CREATE TABLE {} (
                id INT NOT NULL,
                value1 INT NOT NULL,
                value2 INT NOT NULL,
                value3 INT NOT NULL
            )",
            table_name
        );

        client
            .execute(create_sql, None, None)
            .await
            .expect("Failed to create test table");
        client.close_query().await.expect("Failed to close query");

        // Prepare large dataset to ensure bulk copy takes measurable time
        // Use 10,000 rows with smaller batches to extend operation duration
        let test_data: Vec<TestUser> = (1..=10000)
            .map(|i| TestUser {
                id: i,
                value1: i * 10,
                value2: i * 20,
                value3: i * 30,
            })
            .collect();

        // Create a second connection for lock monitoring
        let mut lock_monitor_client = begin_connection(&datasource).await;

        // Get the session ID of the bulk copy connection
        client
            .execute("SELECT @@SPID as session_id".to_string(), None, None)
            .await
            .expect("Failed to get session ID");

        let session_id: i32 = if let Some(resultset) = client.get_current_resultset()
            && let Some(row) = resultset
                .next_row()
                .await
                .expect("Failed to read session ID")
        {
            match row[0] {
                ColumnValues::SmallInt(id) => id as i32,
                ColumnValues::Int(id) => id,
                _ => panic!("Unexpected session ID type"),
            }
        } else {
            panic!("Could not retrieve session ID");
        };
        client.close_query().await.expect("Failed to close query");

        println!("Bulk copy will run on session ID: {}", session_id);

        // Use Arc and Mutex for shared state between tasks
        use std::sync::{Arc, Mutex};
        let lock_detected = Arc::new(Mutex::new(false));
        let lock_detected_clone = Arc::clone(&lock_detected);

        // Spawn async task for bulk copy operation
        let table_name_clone = table_name.clone();
        let bulk_copy_task = tokio::spawn(async move {
            // Sleep briefly to ensure lock monitor is ready and polling
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            let bulk_copy = BulkCopy::new(&mut client, &table_name_clone);
            bulk_copy
                .table_lock(true) // Enable TABLOCK - this should acquire BU lock
                .batch_size(500) // Use smaller batches to extend operation duration
                .write_to_server_zerocopy(&test_data)
                .await
                .expect("Bulk copy with table_lock failed")
        });

        // Start monitoring immediately - bulk copy will start after 200ms delay
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Query sys.dm_tran_locks to find BU (Bulk Update) lock
        // We look for locks on our specific session that are on OBJECT or TABLE resources
        let lock_query = format!(
            "SELECT 
                request_session_id,
                resource_type,
                resource_description,
                request_mode,
                request_status
            FROM sys.dm_tran_locks
            WHERE request_session_id = {}
                AND resource_type IN ('OBJECT', 'TABLE', 'HOBT', 'PAGE')
                AND request_mode LIKE '%BU%'",
            session_id
        );

        // Poll for locks aggressively - check every 10ms for up to 100 attempts (1 second window)
        let mut attempts = 0;
        let max_attempts = 100;

        while attempts < max_attempts {
            lock_monitor_client
                .execute(lock_query.clone(), None, None)
                .await
                .expect("Failed to query locks");

            if let Some(resultset) = lock_monitor_client.get_current_resultset() {
                while let Some(row) = resultset.next_row().await.expect("Failed to read lock row") {
                    println!("Lock detected:");
                    println!("  Session ID: {:?}", row[0]);
                    println!("  Resource Type: {:?}", row[1]);
                    println!("  Resource Description: {:?}", row[2]);
                    println!("  Lock Mode: {:?}", row[3]);
                    println!("  Lock Status: {:?}", row[4]);

                    // Check if this is a BU lock
                    if let ColumnValues::String(lock_mode) = &row[3] {
                        let lock_mode_str = format!("{:?}", lock_mode);
                        if lock_mode_str.contains("BU") {
                            *lock_detected_clone.lock().unwrap() = true;
                            println!("BU (Bulk Update) lock CONFIRMED!");
                        }
                    }
                }
            }
            lock_monitor_client.close_query().await.ok();

            if *lock_detected_clone.lock().unwrap() {
                break;
            }

            attempts += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Wait for bulk copy to complete
        let result = bulk_copy_task.await.expect("Bulk copy task panicked");

        println!("Bulk copy with table_lock completed: {:?}", result);
        assert_eq!(
            result.rows_affected, 10000,
            "Expected 10000 rows to be inserted"
        );

        // Verify lock was detected
        let lock_was_detected = *lock_detected.lock().unwrap();

        assert!(
            lock_was_detected,
            "Expected BU (Bulk Update) lock to be detected during bulk copy with table_lock=true. \
             The lock monitor polled {} times but did not observe a BU lock on session {}.",
            max_attempts, session_id
        );

        // Verify data was inserted
        lock_monitor_client
            .execute(format!("SELECT COUNT(*) FROM {}", table_name), None, None)
            .await
            .expect("Failed to count rows");

        if let Some(resultset) = lock_monitor_client.get_current_resultset()
            && let Some(row) = resultset.next_row().await.expect("Failed to read count")
        {
            assert_eq!(
                row[0],
                ColumnValues::Int(10000),
                "Expected 10000 rows in table"
            );
        }
        lock_monitor_client.close_query().await.ok();

        // Cleanup - drop the persistent table
        lock_monitor_client
            .execute(format!("DROP TABLE {}", table_name), None, None)
            .await
            .expect("Failed to drop test table");
        lock_monitor_client.close_query().await.ok();

        println!("Table lock locking behavior test passed: BU lock was acquired during bulk copy");
    }
}
