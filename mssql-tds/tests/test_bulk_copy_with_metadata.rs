// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_integration_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use async_trait::async_trait;
    use mssql_tds::connection::bulk_copy::{BulkCopy, BulkLoadRow};
    use mssql_tds::connection::metadata_retriever::SelectTop0Retriever;
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
    use mssql_tds::core::TdsResult;
    use mssql_tds::datatypes::bulk_copy_metadata::SqlDbType;
    use mssql_tds::datatypes::column_values::ColumnValues;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    // Helper function to create BulkCopy with a specific retriever
    fn create_bulk_copy_with_retriever<'a>(
        client: &'a mut TdsClient,
        table_name: &'static str,
        use_select_top0: bool,
    ) -> BulkCopy<'a> {
        if use_select_top0 {
            BulkCopy::with_retriever(client, table_name, Box::new(SelectTop0Retriever::new()))
        } else {
            BulkCopy::new(client, table_name)
        }
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_basic() {
        test_retrieve_destination_metadata_basic_impl(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_basic_with_select_top0() {
        test_retrieve_destination_metadata_basic_impl(true).await;
    }

    async fn test_retrieve_destination_metadata_basic_impl(use_select_top0: bool) {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let table_name = if use_select_top0 {
            "#MetadataTestSelectTop0"
        } else {
            "#MetadataTest"
        };

        // Drop table if it exists from previous run
        let drop_sql = format!("IF OBJECT_ID('tempdb..{}') IS NOT NULL DROP TABLE {}", table_name, table_name);
        client.execute(drop_sql, None, None).await.ok();
        client.close_query().await.ok();

        // Create temp table with various data types
        let create_sql = format!(
            "CREATE TABLE {} (
                    id INT NOT NULL,
                    name NVARCHAR(100) NOT NULL,
                    age TINYINT NULL,
                    salary DECIMAL(18, 2) NULL,
                    active BIT NOT NULL
                )",
            table_name
        );

        client
            .execute(create_sql, None, None)
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Retrieve metadata
        let mut bulk_copy = create_bulk_copy_with_retriever(&mut client, table_name, use_select_top0);
        let metadata = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata");

        // Verify we got 5 columns
        assert_eq!(metadata.len(), 5, "Expected 5 columns");

        // Verify column names and ordinals
        assert_eq!(metadata[0].name, "id");
        assert_eq!(metadata[0].ordinal, 0);
        assert_eq!(metadata[1].name, "name");
        assert_eq!(metadata[1].ordinal, 1);
        assert_eq!(metadata[2].name, "age");
        assert_eq!(metadata[2].ordinal, 2);
        assert_eq!(metadata[3].name, "salary");
        assert_eq!(metadata[3].ordinal, 3);
        assert_eq!(metadata[4].name, "active");
        assert_eq!(metadata[4].ordinal, 4);

        // Verify nullable flags
        assert!(!metadata[0].is_nullable, "id should not be nullable");
        assert!(!metadata[1].is_nullable, "name should not be nullable");
        assert!(metadata[2].is_nullable, "age should be nullable");
        assert!(metadata[3].is_nullable, "salary should be nullable");
        assert!(!metadata[4].is_nullable, "active should not be nullable");

        // Verify types
        assert_eq!(metadata[0].sql_type, SqlDbType::Int);
        assert_eq!(metadata[1].sql_type, SqlDbType::NVarChar);
        assert_eq!(metadata[2].sql_type, SqlDbType::TinyInt);
        assert_eq!(metadata[3].sql_type, SqlDbType::Decimal);
        assert_eq!(metadata[4].sql_type, SqlDbType::Bit);

        // Verify precision and scale for DECIMAL column
        assert_eq!(metadata[3].precision, 18);
        assert_eq!(metadata[3].scale, 2);

        let retriever_type = if use_select_top0 {
            "SelectTop0Retriever"
        } else {
            "SystemCatalogRetriever"
        };
        println!(
            "Metadata test passed with {}: Retrieved {} columns",
            retriever_type,
            metadata.len()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_with_identity() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table with identity column
        client
            .execute(
                "CREATE TABLE #IdentityTest (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    value NVARCHAR(50) NOT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Retrieve metadata
        let mut bulk_copy = BulkCopy::new(&mut client, "#IdentityTest");
        let metadata = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata");

        // Verify identity column is marked correctly
        assert_eq!(metadata.len(), 2);
        assert!(
            metadata[0].is_identity,
            "id column should be marked as identity"
        );
        assert!(
            !metadata[1].is_identity,
            "value column should not be identity"
        );

        println!("Identity metadata test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_with_computed() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table with computed column
        client
            .execute(
                "CREATE TABLE #ComputedTest (
                    id INT NOT NULL,
                    value1 INT NOT NULL,
                    value2 INT NOT NULL,
                    total AS (value1 + value2)
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Retrieve metadata
        let mut bulk_copy = BulkCopy::new(&mut client, "#ComputedTest");
        let metadata = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata");

        // Verify computed column is marked correctly
        assert_eq!(metadata.len(), 4);
        assert!(
            !metadata[0].is_computed,
            "id column should not be computed"
        );
        assert!(
            !metadata[1].is_computed,
            "value1 column should not be computed"
        );
        assert!(
            !metadata[2].is_computed,
            "value2 column should not be computed"
        );
        assert!(
            metadata[3].is_computed,
            "total column should be marked as computed"
        );

        println!("Computed column metadata test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_caching() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table
        client
            .execute(
                "CREATE TABLE #CacheTest (
                    id INT NOT NULL,
                    name NVARCHAR(100) NOT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Retrieve metadata twice
        let mut bulk_copy = BulkCopy::new(&mut client, "#CacheTest");
        let metadata1 = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata (first call)");

        let metadata2 = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata (second call)");

        // Both should return the same metadata
        assert_eq!(metadata1.len(), metadata2.len());
        assert_eq!(metadata1[0].name, metadata2[0].name);
        assert_eq!(metadata1[1].name, metadata2[1].name);

        println!("Metadata caching test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_string_types() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table with various string types
        client
            .execute(
                "CREATE TABLE #StringTest (
                    col_varchar VARCHAR(50),
                    col_nvarchar NVARCHAR(100),
                    col_char CHAR(10),
                    col_nchar NCHAR(20),
                    col_varchar_max VARCHAR(MAX),
                    col_nvarchar_max NVARCHAR(MAX)
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Retrieve metadata
        let mut bulk_copy = BulkCopy::new(&mut client, "#StringTest");
        let metadata = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata");

        // Verify types and lengths
        use mssql_tds::datatypes::bulk_copy_metadata::SqlDbType;
        assert_eq!(metadata.len(), 6);
        
        // VARCHAR(50)
        assert_eq!(metadata[0].sql_type, SqlDbType::VarChar);
        assert_eq!(metadata[0].max_length, 50);
        
        // NVARCHAR(100) - uses 2 bytes per char
        assert_eq!(metadata[1].sql_type, SqlDbType::NVarChar);
        assert_eq!(metadata[1].max_length, 200);
        
        // CHAR(10)
        assert_eq!(metadata[2].sql_type, SqlDbType::Char);
        assert_eq!(metadata[2].max_length, 10);
        
        // NCHAR(20) - uses 2 bytes per char
        assert_eq!(metadata[3].sql_type, SqlDbType::NChar);
        assert_eq!(metadata[3].max_length, 40);
        
        // VARCHAR(MAX)
        assert_eq!(metadata[4].sql_type, SqlDbType::VarChar);
        assert_eq!(metadata[4].max_length, -1);
        
        // NVARCHAR(MAX)
        assert_eq!(metadata[5].sql_type, SqlDbType::NVarChar);
        assert_eq!(metadata[5].max_length, -1);

        println!("String types metadata test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_numeric_types() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table with various numeric types
        client
            .execute(
                "CREATE TABLE #NumericTest (
                    col_tinyint TINYINT,
                    col_smallint SMALLINT,
                    col_int INT,
                    col_bigint BIGINT,
                    col_decimal DECIMAL(10, 3),
                    col_numeric NUMERIC(15, 5),
                    col_money MONEY,
                    col_smallmoney SMALLMONEY,
                    col_real REAL,
                    col_float FLOAT
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        client.close_query().await.expect("Failed to close query");

        // Retrieve metadata
        let mut bulk_copy = BulkCopy::new(&mut client, "#NumericTest");
        let metadata = bulk_copy
            .retrieve_destination_metadata()
            .await
            .expect("Failed to retrieve metadata");

        // Verify types
        use mssql_tds::datatypes::bulk_copy_metadata::SqlDbType;
        assert_eq!(metadata.len(), 10);
        
        assert_eq!(metadata[0].sql_type, SqlDbType::TinyInt);
        assert_eq!(metadata[1].sql_type, SqlDbType::SmallInt);
        assert_eq!(metadata[2].sql_type, SqlDbType::Int);
        assert_eq!(metadata[3].sql_type, SqlDbType::BigInt);
        assert_eq!(metadata[4].sql_type, SqlDbType::Decimal);
        assert_eq!(metadata[5].sql_type, SqlDbType::Numeric);
        assert_eq!(metadata[6].sql_type, SqlDbType::Money);
        assert_eq!(metadata[7].sql_type, SqlDbType::SmallMoney);
        assert_eq!(metadata[8].sql_type, SqlDbType::Real);
        assert_eq!(metadata[9].sql_type, SqlDbType::Float);

        // Verify precision and scale for DECIMAL and NUMERIC
        assert_eq!(metadata[4].precision, 10);
        assert_eq!(metadata[4].scale, 3);
        assert_eq!(metadata[5].precision, 15);
        assert_eq!(metadata[5].scale, 5);

        println!("Numeric types metadata test passed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_retrieve_destination_metadata_nonexistent_table() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Try to retrieve metadata for a non-existent table
        let mut bulk_copy = BulkCopy::new(&mut client, "#NonExistentTable");
        let result = bulk_copy.retrieve_destination_metadata().await;

        // Should return an error
        assert!(
            result.is_err(),
            "Expected error for non-existent table"
        );

        if let Err(e) = result {
            println!("Expected error occurred: {:?}", e);
        }
    }
}
