// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_integration_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use mssql_tds::connection::bulk_copy::{BulkCopy, BulkCopyRow};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::datatypes::column_values::ColumnValues;
    use mssql_tds::datatypes::sql_string::SqlString;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    // Define a simple test data structure
    #[derive(Debug, Clone)]
    struct TestUser {
        id: i32,
        name: String,
        age: i16,
        active: bool,
    }

    impl BulkCopyRow for TestUser {
        fn to_column_values(&self) -> Vec<ColumnValues> {
            vec![
                ColumnValues::Int(self.id),
                ColumnValues::String(SqlString::from_utf8_string(self.name.clone())),
                ColumnValues::SmallInt(self.age),
                ColumnValues::Bit(self.active),
            ]
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
                    name NVARCHAR(100) NOT NULL,
                    age SMALLINT NOT NULL,
                    active BIT NOT NULL
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
                name: "Alice".to_string(),
                age: 30,
                active: true,
            },
            TestUser {
                id: 2,
                name: "Bob".to_string(),
                age: 25,
                active: false,
            },
            TestUser {
                id: 3,
                name: "Charlie".to_string(),
                age: 35,
                active: true,
            },
        ];

        // Execute bulk copy using public API
        let result = {
            let bulk_copy = BulkCopy::new(&mut client, "#BulkCopyMetadataTest");
            bulk_copy
                .batch_size(1000)
                .write_to_server(test_data.into_iter())
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

        if let Some(resultset) = client.get_current_resultset() {
            if let Some(row) = resultset.next_row().await.expect("Failed to read count") {
                println!("DEBUG: Actual rows in database: {:?}", row[0]);
            }
        }
        client
            .close_query()
            .await
            .expect("Failed to close count query");

        assert_eq!(result.rows_affected, 3, "Expected 3 rows to be inserted");
    }
}
