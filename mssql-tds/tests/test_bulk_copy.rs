// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_integration_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use mssql_tds::connection::bulk_copy::{BulkCopy, BulkCopyRow};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
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

        fn column_metadata() -> Vec<BulkCopyColumnMetadata>
        where
            Self: Sized,
        {
            vec![
                BulkCopyColumnMetadata::new("id", SqlDbType::Int, SqlDbType::Int.to_tds_type())
                    .with_length(4, TypeLength::Fixed(4))
                    .with_nullable(false),
                BulkCopyColumnMetadata::new(
                    "name",
                    SqlDbType::NVarChar,
                    SqlDbType::NVarChar.to_tds_type(),
                )
                .with_length(200, TypeLength::Variable(200))
                .with_nullable(false),
                BulkCopyColumnMetadata::new(
                    "age",
                    SqlDbType::SmallInt,
                    SqlDbType::SmallInt.to_tds_type(),
                )
                .with_length(2, TypeLength::Fixed(2))
                .with_nullable(false),
                BulkCopyColumnMetadata::new("active", SqlDbType::Bit, SqlDbType::Bit.to_tds_type())
                    .with_length(1, TypeLength::Fixed(1))
                    .with_nullable(false),
            ]
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

        // Execute bulk copy
        let result = {
            let bulk_copy = BulkCopy::new(&mut client, "#BulkCopyTest");
            bulk_copy
                .batch_size(1000)
                .write_to_server(test_data.into_iter())
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

        // Verify the data was inserted
        client
            .execute(
                "SELECT id, name, age, active FROM #BulkCopyTest ORDER BY id".to_string(),
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
                        if let ColumnValues::String(s) = &row[1] {
                            assert_eq!(s.to_utf8_string(), "Alice");
                        } else {
                            panic!("Expected string for name");
                        }
                        assert_eq!(row[2], ColumnValues::SmallInt(30));
                        assert_eq!(row[3], ColumnValues::Bit(true));
                    }
                    2 => {
                        assert_eq!(row[0], ColumnValues::Int(2));
                        if let ColumnValues::String(s) = &row[1] {
                            assert_eq!(s.to_utf8_string(), "Bob");
                        } else {
                            panic!("Expected string for name");
                        }
                        assert_eq!(row[2], ColumnValues::SmallInt(25));
                        assert_eq!(row[3], ColumnValues::Bit(false));
                    }
                    3 => {
                        assert_eq!(row[0], ColumnValues::Int(3));
                        if let ColumnValues::String(s) = &row[1] {
                            assert_eq!(s.to_utf8_string(), "Charlie");
                        } else {
                            panic!("Expected string for name");
                        }
                        assert_eq!(row[2], ColumnValues::SmallInt(35));
                        assert_eq!(row[3], ColumnValues::Bit(true));
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

        client.close_query().await.expect("Failed to close query");

        // Generate 100 rows (reduced from 1000 for faster testing)
        let test_data: Vec<TestUser> = (1..=100)
            .map(|i| TestUser {
                id: i,
                name: format!("User{i}"),
                age: (20 + (i % 50)) as i16,
                active: i % 2 == 0,
            })
            .collect();

        // Execute bulk copy - use default batch size (all in one batch)
        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyLarge");
            bulk_copy
                .write_to_server(test_data.into_iter())
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

        if let Some(resultset) = client.get_current_resultset() {
            if let Some(row) = resultset.next_row().await.expect("Failed to read row") {
                assert_eq!(row[0], ColumnValues::Int(100));
            }
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
                    name NVARCHAR(100) NULL,
                    age SMALLINT NULL
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
            name: Option<String>,
            age: Option<i16>,
        }

        impl BulkCopyRow for NullableUser {
            fn to_column_values(&self) -> Vec<ColumnValues> {
                vec![
                    ColumnValues::Int(self.id),
                    self.name
                        .as_ref()
                        .map(|s| ColumnValues::String(SqlString::from_utf8_string(s.clone())))
                        .unwrap_or(ColumnValues::Null),
                    self.age
                        .map(ColumnValues::SmallInt)
                        .unwrap_or(ColumnValues::Null),
                ]
            }

            fn column_metadata() -> Vec<BulkCopyColumnMetadata> {
                vec![
                    BulkCopyColumnMetadata::new("id", SqlDbType::Int, 0x38)
                        .with_length(4, TypeLength::Fixed(4))
                        .with_nullable(false),
                    BulkCopyColumnMetadata::new("name", SqlDbType::NVarChar, 0xE7)
                        .with_length(200, TypeLength::Variable(200))
                        .with_nullable(true),
                    BulkCopyColumnMetadata::new("age", SqlDbType::SmallInt, 0x34)
                        .with_length(2, TypeLength::Fixed(2))
                        .with_nullable(true),
                ]
            }
        }

        let test_data = vec![
            NullableUser {
                id: 1,
                name: Some("Alice".to_string()),
                age: Some(30),
            },
            NullableUser {
                id: 2,
                name: None,
                age: Some(25),
            },
            NullableUser {
                id: 3,
                name: Some("Charlie".to_string()),
                age: None,
            },
            NullableUser {
                id: 4,
                name: None,
                age: None,
            },
        ];

        let result = {
            let mut bulk_copy = BulkCopy::new(&mut client, "#BulkCopyNulls");
            bulk_copy
                .write_to_server(test_data.into_iter())
                .await
                .expect("Bulk copy failed")
        };

        assert_eq!(result.rows_affected, 4);

        // Verify the data
        client
            .execute(
                "SELECT id, name, age FROM #BulkCopyNulls ORDER BY id".to_string(),
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
                        assert!(matches!(row[1], ColumnValues::String(_)));
                        assert_eq!(row[2], ColumnValues::SmallInt(30));
                    }
                    2 => {
                        assert_eq!(row[0], ColumnValues::Int(2));
                        assert_eq!(row[1], ColumnValues::Null);
                        assert_eq!(row[2], ColumnValues::SmallInt(25));
                    }
                    3 => {
                        assert_eq!(row[0], ColumnValues::Int(3));
                        assert!(matches!(row[1], ColumnValues::String(_)));
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
                    name NVARCHAR(100) NOT NULL
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
                .write_to_server(test_data.into_iter())
                .await
                .expect("Bulk copy should handle empty dataset")
        };

        assert_eq!(result.rows_affected, 0);
    }
}
