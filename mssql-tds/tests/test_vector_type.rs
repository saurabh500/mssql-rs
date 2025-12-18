// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Integration tests for SQL Server Vector data type deserialization.
//! These tests require SQL Server 2025 or later with Vector support enabled.
//!
//! Note: These tests will fail if connecting to a SQL Server version that doesn't support
//! the Vector data type (prior to SQL Server 2025). In the future, these tests should be
//! conditionally enabled based on server version detection.

#[cfg(test)]
mod common;

#[cfg(test)]
mod vector_integration_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::datatypes::column_values::ColumnValues;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    /// Test basic vector deserialization with a simple 3-dimensional vector
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_basic_deserialization() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Query a simple 3-dimensional vector
        let query = "SELECT CAST('[1.0, 2.0, 3.0]' AS VECTOR(3)) AS VectorColumn";

        client.execute(query.to_string(), None, None).await.unwrap();

        // Get the result set
        if let Some(resultset) = client.get_current_resultset() {
            // Verify metadata
            let columns = resultset.get_metadata();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].column_name, "VectorColumn");

            // Read the row
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                assert_eq!(row.len(), 1);

                // Verify the vector data
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        assert_eq!(vector.dimension_count(), 3);
                        let values = vector.as_f32().expect("Should be Float32 vector");
                        assert_eq!(values.len(), 3);
                        assert!((values[0] - 1.0).abs() < 0.001);
                        assert!((values[1] - 2.0).abs() < 0.001);
                        assert!((values[2] - 3.0).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector column value, got: {:?}", row[0]),
                }
            }
            assert_eq!(row_count, 1, "Expected 1 row");
        } else {
            panic!("Expected a result set");
        }
    }

    /// Test vector with single dimension
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_single_dimension() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let query = "SELECT CAST('[42.5]' AS VECTOR(1)) AS SingleVector";

        client.execute(query.to_string(), None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        assert_eq!(vector.dimension_count(), 1);
                        let values = vector.as_f32().unwrap();
                        assert!((values[0] - 42.5).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector column value"),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }

    /// Test vector with maximum dimensions (1998)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_max_dimensions() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create a vector with 1998 dimensions (max supported)
        let vector_values: Vec<String> = (0..1998).map(|i| format!("{}.0", i)).collect();
        let vector_literal = format!("[{}]", vector_values.join(", "));
        let query = format!(
            "SELECT CAST('{}' AS VECTOR(1998)) AS MaxVector",
            vector_literal
        );

        client.execute(query, None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        assert_eq!(vector.dimension_count(), 1998);
                        let values = vector.as_f32().unwrap();
                        assert_eq!(values.len(), 1998);

                        // Spot check a few values
                        assert!((values[0] - 0.0).abs() < 0.001);
                        assert!((values[100] - 100.0).abs() < 0.001);
                        assert!((values[1997] - 1997.0).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector column value"),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }

    /// Test NULL vector value
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_null_value() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let query = "SELECT CAST(NULL AS VECTOR(3)) AS NullVector";

        client.execute(query.to_string(), None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                match &row[0] {
                    ColumnValues::Null => {
                        // Expected - NULL vector should be ColumnValues::Null
                    }
                    _ => panic!("Expected Null column value, got: {:?}", row[0]),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }

    /// Test vector with mixed positive/negative/zero values
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_mixed_values() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let query = "SELECT CAST('[0.0, -1.0, 1.0, -100.5, 100.5]' AS VECTOR(5)) AS MixedVector";

        client.execute(query.to_string(), None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        assert_eq!(vector.dimension_count(), 5);
                        let values = vector.as_f32().unwrap();
                        assert!((values[0] - 0.0).abs() < 0.001);
                        assert!((values[1] - (-1.0)).abs() < 0.001);
                        assert!((values[2] - 1.0).abs() < 0.001);
                        assert!((values[3] - (-100.5)).abs() < 0.001);
                        assert!((values[4] - 100.5).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector column value"),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }

    /// Test multiple vector columns in a single query
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_multiple_vector_columns() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let query = "SELECT 
            CAST('[1.0, 2.0]' AS VECTOR(2)) AS Vec1,
            CAST('[3.0, 4.0, 5.0]' AS VECTOR(3)) AS Vec2,
            42 AS IntCol";

        client.execute(query.to_string(), None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let columns = resultset.get_metadata();
            assert_eq!(columns.len(), 3);

            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                assert_eq!(row.len(), 3);

                // First vector
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        assert_eq!(vector.dimension_count(), 2);
                        let values = vector.as_f32().unwrap();
                        assert!((values[0] - 1.0).abs() < 0.001);
                        assert!((values[1] - 2.0).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector in first column"),
                }

                // Second vector
                match &row[1] {
                    ColumnValues::Vector(vector) => {
                        assert_eq!(vector.dimension_count(), 3);
                        let values = vector.as_f32().unwrap();
                        assert!((values[0] - 3.0).abs() < 0.001);
                        assert!((values[1] - 4.0).abs() < 0.001);
                        assert!((values[2] - 5.0).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector in second column"),
                }

                // Integer column
                match &row[2] {
                    ColumnValues::Int(value) => {
                        assert_eq!(*value, 42);
                    }
                    _ => panic!("Expected Int in third column"),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }

    /// Test vector in a table with multiple rows
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_multiple_rows() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Create temp table with vector column
        let setup = "
            CREATE TABLE #VectorTest (
                Id INT,
                Embedding VECTOR(3)
            );
            INSERT INTO #VectorTest VALUES (1, CAST('[1.0, 2.0, 3.0]' AS VECTOR(3)));
            INSERT INTO #VectorTest VALUES (2, CAST('[4.0, 5.0, 6.0]' AS VECTOR(3)));
            INSERT INTO #VectorTest VALUES (3, NULL);
        ";

        client.execute(setup.to_string(), None, None).await.unwrap();

        // Consume setup results
        while client.move_to_next().await.unwrap() {}

        // Query the data
        client
            .execute(
                "SELECT Id, Embedding FROM #VectorTest ORDER BY Id".to_string(),
                None,
                None,
            )
            .await
            .unwrap();

        let expected_values = [Some(vec![1.0, 2.0, 3.0]), Some(vec![4.0, 5.0, 6.0]), None];

        let mut row_index = 0;
        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await.unwrap() {
                assert_eq!(row.len(), 2);

                // Check Id
                match &row[0] {
                    ColumnValues::Int(id) => {
                        assert_eq!(*id, (row_index + 1) as i32);
                    }
                    _ => panic!("Expected Int for Id"),
                }

                // Check Vector/Null
                match (&row[1], &expected_values[row_index]) {
                    (ColumnValues::Vector(vector), Some(expected)) => {
                        let values = vector.as_f32().unwrap();
                        for (i, &expected_val) in expected.iter().enumerate() {
                            assert!((values[i] - expected_val).abs() < 0.001);
                        }
                    }
                    (ColumnValues::Null, None) => {
                        // Expected NULL
                    }
                    (actual, expected) => {
                        panic!(
                            "Mismatch at row {}: expected {:?}, got {:?}",
                            row_index, expected, actual
                        );
                    }
                }

                row_index += 1;
            }
        }
        assert_eq!(row_index, 3, "Expected 3 rows");
    }

    /// Test vector with very small float values (near zero)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_small_values() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let query = "SELECT CAST('[0.0001, 0.0002, 0.0003]' AS VECTOR(3)) AS SmallVector";

        client.execute(query.to_string(), None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        let values = vector.as_f32().unwrap();
                        assert!((values[0] - 0.0001).abs() < 0.00001);
                        assert!((values[1] - 0.0002).abs() < 0.00001);
                        assert!((values[2] - 0.0003).abs() < 0.00001);
                    }
                    _ => panic!("Expected Vector column value"),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }

    /// Test vector with large float values
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_vector_large_values() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        let query = "SELECT CAST('[123456.79, -987654.3, 0.0]' AS VECTOR(3)) AS LargeVector";

        client.execute(query.to_string(), None, None).await.unwrap();

        if let Some(resultset) = client.get_current_resultset() {
            let mut row_count = 0;
            while let Some(row) = resultset.next_row().await.unwrap() {
                row_count += 1;
                match &row[0] {
                    ColumnValues::Vector(vector) => {
                        let values = vector.as_f32().unwrap();
                        assert!((values[0] - 123_456.79).abs() < 0.01);
                        assert!((values[1] - (-987_654.3)).abs() < 0.01);
                        assert!((values[2] - 0.0).abs() < 0.001);
                    }
                    _ => panic!("Expected Vector column value"),
                }
            }
            assert_eq!(row_count, 1, "Expected exactly 1 row");
        }
    }
}
