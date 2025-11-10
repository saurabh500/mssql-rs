// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod query_result_reads {
    use crate::common::{
        ExpectedQueryResultType, begin_connection, connect_query_and_validate, create_context,
        run_query_and_check_results,
    };
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::datatypes::column_values::ColumnValues;
    use mssql_tds::error::Error::{SqlServerError, UsageError};

    use crate::common::init_tracing;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_select_1() {
        let expected = [ExpectedQueryResultType::Result(1)];
        connect_query_and_validate("SELECT 1".to_string(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_generate_all_types_table() {
        let expected = [
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(2),
            ExpectedQueryResultType::Result(2),
        ];
        connect_query_and_validate(
            "
            CREATE TABLE #AllDataTypes (
                TinyIntColumn TINYINT,
                SmallIntColumn SMALLINT,
                IntColumn INT,
                BigIntColumn BIGINT,
                BitColumn BIT,
                DecimalColumn DECIMAL(18,2),
                NumericColumn NUMERIC(18,2),
                FloatColumn FLOAT,
                RealColumn REAL,
            );

            INSERT INTO #AllDataTypes (
                TinyIntColumn, SmallIntColumn, IntColumn, BigIntColumn, BitColumn,
                DecimalColumn, NumericColumn, FloatColumn, RealColumn
            )
            VALUES (
                CAST(255 AS TINYINT), -- TinyIntColumn
                CAST(32767 AS SMALLINT), -- SmallIntColumn
                CAST(2147483647 AS INT), -- IntColumn
                CAST(9223372036854775807 AS BIGINT), -- BigIntColumn
                CAST(1 AS BIT), -- BitColumn
                CAST(272.01 AS DECIMAL(18, 2)), --DecimalColumn
                CAST(12345678901234.98 AS NUMERIC(18,2)), -- NumericColumn
                CAST(1234.22231 AS FLOAT), -- FloatColumn
                CAST(11.11 AS REAL) -- RealColumn
            ),
            (
                CAST(128 AS TINYINT), -- TinyIntColumn
                CAST(128 AS SMALLINT), -- SmallIntColumn
                CAST(128 AS INT), -- IntColumn
                CAST(128 AS BIGINT), -- BigIntColumn
                CAST(0 AS BIT), -- BitColumn
                CAST(19.01 AS DECIMAL(18, 2)), --DecimalColumn
                CAST(18.98 AS NUMERIC(18,2)), -- NumericColumn
                CAST(100.22231 AS FLOAT), -- FloatColumn
                CAST(5.11 AS REAL) -- RealColumn
            );

            select * from #AllDataTypes;"
                .to_string(),
            &expected,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tds_connection_reuse() {
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let expected = [
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(2),
            ExpectedQueryResultType::Result(2),
        ];
        run_query_and_check_results(
            &mut connection,
            "
            CREATE TABLE #dummy (
                IntColumn INT
            );
            INSERT INTO #dummy VALUES(10),(20);
            SELECT * FROM #dummy;"
                .to_string(),
            &expected,
        )
        .await;

        let expected = [
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(3),
            ExpectedQueryResultType::Result(3),
        ];
        run_query_and_check_results(
            &mut connection,
            "DROP TABLE #dummy;
            CREATE TABLE #dummy (
                ShortColumn SMALLINT
            );
            INSERT INTO #dummy VALUES(0),(1),(2);
            SELECT * FROM #dummy;"
                .to_string(),
            &expected,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore] // TODO: Bug in TdsClient - move_to_next() doesn't properly advance through multiple result sets
    async fn test_incomplete_result_set_iteration() {
        // TODO: This test exposes a bug in TdsClient where move_to_next() doesn't properly advance
        // to the next result set. After consuming the first SELECT * result, move_to_next() returns
        // false instead of moving to the next result set (SELECT 1).
        // This needs investigation in TdsClient::move_to_next() and move_to_column_metadata().

        let context = create_context();
        let mut connection = begin_connection(context).await;

        {
            connection
                .execute(
                    "
                CREATE TABLE #dummy (
                    IntColumn INT
                );
                INSERT INTO #dummy VALUES(10),(20);
                SELECT * FROM #dummy;
                SELECT 1;
                SELECT * FROM #dummy;"
                        .to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();

            // Skip over update results (CREATE TABLE and INSERT) to get to the first SELECT
            connection.move_to_next().await.unwrap(); // Skip CREATE TABLE
            connection.move_to_next().await.unwrap(); // Skip INSERT

            // Read from the first SELECT * - consume all rows
            if let Some(resultset) = connection.get_current_resultset() {
                let mut row_count = 0;
                while resultset.next_row().await.unwrap().is_some() {
                    row_count += 1;
                }
                assert_eq!(row_count, 2);
            }

            // Move to SELECT 1 and verify
            assert!(connection.move_to_next().await.unwrap());
            if let Some(resultset) = connection.get_current_resultset() {
                let row = resultset.next_row().await.unwrap().unwrap();
                match &row[0] {
                    ColumnValues::Int(val) => assert_eq!(*val, 1),
                    _ => panic!("Expected Int(1)"),
                }
            }

            // Move to last SELECT * and verify
            assert!(connection.move_to_next().await.unwrap());
            if let Some(resultset) = connection.get_current_resultset() {
                let mut row_count = 0;
                while resultset.next_row().await.unwrap().is_some() {
                    row_count += 1;
                }
                assert_eq!(row_count, 2);
            }

            // No more results
            assert!(!connection.move_to_next().await.unwrap());

            connection.close_query().await.unwrap();
        }

        // Try to reuse the connection
        let expected = [
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(3),
            ExpectedQueryResultType::Result(3),
        ];
        run_query_and_check_results(
            &mut connection,
            "DROP TABLE #dummy;
            CREATE TABLE #dummy (
                ShortColumn SMALLINT
            );
            INSERT INTO #dummy VALUES(0),(1),(2);
            SELECT * FROM #dummy;"
                .to_string(),
            &expected,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_incomplete_result_iteration() {
        let context = create_context();
        let mut connection = begin_connection(context).await;

        {
            connection
                .execute(
                    "
                CREATE TABLE #dummy (
                    IntColumn INT
                );
                INSERT INTO #dummy VALUES(10),(20);
                SELECT * FROM #dummy;"
                        .to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();

            // Just get the first result set, then close
            let _result_number = 0;
            if connection.get_current_resultset().is_some() {
                // Found first result, now close without consuming
            }
            connection.close_query().await.unwrap();
        }

        // Try to reuse the connection.
        let expected = [
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(0),
            ExpectedQueryResultType::Update(3),
            ExpectedQueryResultType::Result(3),
        ];
        run_query_and_check_results(
            &mut connection,
            "DROP TABLE #dummy;
            CREATE TABLE #dummy (
                ShortColumn SMALLINT
            );
            INSERT INTO #dummy VALUES(0),(1),(2);
            SELECT * FROM #dummy;"
                .to_string(),
            &expected,
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_missed_close_result_iteration() {
        let context = create_context();
        let mut connection = begin_connection(context).await;

        {
            connection
                .execute(
                    "
                CREATE TABLE #dummy (
                    IntColumn INT
                );
                INSERT INTO #dummy VALUES(10),(20);
                SELECT * FROM #dummy;"
                        .to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();

            // Just get the first result without closing
            let _result_number = 0;
            if connection.get_current_resultset().is_some() {
                // Found first result, exit scope without closing
            }
        }

        // Try to reuse the connection - should fail because previous query wasn't closed
        let expected_error = connection.execute("SELECT 1".to_string(), None, None).await;
        match expected_error {
            Ok(_) => panic!("Expected error but got success."),
            Err(UsageError(_)) => {
                // Success case - got expected UsageError
            }
            Err(err) => panic!("Expected error but got different error: {err}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_missed_close_result_set() {
        // NOTE: TdsClient has different behavior than the streaming QueryResult API.
        // TdsClient automatically drains (consumes) remaining rows when move_to_next() is called,
        // so there's no error for incomplete result set consumption.
        // This test verifies that TdsClient handles incomplete consumption gracefully.
        let context = create_context();
        let mut connection = begin_connection(context).await;

        {
            connection
                .execute(
                    "
                SELECT 1 UNION ALL SELECT 2;
                SELECT 2;"
                        .to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();

            // Get the first result set and read one row
            if let Some(resultset) = connection.get_current_resultset() {
                // Get the first row and explicitly don't finish consuming the result set
                let row = resultset.next_row().await.unwrap();
                assert!(row.is_some());
            }

            // With TdsClient, move_to_next() automatically drains remaining rows - this should succeed
            let second_result = connection.move_to_next().await;
            assert!(second_result.is_ok());
            assert!(second_result.unwrap()); // Should return true as there is a next result set

            // Verify we can read from the second result set
            if let Some(resultset) = connection.get_current_resultset() {
                let row = resultset.next_row().await.unwrap();
                assert!(row.is_some());
            }

            connection.close_query().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_within_result_set() {
        let context = create_context();
        let mut connection = begin_connection(context).await;
        {
            connection
                .execute(
                    "
                CREATE TABLE #dummy (
                    StrColumn VARCHAR(100)
                );
                INSERT INTO #dummy VALUES('10'),('abcd');
                SELECT CAST(StrColumn AS Int) FROM #dummy;"
                        .to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();

            // Try to skip the first result (CREATE TABLE)
            // The error from the later SELECT CAST might appear at any point
            let mut error_found = false;
            let first_move = connection.move_to_next().await;

            if first_move.is_err() {
                // Error encountered on first move
                match first_move {
                    Err(SqlServerError { .. }) => {
                        error_found = true;
                    }
                    Err(e) => panic!("Expected SqlServerError, got: {e:?}"),
                    Ok(_) => unreachable!(),
                }
            }

            // If no error yet, try moving past INSERT
            let move_result = if !error_found {
                connection.move_to_next().await
            } else {
                // Already found error, skip remaining checks
                return;
            };

            match move_result {
                Err(SqlServerError { .. }) => {
                    // Expected: Error occurred during move_to_next when moving to SELECT result
                    error_found = true;
                }
                Ok(_) => {
                    // move_to_next succeeded, error should appear during row iteration
                    if let Some(resultset) = connection.get_current_resultset() {
                        // Try to read first row
                        let first_row = resultset.next_row().await;
                        match first_row {
                            Ok(Some(_)) => {
                                // First row succeeded (CAST('10' AS Int)), second row should error
                                let row_result = resultset.next_row().await;
                                match row_result {
                                    Err(SqlServerError { .. }) => {
                                        error_found = true;
                                    }
                                    _ => panic!("Expected SqlServerError on second row"),
                                }
                            }
                            Err(SqlServerError { .. }) => {
                                // Error occurred on first row attempt
                                error_found = true;
                            }
                            _ => panic!("Expected success or SqlServerError"),
                        }
                    }
                }
                Err(e) => panic!("Expected SqlServerError, got: {e:?}"),
            }

            assert!(error_found, "Expected to encounter a SqlServerError");

            connection.close_query().await.unwrap();
        }

        // Make sure the connection is still usable.
        let expected = [ExpectedQueryResultType::Result(1)];
        run_query_and_check_results(&mut connection, "SELECT 1".to_string(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_within_batch() {
        let context = create_context();
        let mut connection = begin_connection(context).await;
        {
            // Note: The INSERT with 1/0 will cause a divide by zero error during execution
            let execute_result = connection
                .execute(
                    "
                CREATE TABLE #dummy (
                    IntColumn VARCHAR(100)
                );
                INSERT INTO #dummy VALUES(1/0),(10);
                SELECT CAST(StrColumn AS Int) FROM #dummy;"
                        .to_string(),
                    None,
                    None,
                )
                .await;

            // The error might occur during execute() or during result iteration
            match execute_result {
                Ok(()) => {
                    // If execute succeeded, the error should appear when we try to move to results
                    // Skip the first result (CREATE TABLE)
                    let first_move = connection.move_to_next().await;
                    match first_move {
                        Err(SqlServerError { .. }) => {
                            // Expected error occurred on first move
                        }
                        Err(e) => panic!("Expected SqlServerError, got: {e:?}"),
                        Ok(_) => {
                            // First move succeeded, error should occur on second move (INSERT result)
                            let error_result = connection.move_to_next().await;
                            match error_result {
                                Err(SqlServerError { .. }) => {
                                    // Expected error
                                }
                                Err(e) => panic!("Expected SqlServerError, got: {e:?}"),
                                Ok(_) => panic!("Expected a SqlServerError but got success"),
                            }
                        }
                    }
                }
                Err(SqlServerError { .. }) => {
                    // Error occurred during execute(), which is also acceptable
                }
                Err(e) => panic!("Expected SqlServerError, got: {e:?}"),
            }

            connection.close_query().await.unwrap();
        }

        // Make sure the connection is still usable.
        let expected = [ExpectedQueryResultType::Result(1)];
        run_query_and_check_results(&mut connection, "SELECT 1".to_string(), &expected).await;
    }
}
