#[cfg(test)]
mod common;

mod query_result_reads {
    use crate::common::{
        begin_connection, connect_query_and_validate, create_context, run_query_and_check_results,
        ExpectedQueryResultType,
    };
    use futures::StreamExt;
    use tds_x::error::Error::{SqlServerError, UsageError};
    use tds_x::query::result::QueryResultType;

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
    async fn test_incomplete_result_set_iteration() {
        let context = create_context();
        let mut connection = begin_connection(context).await;

        {
            let batch_result = connection
                .execute(
                    "
                CREATE TABLE #dummy (
                    IntColumn INT
                );
                INSERT INTO #dummy VALUES(10),(20);
                SELECT * FROM #dummy;
                SELECT 1;
                SELECT * FROM #dummy;
                SELECT * FROM #dummy;
                SELECT * FROM #dummy"
                        .to_string(),
                    None,
                    None,
                )
                .await;

            // Skip over update results and iterate over result sets.
            // Special behavior for the first and third SELECT * from #dummy (index = 2, 4, 6)  to just get one row.
            // These cases result in incomplete stream consumption. These happen to be even numbered indices,
            // but that is coincidental.
            let mut result_stream = batch_result.unwrap().stream_results();
            let mut result_number = 0;
            let expected_row_counts = [0, 0, 1, 1, 1, 2, 1];
            while let Some(result_type) = result_stream.next().await {
                match result_type.unwrap() {
                    QueryResultType::DmlResult(_) => {}
                    QueryResultType::ResultSet(result_set) => {
                        let mut row_number = 0;
                        println!(
                            "Result number {:?}: {:?}",
                            result_number,
                            result_set.get_metadata()
                        );
                        let mut row_stream = result_set.into_row_stream().unwrap();
                        while let Some(row) = row_stream.next().await {
                            let mut unwrapped_row = row.unwrap();
                            print!("Row {row_number:?}: ");
                            while let Some(cell) = unwrapped_row.next().await {
                                print!("{:?},", cell.unwrap());
                            }
                            println!();
                            row_number += 1;
                            if result_number == 2 || result_number == 4 || result_number == 6 {
                                break;
                            }
                        }
                        row_stream.close().await.unwrap();

                        assert_eq!(row_number, expected_row_counts[result_number]);
                    }
                }
                result_number += 1;
            }
            result_stream.close().await.unwrap();
        }

        // Try to reuse the connection. Note that the last result set was only partially consumed.
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
            let batch_result = connection
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
                .await;

            // Just get one result.
            let result_number = 0;
            let mut result_stream = batch_result.unwrap().stream_results();
            while result_stream.next().await.is_some() {
                if result_number == 0 {
                    break;
                }
            }
            result_stream.close().await.unwrap();
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
            let batch_result = connection
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
                .await;

            // Just get one result.
            let result_number = 0;
            let mut result_stream = batch_result.unwrap().stream_results();
            while result_stream.next().await.is_some() {
                if result_number == 0 {
                    break;
                }
            }
        }

        // Try to reuse the connection.
        let expected_error = connection.execute("SELECT 1".to_string(), None, None).await;
        match expected_error {
            Ok(_) => panic!("Expected error but got success."),
            Err(UsageError(_)) => {
                // Success case.
            }
            Err(err) => panic!("Expected error but got different error: {err}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_missed_close_result_set() {
        let context = create_context();
        let mut connection = begin_connection(context).await;

        {
            let batch_result = connection
                .execute(
                    "
                SELECT 1 UNION ALL SELECT 2;
                SELECT 2;"
                        .to_string(),
                    None,
                    None,
                )
                .await;

            let mut result_stream = batch_result.unwrap().stream_results();
            let first_result = result_stream.next().await.unwrap().unwrap();
            match first_result {
                QueryResultType::DmlResult(_) => panic!("Unexpected DML result."),
                QueryResultType::ResultSet(result_set) => {
                    // Get the first row and explicitly don't close the result set.
                    let _ = result_set.into_row_stream().unwrap().next().await.unwrap();
                }
            }
            let second_result_should_fail = result_stream.next().await.unwrap();
            match second_result_should_fail {
                Ok(_) => panic!("Expected error but got success."),
                Err(UsageError(_)) => {
                    // Success case.
                }
                Err(err) => panic!("Expected error but got different error: {err}"),
            }
            result_stream.close().await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_error_within_result_set() {
        let context = create_context();
        let mut connection = begin_connection(context).await;
        {
            let results = connection
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
            let mut result_stream = results.stream_results();

            // Skip the first two results.
            result_stream.next().await.unwrap().unwrap();
            result_stream.next().await.unwrap().unwrap();

            // Next result should be a result set with an error on the second row.
            let query_result_type = result_stream.next().await.unwrap().unwrap();
            match query_result_type {
                QueryResultType::ResultSet(result_set) => {
                    let mut row_stream = result_set.into_row_stream().unwrap();

                    // First row should be OK.
                    row_stream.next().await.unwrap().unwrap();

                    // Second row should be a SqlServerError
                    let row_result = row_stream.next().await.unwrap();
                    match row_result {
                        Err(SqlServerError { .. }) => {}
                        _ => panic!("Expected a SqlServerError"),
                    };
                    row_stream.close().await.unwrap();
                }
                _ => panic!("unexpected query result type"),
            }
            result_stream.close().await.unwrap();
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
            let results = connection
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
                .await
                .unwrap();
            let mut result_stream = results.stream_results();

            // Skip the first result.
            result_stream.next().await.unwrap().unwrap();

            // Next result should be an error.
            let error_result_type = result_stream.next().await.unwrap();
            match error_result_type {
                Err(SqlServerError { .. }) => {}
                _ => panic!("Expected a SqlServerError"),
            }
            result_stream.close().await.unwrap();
        }

        // Make sure the connection is still usable.
        let expected = [ExpectedQueryResultType::Result(1)];
        run_query_and_check_results(&mut connection, "SELECT 1".to_string(), &expected).await;
    }
}
