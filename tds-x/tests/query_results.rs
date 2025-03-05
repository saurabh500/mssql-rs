#[cfg(test)]
mod query_result_reads {
    use std::env;

    use dotenv::dotenv;
    use futures::StreamExt;
    use tds_x::core::TdsResult;
    use tds_x::{
        connection::{client_context::ClientContext, tds_connection::TdsConnection},
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        core::EncryptionSetting,
        query::result::{BatchResult, QueryResultType},
    };

    enum ExpectedQueryResultType {
        Update(u64),
        Result(u64),
    }

    #[allow(clippy::assertions_on_constants)]
    async fn assert_matches_expected(qrt: QueryResultType<'_>, expected: &ExpectedQueryResultType) {
        match (qrt, expected) {
            (QueryResultType::ResultSet(_), ExpectedQueryResultType::Update(_)) => {
                assert!(false)
            }
            (QueryResultType::Update(_), ExpectedQueryResultType::Result(_)) => {
                assert!(false)
            }
            (
                QueryResultType::ResultSet(result_set),
                ExpectedQueryResultType::Result(expected_row_count),
            ) => {
                let mut actual_rows: u64 = 0;
                println!("Columns: {:?}", result_set.get_metadata());
                let mut row_stream = result_set.into_row_stream().await.unwrap();
                while let Some(row) = row_stream.next().await {
                    let mut unwrapped_row = row.unwrap();
                    print!("Row {:?}: ", actual_rows);
                    while let Some(cell) = unwrapped_row.next().await {
                        print!("{:?},", cell.unwrap().get_value());
                    }
                    println!();
                    actual_rows += 1;
                }
                assert_eq!(actual_rows, *expected_row_count);
            }
            (
                QueryResultType::Update(rows_affected),
                ExpectedQueryResultType::Update(expected_row_count),
            ) => {
                assert_eq!(rows_affected, *expected_row_count as i64);
            }
        }
    }

    pub fn create_context() -> ClientContext {
        dotenv().ok();
        ClientContext {
            server_name: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: 1433,
            user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
            password: env::var("SQL_PASSWORD").expect("SQL_PASSWORD environment variable not set"),
            database: "master".to_string(),
            encryption: EncryptionSetting::On,
            ..Default::default()
        }
    }

    pub async fn begin_connection(client_context: &ClientContext) -> Box<TdsConnection> {
        create_connection(client_context).await.unwrap()
    }

    pub async fn create_connection(context: &ClientContext) -> TdsResult<Box<TdsConnection>> {
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(context).await?;
        Ok(Box::new(connection_result))
    }

    async fn validate_results(
        batch_result: BatchResult<'_>,
        expected_results: &[ExpectedQueryResultType],
    ) {
        let mut query_result_stream = batch_result.stream_results();
        let mut expected_index = 0;
        println!("Before looping.");
        while let Some(query_result_type) = query_result_stream.next().await {
            println!("Current index {:?}", expected_index);
            assert!(expected_index < expected_results.len());
            let qrt = query_result_type.unwrap();
            assert_matches_expected(qrt, &expected_results[expected_index]).await;
            expected_index += 1;
        }
    }

    async fn run_query_and_check_results<'a, 'n>(
        connection: &'a mut TdsConnection<'n>,
        query: String,
        expected_results: &[ExpectedQueryResultType],
    ) where
        'n: 'a,
    {
        let results = connection.execute(query).await;
        validate_results(results.unwrap(), expected_results).await;
    }

    async fn connect_query_and_validate(
        query: String,
        expected_results: &[ExpectedQueryResultType],
    ) {
        let context: ClientContext = create_context();
        let mut connection = begin_connection(&context).await;
        run_query_and_check_results(&mut connection, query, expected_results).await;
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
        let mut connection = begin_connection(&context).await;
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
        let mut connection = begin_connection(&context).await;

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
                    QueryResultType::Update(_) => {}
                    QueryResultType::ResultSet(result_set) => {
                        let mut row_number = 0;
                        println!(
                            "Result number {:?}: {:?}",
                            result_number,
                            result_set.get_metadata()
                        );
                        let mut row_stream = result_set.into_row_stream().await.unwrap();
                        while let Some(row) = row_stream.next().await {
                            let mut unwrapped_row = row.unwrap();
                            print!("Row {:?}: ", row_number);
                            while let Some(cell) = unwrapped_row.next().await {
                                print!("{:?},", cell.unwrap().get_value());
                            }
                            println!();
                            row_number += 1;
                            if result_number == 2 || result_number == 4 || result_number == 6 {
                                break;
                            }
                        }

                        assert_eq!(row_number, expected_row_counts[result_number]);
                    }
                }
                result_number += 1;
            }
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
        let mut connection = begin_connection(&context).await;

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
}
