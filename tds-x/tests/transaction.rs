mod common;

#[cfg(test)]
mod transactions {
    use crate::common::{
        begin_connection, create_context, run_query_and_check_results, ExpectedQueryResultType,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_rollback_transaction() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(&context).await;
        run_query_and_check_results(
            connection.as_mut(),
            "SET IMPLICIT_TRANSACTIONS ON".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int(col int)".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "INSERT INTO #dummy_int VALUES(1)".to_string(),
            &[ExpectedQueryResultType::Update(1)],
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "ROLLBACK transaction".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int(col int)".to_string(),
            &expected,
        )
        .await; // Should be able to create this table if rolled back.
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_commit_transaction() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(&context).await;
        run_query_and_check_results(
            connection.as_mut(),
            "SET IMPLICIT_TRANSACTIONS ON".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "INSERT INTO #dummy_int2 VALUES(1)".to_string(),
            &[ExpectedQueryResultType::Update(1)],
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "COMMIT transaction".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(1)],
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_switch_back_to_autocommit() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(&context).await;
        run_query_and_check_results(
            connection.as_mut(),
            "SET IMPLICIT_TRANSACTIONS ON".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "INSERT INTO #dummy_int2 VALUES(1)".to_string(),
            &[ExpectedQueryResultType::Update(1)],
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "SET IMPLICIT_TRANSACTIONS OFF".to_string(),
            &expected,
        )
        .await;
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(1)],
        )
        .await;
    }
}
