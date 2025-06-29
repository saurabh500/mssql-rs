mod common;

#[cfg(test)]
mod transactions {
    use crate::common::{
        begin_connection, create_context, run_query_and_check_results, validate_results,
        ExpectedQueryResultType,
    };
    use tds_x::message::transaction_management::{
        CreateTxnParams, TransactionIsolationLevel, TransactionManagementType,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sql_rollback_transaction() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
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
    async fn test_sql_commit_transaction() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
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
    async fn test_sql_switch_back_to_autocommit() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_getdtc() {
        let expected = [ExpectedQueryResultType::Result(1)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(TransactionManagementType::GetDtcAddress, None, None)
            .await;

        validate_results(begin_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_begin_named_rollback() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test01".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        let rollback_result = connection
            .send_transaction(
                TransactionManagementType::Rollback {
                    name: Some("test01".to_string()),
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(rollback_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_begin_unnamed_rollback() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: None,
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        let rollback_result = connection
            .send_transaction(
                TransactionManagementType::Rollback {
                    name: None,
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(rollback_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_begin_named_no_rollback() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test01".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_begin_unnamed_no_rollback() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: None,
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_commit_no_new_transaction() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test02".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test02".to_string()),
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_commit_new_transaction_named() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test03".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test03".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: Some("test04".to_string()),
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;

        // Commit the new transaction
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test04".to_string()),
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_commit_new_unnamed() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;

        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test05".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test05".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: None,
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;

        // Commit the new unnamed transaction
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: None,
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_commit_new_unnamed_loop_10x() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;

        let mut counter = 0;
        loop {
            let begin_result = connection
                .send_transaction(
                    TransactionManagementType::Begin(CreateTxnParams {
                        level: TransactionIsolationLevel::ReadCommitted,
                        name: Some("test05".to_string()),
                    }),
                    None,
                    None,
                )
                .await;

            validate_results(begin_result.unwrap(), &expected).await;

            run_query_and_check_results(
                connection.as_mut(),
                "CREATE TABLE #dummy_int2(col int)".to_string(),
                &expected,
            )
            .await;

            let commit_result = connection
                .send_transaction(
                    TransactionManagementType::Commit {
                        name: Some("test05".to_string()),
                        create_txn_params: Some(CreateTxnParams {
                            level: TransactionIsolationLevel::NoChange,
                            name: None,
                        }),
                    },
                    None,
                    None,
                )
                .await;

            validate_results(commit_result.unwrap(), &expected).await;

            // Ensure table is still there.
            run_query_and_check_results(
                connection.as_mut(),
                "SELECT * FROM #dummy_int2".to_string(),
                &[ExpectedQueryResultType::Result(0)],
            )
            .await;

            // Commit the new unnamed transaction
            let commit_result = connection
                .send_transaction(
                    TransactionManagementType::Commit {
                        name: None,
                        create_txn_params: None,
                    },
                    None,
                    None,
                )
                .await;

            validate_results(commit_result.unwrap(), &expected).await;
            counter += 1;
            if counter == 10 {
                break;
            }

            run_query_and_check_results(
                connection.as_mut(),
                "DROP TABLE #dummy_int2".to_string(),
                &expected,
            )
            .await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_rollback_no_new_transaction() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test06".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        // Commit the creation of the table and start a new transaction with a new name.
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test06".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: Some("test07".to_string()),
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;

        // Drop the test table so that we can rollback and check that it is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "DROP TABLE #dummy_int2".to_string(),
            &expected,
        )
        .await;

        let rollback_result = connection
            .send_transaction(
                TransactionManagementType::Rollback {
                    name: Some("test07".to_string()),
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(rollback_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_rollback_new_transaction_named() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test08".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        // Commit the creation of the table and start a new transaction with a new name.
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test08".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: Some("test09".to_string()),
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;

        // Drop the test table so that we can rollback and check that it is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "DROP TABLE #dummy_int2".to_string(),
            &expected,
        )
        .await;

        let rollback_result = connection
            .send_transaction(
                TransactionManagementType::Rollback {
                    name: Some("test09".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: Some("test10".to_string()),
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(rollback_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;

        // Commit the new named transaction
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test10".to_string()),
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_rollback_new_unnamed() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test11".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        // Commit the creation of the table and start a new transaction with a new name.
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: Some("test11".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: Some("test12".to_string()),
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;

        // Drop the test table so that we can rollback and check that it is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "DROP TABLE #dummy_int2".to_string(),
            &expected,
        )
        .await;

        let rollback_result = connection
            .send_transaction(
                TransactionManagementType::Rollback {
                    name: Some("test12".to_string()),
                    create_txn_params: Some(CreateTxnParams {
                        level: TransactionIsolationLevel::NoChange,
                        name: None,
                    }),
                },
                None,
                None,
            )
            .await;

        validate_results(rollback_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;

        // Commit the new unnamed transaction
        let commit_result = connection
            .send_transaction(
                TransactionManagementType::Commit {
                    name: None,
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(commit_result.unwrap(), &expected).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_req_savepoint() {
        let expected = [ExpectedQueryResultType::Update(0)];
        let context = create_context();
        let mut connection = begin_connection(context).await;
        let begin_result = connection
            .send_transaction(
                TransactionManagementType::Begin(CreateTxnParams {
                    level: TransactionIsolationLevel::ReadCommitted,
                    name: Some("test13".to_string()),
                }),
                None,
                None,
            )
            .await;

        validate_results(begin_result.unwrap(), &expected).await;

        run_query_and_check_results(
            connection.as_mut(),
            "CREATE TABLE #dummy_int2(col int)".to_string(),
            &expected,
        )
        .await;

        // Create a savepoint where this table exists.
        let save_result = connection
            .send_transaction(
                TransactionManagementType::Save("test14".to_string()),
                None,
                None,
            )
            .await;

        validate_results(save_result.unwrap(), &expected).await;

        // Drop the test table so that we can rollback and check that it is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "DROP TABLE #dummy_int2".to_string(),
            &expected,
        )
        .await;

        // Rollback to the savepoint
        let rollback_result = connection
            .send_transaction(
                TransactionManagementType::Rollback {
                    name: Some("test14".to_string()),
                    create_txn_params: None,
                },
                None,
                None,
            )
            .await;

        validate_results(rollback_result.unwrap(), &expected).await;

        // Ensure table is still there.
        run_query_and_check_results(
            connection.as_mut(),
            "SELECT * FROM #dummy_int2".to_string(),
            &[ExpectedQueryResultType::Result(0)],
        )
        .await;
    }
}
