// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod client_based_iterators {
    use crate::common::{create_context, init_tracing};
    use futures::lock::Mutex;
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
    use mssql_tds::datatypes::sqltypes::SqlType;
    use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
    use std::sync::Arc;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test]
    async fn test_multiquery_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let query = "SELECT TOP(2) * FROM sys.databases; SELECT 1";

        client.execute(query.to_string(), None, None).await?;
        let mut row_count = 0;
        loop {
            while client.next_row().await?.is_some() {
                row_count += 1;
            }

            if !client.move_to_next().await? {
                break;
            }
        }
        assert_eq!(
            row_count, 3,
            "Expected 3 rows from the multi-query execution"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_orderby_token_in_query() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let query = "SELECT TOP 1 
            name, 
            database_id, 
            create_date 
            FROM sys.databases 
            ORDER BY name;";

        client.execute(query.to_string(), None, None).await?;
        let mut row_count = 0;
        loop {
            while client.next_row().await?.is_some() {
                row_count += 1;
            }

            if !client.move_to_next().await? {
                break;
            }
        }
        assert_eq!(
            row_count, 1,
            "Expected 3 rows from the multi-query execution"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_incomplete_resultset_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let query = "SELECT TOP(2) * FROM sys.databases; SELECT 1";

        client.execute(query.to_string(), None, None).await?;
        let mut row_count = 0;

        if client.next_row().await?.is_some() {
            row_count += 1;
        }
        client.close_query().await?;

        assert_eq!(
            row_count, 1,
            "Expected 1 row from the incomplete result set execution"
        );
        let mut row_count = 0;
        client.execute(query.to_string(), None, None).await?;
        loop {
            while client.next_row().await?.is_some() {
                row_count += 1;
            }
            if !client.move_to_next().await? {
                break;
            }
        }

        client.close_query().await?;
        assert_eq!(
            row_count, 3,
            "Expected 3 rows from the multi-query execution on connection reuse."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_bad_query_error_followed_by_valid_query() -> Result<(), Box<dyn std::error::Error>>
    {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let query = "bad bad query";

        let err = client.execute(query.to_string(), None, None).await;
        assert!(err.is_err(), "Expected error for bad query");

        let query = "SELECT TOP(2) * FROM sys.databases; SELECT 1";
        client.execute(query.to_string(), None, None).await?;
        let mut row_count = 0;
        loop {
            while client.next_row().await?.is_some() {
                row_count += 1;
            }
            if !client.move_to_next().await? {
                break;
            }
        }
        assert_eq!(
            row_count, 3,
            "Expected 3 rows from the valid query execution after bad query"
        );
        Ok(())
    }

    // This test will fail in Azure since DB creation from TSQL as well as USE statements are not allowed.
    #[tokio::test]
    async fn test_use_database_statement() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let create_database_query = "IF DB_ID('TestDB') IS NULL CREATE DATABASE TestDB";

        client
            .execute(create_database_query.to_string(), None, None)
            .await?;
        let use_database_query = "USE TestDB";
        client
            .execute(use_database_query.to_string(), None, None)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_stored_proc_with_query_and_output() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let client = provider.create_client(context, None).await?;
        let client = Arc::new(Mutex::new(client));

        // Create a stored procedure with an output parameter
        let create_proc = "CREATE PROCEDURE #test_proc        
             @paramIn int,
            @paramOut int output
         AS
         BEGIN
            select 1
           set @paramOut = @paramIn
         END";
        client
            .lock()
            .await
            .execute(create_proc.to_string(), None, None)
            .await?;
        client.lock().await.close_query().await?;

        let proc_name = "#test_proc".to_string();
        let named_parameters = vec![
            RpcParameter::new(
                Some("@paramIn".to_string()),
                StatusFlags::NONE,
                SqlType::Int(Some(42)),
            ),
            RpcParameter::new(
                Some("@paramOut".to_string()),
                StatusFlags::BY_REF_VALUE,
                SqlType::Int(None),
            ),
        ];
        client
            .lock()
            .await
            .execute_stored_procedure(proc_name, None, Some(named_parameters), None, None)
            .await?;
        let mut binding = client.lock().await;
        let result_set = binding.get_current_resultset();
        if let Some(result_set) = result_set {
            let _ = result_set.get_metadata();
            let mut row_count = 0;

            while (result_set.next_row().await?).is_some() {
                row_count += 1;
            }
            assert_eq!(
                row_count, 1,
                "Expected 1 row from the stored procedure execution with output parameter"
            );
        } else {
            panic!("Expected a result set from stored procedure execution, but got None");
        }

        // Move once more till we read the return values.
        while binding.move_to_next().await? {
            // Continue to next result set if available
        }

        let output_param = binding.get_return_values();

        assert!(output_param.len() == 1);

        Ok(())
    }
}
