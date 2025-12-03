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

    #[tokio::test]
    async fn test_query_date_time_types_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;

        // Query that returns various date/time types with explicit scales
        let query = r#"
            SELECT 
                CAST('14:30:45.1234567' AS TIME(7)) AS time_col,
                CAST('2024-03-15' AS DATE) AS date_col,
                CAST('2024-03-15 14:30:45.123' AS DATETIME) AS datetime_col,
                CAST('2024-03-15 14:30:45.1234567' AS DATETIME2(7)) AS datetime2_col,
                CAST('2024-03-15 14:30:00' AS SMALLDATETIME) AS smalldatetime_col,
                CAST('2024-03-15 14:30:45.1234567 +05:30' AS DATETIMEOFFSET(7)) AS datetimeoffset_col
        "#;

        client.execute(query.to_string(), None, None).await?;

        // Get metadata and verify it was parsed correctly
        let resultset = client
            .get_current_resultset()
            .expect("Expected a resultset");
        let metadata = resultset.get_metadata();

        // Verify we have 6 columns
        assert_eq!(metadata.len(), 6, "Expected 6 date/time columns");

        // Verify TIME(7) metadata - should have length 5 and scale 7
        let time_col = &metadata[0];
        assert_eq!(time_col.column_name, "time_col");
        assert_eq!(time_col.type_info.length, 5, "TIME(7) should have length 5");
        let time_scale = time_col.get_scale();
        assert_eq!(time_scale, 7, "TIME(7) should have scale 7");

        // Verify DATE metadata - should have length 3
        let date_col = &metadata[1];
        assert_eq!(date_col.column_name, "date_col");
        assert_eq!(date_col.type_info.length, 3, "DATE should have length 3");

        // Verify DATETIME metadata - should have length 8
        let datetime_col = &metadata[2];
        assert_eq!(datetime_col.column_name, "datetime_col");
        assert_eq!(
            datetime_col.type_info.length, 8,
            "DATETIME should have length 8"
        );

        // Verify DATETIME2(7) metadata - should have length 8 (5 for time + 3 for date) and scale 7
        let datetime2_col = &metadata[3];
        assert_eq!(datetime2_col.column_name, "datetime2_col");
        assert_eq!(
            datetime2_col.type_info.length, 8,
            "DATETIME2(7) should have length 8"
        );
        let datetime2_scale = datetime2_col.get_scale();
        assert_eq!(datetime2_scale, 7, "DATETIME2(7) should have scale 7");

        // Verify SMALLDATETIME metadata - should have length 4
        let smalldatetime_col = &metadata[4];
        assert_eq!(smalldatetime_col.column_name, "smalldatetime_col");
        assert_eq!(
            smalldatetime_col.type_info.length, 4,
            "SMALLDATETIME should have length 4"
        );

        // Verify DATETIMEOFFSET(7) metadata - should have length 10 (5 for time + 3 for date + 2 for offset) and scale 7
        let datetimeoffset_col = &metadata[5];
        assert_eq!(datetimeoffset_col.column_name, "datetimeoffset_col");
        assert_eq!(
            datetimeoffset_col.type_info.length, 10,
            "DATETIMEOFFSET(7) should have length 10"
        );
        let datetimeoffset_scale = datetimeoffset_col.get_scale();
        assert_eq!(
            datetimeoffset_scale, 7,
            "DATETIMEOFFSET(7) should have scale 7"
        );

        // Also verify we can read the actual values
        let row = resultset.next_row().await?.expect("Expected a row");

        // Just verify we got values of the right types
        match &row[0] {
            mssql_tds::datatypes::column_values::ColumnValues::Time(_) => {}
            _ => panic!("Expected Time value"),
        }

        match &row[1] {
            mssql_tds::datatypes::column_values::ColumnValues::Date(_) => {}
            _ => panic!("Expected Date value"),
        }

        match &row[2] {
            mssql_tds::datatypes::column_values::ColumnValues::DateTime(_) => {}
            _ => panic!("Expected DateTime value"),
        }

        match &row[3] {
            mssql_tds::datatypes::column_values::ColumnValues::DateTime2(_) => {}
            _ => panic!("Expected DateTime2 value"),
        }

        match &row[4] {
            mssql_tds::datatypes::column_values::ColumnValues::SmallDateTime(_) => {}
            _ => panic!("Expected SmallDateTime value"),
        }

        match &row[5] {
            mssql_tds::datatypes::column_values::ColumnValues::DateTimeOffset(_) => {}
            _ => panic!("Expected DateTimeOffset value"),
        }

        Ok(())
    }
}
