// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod rpc_results {
    use crate::common::{begin_connection, create_context, get_scalar_value, init_tracing};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::datatypes::column_values::ColumnValues;
    use mssql_tds::datatypes::sqltypes::SqlType;
    use mssql_tds::{
        core::TdsResult,
        message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
        token::tokenitems::ReturnValueStatus,
    };

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test]
    async fn test_stored_proc() {
        let context = create_context();
        let mut connection = begin_connection(context).await;

        // Create a query to setup the stored procedure. This will be a Sql Batch execution.
        let stored_procedure_setup_query = "CREATE PROCEDURE #TempScrollProc
                @InputInt INT,
                @OutputInt INT OUTPUT
            AS
            BEGIN
                SET @OutputInt = @InputInt;
            END;";

        // This should setup the temp stored procedure on this connection.
        execute_non_query(&mut connection, stored_procedure_setup_query.to_string())
            .await
            .unwrap();

        // Do the actual test of the stored procedure.
        let param_value = SqlType::Int(Some(45612));
        let param1 = RpcParameter::new(
            Some("@InputInt".to_string()),
            StatusFlags::NONE,
            param_value,
        );

        let param2 = RpcParameter::new(
            Some("@OutputInt".to_string()),
            StatusFlags::BY_REF_VALUE, // Output parameter
            SqlType::Int(None),        // This is an output parameter. Set to null.
        );

        let named_parameters = vec![param1, param2];

        let stored_procedure_query = "#TempScrollProc";

        connection
            .execute_stored_procedure(
                stored_procedure_query.to_string(),
                None,
                Some(named_parameters),
                None,
                None,
            )
            .await
            .unwrap();

        let returned_parameters = connection.get_return_values();
        assert_eq!(returned_parameters.len(), 1);
        let returned_parameter = returned_parameters.first().unwrap();
        assert_eq!(returned_parameter.param_name, "@OutputInt".to_string());
        assert_eq!(returned_parameter.value, ColumnValues::Int(45612));
        assert_eq!(returned_parameter.status, ReturnValueStatus::OutputParam);
    }

    #[tokio::test]
    async fn test_stored_proc_stream_results() {
        let context = create_context();
        let mut connection = begin_connection(context).await;

        // Create a query to setup the stored procedure. This will be a Sql Batch execution.
        let stored_procedure_setup_query = "CREATE PROCEDURE #TempScrollProc
                @InputInt INT,
                @OutputInt INT OUTPUT
            AS
            BEGIN
                SET @OutputInt = @InputInt;
            END;";

        // This should setup the temp stored procedure on this connection.
        execute_non_query(&mut connection, stored_procedure_setup_query.to_string())
            .await
            .unwrap();

        // Do the actual test of the stored procedure.
        let param_value = SqlType::Int(Some(45612));
        let param1 = RpcParameter::new(
            Some("@InputInt".to_string()),
            StatusFlags::NONE,
            param_value,
        );

        let param2 = RpcParameter::new(
            Some("@OutputInt".to_string()),
            StatusFlags::BY_REF_VALUE, // Output parameter
            SqlType::Int(None),        // This is an output parameter. Set to null.
        );

        let named_parameters = vec![param1, param2];

        let stored_procedure_query = "#TempScrollProc";

        connection
            .execute_stored_procedure(
                stored_procedure_query.to_string(),
                None,
                Some(named_parameters),
                None,
                None,
            )
            .await
            .unwrap();

        // Drain all result sets
        loop {
            if let Some(resultset) = connection.get_current_resultset() {
                while resultset.next_row().await.unwrap().is_some() {}
            }
            if !connection.move_to_next().await.unwrap() {
                break;
            }
        }

        let return_values = connection.get_return_values();
        assert_eq!(return_values.len(), 1);
        let returned_parameter = return_values.first().unwrap();
        assert_eq!(returned_parameter.param_name, "@OutputInt".to_string());
        assert_eq!(returned_parameter.value, ColumnValues::Int(45612));
        assert_eq!(returned_parameter.status, ReturnValueStatus::OutputParam);
    }

    #[tokio::test]
    async fn test_sp_execute_sql_multi_param() {
        let query = "select name from sys.databases where database_id = @database_id and compatibility_level > @compat_level";
        let database_id_param = RpcParameter::new(
            Some("@database_id".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(1)),
        );

        let compat_level_param = RpcParameter::new(
            Some("@compat_level".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(100)),
        );

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let named_parameters = vec![database_id_param, compat_level_param];

        connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let scalar_value = get_scalar_value(&mut connection).await.unwrap();

        if let Some(ColumnValues::String(value)) = scalar_value {
            assert_eq!(value.to_utf8_string(), "master".to_string());
        } else {
            unreachable!("Expected a string value");
        }
    }

    #[tokio::test]
    async fn test_sp_execute_sql_single_param() {
        let query = "select name from sys.databases where database_id = @database_id";
        let database_id_param = RpcParameter::new(
            Some("@database_id".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(1)),
        );

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let named_parameters = vec![database_id_param];

        connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let scalar_value = get_scalar_value(&mut connection).await.unwrap();

        if let Some(ColumnValues::String(value)) = scalar_value {
            assert_eq!(value.to_utf8_string(), "master".to_string());
        } else {
            unreachable!("Expected a string value");
        }
    }

    // TODO: TdsClient does not support execute_sp_prepare/unprepare yet
    // These methods exist only on TdsConnection
    #[tokio::test]
    #[ignore]
    async fn test_sp_prepare_and_unprepare_multi_param() {
        let query = "select name from sys.databases where database_id = @database_id and compatibility_level > @compat_level";
        let database_id_param = RpcParameter::new(
            Some("@database_id".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(1)),
        );

        let compat_level_param = RpcParameter::new(
            Some("@compat_level".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(100)),
        );

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let named_parameters = vec![database_id_param, compat_level_param];

        // TODO: Implement execute_sp_prepare on TdsClient
        // let handle = connection
        //     .execute_sp_prepare(query.to_string(), named_parameters, None, None)
        //     .await
        //     .unwrap();

        // assert!(handle > 0);

        // This should simply complete and be successful.
        // let result = connection.execute_sp_unprepare(handle, None, None).await;
        // assert!(result.is_ok());
    }

    // TODO: TdsClient does not support execute_sp_prepexec/execute/unprepare yet
    // These methods exist only on TdsConnection
    #[tokio::test]
    #[ignore]
    async fn test_sp_prepareexec_and_unprepare_multi_param() {
        let _query = "select name from sys.databases where database_id = @database_id and compatibility_level > @compat_level";
        let _database_id_param = RpcParameter::new(
            Some("@database_id".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(1)),
        );

        let _compat_level_param = RpcParameter::new(
            Some("@compat_level".to_string()),
            StatusFlags::NONE,
            SqlType::Int(Some(100)),
        );

        let _context = create_context();
        let mut _connection = begin_connection(_context).await;

        let _named_parameters = vec![_database_id_param, _compat_level_param];

        // TODO: Implement execute_sp_prepexec, execute_sp_execute, execute_sp_unprepare on TdsClient
        // let mut batch_result = connection
        //     .execute_sp_prepexec(query.to_string(), named_parameters.clone(), None, None)
        //     .await
        //     .unwrap();

        // // TODD: WE need to check for data being returned as well, but right now the BatchResult ownership is transferred to
        // // the iterators when retrieving data. Hence we cannot use the close() APis and iterators in tandem right now.
        // // Once Batch result is enhanced we need to enhance this test as well.
        // batch_result.close().await.unwrap();
        // let out_params = batch_result.retrieve_output_params().unwrap();
        // assert!(out_params.is_some());
        // let out_params = out_params.unwrap();
        // assert_eq!(out_params.len(), 1);

        // let handle_param = out_params.first().unwrap();
        // let retrieved_handle = if let ColumnValues::Int(handle) = handle_param.value {
        //     assert!(handle > 0);
        //     handle
        // } else {
        //     unreachable!("Expected a handle value");
        // };
        // assert_eq!(handle_param.status, ReturnValueStatus::OutputParam);

        // let second_result = connection
        //     .execute_sp_execute(retrieved_handle, None, Some(named_parameters), None, None)
        //     .await
        //     .unwrap();
        // let scalar_value = get_scalar_value(second_result).await.unwrap();
        // if let Some(ColumnValues::String(value)) = scalar_value {
        //     assert_eq!(value.to_utf8_string(), "master".to_string());
        // } else {
        //     unreachable!("Expected a string value");
        // }

        // let result = connection
        //     .execute_sp_unprepare(retrieved_handle, None, None)
        //     .await;
        // assert!(result.is_ok());
    }

    // Executes the query and reads till the end of the result.
    async fn execute_non_query(
        connection: &mut mssql_tds::connection::tds_client::TdsClient,
        query: String,
    ) -> TdsResult<()> {
        connection.execute(query, None, None).await?;

        // Drain all result sets
        loop {
            if let Some(resultset) = connection.get_current_resultset() {
                while resultset.next_row().await?.is_some() {}
            }
            if !connection.move_to_next().await? {
                break;
            }
        }

        Ok(())
    }
}
