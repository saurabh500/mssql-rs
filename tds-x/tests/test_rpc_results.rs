#[cfg(test)]
mod common;

mod rpc_results {
    use crate::common::{begin_connection, create_context, init_tracing};
    use futures::StreamExt;
    use tds_x::{
        connection::tds_connection::TdsConnection,
        core::TdsResult,
        datatypes::{decoder::ColumnValues, sqldatatypes::TdsDataType},
        message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
        query::result::{BatchResult, QueryResultType},
        token::tokenitems::ReturnValueStatus,
    };

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test]
    async fn test_stored_proc() {
        let context = create_context();
        let mut connection = begin_connection(&context).await;

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

        let param1 = RpcParameter::new(
            Some("@InputInt".to_string()),
            StatusFlags::NONE,
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(45612),
        );

        let param2 = RpcParameter::new(
            Some("@OutputInt".to_string()),
            StatusFlags::BY_REF_VALUE, // Output parameter
            &TdsDataType::Int4,
            false,
            &ColumnValues::Null, // This is an output parameter. Set to null.
        );

        let named_parameters = vec![param1, param2];

        let stored_procedure_query = "#TempScrollProc";

        let mut result = connection
            .execute_stored_procedure(
                stored_procedure_query.to_string(),
                None,
                Some(&named_parameters),
            )
            .await
            .unwrap();
        let return_values = result.close().await.unwrap();

        assert!(return_values.is_some());
        let returned_parameters = return_values.unwrap();
        assert_eq!(returned_parameters.len(), 1);
        let returned_parameter = returned_parameters.first().unwrap();
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
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(1),
        );

        let compat_level_param = RpcParameter::new(
            Some("@compat_level".to_string()),
            StatusFlags::NONE,
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(100),
        );

        let context = create_context();
        let mut connection = begin_connection(&context).await;

        let named_parameters = vec![database_id_param, compat_level_param];

        let batch_result = connection
            .execute_sp_executesql(query.to_string(), named_parameters)
            .await
            .unwrap();

        let scalar_value = get_scalar_value(batch_result).await.unwrap();

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
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(1),
        );

        let context = create_context();
        let mut connection = begin_connection(&context).await;

        let named_parameters = vec![database_id_param];

        let batch_result = connection
            .execute_sp_executesql(query.to_string(), named_parameters)
            .await
            .unwrap();

        let scalar_value = get_scalar_value(batch_result).await.unwrap();

        if let Some(ColumnValues::String(value)) = scalar_value {
            assert_eq!(value.to_utf8_string(), "master".to_string());
        } else {
            unreachable!("Expected a string value");
        }
    }

    #[tokio::test]
    async fn test_sp_prepare_and_unprepare_multi_param() {
        let query = "select name from sys.databases where database_id = @database_id and compatibility_level > @compat_level";
        let database_id_param = RpcParameter::new(
            Some("@database_id".to_string()),
            StatusFlags::NONE,
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(1),
        );

        let compat_level_param = RpcParameter::new(
            Some("@compat_level".to_string()),
            StatusFlags::NONE,
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(100),
        );

        let context = create_context();
        let mut connection = begin_connection(&context).await;

        let named_parameters = vec![database_id_param, compat_level_param];

        let handle = connection
            .execute_sp_prepare(query.to_string(), named_parameters)
            .await
            .unwrap();

        assert!(handle > 0);

        // This should simply complete and be successful.
        let result = connection.execute_sp_unprepare(handle).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sp_prepareexec_and_unprepare_multi_param() {
        let query = "select name from sys.databases where database_id = @database_id and compatibility_level > @compat_level";
        let database_id_param = RpcParameter::new(
            Some("@database_id".to_string()),
            StatusFlags::NONE,
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(1),
        );

        let compat_level_param = RpcParameter::new(
            Some("@compat_level".to_string()),
            StatusFlags::NONE,
            &TdsDataType::IntN,
            false,
            &ColumnValues::Int(100),
        );

        let context = create_context();
        let mut connection = begin_connection(&context).await;

        let named_parameters = vec![database_id_param, compat_level_param];

        let mut batch_result = connection
            .execute_sp_prepexec(query.to_string(), &named_parameters)
            .await
            .unwrap();

        // TODD: WE need to check for data being returned as well, but right now the BatchResult ownership is transferred to
        // the iterators when retrieving data. Hence we cannot use the close() APis and iterators in tandem right now.
        // Once Batch result is enhanced we need to enhance this test as well.
        let out_params = batch_result.close().await.unwrap();
        assert!(out_params.is_some());
        let out_params = out_params.unwrap();
        assert_eq!(out_params.len(), 1);

        let handle_param = out_params.first().unwrap();
        let retrieved_handle = if let ColumnValues::Int(handle) = handle_param.value {
            assert!(handle > 0);
            handle
        } else {
            unreachable!("Expected a handle value");
        };
        assert_eq!(handle_param.status, ReturnValueStatus::OutputParam);

        let second_result = connection
            .execute_sp_execute(retrieved_handle, None, Some(&named_parameters))
            .await
            .unwrap();
        let scalar_value = get_scalar_value(second_result).await.unwrap();
        if let Some(ColumnValues::String(value)) = scalar_value {
            assert_eq!(value.to_utf8_string(), "master".to_string());
        } else {
            unreachable!("Expected a string value");
        }

        let result = connection.execute_sp_unprepare(retrieved_handle).await;
        assert!(result.is_ok());
    }

    // Returns the first column of the first row of the result set, and drains the resultset.
    async fn get_scalar_value<'a, 'n>(
        batch_result: BatchResult<'n>,
    ) -> TdsResult<Option<ColumnValues>>
    where
        'n: 'a,
    {
        let mut result = None;
        let mut query_result_stream = batch_result.stream_results();

        while let Some(query_result_type) = query_result_stream.next().await {
            let qrt = query_result_type.unwrap();
            match qrt {
                QueryResultType::Update(_) => {
                    // Do Nothing. Skip;
                }
                QueryResultType::ResultSet(rs) => {
                    let mut rowstream = rs.into_row_stream().unwrap();
                    while let Some(row) = rowstream.next().await {
                        let mut unwrapped_row = row.unwrap();

                        if let Some(cell) = unwrapped_row.next().await {
                            result = Some(cell.unwrap().get_value());
                        }
                        if result.is_some() {
                            break;
                        }
                    }
                    rowstream.close().await?;
                }
            }
            if result.is_some() {
                query_result_stream.close().await?;
                break;
            }
        }

        Ok(result)
    }

    // Executes the query and reads till the end of the result.
    async fn execute_non_query<'a, 'n>(
        connection: &'a mut TdsConnection<'n>,
        query: String,
    ) -> TdsResult<()>
    where
        'n: 'a,
    {
        let batch_result = connection.execute(query).await?;
        let mut query_result_stream = batch_result.stream_results();

        while let Some(query_result_type) = query_result_stream.next().await {
            let qrt = query_result_type.unwrap();
            match qrt {
                QueryResultType::Update(_) => {
                    // Do Nothing. Skip;
                }
                QueryResultType::ResultSet(mut rs) => {
                    // Iterate till the end
                    rs.close().await?;
                }
            }
        }

        Ok(())
    }
}
