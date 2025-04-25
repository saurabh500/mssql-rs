#[cfg(test)]
mod common;

mod rpc_results {
    use crate::common::{begin_connection, create_context};
    use futures::StreamExt;
    use tds_x::{
        connection::tds_connection::TdsConnection,
        core::TdsResult,
        datatypes::{decoder::ColumnValues, sqldatatypes::TdsDataType},
        message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
        query::result::QueryResultType,
        token::tokenitems::ReturnValueStatus,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
            &TdsDataType::IntN,
            false,
            &ColumnValues::Null, // This is an output parameter. Set to null.
        );

        let named_parameters = vec![param1, param2];

        let stored_procedure_query = "#TempScrollProc";

        let mut result = connection
            .execute_stored_procedure(
                stored_procedure_query.to_string(),
                None,
                Some(named_parameters),
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
