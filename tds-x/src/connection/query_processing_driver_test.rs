#[cfg(test)]
#[cfg(not(target_os = "macos"))]
pub(crate) mod query_processing_driver {
    use dotenv::dotenv;
    use std::env;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    use crate::{
        connection::{
            client_context::{ClientContext, TransportContext},
            tds_connection::TdsConnection,
        },
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        core::{EncryptionSetting, TdsResult},
        datatypes::{decoder::ColumnValues, sqldatatypes::TdsDataType},
        message::{
            batch::SqlBatch,
            messages::Request,
            parameters::rpc_parameters::{RpcParameter, StatusFlags},
            rpc::{RpcType, SqlRpc},
        },
        read_write::{
            reader_writer::NetworkReader,
            token_stream::{GenericTokenParserRegistry, ParserContext, TokenStreamReader},
        },
        token::tokens::{DoneStatus, Tokens},
    };

    #[tokio::test]
    async fn test_stored_proc_execution_no_panic() {
        dotenv().ok();

        let enable_trace = env::var("ENABLE_TRACE")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap();

        if enable_trace {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(Level::TRACE)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("Setting default subscriber failed");
            // Setup the TDS connection.
        }

        let transport = TransportContext::Tcp {
            host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: env::var("DB_PORT")
                .expect("DB_PORT environment variable not set")
                .parse::<u16>()
                .expect("DB_PORT must be a valid u16"),
        };
        let context = ClientContext {
            transport_context: transport,
            user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
            password: env::var("SQL_PASSWORD").expect("SQL_PASSWORD environment variable not set"),
            encryption: EncryptionSetting::On,
            // database: "drivers".to_string(),
            ..Default::default()
        };

        let mut connection = create_connection(&context).await.unwrap();

        // Create a query to setup the stored procedure. This will be a Sql Batch execution.
        let stored_procedure_setup_query = "CREATE PROCEDURE #TempScrollProc
                @InputInt INT,
                @OutputInt INT OUTPUT
            AS
            BEGIN
                SET @OutputInt = @InputInt;
            END;";

        // This should setup the temp stored procedure on this connection.
        submit_sql_batch(&mut connection, stored_procedure_setup_query.to_string())
            .await
            .unwrap();

        let param1 = RpcParameter::new(
            Some("@InputInt".to_string()),
            StatusFlags::NONE,
            &crate::datatypes::sqldatatypes::TdsDataType::IntN,
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
        // Use the connection to execute SqlRpc with the stored procedure name and parameters.
        submit_stored_procedure(
            &mut connection,
            "#TempScrollProc".to_string(),
            named_parameters,
        )
        .await
        .unwrap();
    }

    async fn submit_stored_procedure(
        connection: &mut Box<TdsConnection<'_>>,
        stored_proc_name: String,
        named_parameters: Vec<RpcParameter<'_>>,
    ) -> TdsResult<()> {
        let database_collation = connection.negotiated_settings.database_collation;

        let rpc = SqlRpc::new(
            RpcType::Named {
                name: stored_proc_name,
            },
            None,
            Some(named_parameters),
            &database_collation,
            &connection.execution_context,
        );

        rpc.serialize(connection.transport.as_mut()).await?;

        // Now read the results.
        let packet_reader = connection.transport.get_packet_reader();
        let mut token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );

        let mut parser_context = ParserContext::default();
        let mut _row_count = 0;
        while let Ok(token) = token_stream_reader.receive_token(&parser_context).await {
            // let token = token_stream_reader.receive_token().await?;
            match token {
                Tokens::Done(t1) => {
                    println!("Received Done token: {:?}", t1);
                    if !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::DoneInProc(t1) => {
                    println!("Received DoneInProc token: {:?}", t1);
                    parser_context = ParserContext::None(());
                }
                Tokens::DoneProc(t1) => {
                    println!("Received DoneProc token: {:?}", t1);
                    parser_context = ParserContext::None(());
                    if !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::EnvChange(t1) => {
                    println!("Received EnvChange token: {:?}", t1);
                }
                Tokens::Error(t1) => {
                    println!("Received Error token: {:?}", t1);
                }
                Tokens::FeatureExtAck(t1) => {
                    println!("Received FeatureExtAck token: {:?}", t1);
                }
                Tokens::ColMetadata(column_metadata) => {
                    println!("Received ColMetadata token: {:?}", column_metadata);
                    _row_count = 0;
                    parser_context = ParserContext::ColumnMetadata(column_metadata);
                }
                Tokens::Row(row) => {
                    // Just print the first row, to avoid cluttering the output
                    // if row_count == 0 {
                    //     println!("Received Row Data: {:?}", row);
                    // }
                    _row_count += 1;
                    println!("Received Row Index: {:?}", row);
                }
                _ => {
                    println!("Received token: {:?}", token);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_single_query_no_panic() {
        execute_test_query("sp_who2").await.unwrap();
    }

    #[tokio::test]
    async fn test_multi_queries() {
        let query1 = "CREATE PROCEDURE #TempScrollProc
            @InputInt INT,
            @OutputInt INT OUTPUT
        AS
        BEGIN
            SET @OutputInt = @InputInt;
        END;";

        let query2 = "
            -- Declare a variable to hold the output
            DECLARE @Result INT;

            -- Execute the stored procedure
            EXEC #TempScrollProc 
                @InputInt = 123, 
                @OutputInt = @Result OUTPUT;

            -- Show the result
            SELECT @Result AS EchoedOutput;";

        let all_queries = vec![query1, query2];
        execute_test_multi_query(all_queries).await.unwrap();
    }

    #[tokio::test]
    async fn test_multi_query_no_panic() {
        execute_test_query("select * from sys.databases; select * from sys.columns")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_multi_mixed_queries_no_panic() {
        execute_test_query(
            "
            select * from sys.databases; 
            CREATE TABLE #TempTable (ID BIGINT);    
            select * from sys.columns; 
            INSERT INTO #TempTable (ID) VALUES (100), (200), (300); 
            select * from #TempTable;
            UPDATE #TempTable SET ID = 200000 WHERE ID = 200; 
            SELECT * FROM #TempTable;
            DELETE FROM #TempTable WHERE ID = 300;
            SELECT * FROM #TempTable;
        ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_data_types_numerics_no_panic() {
        execute_test_query(
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
                DateSmallColumn SMALLDATETIME NULL,
                DateTimeColumn DATETIME NULL
            );
        
            INSERT INTO #AllDataTypes (
                TinyIntColumn, SmallIntColumn, IntColumn, BigIntColumn, BitColumn, 
                DecimalColumn, NumericColumn, FloatColumn, RealColumn, DateSmallColumn, DateTimeColumn
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
                CAST(11.11 AS REAL), -- RealColumn
                CAST('1/1/2000 1:00' as SMALLDATETIME),
                CAST('1/1/2000 1:00' as DATETIME)
            );
            select * from #AllDataTypes;",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_strings_no_panic() {
        execute_test_query(
            "SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS NVARCHAR(500)) COLLATE Latin1_General_100_CI_AS_SC_UTF8; 
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS NCHAR(500)) COLLATE Latin1_General_100_CI_AS_SC_UTF8;
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS VARCHAR(500)) COLLATE Latin1_General_100_CI_AS_SC_UTF8; 
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS CHAR(500)) COLLATE Latin1_General_100_CI_AS_SC_UTF8; 
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS NVARCHAR(MAX)) COLLATE Latin1_General_100_CI_AS_SC_UTF8; 
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS VARCHAR(MAX)) COLLATE Latin1_General_100_CI_AS_SC_UTF8; ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_data_types_numerics_null_values_no_panic() {
        execute_test_query(
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
                null, null, null, null, null, null, null, null, null
            );
            select * from #AllDataTypes",
        )
        .await
        .unwrap();
    }

    pub async fn execute_test_query(query: &str) -> TdsResult<()> {
        dotenv().ok();

        let transport = TransportContext::Tcp {
            host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: env::var("DB_PORT")
                .expect("DB_PORT environment variable not set")
                .parse::<u16>()
                .expect("DB_PORT must be a valid u16"),
        };
        let context = ClientContext {
            transport_context: transport,
            user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
            password: env::var("SQL_PASSWORD").expect("SQL_PASSWORD environment variable not set"),
            encryption: EncryptionSetting::On,
            // database: "drivers".to_string(),
            ..Default::default()
        };
        let mut connection = create_connection(&context).await.unwrap();

        submit_sql_batch(&mut connection, query.to_string()).await
    }

    pub async fn execute_test_multi_query(query: Vec<&str>) -> TdsResult<()> {
        dotenv().ok();

        let transport = TransportContext::Tcp {
            host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: env::var("DB_PORT")
                .expect("DB_PORT environment variable not set")
                .parse::<u16>()
                .expect("DB_PORT must be a valid u16"),
        };
        let context = ClientContext {
            transport_context: transport,
            user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
            password: env::var("SQL_PASSWORD").expect("SQL_PASSWORD environment variable not set"),
            encryption: EncryptionSetting::On,
            // database: "drivers".to_string(),
            ..Default::default()
        };
        let mut connection = create_connection(&context).await.unwrap();
        for q in query {
            println!("Executing query: {}", q);
            submit_sql_batch(&mut connection, q.to_string()).await?;
        }
        Ok(())
        // submit_sql_batch(connection, query.to_string()).await
    }

    pub async fn create_connection(context: &ClientContext) -> TdsResult<Box<TdsConnection>> {
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(context).await?;
        Ok(Box::new(connection_result))
    }

    pub async fn submit_sql_batch(
        tds_connection: &mut Box<TdsConnection<'_>>,
        sql_command: String,
    ) -> TdsResult<()> {
        let batch = SqlBatch::new(sql_command, &tds_connection.execution_context);
        batch.serialize(tds_connection.transport.as_mut()).await?;

        let packet_reader = tds_connection.transport.get_packet_reader();
        let mut token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );

        let mut parser_context = ParserContext::default();
        let mut _row_count = 0;
        while let Ok(token) = token_stream_reader.receive_token(&parser_context).await {
            // let token = token_stream_reader.receive_token().await?;
            match token {
                Tokens::Done(t1) => {
                    println!("Received Done token: {:?}", t1);
                    if !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::DoneInProc(t1) => {
                    println!("Received DoneInProc token: {:?}", t1);
                    parser_context = ParserContext::None(());
                }
                Tokens::DoneProc(t1) => {
                    println!("Received DoneProc token: {:?}", t1);
                    parser_context = ParserContext::None(());
                    if !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::EnvChange(t1) => {
                    println!("Received EnvChange token: {:?}", t1);
                }
                Tokens::Error(t1) => {
                    println!("Received Error token: {:?}", t1);
                }
                Tokens::FeatureExtAck(t1) => {
                    println!("Received FeatureExtAck token: {:?}", t1);
                }
                Tokens::ColMetadata(column_metadata) => {
                    println!("Received ColMetadata token: {:?}", column_metadata);
                    _row_count = 0;
                    parser_context = ParserContext::ColumnMetadata(column_metadata);
                }
                Tokens::Row(row) => {
                    // Just print the first row, to avoid cluttering the output
                    // if row_count == 0 {
                    //     println!("Received Row Data: {:?}", row);
                    // }
                    _row_count += 1;
                    println!("Received Row Index: {:?}", row);
                }
                _ => {
                    println!("Received token: {:?}", token);
                }
            }
        }
        Ok(())
    }
}
