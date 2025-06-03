#[cfg(not(target_os = "macos"))]
pub(crate) mod query_processing_driver {
    use async_trait::async_trait;
    use core::panic;
    use dotenv::dotenv;
    use std::env;
    use std::time::Duration;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    use crate::core::EncryptionOptions;
    use crate::error::Error;
    use crate::message::headers::{write_headers, TdsHeaders, TransactionDescriptorHeader};
    use crate::message::messages::PacketType;
    use crate::read_write::packet_writer::PacketWriter;
    use crate::{
        connection::{
            client_context::{ClientContext, TransportContext},
            tds_connection::TdsConnection,
        },
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        core::{EncryptionSetting, TdsResult},
        datatypes::{decoder::ColumnValues, sql_string::SqlString, sqldatatypes::TdsDataType},
        message::{
            batch::SqlBatch,
            messages::Request,
            parameters::rpc_parameters::{build_parameter_list_string, RpcParameter, StatusFlags},
            rpc::{RpcProcs, RpcType, SqlRpc},
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
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: trust_server_certificate(),
                host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
            },
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
            &TdsDataType::Int4,
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

    #[tokio::test]
    async fn test_rpc_no_panic() {
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
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: trust_server_certificate(),
                host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
            },
            // database: "drivers".to_string(),
            ..Default::default()
        };

        let mut connection = create_connection(&context).await.unwrap();
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

        let named_parameters = vec![database_id_param, compat_level_param];

        // Use the connection to execute SqlRpc with the stored procedure name and parameters.
        let database_collation = connection.negotiated_settings.database_collation;

        let sql_statement_value =
            ColumnValues::String(SqlString::from_utf8_string(query.to_string()));

        // Create the parameter list for sp_execute_sql
        let statement_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &sql_statement_value,
        );

        // Build the comma separated list of parameters
        let mut params_list_as_string = String::new();

        build_parameter_list_string(&named_parameters, &mut params_list_as_string);

        print!("Params list: {}", params_list_as_string);
        let params_as_sql_string =
            ColumnValues::String(SqlString::from_utf8_string(params_list_as_string));

        let params_parameter = RpcParameter::new(
            None,
            StatusFlags::NONE,
            &TdsDataType::NVarChar,
            false,
            &params_as_sql_string,
        );

        let handle_parameter = RpcParameter::new(
            None,
            StatusFlags::BY_REF_VALUE,
            &TdsDataType::Int4,
            false,
            &ColumnValues::Null,
        );

        let positional_parameters_list =
            vec![handle_parameter, params_parameter, statement_parameter];
        let positional_parameters = Some(&positional_parameters_list);

        // Build the RPC request.
        let rpc = SqlRpc::new(
            RpcType::ProcId(RpcProcs::PrepExec),
            positional_parameters,
            Some(&named_parameters),
            &database_collation,
            &connection.execution_context,
        );

        rpc.serialize_and_handle_timeout(connection.as_mut(), None, None)
            .await
            .unwrap();
        iterate_over_rpc_tokens(&mut connection).await;
    }

    async fn submit_stored_procedure(
        connection: &mut Box<TdsConnection<'_>>,
        stored_proc_name: String,
        named_parameters: Vec<RpcParameter<'_>>,
    ) -> TdsResult<()> {
        let database_collation = connection.negotiated_settings.database_collation;

        let rpc = SqlRpc::new(
            RpcType::Named(stored_proc_name),
            None,
            Some(&named_parameters),
            &database_collation,
            &connection.execution_context,
        );

        rpc.serialize_and_handle_timeout(connection.as_mut(), None, None)
            .await?;

        iterate_over_rpc_tokens(connection).await;
        Ok(())
    }

    async fn iterate_over_rpc_tokens(connection: &mut Box<TdsConnection<'_>>) {
        // Now read the results.
        let packet_reader = connection.transport.get_packet_reader();
        let mut token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );

        let mut parser_context = ParserContext::default();
        let mut _row_count = 0;
        while let Ok(token) = token_stream_reader
            .receive_token(&parser_context, None, None)
            .await
        {
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
                    panic!("Error token received: {:?}", t1);
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
                MoneyColumn MONEY NOT NULL,
                SmallMoneyColumn SMALLMONEY NOT NULL,
                MoneyNColumn MONEY NULL,
                SmallMoneyNColumn SMALLMONEY NULL,
                DateSmallColumn SMALLDATETIME NULL,
                DateTimeColumn DATETIME NULL,
            );
        
            INSERT INTO #AllDataTypes (
                TinyIntColumn, SmallIntColumn, IntColumn, BigIntColumn, BitColumn, 
                DecimalColumn, NumericColumn, FloatColumn, RealColumn,
                MoneyColumn, SmallMoneyColumn, MoneyNColumn, SmallMoneyNColumn,
                DateSmallColumn, DateTimeColumn
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
                CAST(1234.5678 AS MONEY), -- MoneyColumn
                CAST(5678.1234 AS SMALLMONEY), -- SmallMoneyColumn
                CAST(1234.0 AS MONEY), -- MoneyNColumn
                CAST(567.89 AS SMALLMONEY), -- SmallMoneyNColumn
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
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS VARCHAR(MAX)) COLLATE Latin1_General_100_CI_AS_SC_UTF8; 
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' as NTEXT);
            SELECT CAST(NULL as NTEXT);
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' as TEXT) as TextColumn;
            SELECT CAST(NULL as TEXT);",
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

    #[tokio::test]
    async fn test_money_no_panic() {
        execute_test_query(
            "
                -- Test null values
                SELECT CAST(NULL AS MONEY);
                SELECT CAST(NULL AS SMALLMONEY);
                -- Test whole numbers
                SELECT CAST(123 AS MONEY);
                SELECT CAST(123 AS SMALLMONEY);
                -- Test max values
                SELECT CAST(922337203685477.5807 AS MONEY);
                SELECT CAST(214748.3647 AS SMALLMONEY); -- TODO: Fix precision lost
                SELECT CAST(-922337203685477.5808 AS MONEY);
                SELECT CAST(-214748.3648 AS SMALLMONEY); -- TODO: Fix precision lost
                ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_datetimes_no_panic() {
        execute_test_query(
            "
        -- Test null values
        SELECT CAST(NULL AS SMALLDATETIME);
        SELECT CAST(NULL AS DATETIME);
        -- Test typical values
        SELECT CAST('2019-06-06 12:01:01' AS SMALLDATETIME);
        SELECT CAST('2019-06-06 12:01:01.11' AS DATETIME);
        -- Test max values
        SELECT CAST('2079-06-06 23:59:00' AS SMALLDATETIME);
        SELECT CAST('9999-12-31  23:59:59.997' AS DATETIME)
        -- Test min values
        SELECT CAST('1900-01-01 00:00:00' AS SMALLDATETIME);
        SELECT CAST('1900-01-01 00:00:00' AS DATETIME)",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_tds73_datetimes_no_panic() {
        execute_test_query(
            "
        -- Test null values
        SELECT CAST(NULL AS DATE);
        SELECT CAST(NULL AS TIME);
        SELECT CAST(NULL AS DATETIME2);
        SELECT CAST(NULL AS DATETIMEOFFSET);
        -- Test typical values
        SELECT CAST('2019-06-06' AS DATE);
        SELECT CAST('12:10:15.113244' AS TIME);
        SELECT CAST('2019-06-06 12:01:01.11' AS DATETIME2);
        SELECT CAST('2019-06-06 12:01:01.11 +10:12' AS DATETIMEOFFSET);
        -- Test max values
        SELECT CAST('9999-12-31' AS DATE);
        SELECT CAST('12:10:15.113244' AS TIME);
        SELECT CAST('9999-12-31 23:59:59.9999999' AS DATETIME2);
        SELECT CAST('9999-12-31 23:59:59.9999999 +14:00' AS DATETIMEOFFSET);
        -- Test min values
        SELECT CAST('0001-01-01' AS DATE);
        SELECT CAST('00:00:00' AS TIME);
        SELECT CAST('0001-01-01 00:00:00' AS DATETIME2);
        SELECT CAST('0001-01-01 00:00:00 -14:00' AS DATETIMEOFFSET)",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_cancel_partially_sent_request() {
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
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: trust_server_certificate(),
                host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
            },
            //      database: "drivers".to_string(),
            packet_size: 512, // Minimal packet size for testing.
            ..Default::default()
        };

        // Dummy request implementation which sends out a SqlBatch split into multiple packets
        // with delays in between.
        struct SlowRequest {
            pub headers: Vec<TdsHeaders>,
        }

        impl Default for SlowRequest {
            fn default() -> Self {
                let transaction_descriptor_header =
                    TransactionDescriptorHeader::create_non_transaction_header();
                Self {
                    headers: Vec::from([transaction_descriptor_header.into()]),
                }
            }
        }

        #[async_trait]
        impl Request for SlowRequest {
            fn packet_type(&self) -> PacketType {
                PacketType::SqlBatch
            }

            async fn serialize<'a, 'b>(
                &'a self,
                packet_writer: &'a mut PacketWriter<'b>,
            ) -> TdsResult<()>
            where
                'b: 'a,
            {
                // Copied from SqlBatch, but with a hard-coded command split into two packets.
                write_headers(&self.headers, packet_writer).await?;
                let long_text = "a".repeat(512);
                packet_writer
                    .write_string_unicode_async(long_text.as_str())
                    .await?;
                // Sleep to force a cancel on the next write.
                // Note that the timeout interrupts the IO in PacketWriter only, and not the
                // entire serialize() function to avoid writing partial packets.
                // This prevents the test from interrupting the sleep function though.
                tokio::time::sleep(Duration::from_secs(3)).await;
                packet_writer.finalize().await?;
                Ok(())
            }
        }

        let mut connection = create_connection(&context).await.unwrap();
        let slow_request = SlowRequest::default();
        match slow_request
            .serialize_and_handle_timeout(&mut connection, Some(2), None)
            .await
        {
            Ok(_) => {
                std::panic!("Operation should not have succeeded.")
            }
            Err(error) => match error {
                Error::TimeoutError(_) => {} // Success
                _ => {
                    std::panic!("Expected timeout error but got {:?}", error);
                }
            },
        }
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
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: trust_server_certificate(),
                host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
            },
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
            encryption_options: EncryptionOptions {
                mode: EncryptionSetting::On,
                trust_server_certificate: trust_server_certificate(),
                host_name_in_cert: env::var("CERT_HOST_NAME").ok(),
            },
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
        let connection_result = provider.create_connection(context, None).await?;
        Ok(Box::new(connection_result))
    }

    pub async fn submit_sql_batch(
        tds_connection: &mut Box<TdsConnection<'_>>,
        sql_command: String,
    ) -> TdsResult<()> {
        let batch = SqlBatch::new(sql_command, &tds_connection.execution_context);
        batch
            .serialize_and_handle_timeout(tds_connection.as_mut(), None, None)
            .await?;

        let packet_reader = tds_connection.transport.get_packet_reader();
        let mut token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );

        let mut parser_context = ParserContext::default();
        let mut _row_count = 0;
        loop {
            let token = token_stream_reader
                .receive_token(&parser_context, None, None)
                .await?;
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
                    panic!("Error token received: {:?}", t1);
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

    fn trust_server_certificate() -> bool {
        env::var("TRUST_SERVER_CERTIFICATE")
            .map(|v| v.parse().unwrap_or(false))
            .unwrap_or(false)
    }
}
