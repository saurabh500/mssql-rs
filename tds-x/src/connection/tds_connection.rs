use super::transport::network_transport::NetworkTransport;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::message::batch::SqlBatch;
use crate::message::messages::Request;
use crate::query::result::BatchResult;
use std::io::Error;

pub struct TdsConnection<'a> {
    pub(crate) transport: Box<NetworkTransport<'a>>,
    pub(crate) negotiated_settings: NegotiatedSettings,
}

impl<'connection, 'result> TdsConnection<'connection> {
    pub async fn execute(
        &'result mut self,
        sql_command: String,
    ) -> Result<BatchResult<'result>, Error>
    where
        'connection: 'result,
    {
        let batch = SqlBatch::new(sql_command);

        batch.serialize(self.transport.as_mut()).await?;
        // let response = SqlQueryResponse::new(tds_connection);

        Ok(BatchResult::new(self))
    }
}

// Remove the ignore attribute to run the test.

#[cfg(test)]
mod query_processing_driver {
    use std::io::Error;

    use crate::{
        connection::client_context::ClientContext,
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        message::{batch::SqlBatch, messages::Request},
        read_write::{
            reader_writer::NetworkReader,
            token_stream::{GenericTokenParserRegistry, ParserContext, TokenStreamReader},
        },
        token::tokens::{DoneStatus, Tokens},
    };

    use super::TdsConnection;

    #[ignore]
    #[tokio::test]
    async fn test_single_query_no_panic() {
        execut_test_query("select * from sys.databases")
            .await
            .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_multi_query_no_panic() {
        execut_test_query("select * from sys.databases; select * from sys.columns")
            .await
            .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_multi_mixed_queries_no_panic() {
        execut_test_query(
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

    #[ignore]
    #[tokio::test]
    async fn test_data_types_numerics_no_panic() {
        execut_test_query(
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
            );
            select * from #AllDataTypes;",
        )
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_strings_no_panic() {
        execut_test_query(
            "
            SELECT CAST('SOMETHING SOMETHING SOMETHING SOMETHING' AS VARCHAR(MAX))
            
        ",
        )
        .await
        .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_data_types_numerics_null_values_no_panic() {
        execut_test_query(
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

    pub async fn execut_test_query(query: &str) -> Result<(), Error> {
        let context = ClientContext {
            server_name: "saurabhsingh.database.windows.net".to_string(),
            port: 1433,
            user_name: "saurabh".to_string(),
            password: std::fs::read_to_string("/tmp/password")
                .expect("Failed to read password file")
                .trim()
                .to_string(),
            database: "drivers".to_string(),
            ..Default::default()
        };
        let connection = create_connection(&context).await.unwrap();

        submit_sql_batch(connection, query.to_string()).await
    }

    pub async fn create_connection(context: &ClientContext) -> Result<Box<TdsConnection>, Error> {
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(context).await?;
        Ok(Box::new(connection_result))
    }

    pub async fn submit_sql_batch(
        mut tds_connection: Box<TdsConnection<'_>>,
        sql_command: String,
    ) -> Result<(), Error> {
        let batch = SqlBatch::new(sql_command);
        batch.serialize(tds_connection.transport.as_mut()).await?;

        let packet_reader = tds_connection.transport.get_packet_reader();
        let mut token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );

        let mut parser_context = ParserContext::default();
        let mut row_count = 0;
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
                    row_count = 0;
                    parser_context = ParserContext::ColumnMetadata(column_metadata);
                }
                Tokens::Row(row) => {
                    // Just print the first row, to avoid cluttering the output
                    if row_count == 0 {
                        println!("Received Row Data: {:?}", row);
                    }
                    row_count += 1;
                    // println!("Received Row Index: {:?}", row_count);
                }
                _ => {
                    println!("Received token: {:?}", token);
                }
            }
        }
        Ok(())
    }
}
