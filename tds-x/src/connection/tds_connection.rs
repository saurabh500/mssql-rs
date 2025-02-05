use super::transport::network_transport::NetworkTransport;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::query::result::QueryResult;

pub struct TdsConnection<'a> {
    pub(crate) transport: Box<NetworkTransport<'a>>,
    pub(crate) negotiated_settings: NegotiatedSettings,
}

impl TdsConnection<'_> {
    pub async fn execute(&self, _query_text: String) -> QueryResult {
        todo!()
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
            packet_reader::PacketReader,
            reader_writer::NetworkReaderWriterImpl,
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
        let mut nrw = NetworkReaderWriterImpl {
            transport: tds_connection.transport.as_mut(),
            packet_size: tds_connection
                .negotiated_settings
                .session_settings
                .packet_size,
        };

        batch.serialize(&mut nrw).await?;
        // let response = SqlQueryResponse::new(tds_connection);

        let packet_reader = PacketReader::new(&mut nrw);
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
                    // println!("Received ColMetadata token: {:?}", column_metadata);
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
