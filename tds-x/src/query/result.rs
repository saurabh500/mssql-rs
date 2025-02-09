use crate::connection::tds_connection::TdsConnection;
use crate::datatypes::decoder::ColumnValues;
use crate::handler::handler_factory::NegotiatedSettings;
use crate::query::metadata::ColumnMetadata;
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::token_stream::{
    GenericTokenParserRegistry, ParserContext, TokenStreamReader,
};
use crate::token::tokens::{ColMetadataToken, DoneStatus, Tokens};
use futures::Stream;
use std::future::Future;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::vec::IntoIter;

pub enum QueryResultType<'result> {
    Update(i64, BatchResult<'result>),
    ResultSet(ResultSet<'result>),
}

impl<'result> QueryResultType<'result> {
    fn is_last(&self) -> bool {
        match self {
            QueryResultType::Update(_, batch_result) => batch_result.received_last,
            QueryResultType::ResultSet(result_set) => result_set.parent_batch.received_last,
        }
    }

    fn get_batch_result(self) -> BatchResult<'result> {
        match self {
            QueryResultType::Update(_, batch_result) => batch_result,
            QueryResultType::ResultSet(result_set) => result_set.parent_batch,
        }
    }
}

impl QueryResultType<'_> {
    async fn from_token(
        token: Tokens,
        mut parent_batch: BatchResult<'_>,
    ) -> Result<QueryResultType<'_>, Error> {
        match token {
            Tokens::Done(t1) => {
                println!("Received Done token: {:?}", t1);
                if !t1.status.contains(DoneStatus::MORE) {
                    parent_batch.received_last = true;
                }
                Ok(QueryResultType::Update(t1.row_count as i64, parent_batch))
            }
            Tokens::DoneInProc(t1) => {
                println!("Received DoneInProc token: {:?}", t1);
                if !t1.status.contains(DoneStatus::MORE) {
                    parent_batch.received_last = true;
                }
                parent_batch.parser_context = ParserContext::None(());
                Ok(QueryResultType::Update(t1.row_count as i64, parent_batch))
            }
            Tokens::DoneProc(t1) => {
                println!("Received DoneProc token: {:?}", t1);
                if !t1.status.contains(DoneStatus::MORE) {
                    parent_batch.received_last = true;
                }
                parent_batch.parser_context = ParserContext::None(());
                Ok(QueryResultType::Update(t1.row_count as i64, parent_batch))
            }
            Tokens::EnvChange(t1) => {
                println!("Received EnvChange token: {:?}", t1);
                todo!()
                //QueryResultType::Update(0)
            }
            Tokens::Error(t1) => {
                println!("Received Error token: {:?}", t1);
                panic!("Received error token: {:?}", t1);
            }
            Tokens::FeatureExtAck(t1) => {
                println!("Received FeatureExtAck token: {:?}", t1);
                todo!()
            }
            Tokens::ColMetadata(column_metadata) => {
                // println!("Received ColMetadata token: {:?}", column_metadata);
                // Start a QueryResultType::ResultSet here.
                // ResultSet needs to notify BatchResultType if there's another result
                // when it sees the Done token.
                parent_batch.parser_context =
                    ParserContext::ColumnMetadata(column_metadata.clone());
                Ok(QueryResultType::ResultSet(ResultSet::new(
                    parent_batch,
                    column_metadata,
                )))
            }
            Tokens::Row(row) => {
                // Just print the first row, to avoid cluttering the output
                // println!("Received Row Index: {:?}", row_count);
                panic!("Received row token: {:?}", row);
                //panic!("Received unexpected token: {:?}", token)
            }
            _ => {
                //println!("Received token: {:?}", token);
                panic!("Received unexpected token: {:?}", token)
            }
        }
    }
}

pub struct BatchResult<'result> {
    negotiated_settings: &'result mut NegotiatedSettings,
    token_stream_reader: TokenStreamReader<'result>,
    parser_context: ParserContext,
    received_last: bool,
}

impl<'connection, 'result> BatchResult<'result>
where
    'connection: 'result,
{
    pub(crate) fn new(
        tds_connection: &'result mut TdsConnection<'connection>,
    ) -> BatchResult<'result> {
        let packet_reader = PacketReader::new(tds_connection.transport.as_mut());
        let token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );
        BatchResult {
            negotiated_settings: &mut tds_connection.negotiated_settings,
            token_stream_reader,
            parser_context: ParserContext::default(),
            received_last: false,
        }
    }

    pub fn stream_results(
        self,
    ) -> impl Stream<Item = Result<QueryResultType<'result>, Error>> + 'result {
        QueryResultTypeStream::new(self)
    }

    pub fn get_all_results(self) -> Vec<QueryResultType<'result>> {
        todo!()
    }
}

#[allow(clippy::type_complexity)]
pub struct QueryResultTypeStream<'result> {
    initial_batch_result: Option<BatchResult<'result>>,
    previous_result: Option<Result<QueryResultType<'result>, Error>>,
    executing_future:
        Option<Pin<Box<dyn Future<Output = Result<QueryResultType<'result>, Error>> + 'result>>>,
}

impl<'result> QueryResultTypeStream<'result> {
    fn new(initial_batch_result: BatchResult<'result>) -> QueryResultTypeStream<'result> {
        QueryResultTypeStream {
            initial_batch_result: Some(initial_batch_result),
            previous_result: None,
            executing_future: None,
        }
    }
}

impl<'result> Stream for QueryResultTypeStream<'result> {
    type Item = Result<QueryResultType<'result>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(initial_batch_result) = self.initial_batch_result.take() {
            // Dummy previous result to just hold the initial batch. The caller never sees this.
            self.previous_result = Some(Ok(QueryResultType::Update(0, initial_batch_result)));
        }

        // Take back the batch result from the previous result.
        if let Some(previous_result) = self.previous_result.take() {
            let previous_result = previous_result?;

            // Verify that the done token without a more hasn't already been hit.
            if previous_result.is_last() {
                return Poll::Ready(None);
            }

            // Start getting the next token.
            let future = async move {
                let mut batch_result = previous_result.get_batch_result();
                let token = batch_result
                    .token_stream_reader
                    .receive_token(&batch_result.parser_context)
                    .await;
                QueryResultType::from_token(token.unwrap(), batch_result).await
            };
            self.executing_future = Some(Box::pin(future));
        }

        // Poll the executing future.
        if let Some(mut future) = self.executing_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Pending => {
                    // Put this future back so that it can be polled again.
                    self.executing_future = Some(future);
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(Some(result)),
            }
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct ResultSet<'result> {
    metadata: Vec<ColumnMetadata>,
    parent_batch: BatchResult<'result>,
    received_last_row: bool,
    row_count: Option<u64>,
}

impl<'result> ResultSet<'result> {
    pub(crate) fn new(parent_batch: BatchResult<'result>, col_token: ColMetadataToken) -> Self {
        ResultSet {
            metadata: col_token.columns,
            parent_batch,
            received_last_row: false,
            row_count: None,
        }
    }

    pub async fn get_all_data(self) -> Result<Vec<Vec<CellData>>, Error> {
        // Internally iterate over the row data and cache all the CellDatas into vectors.
        todo!();
    }

    pub fn get_metadata(&self) -> &Vec<ColumnMetadata> {
        self.metadata.as_ref()
    }

    pub async fn into_row_stream(self) -> Result<RowStream<'result>, Error> {
        Ok(RowStream::new(self))
    }
}

#[allow(clippy::type_complexity)]
pub struct RowStream<'result> {
    parent_result_set: Option<ResultSet<'result>>,
    previous_result: Option<Result<RowData<'result>, Error>>,
    executing_future:
        Option<Pin<Box<dyn Future<Output = Result<RowData<'result>, Error>> + 'result>>>,
}

impl<'result> RowStream<'result> {
    fn new(parent_result_set: ResultSet<'result>) -> Self {
        RowStream {
            parent_result_set: Some(parent_result_set),
            previous_result: None,
            executing_future: None,
        }
    }

    async fn from_token(
        token: Tokens,
        mut parent_result: ResultSet<'result>,
    ) -> Result<RowData<'result>, Error> {
        match token {
            Tokens::Done(t1) => {
                println!("Received Done token: {:?}", t1);
                if !t1.status.contains(DoneStatus::MORE) {
                    parent_result.parent_batch.received_last = true;
                }
                parent_result.received_last_row = true;
                parent_result.row_count = Some(t1.row_count);
                Ok(RowData::new(Vec::new(), parent_result))
            }
            Tokens::DoneInProc(t1) => {
                println!("Received DoneInProc token: {:?}", t1);
                if !t1.status.contains(DoneStatus::MORE) {
                    parent_result.parent_batch.received_last = true;
                }
                parent_result.received_last_row = true;
                parent_result.row_count = Some(t1.row_count);
                Ok(RowData::new(Vec::new(), parent_result))
            }
            Tokens::DoneProc(t1) => {
                println!("Received DoneProc token: {:?}", t1);
                if !t1.status.contains(DoneStatus::MORE) {
                    parent_result.parent_batch.received_last = true;
                }
                parent_result.received_last_row = true;
                parent_result.row_count = Some(t1.row_count);
                Ok(RowData::new(Vec::new(), parent_result))
            }
            Tokens::Error(t1) => {
                println!("Received Error token: {:?}", t1);
                panic!("Received error token: {:?}", t1);
            }
            Tokens::Row(row) => Ok(RowData::new(row.all_values, parent_result)),
            _ => {
                //println!("Received token: {:?}", token);
                panic!("Received unexpected token: {:?}", token)
            }
        }
    }
}

impl<'result> Stream for RowStream<'result> {
    type Item = Result<RowData<'result>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(parent_result) = self.parent_result_set.take() {
            // Dummy previous result to just hold the parent ResultSet. The caller never sees this.
            self.previous_result = Some(Ok(RowData::new(Vec::new(), parent_result)));
        }

        // Take back the parent ResultSet from the previous result.
        if let Some(previous_result) = self.previous_result.take() {
            let mut previous_result = previous_result?;

            // Verify that the done token hasn't already been hit.
            let mut parent_result_set = previous_result.parent_result.take().unwrap();
            if parent_result_set.received_last_row {
                return Poll::Ready(None);
            }

            // Start getting the next token.
            let future = async move {
                let token = parent_result_set
                    .parent_batch
                    .token_stream_reader
                    .receive_token(&parent_result_set.parent_batch.parser_context)
                    .await;
                Self::from_token(token.unwrap(), parent_result_set).await
            };
            self.executing_future = Some(Box::pin(future));
        }

        // Poll the executing future.
        if let Some(mut future) = self.executing_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Pending => {
                    // Put this future back so that it can be polled again.
                    self.executing_future = Some(future);
                    Poll::Pending
                }
                Poll::Ready(result) => {
                    if result.as_ref().unwrap().is_terminal() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(result))
                    }
                }
            }
        } else {
            Poll::Ready(None)
        }
    }
}

pub struct RowData<'result> {
    iterator: IntoIter<ColumnValues>,
    parent_result: Option<ResultSet<'result>>,
}

impl RowData<'_> {
    fn new(column_values: Vec<ColumnValues>, parent_result: ResultSet<'_>) -> RowData<'_> {
        RowData {
            iterator: column_values.into_iter(),
            parent_result: Some(parent_result),
        }
    }

    fn is_terminal(&self) -> bool {
        self.parent_result.as_ref().unwrap().received_last_row
    }
}

// Todo: The token parser currently deserializes all row values at once.
// It should be streamed instead. This implements the Stream traits to force callers
// to use the row as a Stream to avoid having to change calling code when the implementation
// changes to use a streaming token parser.
impl Stream for RowData<'_> {
    type Item = Result<CellData, Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.iterator.next();
        result.map_or(Poll::Ready(None), |value| {
            Poll::Ready(Some(Ok(CellData::new(value))))
        })
    }
}

pub struct CellData {
    protocol_data: Vec<u8>,
    column_value: ColumnValues,
}

impl CellData {
    fn new(column_value: ColumnValues) -> CellData {
        CellData {
            protocol_data: Vec::new(),
            column_value,
        }
    }

    fn get_value(self) -> ColumnValues {
        self.column_value
    }

    fn into_byte_stream(self) -> bytes::Bytes {
        todo!()
    }

    fn get_bytes(&self) -> &[u8] {
        self.protocol_data.as_ref()
    }
}
