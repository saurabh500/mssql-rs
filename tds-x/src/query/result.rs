use crate::connection::tds_connection::TdsConnection;
use crate::core::TdsResult;
use crate::datatypes::decoder::ColumnValues;
use crate::query::metadata::ColumnMetadata;
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::token_stream::{
    GenericTokenParserRegistry, ParserContext, TokenStreamReader,
};
use crate::token::tokens::{ColMetadataToken, DoneStatus, Tokens};
use futures::executor::block_on;
use futures::task::AtomicWaker;
use futures::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::vec::IntoIter;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument, trace};

pub enum QueryResultType<'result> {
    Update(i64),
    ResultSet(ResultSet<'result>),
}

impl QueryResultType<'_> {
    async fn next_result(
        parent_batch: Arc<Mutex<BatchResult<'_>>>,
        processing_signal: DeferredSignal,
    ) -> TdsResult<QueryResultType<'_>> {
        let mut parent_batch_ref = parent_batch.lock().await;
        let token = parent_batch_ref.next_token().await?;
        match token {
            Tokens::Done(t1) => {
                println!("Received Done token: {:?}", t1);
                {
                    if !t1.status.contains(DoneStatus::MORE) {
                        parent_batch_ref.received_last = true;
                    }
                    parent_batch_ref.parser_context = ParserContext::None(());
                }
                Ok(QueryResultType::Update(t1.row_count as i64))
            }
            Tokens::DoneInProc(t1) => {
                println!("Received DoneInProc token: {:?}", t1);
                {
                    if !t1.status.contains(DoneStatus::MORE) {
                        parent_batch_ref.received_last = true;
                    }
                    parent_batch_ref.parser_context = ParserContext::None(());
                }
                Ok(QueryResultType::Update(t1.row_count as i64))
            }
            Tokens::DoneProc(t1) => {
                println!("Received DoneProc token: {:?}", t1);
                {
                    if !t1.status.contains(DoneStatus::MORE) {
                        parent_batch_ref.received_last = true;
                    }
                    parent_batch_ref.parser_context = ParserContext::None(());
                }
                Ok(QueryResultType::Update(t1.row_count as i64))
            }
            Tokens::EnvChange(t1) => {
                println!("Received EnvChange token: {:?}", t1);
                todo!()
                //QueryResultType::Update(0)
            }
            Tokens::Error(t1) => {
                println!("Received Error token: {:?}", t1);

                // TODO: Do not panic. Get the error out to the user, and then drain any more data
                // out and be done. Error tokens can also be raised because the Stored Proc has a
                // exception being raised. in those cases, the query execution failure means
                // user
                error!(?t1);
                todo!("Received error token: {:?}", t1)
            }
            Tokens::FeatureExtAck(t1) => {
                println!("Received FeatureExtAck token: {:?}", t1);
                todo!()
            }
            Tokens::ColMetadata(column_metadata) => {
                // Start a QueryResultType::ResultSet here.
                // ResultSet needs to notify BatchResultType if there's another result
                // when it sees the Done token.
                info!(?column_metadata);
                parent_batch_ref.parser_context =
                    ParserContext::ColumnMetadata(column_metadata.clone());
                Ok(QueryResultType::ResultSet(ResultSet::new(
                    parent_batch.clone(),
                    processing_signal,
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
    //  negotiated_settings: &'result mut NegotiatedSettings,
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
        debug!("Batch result created.");
        let packet_reader = PacketReader::new(tds_connection.transport.as_mut());
        let token_stream_reader = TokenStreamReader::new(
            packet_reader,
            Box::new(GenericTokenParserRegistry::default()),
        );
        BatchResult {
            // TODO: Holding a mutable borrow of negotiated_settings prevents BatchResult from implementing
            // Send or Sync, which makes it illegal to use in an Arc<Mutex<>>. However the negotiated_settings
            // are needed if there's a SET statement to update the settings.
            // This code will likely need to change such that the negotiated settings get cloned, updated
            // by reading the tokens, then propagated-by-copy back to the original one.

            //negotiated_settings: &mut tds_connection.negotiated_settings,
            token_stream_reader,
            parser_context: ParserContext::default(),
            received_last: false,
        }
    }

    pub fn stream_results(
        self,
    ) -> impl Stream<Item = TdsResult<QueryResultType<'result>>> + 'result {
        QueryResultTypeStream::new(self)
    }

    pub fn get_all_results(self) -> Vec<QueryResultType<'result>> {
        todo!()
    }

    async fn next_token(&mut self) -> TdsResult<Tokens> {
        self.token_stream_reader
            .receive_token(&self.parser_context)
            .await
    }

    #[instrument(skip(self))]
    fn drain_stream(&mut self, drain_until_first_done: bool) {
        while let Ok(token) = block_on(self.token_stream_reader.receive_token(&self.parser_context))
        {
            match token {
                Tokens::Done(t1) => {
                    info!(?t1);
                    self.parser_context = ParserContext::None(());
                    if !t1.status.contains(DoneStatus::MORE) {
                        self.received_last = true;
                    }
                    if drain_until_first_done || !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::DoneInProc(t1) => {
                    info!(?t1);
                    self.parser_context = ParserContext::None(());
                    if !t1.status.contains(DoneStatus::MORE) {
                        self.received_last = true;
                    }
                    if drain_until_first_done || !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::DoneProc(t1) => {
                    info!(?t1);
                    self.parser_context = ParserContext::None(());
                    if !t1.status.contains(DoneStatus::MORE) {
                        self.received_last = true;
                    }
                    if drain_until_first_done || !t1.status.contains(DoneStatus::MORE) {
                        break;
                    }
                }
                Tokens::ColMetadata(column_metadata) => {
                    info!(?column_metadata);
                    self.parser_context = ParserContext::ColumnMetadata(column_metadata.clone());
                }
                _ => {
                    info!(?token);
                }
            }
        }
    }
}

#[allow(clippy::type_complexity)]
pub struct QueryResultTypeStream<'result> {
    // Because we are passing in ref-counted objects to Futures, we must use
    // Arc<Mutex<resource>> and Arc<AtomicBool> because the thread running the future may
    // differ from the originating thread. Rc<RefCell<resource>> and Rc<Cell<bool>> won't work.
    batch_result: Arc<Mutex<BatchResult<'result>>>,
    processing_flag: Arc<AtomicBool>,
    executing_future:
        Option<Pin<Box<dyn Future<Output = TdsResult<QueryResultType<'result>>> + 'result>>>,
}

impl<'result> QueryResultTypeStream<'result> {
    fn new(initial_batch_result: BatchResult<'result>) -> QueryResultTypeStream<'result> {
        QueryResultTypeStream {
            batch_result: Arc::new(Mutex::new(initial_batch_result)),
            processing_flag: Arc::new(AtomicBool::new(false)),
            executing_future: None,
        }
    }

    fn evaluate_executing_future(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<TdsResult<QueryResultType<'result>>>> {
        // Poll the executing future. Note that this may have just been created.
        assert!(self.executing_future.is_some());
        if let Some(mut future) = self.executing_future.take() {
            trace!("Consuming future.");
            match future.as_mut().poll(cx) {
                Poll::Pending => {
                    trace!("Future pending.");
                    // Put this future back so that it can be polled again.
                    self.executing_future = Some(future);
                    Poll::Pending
                }
                Poll::Ready(result) => {
                    trace!("Future ready.");
                    Poll::Ready(Some(result))
                }
            }
        } else {
            panic!("Executing future not available.");
        }
    }
}

impl<'result> Stream for QueryResultTypeStream<'result> {
    type Item = TdsResult<QueryResultType<'result>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.executing_future.is_some() {
            self.evaluate_executing_future(cx)
        } else if !self.processing_flag.load(Ordering::Acquire) {
            // If there is no active future, and the processing flag is off,
            // there is no work being done. The previous item may have been
            // the final, otherwise start getting the next token.
            debug!("Borrowing batch result in stream.");
            if self.batch_result.try_lock().unwrap().received_last {
                debug!("Received last.");
                // Nothing is processing and we have received the last item.
                Poll::Ready(None)
            } else {
                // Start processing the next item.
                debug!("Start processing.");
                self.processing_flag.store(true, Ordering::Release);

                debug!("Starting future.");
                let batch_ref = self.batch_result.clone();
                let processing_flag_ref = self.processing_flag.clone();
                let future = QueryResultType::next_result(
                    batch_ref,
                    DeferredSignal::new(processing_flag_ref, cx.waker().clone()),
                );
                self.executing_future = Some(Box::pin(future));
                self.evaluate_executing_future(cx)
            }
        } else {
            Poll::Pending
        }
    }
}

impl Drop for QueryResultTypeStream<'_> {
    fn drop(&mut self) {
        let mut batch_result = self.batch_result.try_lock().unwrap();
        if !batch_result.received_last {
            batch_result.drain_stream(false);
            batch_result.received_last = true;
        }
    }
}

pub struct ResultSet<'result> {
    metadata: Vec<ColumnMetadata>,
    parent_batch: Arc<Mutex<BatchResult<'result>>>,
    processing_signal: DeferredSignal,
    received_last_row: bool,
    row_count: Option<u64>,
}

impl<'result> ResultSet<'result> {
    fn new(
        parent_batch: Arc<Mutex<BatchResult<'result>>>,
        processing_signal: DeferredSignal,
        col_token: ColMetadataToken,
    ) -> Self {
        ResultSet {
            metadata: col_token.columns,
            parent_batch,
            processing_signal,
            received_last_row: false,
            row_count: None,
        }
    }

    pub async fn get_all_data(self) -> TdsResult<Vec<Vec<CellData>>> {
        // Internally iterate over the row data and cache all the CellDatas into vectors.
        todo!();
    }

    pub fn get_metadata(&self) -> &Vec<ColumnMetadata> {
        self.metadata.as_ref()
    }

    pub fn into_row_stream(self) -> TdsResult<RowStream<'result>> {
        Ok(RowStream::new(self))
    }

    // Retrieves the next token and updates internal state about this result set and its parent batch.
    async fn next_row_token(&mut self) -> TdsResult<Tokens> {
        let mut parent_batch_mut = self.parent_batch.lock().await;
        let token_result = parent_batch_mut.next_token().await;
        let token_ref = token_result.as_ref().unwrap();

        match token_ref {
            Tokens::Done(t1) => {
                debug!(?t1);
                self.received_last_row = true;
                self.row_count = Some(t1.row_count);

                if !t1.status.contains(DoneStatus::MORE) {
                    parent_batch_mut.received_last = true;
                }
                parent_batch_mut.parser_context = ParserContext::None(());
            }
            Tokens::DoneInProc(t1) => {
                debug!(?t1);
                self.received_last_row = true;
                self.row_count = Some(t1.row_count);

                if !t1.status.contains(DoneStatus::MORE) {
                    parent_batch_mut.received_last = true;
                }
                parent_batch_mut.parser_context = ParserContext::None(());
            }
            Tokens::DoneProc(t1) => {
                self.received_last_row = true;
                self.row_count = Some(t1.row_count);

                if !t1.status.contains(DoneStatus::MORE) {
                    parent_batch_mut.received_last = true;
                }
                parent_batch_mut.parser_context = ParserContext::None(());
            }
            Tokens::Error(t1) => {
                error!(?t1);
                todo!(
                    "Received error token, change this to not error/panic. : {:?}",
                    t1
                );
            }
            Tokens::Row(_row) => {}
            _ => {
                unreachable!("Received unexpected token: {:?}", token_ref)
            }
        };

        token_result
    }
}

impl Drop for ResultSet<'_> {
    fn drop(&mut self) {
        trace!("ResultSet::drop");
        let mut parent_batch = self.parent_batch.try_lock().unwrap();
        if !self.received_last_row {
            parent_batch.drain_stream(true);
            self.received_last_row = true;
        }
    }
}

#[allow(clippy::type_complexity)]
pub struct RowStream<'result> {
    result_set: Arc<Mutex<ResultSet<'result>>>,
    processing_flag: Arc<AtomicBool>,
    executing_future: Option<Pin<Box<dyn Future<Output = TdsResult<RowData>> + 'result>>>,
}

impl<'result> RowStream<'result> {
    fn new(parent_result_set: ResultSet<'result>) -> Self {
        RowStream {
            result_set: Arc::new(Mutex::new(parent_result_set)),
            processing_flag: Arc::new(AtomicBool::new(false)),
            executing_future: None,
        }
    }

    fn evaluate_executing_future(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<TdsResult<RowData>>> {
        // Poll the executing future. Note that this may have just been created.
        assert!(self.executing_future.is_some());
        if let Some(mut future) = self.executing_future.take() {
            match future.as_mut().poll(cx) {
                Poll::Pending => {
                    // Put this future back so that it can be polled again.
                    self.executing_future = Some(future);
                    Poll::Pending
                }
                Poll::Ready(result) => {
                    let row_data_ref = result.as_ref().unwrap();
                    if row_data_ref.is_terminal {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(result))
                    }
                }
            }
        } else {
            panic!("Executing future not available.");
        }
    }

    async fn next_row(
        parent_result: Arc<Mutex<ResultSet<'_>>>,
        _processing_signal: DeferredSignal,
    ) -> TdsResult<RowData> {
        let token = parent_result.lock().await.next_row_token().await;
        match token? {
            Tokens::Done(_) => Ok(RowData::new(Vec::new())),
            Tokens::DoneInProc(_) => Ok(RowData::new(Vec::new())),
            Tokens::DoneProc(_) => Ok(RowData::new(Vec::new())),
            Tokens::Row(row) => Ok(RowData::new(row.all_values)),
            _ => {
                panic!("Received unexpected token");
            }
        }
    }
}

impl Stream for RowStream<'_> {
    type Item = TdsResult<RowData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.executing_future.is_some() {
            self.evaluate_executing_future(cx)
        } else if !self.processing_flag.load(Ordering::Acquire) {
            if self.result_set.try_lock().unwrap().received_last_row {
                Poll::Ready(None)
            } else {
                self.processing_flag.store(true, Ordering::Release);

                let result_set = self.result_set.clone();
                let processing_flag_ref = self.processing_flag.clone();
                let future = Self::next_row(
                    result_set,
                    DeferredSignal::new(processing_flag_ref, cx.waker().clone()),
                );
                self.executing_future = Some(Box::pin(future));

                self.evaluate_executing_future(cx)
            }
        } else {
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub struct RowData {
    is_terminal: bool,
    iterator: IntoIter<ColumnValues>,
}

impl RowData {
    fn new(column_values: Vec<ColumnValues>) -> RowData {
        RowData {
            is_terminal: column_values.is_empty(),
            iterator: column_values.into_iter(),
        }
    }

    fn is_terminal(&self) -> bool {
        self.is_terminal
    }
}

// Todo: The token parser currently deserializes all row values at once.
// It should be streamed instead. This implements the Stream traits to force callers
// to use the row as a Stream to avoid having to change calling code when the implementation
// changes to use a streaming token parser.
impl Stream for RowData {
    type Item = TdsResult<CellData>;

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

    pub fn get_value(self) -> ColumnValues {
        self.column_value
    }

    fn into_byte_stream(self) -> bytes::Bytes {
        todo!()
    }

    fn get_bytes(&self) -> &[u8] {
        self.protocol_data.as_ref()
    }
}

// Resets the flag when destructed and wakes.
struct DeferredSignal {
    atomic_waker: AtomicWaker,
    waker: Waker,
    flag: Arc<AtomicBool>,
}

impl DeferredSignal {
    fn new(flag: Arc<AtomicBool>, waker: Waker) -> Self {
        assert!(flag.load(Ordering::Acquire));
        let atomic_waker = AtomicWaker::new();
        let result = DeferredSignal {
            flag,
            atomic_waker,
            waker,
        };
        result.atomic_waker.register(&result.waker);
        result
    }
}

impl Drop for DeferredSignal {
    fn drop(&mut self) {
        assert!(self.flag.load(Ordering::Acquire));
        self.flag.store(false, Ordering::Release);
        self.atomic_waker.wake();
    }
}
