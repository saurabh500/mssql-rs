mod api_wrapper;

use crate::api_wrapper::{
    BoolFuture, ClientContext, ColumnValue, QueryResultType, QueryResultTypeFuture,
    QueryResultTypeStream, RowData, RowStream, TdsConnection, TdsConnectionFuture,
};
use futures::StreamExt;
use std::marker::PhantomData;
use tds_x::connection::client_context::TransportContext;
use tds_x::connection_provider::tds_connection_provider::TdsConnectionProvider;
use tds_x::core::TdsResult;
use tds_x::error::Error;
use tokio::runtime::Runtime;

thread_local! {
    static CURRENT_THREAD_RUNTIME: Runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build().unwrap();
}

// This is needed because type statements in extern "Rust" blocks trigger false
// positive needless_lifetime warnings when using cargo bclippy. The suppression
// cannot be applied to just type statements and has to be applied to the whole block.
// Note that function definitions in extern "Rust" do generate valid needless_lifetime
// warnings though.
#[allow(clippy::needless_lifetimes)]
#[cxx::bridge(namespace = "cxx_ffi")]
mod ffi {
    #[derive(Debug, Copy, Clone)]
    #[repr(u8)]
    pub enum ResultType {
        DmlResult,
        ResultSet,
    }

    extern "Rust" {
        type ClientContext;
        #[allow(clippy::needless_lifetimes)]
        type TdsConnection<'a, 'result>;
        type QueryResultTypeStream<'result>;
        type QueryResultType<'result>;
        type RowStream<'result>;
        type RowData;
        type ColumnValue;

        // TdsConnection methods.
        #[allow(clippy::needless_lifetimes)]
        unsafe fn create_connection<'a, 'result>(
            context: &'a ClientContext,
        ) -> Result<Box<TdsConnection<'a, 'result>>>;
        fn create_client_context(
            host: String,
            port: u16,
            user: String,
            password: String,
            catalog: String,
        ) -> Box<ClientContext>;
        unsafe fn execute<'a, 'result>(
            self: &'a mut TdsConnection<'a, 'result>,
            sql_command: String,
        ) -> Box<QueryResultTypeStream<'result>>;

        // QueryResultTypeStream methods.
        // TODO Add an executeAndGetAllResults() that returns all data in memory.
        unsafe fn next<'result>(self: &'result mut QueryResultTypeStream<'result>) -> Result<bool>;
        unsafe fn current_result<'result>(
            self: &'result mut QueryResultTypeStream<'result>,
        ) -> Result<Box<QueryResultType<'result>>>;

        // QueryResultType methods.
        unsafe fn get_type(self: &QueryResultType<'_>) -> ResultType;

        // This should ResultSet instead if we want to allow callers to choose between
        // in-memory and streaming for individual results in the batch.
        unsafe fn take_result_set<'result>(
            self: &'result mut QueryResultType<'result>,
        ) -> Result<Box<RowStream<'result>>>;
        unsafe fn take_dml_result<'result>(
            self: &'result mut QueryResultType<'result>,
        ) -> Result<u64>;

        // RowStream methods
        unsafe fn next<'result>(self: &'result mut RowStream<'result>) -> Result<bool>;
        unsafe fn current_row<'result>(
            self: &'result mut RowStream<'result>,
        ) -> Result<Box<RowData>>;

        // RowData methods
        unsafe fn next(self: &mut RowData) -> Result<bool>;
        unsafe fn current_cell(self: &mut RowData) -> Result<Box<ColumnValue>>;

        // ColumnValue methods
        unsafe fn print_column_value(self: &mut ColumnValue) -> Result<()>;

        // Async APIs. Possibly should go into a separate namespace.
        type TdsConnectionFuture<'a, 'result>;
        type QueryResultTypeFuture<'result>;
        type BoolFuture<'result>;

        unsafe fn create_connection_async<'result>(
            context: &ClientContext,
        ) -> Box<TdsConnectionFuture<'_, 'result>>;

        // TdsConnectionFuture
        unsafe fn await_connection<'a, 'result>(
            self: &mut TdsConnectionFuture<'a, 'result>,
        ) -> Result<Box<TdsConnection<'a, 'result>>>;

        // TdsConnection
        unsafe fn execute_async<'a, 'result>(
            self: &'a mut TdsConnection<'a, 'result>,
            sql_command: String,
        ) -> Box<QueryResultTypeFuture<'result>>;

        // QueryResultTypeFuture
        unsafe fn await_query_result_type<'result>(
            self: &'result mut QueryResultTypeFuture<'result>,
        ) -> Result<Box<QueryResultTypeStream<'result>>>;

        // QueryResultTypeStream
        unsafe fn next_async<'result>(
            self: &'result mut QueryResultTypeStream<'result>,
        ) -> Box<BoolFuture<'result>>;

        // BoolFuture
        unsafe fn await_bool<'result>(self: &'result mut BoolFuture<'result>) -> Result<bool>;

        // RowStream
        unsafe fn next_async<'result>(
            self: &'result mut RowStream<'result>,
        ) -> Box<BoolFuture<'result>>;

        // RowData
        unsafe fn next_async(self: &mut RowData) -> Box<BoolFuture<'_>>;

    }
}

unsafe fn create_connection(context: &ClientContext) -> TdsResult<Box<TdsConnection>> {
    CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(create_connection_async_internal(context)))
}

pub fn create_client_context(
    host: String,
    port: u16,
    user: String,
    password: String,
    catalog: String,
) -> Box<ClientContext> {
    // Set the Rust client context values here...
    // Demo-only - hardcode the ClientContext.
    let tds_client_context = tds_x::connection::client_context::ClientContext {
        transport_context: TransportContext::Tcp { host, port },
        user_name: user,
        password,
        database: catalog,
        ..Default::default()
    };

    Box::new(ClientContext {
        context: tds_client_context,
    })
}

async fn create_connection_async_internal(
    context: &ClientContext,
) -> TdsResult<Box<TdsConnection>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;
    let provider = TdsConnectionProvider {};
    match provider
        .create_connection(context.context.clone(), None)
        .await
    {
        Ok(connection) => {
            println!("Successfully connected");
            Ok(Box::new(TdsConnection {
                connection,
                unused: PhantomData,
                unused2: PhantomData,
            }))
        }
        Err(e) => {
            println!("Error: {:?}", e.to_string());
            Err(e)
        }
    }
}

unsafe fn create_connection_async<'a, 'result>(
    context: &'a ClientContext,
) -> Box<TdsConnectionFuture<'a, 'result>>
where
    'a: 'result,
{
    let future = create_connection_async_internal(context);
    std::mem::transmute(Box::new(TdsConnectionFuture {
        future_connection: Some(Box::new(Box::pin(future))),
    }))
}

impl<'a, 'result> TdsConnection<'a, 'result> {
    pub fn execute(&'a mut self, sql_command: String) -> Box<QueryResultTypeStream<'result>> {
        CURRENT_THREAD_RUNTIME
            .with(|rt| rt.block_on(self.execute_async_internal(sql_command.clone())))
    }

    async fn execute_async_internal(
        &'a mut self,
        sql_command: String,
    ) -> Box<QueryResultTypeStream<'result>> {
        println!("Executing SQL query: {}", sql_command);
        let batch_results = self
            .connection
            .execute(sql_command, None, None)
            .await
            .unwrap();
        // Demo-only print the result stdout.
        let result_stream = batch_results.stream_results();
        Box::new(QueryResultTypeStream {
            results: result_stream,
            current_result: None,
        })
    }
}

// TODO: Generalize the next()/current_...() pattern used for Stream-wrapping structs
// instead of duplicating the code.

impl<'result> QueryResultTypeStream<'result> {
    pub fn next(&mut self) -> TdsResult<bool> {
        CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(self.next_async_internal()))
    }

    async fn next_async_internal(&mut self) -> TdsResult<bool> {
        let result_option = self.results.next().await;

        result_option.map_or_else(
            || Ok(false),
            |res| {
                self.current_result = Some(res?);
                Ok(true)
            },
        )
    }

    pub fn current_result(&'result mut self) -> TdsResult<Box<QueryResultType<'result>>> {
        if let Some(result) = self.current_result.take() {
            let result_type = ffi::ResultType::from(&result);
            match result {
                tds_x::query::result::QueryResultType::DmlResult(dml_result) => {
                    Ok(Box::new(QueryResultType {
                        result_set: None,
                        dml_result: Some(dml_result),
                        result_type,
                    }))
                }
                tds_x::query::result::QueryResultType::ResultSet(result_set) => {
                    Ok(Box::new(QueryResultType {
                        result_set: Some(result_set),
                        dml_result: None,
                        result_type,
                    }))
                }
            }
        } else {
            // Todo: use a more appropriate error.
            Err(Error::ProtocolError("No result available".to_string()))
        }
    }
}

impl<'result> QueryResultType<'result> {
    pub fn get_type(&self) -> ffi::ResultType {
        self.result_type
    }

    pub fn take_result_set(&mut self) -> TdsResult<Box<RowStream<'result>>> {
        if self.result_type == ffi::ResultType::ResultSet {
            let result_set = self.result_set.take().unwrap();
            Ok(Box::new(RowStream {
                row_stream: result_set.into_row_stream()?,
                current_row: None,
            }))
        } else {
            Err(Error::ProtocolError(
                "Cannot convert to result set.".to_string(),
            ))
        }
    }

    pub fn take_dml_result(&mut self) -> TdsResult<u64> {
        if self.result_type == ffi::ResultType::DmlResult {
            let update_result = self.dml_result.take().unwrap();
            Ok(update_result)
        } else {
            Err(Error::ProtocolError(
                "Cannot convert to result set.".to_string(),
            ))
        }
    }
}

impl RowStream<'_> {
    pub fn next(&mut self) -> TdsResult<bool> {
        CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(self.next_async_internal()))
    }

    async fn next_async_internal(&mut self) -> TdsResult<bool> {
        let result_option = self.row_stream.next().await;

        result_option.map_or_else(
            || Ok(false),
            |res| {
                self.current_row = Some(res?);
                Ok(true)
            },
        )
    }

    pub fn current_row(&mut self) -> TdsResult<Box<RowData>> {
        if let Some(current_row) = self.current_row.take() {
            Ok(Box::new(RowData {
                row_data: current_row,
                current_cell: None,
            }))
        } else {
            Err(Error::ProtocolError("No row available".to_string()))
        }
    }
}

impl RowData {
    pub fn next(&mut self) -> TdsResult<bool> {
        CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(self.next_async_internal()))
    }

    async fn next_async_internal(&mut self) -> TdsResult<bool> {
        let result_option = self.row_data.next().await;

        result_option.map_or_else(
            || Ok(false),
            |res| {
                self.current_cell = Some(res?);
                Ok(true)
            },
        )
    }

    pub fn current_cell(&mut self) -> TdsResult<Box<ColumnValue>> {
        if let Some(current_cell) = self.current_cell.take() {
            Ok(Box::new(ColumnValue {
                value: Some(current_cell),
            }))
        } else {
            Err(Error::ProtocolError("No row available.".to_string()))
        }
    }
}

impl ColumnValue {
    pub fn print_column_value(&mut self) -> TdsResult<()> {
        if let Some(unwrapped_column_value) = self.value.take() {
            print!("{:?}", unwrapped_column_value);
            Ok(())
        } else {
            Err(Error::ProtocolError(
                "ColumnValue already consumed.".to_string(),
            ))
        }
    }
}

impl<'a, 'result> TdsConnectionFuture<'a, 'result> {
    pub fn await_connection(&mut self) -> TdsResult<Box<TdsConnection<'a, 'result>>> {
        if let Some(future) = self.future_connection.take() {
            CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(future))
        } else {
            Err(Error::ProtocolError(
                "TdsConnectionFuture already consumed.".to_string(),
            ))
        }
    }
}

impl<'a, 'result> TdsConnection<'a, 'result> {
    pub fn execute_async(&'a mut self, sql_command: String) -> Box<QueryResultTypeFuture<'result>> {
        let future = self.execute_async_internal(sql_command);
        unsafe {
            std::mem::transmute(Box::new(QueryResultTypeFuture {
                future_query_result_type: Some(Box::new(Box::pin(future))),
            }))
        }
    }
}

impl<'result> QueryResultTypeFuture<'result> {
    pub fn await_query_result_type(&mut self) -> TdsResult<Box<QueryResultTypeStream<'result>>> {
        if let Some(future_query_result_type) = self.future_query_result_type.take() {
            Ok(CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(future_query_result_type)))
        } else {
            Err(Error::ProtocolError(
                "QueryResultTypeFuture already consumed.".to_string(),
            ))
        }
    }
}

impl QueryResultTypeStream<'_> {
    pub fn next_async(&mut self) -> Box<BoolFuture> {
        let future = self.next_async_internal();
        Box::new(BoolFuture {
            future_bool: Some(Box::new(Box::pin(future))),
        })
    }
}

impl BoolFuture<'_> {
    pub fn await_bool(&mut self) -> TdsResult<bool> {
        if let Some(future_bool) = self.future_bool.take() {
            CURRENT_THREAD_RUNTIME.with(|rt| rt.block_on(future_bool))
        } else {
            Err(Error::ProtocolError(
                "QueryResultTypeFuture already consumed.".to_string(),
            ))
        }
    }
}

impl RowStream<'_> {
    pub fn next_async(&mut self) -> Box<BoolFuture> {
        let future = self.next_async_internal();
        Box::new(BoolFuture {
            future_bool: Some(Box::new(Box::pin(future))),
        })
    }
}

impl<'result> RowData {
    pub fn next_async(&'result mut self) -> Box<BoolFuture<'result>> {
        let future = self.next_async_internal();
        Box::new(BoolFuture {
            future_bool: Some(Box::new(Box::pin(future))),
        })
    }
}
