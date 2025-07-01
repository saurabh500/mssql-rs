use std::sync::Arc;

use tds_x::{connection::tds_connection::TdsConnection, query::result::QueryResultTypeStream};
use tokio::sync::Mutex;

#[napi]
pub struct Connection {
  pub(crate) tds_client: Arc<Mutex<TdsClient>>,
}

pub(crate) struct TdsClient {
  pub(crate) tds_connection: Arc<Mutex<TdsConnection>>,
  pub(crate) batch_result: Option<Arc<Mutex<TdsQueryResult>>>,
}

pub(crate) struct TdsQueryResult {
  pub(crate) _tds_connection: Arc<Mutex<TdsConnection>>,
  pub(crate) _batch_result: Arc<Mutex<QueryResultTypeStream<'static>>>,
}

impl TdsClient {
  pub fn new(tds_connection: Arc<Mutex<TdsConnection>>) -> Self {
    Self {
      tds_connection,
      batch_result: None,
    }
  }
}

#[napi]
impl Connection {
  #[napi]
  pub async fn execute(&self, query: String) -> napi::Result<()> {
    let mut client = self.tds_client.lock().await;
    let query_result = {
      let mut tds_connection = client.tds_connection.lock().await;
      let batch_result = tds_connection.execute(query.to_string(), None, None).await;
      if batch_result.is_err() {
        return Err(napi::Error::new(
          napi::Status::GenericFailure,
          "Failed to execute query:".to_string(),
        ));
      }
      let batch_result = batch_result.unwrap();
      let stream = batch_result.stream_results();

      // Transmute to static
      let stream: QueryResultTypeStream<'static> = unsafe { std::mem::transmute(stream) };
      TdsQueryResult {
        _tds_connection: client.tds_connection.clone(),
        _batch_result: Arc::new(Mutex::new(stream)),
      }
    };
    client.batch_result = Some(Arc::new(Mutex::new(query_result)));
    Ok(())
  }
}
