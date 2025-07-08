use std::sync::Arc;

use napi::bindgen_prelude::Either3;
use tds_x::connection::tds_client::TdsClient;
use tokio::sync::Mutex;

#[napi]
pub struct Connection {
  pub(crate) tds_client: Arc<Mutex<TdsClient>>,
}

#[napi]
impl Connection {
  #[napi]
  pub async fn execute(&self, query: String) -> napi::Result<()> {
    let mut client = self.tds_client.lock().await;
    let result = client.execute(query, None, None).await;
    match result {
      Ok(_) => Ok(()),
      Err(e) => Err(napi::Error::from_reason(format!(
        "Failed to execute query: {}",
        e
      ))),
    }
  }

  #[napi]
  pub async fn next_row(&self) -> napi::Result<Vec<Either3<u32, String, bool>>> {
    let mut client = self.tds_client.lock().await;
    let _row = client.next_row().await;
    todo!()
  }
}
