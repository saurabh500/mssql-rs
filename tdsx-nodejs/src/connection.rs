use std::sync::Arc;

use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use napi::bindgen_prelude::{Buffer, Either6, Null};
use tds_x::{
    connection::tds_client::TdsClient,
    datatypes::column_values::{ColumnValues, SqlDateTime, SqlSmallDateTime},
};
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
                "Failed to execute query: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn next_row(
        &self,
    ) -> napi::Result<Vec<Either6<i32, String, bool, Buffer, Null, NaiveDateTime>>> {
        let mut client = self.tds_client.lock().await;
        let _row = client.next_row().await.unwrap();
        match _row {
            Some(cols) => {
                let mut ret_val: Vec<Either6<i32, String, bool, Buffer, Null, NaiveDateTime>> =
                    Vec::with_capacity(cols.len());
                for col in cols {
                    match col {
                        ColumnValues::Int(v) => ret_val.push(Either6::A(v)),
                        ColumnValues::Uuid(v) => ret_val.push(Either6::B(v.to_string())),
                        ColumnValues::Bit(v) => ret_val.push(Either6::C(v)),
                        ColumnValues::BigInt(v) => ret_val.push(Either6::B(v.to_string())),
                        ColumnValues::TinyInt(v) => ret_val.push(Either6::B(v.to_string())),
                        ColumnValues::SmallInt(v) => ret_val.push(Either6::B(v.to_string())),
                        ColumnValues::Real(v) => ret_val.push(Either6::B(v.to_string())),
                        ColumnValues::Float(v) => ret_val.push(Either6::B(v.to_string())),
                        ColumnValues::Decimal(_decimal_parts) => todo!(),
                        ColumnValues::Numeric(_decimal_parts) => todo!(),
                        ColumnValues::String(_sql_string) => {
                            ret_val.push(Either6::D(Buffer::from(_sql_string.bytes)))
                        }
                        ColumnValues::DateTime(_sql_date_time) => {
                            let naivedatetime: NaiveDateTime = to_naive_date_time(_sql_date_time);
                            ret_val.push(Either6::F(naivedatetime));
                        }
                        ColumnValues::Date(_sql_date) => todo!(),
                        ColumnValues::Time(_time) => todo!(),
                        ColumnValues::DateTime2(_date_time2) => todo!(),
                        ColumnValues::DateTimeOffset(_date_time_offset) => todo!(),
                        ColumnValues::SmallDateTime(sql_small_date_time) => {
                            let naivedatetime: NaiveDateTime = naive_date_from(sql_small_date_time);
                            ret_val.push(Either6::F(naivedatetime));
                        }
                        ColumnValues::SmallMoney(_sql_small_money) => todo!(),
                        ColumnValues::Money(_sql_money) => todo!(),
                        ColumnValues::Bytes(items) => ret_val.push(Either6::D(Buffer::from(items))),
                        ColumnValues::Xml(_sql_xml) => todo!(),
                        ColumnValues::Null => ret_val.push(Either6::E(Null)),
                        ColumnValues::Json(_sql_json) => todo!(),
                    }
                }
                Ok(ret_val)
            }
            None => Ok(Vec::with_capacity(0)),
        }
    }

    #[napi]
    pub async fn close_query(&self) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.close_query().await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to close query: {e}"
            ))),
        }
    }

    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let mut client = self.tds_client.lock().await;
        let result = client.close_connection().await;
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to close connection: {e}"
            ))),
        }
    }
}

fn to_naive_date_time(sql_date_time: SqlDateTime) -> NaiveDateTime {
    NaiveDateTime::new(
        from_days(sql_date_time.days as i64, 1900),
        from_sec_fragments(sql_date_time.time as i64),
    )
}

#[inline]
fn from_days(days: i64, start_year: i32) -> NaiveDate {
    NaiveDate::from_ymd_opt(start_year, 1, 1).unwrap() + chrono::Duration::days(days)
}

#[inline]
fn from_sec_fragments(sec_fragments: i64) -> NaiveTime {
    NaiveTime::from_hms_opt(0, 0, 0).unwrap()
        + chrono::Duration::nanoseconds(sec_fragments * (1e9 as i64) / 300)
}

#[inline]
fn naive_date_from(value: SqlSmallDateTime) -> NaiveDateTime {
    let base_date = NaiveDate::from_ymd_opt(1900, 1, 1).expect("valid base date");

    base_date.and_hms_opt(0, 0, 0).unwrap()
        + Duration::days(value.days.into())
        + Duration::minutes(value.time.into())
}
