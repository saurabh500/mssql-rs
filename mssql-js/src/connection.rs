// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::sync::Arc;

use mssql_tds::{
    connection::tds_client::{ResultSet, ResultSetClient, TdsClient},
    datatypes::column_values::ColumnValues,
};
use napi::bindgen_prelude::{BigInt, Buffer, Either15, Null};
use tokio::sync::Mutex;

use crate::ffidatatypes::{
    Metadata, NapiDecimalParts, NapiSqlDateTime, NapiSqlDateTime2, NapiSqlDateTimeOffset,
    NapiSqlMoney, NapiSqlTime, RowItem,
};

pub(crate) type RowDataType = Either15<
    i32,                   // A
    BigInt,                // B
    bool,                  // C
    Buffer,                // D
    Null,                  // E
    NapiSqlDateTime,       // F
    u32,                   // G
    NapiSqlTime,           // H
    NapiSqlDateTime,       // I
    NapiSqlDateTime2,      // J
    NapiSqlDateTimeOffset, // K
    NapiSqlMoney,          // L
    NapiDecimalParts,      // M
    f64,                   // N
    String,                // O
>;

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

    fn transform_row(&self, row: Vec<ColumnValues>) -> Vec<RowDataType> {
        let mut ret_val: Vec<RowDataType> = Vec::with_capacity(row.len());
        for col in row {
            match col {
                ColumnValues::Int(v) => ret_val.push(RowDataType::A(v)),
                ColumnValues::Uuid(uuid) => ret_val.push(RowDataType::O(uuid.to_string())),
                ColumnValues::Bit(v) => ret_val.push(RowDataType::C(v)),
                ColumnValues::BigInt(v) => ret_val.push(RowDataType::B(v.into())),
                ColumnValues::TinyInt(v) => ret_val.push(RowDataType::G(v.into())),
                ColumnValues::SmallInt(v) => ret_val.push(RowDataType::A(v.into())),
                ColumnValues::Real(v) => ret_val.push(RowDataType::N(v.into())),
                ColumnValues::Float(v) => ret_val.push(RowDataType::N(v)),
                ColumnValues::Decimal(decimal_parts) => {
                    ret_val.push(RowDataType::M(decimal_parts.into()));
                }
                ColumnValues::Numeric(decimal_parts) => {
                    ret_val.push(RowDataType::M(decimal_parts.into()));
                }
                ColumnValues::String(sql_string) => {
                    ret_val.push(RowDataType::D(Buffer::from(sql_string.bytes)))
                }
                ColumnValues::DateTime(sql_date_time) => {
                    ret_val.push(RowDataType::F(NapiSqlDateTime {
                        days: sql_date_time.days,
                        time: sql_date_time.time,
                    }));
                }
                ColumnValues::Date(sql_date) => {
                    ret_val.push(RowDataType::G(sql_date.get_days()));
                }
                ColumnValues::Time(_time) => ret_val.push(RowDataType::H(NapiSqlTime::from(_time))),
                ColumnValues::DateTime2(_date_time2) => {
                    ret_val.push(RowDataType::J(NapiSqlDateTime2::from(_date_time2)))
                }
                ColumnValues::DateTimeOffset(date_time_offset) => ret_val.push(RowDataType::K(
                    NapiSqlDateTimeOffset::from(date_time_offset),
                )),
                ColumnValues::SmallDateTime(sql_small_date_time) => {
                    ret_val.push(RowDataType::F(NapiSqlDateTime {
                        days: sql_small_date_time.days.into(),
                        time: sql_small_date_time.time.into(),
                    }));
                }
                ColumnValues::SmallMoney(sql_small_money) => {
                    ret_val.push(RowDataType::A(sql_small_money.int_val))
                }
                ColumnValues::Money(sql_money) => ret_val.push(RowDataType::L(sql_money.into())),
                ColumnValues::Bytes(items) => ret_val.push(RowDataType::D(Buffer::from(items))),
                ColumnValues::Xml(sql_xml) => {
                    ret_val.push(RowDataType::D(Buffer::from(sql_xml.bytes)))
                }
                ColumnValues::Null => ret_val.push(RowDataType::E(Null)),
                ColumnValues::Json(sql_json) => {
                    ret_val.push(RowDataType::D(Buffer::from(sql_json.bytes)))
                }
            }
        }
        ret_val
    }

    #[napi]
    pub async fn next_row2(&self) -> napi::Result<Option<Vec<RowItem>>> {
        let mut client = self.tds_client.lock().await;

        let result_set = client.get_current_resultset();
        // Check if the client has a result set.
        if result_set.is_none() {
            return Ok(None);
        }
        let result_set = result_set.unwrap();
        let next_row = result_set
            .next_row()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Failed to get next row: {e}")))?;
        let md = result_set.get_metadata();
        match next_row {
            Some(row) => {
                let transformed_row = self.transform_row(row);
                let col_count = transformed_row.len();
                let mut row_items: Vec<RowItem> = Vec::with_capacity(col_count);
                for (i, item) in transformed_row.into_iter().enumerate() {
                    if let Some(meta) = md.get(i) {
                        let metadata: Metadata = meta.into();
                        let row_item = RowItem {
                            metadata,
                            row_val: item,
                        };
                        row_items.push(row_item);
                    } else {
                        return Err(napi::Error::from_reason(format!(
                            "Metadata length mismatch: expected at least {col_count}, found {}",
                            md.len()
                        )));
                    }
                }
                Ok(Some(row_items))
            }
            None => Ok(None),
        }
    }

    // We dont use RowDataType directly as the return type. The binding generation cannot generate aliases for Rust Types,
    // or replace them. Hence to have compilable typings, its necessary that Either* is directly used in the return type.
    #[napi]
    pub async fn next_row_in_resultset(
        &self,
    ) -> napi::Result<
        Option<
            Vec<
                Either15<
                    i32,
                    BigInt,
                    bool,
                    Buffer,
                    Null,
                    NapiSqlDateTime,
                    u32,
                    NapiSqlTime,
                    NapiSqlDateTime,
                    NapiSqlDateTime2,
                    NapiSqlDateTimeOffset,
                    NapiSqlMoney,
                    NapiDecimalParts,
                    f64,
                    String,
                >,
            >,
        >,
    > {
        let mut client = self.tds_client.lock().await;

        let result_set = client.get_current_resultset();
        // Check if the client has a result set.
        if result_set.is_none() {
            return Ok(None);
        }
        let result_set = result_set.unwrap();
        let next_row = result_set
            .next_row()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Failed to get next row: {e}")))?;

        match next_row {
            Some(row) => {
                let transformed_row = self.transform_row(row);
                Ok(Some(transformed_row))
            }
            None => Ok(None),
        }
    }

    #[napi]
    pub async fn get_metadata(&self) -> napi::Result<Option<Vec<Metadata>>> {
        let mut client = self.tds_client.lock().await;
        let result_set = client.get_current_resultset();
        // Check if the client has a result set.
        if result_set.is_none() {
            return Ok(None);
        }
        let result_set = result_set.unwrap();
        let metadata = result_set.get_metadata();

        let metadata_vec: Vec<Metadata> = metadata.iter().map(|m| m.into()).collect();
        Ok(Some(metadata_vec))
    }

    #[napi]
    pub async fn next_result_set(&self) -> napi::Result<bool> {
        let mut client = self.tds_client.lock().await;
        let result = client.move_to_next().await;
        match result {
            Ok(has_next) => Ok(has_next),
            Err(e) => Err(napi::Error::from_reason(format!(
                "Failed to get next result set: {e}"
            ))),
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
