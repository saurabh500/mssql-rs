// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use mssql_tds::datatypes::column_values::{SqlDateTime2, SqlDateTimeOffset, SqlTime};
use napi::{Error, bindgen_prelude::BigInt};

#[napi(object)]
pub struct NapiSqlDateTime {
    pub days: i32,
    pub time: u32,
}

#[napi(object)]
pub struct NapiSqlTime {
    pub time_nanoseconds: BigInt,
    pub scale: u8,
}

impl From<SqlTime> for NapiSqlTime {
    fn from(sql_time: SqlTime) -> Self {
        NapiSqlTime {
            time_nanoseconds: BigInt::from(sql_time.time_nanoseconds),
            scale: sql_time.scale,
        }
    }
}

#[napi(object)]
pub struct NapiSqlDateTime2 {
    pub days: u32,
    pub time: NapiSqlTime,
}

impl From<SqlDateTime2> for NapiSqlDateTime2 {
    fn from(datetime2: SqlDateTime2) -> Self {
        NapiSqlDateTime2 {
            days: datetime2.days,
            time: NapiSqlTime::from(datetime2.time),
        }
    }
}

impl TryFrom<NapiSqlDateTime2> for SqlDateTime2 {
    type Error = napi::Error;
    fn try_from(napi_sql_datetime2: NapiSqlDateTime2) -> Result<Self, Error> {
        let time = SqlTime::try_from(napi_sql_datetime2.time)
            .map_err(|e| Error::from_reason(format!("Failed to convert NapiSqlTime: {e}")))?;
        Ok(SqlDateTime2 {
            days: napi_sql_datetime2.days,
            time,
        })
    }
}

#[napi(object)]
pub struct NapiSqlDateTimeOffset {
    pub datetime2: NapiSqlDateTime2,
    pub offset: i16,
}

impl From<SqlDateTimeOffset> for NapiSqlDateTimeOffset {
    fn from(datetime_offset: SqlDateTimeOffset) -> Self {
        NapiSqlDateTimeOffset {
            datetime2: NapiSqlDateTime2::from(datetime_offset.datetime2),
            offset: datetime_offset.offset,
        }
    }
}

impl TryFrom<NapiSqlDateTimeOffset> for SqlDateTimeOffset {
    type Error = napi::Error;
    fn try_from(napi_sql_datetime_offset: NapiSqlDateTimeOffset) -> Result<Self, Error> {
        let datetime2 = SqlDateTime2::try_from(napi_sql_datetime_offset.datetime2)
            .map_err(|e| Error::from_reason(format!("Failed to convert NapiSqlDateTime2: {e}")))?;
        Ok(SqlDateTimeOffset {
            datetime2,
            offset: napi_sql_datetime_offset.offset,
        })
    }
}

impl TryFrom<NapiSqlTime> for SqlTime {
    type Error = napi::Error;

    fn try_from(napi_sql_time: NapiSqlTime) -> Result<Self, Self::Error> {
        let converted_bigint = napi_sql_time.time_nanoseconds.get_u64();
        if !converted_bigint.2 {
            return Err(Error::from_reason(format!(
                "Time value {} is not lossless. A value out of range of u64 was provided.",
                converted_bigint.1
            )));
        }
        if converted_bigint.0 {
            return Err(Error::from_reason(format!(
                "Time value {} is negative. Only positive values are allowed.",
                converted_bigint.1
            )));
        }
        Ok(SqlTime {
            time_nanoseconds: napi_sql_time.time_nanoseconds.get_u64().1,
            scale: napi_sql_time.scale,
        })
    }
}
