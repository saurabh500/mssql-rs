// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use std::ops::Deref;

use mssql_tds::datatypes::column_values::{
    SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlSmallDateTime, SqlTime,
};
use napi::{Error, bindgen_prelude::BigInt};

#[napi(object)]
#[derive(Debug)]
pub struct NapiF64 {
    pub value: f64,
}

impl From<f64> for NapiF64 {
    fn from(value: f64) -> Self {
        NapiF64 { value }
    }
}

impl From<f32> for NapiF64 {
    fn from(value: f32) -> Self {
        NapiF64 {
            value: value as f64,
        }
    }
}

impl Deref for NapiF64 {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[napi(object)]
#[derive(Debug)]
pub struct NapiSqlDateTime {
    pub days: i32,
    pub time: u32,
}

impl From<NapiSqlDateTime> for SqlDateTime {
    fn from(datetime: NapiSqlDateTime) -> Self {
        SqlDateTime {
            days: datetime.days,
            time: datetime.time,
        }
    }
}

impl TryFrom<NapiSqlDateTime> for SqlSmallDateTime {
    type Error = napi::Error;
    fn try_from(napi_sql_datetime: NapiSqlDateTime) -> Result<Self, Error> {
        //  check if napi_sql_datetime.days is convertible to u16 else return an error.
        if napi_sql_datetime.days < 0 || napi_sql_datetime.days > u16::MAX as i32 {
            return Err(Error::from_reason(format!(
                "Days value {} is out of range for SmallDateTime.",
                napi_sql_datetime.days
            )));
        }
        //  check if napi_sql_datetime.time is convertible to u32 else return an error.
        if napi_sql_datetime.time > u16::MAX as u32 {
            return Err(Error::from_reason(format!(
                "Time value {} is out of range for SmallDateTime.",
                napi_sql_datetime.time
            )));
        }
        Ok(SqlSmallDateTime {
            days: napi_sql_datetime.days as u16,
            time: napi_sql_datetime.time as u16,
        })
    }
}

#[napi(object)]
#[derive(Debug)]
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
#[derive(Debug)]
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
#[derive(Debug)]
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
