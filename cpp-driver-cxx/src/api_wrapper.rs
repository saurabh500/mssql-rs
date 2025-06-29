// Note: CXX requires that types exposed from Rust to C++ in an extern "Rust" block
// are defined in the same crate. So types from tds-x cannot be exposed directly.
// This module defines wrapper types that are exposed.

use crate::ffi;
use std::future::Future;
use std::marker;
use std::pin::Pin;
use tds_x::core::TdsResult;
use tds_x::datatypes::column_values::ColumnValues;

pub struct TdsConnection<'a, 'result>
where
    'a: 'result,
{
    pub(crate) connection: tds_x::connection::tds_connection::TdsConnection,
    pub(crate) unused: marker::PhantomData<&'result ()>,
    pub(crate) unused2: marker::PhantomData<&'a ()>,
}

pub struct ClientContext {
    pub(crate) context: tds_x::connection::client_context::ClientContext,
}

pub struct QueryResultTypeStream<'result> {
    pub(crate) results: tds_x::query::result::QueryResultTypeStream<'result>,
    pub(crate) current_result: Option<tds_x::query::result::QueryResultType<'result>>,
}

pub struct QueryResultType<'result> {
    pub(crate) result_set: Option<tds_x::query::result::ResultSet<'result>>,
    pub(crate) dml_result: Option<u64>,
    pub(crate) result_type: ffi::ResultType,
}

impl From<&tds_x::query::result::QueryResultType<'_>> for ffi::ResultType {
    fn from(result_type: &tds_x::query::result::QueryResultType) -> Self {
        match result_type {
            tds_x::query::result::QueryResultType::DmlResult(_) => ffi::ResultType::DmlResult,
            tds_x::query::result::QueryResultType::ResultSet(_) => ffi::ResultType::ResultSet,
        }
    }
}

pub struct RowStream<'result> {
    pub(crate) row_stream: tds_x::query::result::RowStream<'result>,
    pub(crate) current_row: Option<tds_x::query::result::RowData>,
}

pub struct RowData {
    pub(crate) row_data: tds_x::query::result::RowData,
    pub(crate) current_cell: Option<ColumnValues>,
}

pub struct ColumnValue {
    pub(crate) value: Option<ColumnValues>,
}

#[allow(clippy::type_complexity)]
pub struct TdsConnectionFuture<'a, 'result>
where
    'a: 'result,
{
    pub(crate) future_connection:
        Option<Box<Pin<Box<dyn Future<Output = TdsResult<Box<TdsConnection<'a, 'result>>>> + 'a>>>>,
}

#[allow(clippy::type_complexity)]
pub struct QueryResultTypeFuture<'result> {
    pub(crate) future_query_result_type:
        Option<Box<Pin<Box<dyn Future<Output = Box<QueryResultTypeStream<'result>>> + 'result>>>>,
}

#[allow(clippy::type_complexity)]
pub struct BoolFuture<'result> {
    pub(crate) future_bool: Option<Box<Pin<Box<dyn Future<Output = TdsResult<bool>> + 'result>>>>,
}
