use crate::query::metadata::ColumnMetadata;
use crate::read_write::token_stream::TokenStreamReader;
use futures::Stream;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

pub enum QueryResultType<'a, 'n> {
    Update(i64),
    ResultSet(ResultSet<RowIter<RowData<'a, 'n>, CellData>, RowData<'a, 'n>>),
}

pub struct QueryResult<'a, 'n> {
    token_stream_reader: &'a mut TokenStreamReader<'n>,
}

impl<'a, 'n> Stream for QueryResult<'a, 'n> {
    type Item = QueryResultType<'a, 'n>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct ResultSet<StreamType, ValueType>
where
    StreamType: Stream<Item = Result<ValueType, Error>>,
{
    metadata: Vec<ColumnMetadata>,
    row_stream: Box<StreamType>,
}

impl<'a, 'n> ResultSet<RowIter<RowData<'a, 'n>, CellData>, RowData<'a, 'n>> {
    pub(crate) fn new(_token_stream: TokenStreamReader) -> Self {
        todo!()
    }

    pub async fn get_all_data(&mut self) -> Result<Vec<Vec<CellData>>, Error> {
        // Internally iterate over the row data and cache all the CellDatas into vectors.
        todo!();
    }

    pub fn get_metadata(&self) -> &Vec<ColumnMetadata> {
        self.metadata.as_ref()
    }

    pub async fn into_row_stream(self) -> Result<Box<RowIter<RowData<'a, 'n>, CellData>>, Error> {
        Ok(self.row_stream)
    }
}

pub struct RowIter<StreamType, ValueType>
where
    StreamType: Stream<Item = Result<ValueType, Error>>,
{
    rows: StreamType,
}

impl<'a, 'n> Stream for RowIter<RowData<'a, 'n>, CellData> {
    type Item = Result<RowData<'a, 'n>, Error>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct RowData<'a, 'n> {
    token_stream: &'a mut TokenStreamReader<'n>,
}

impl Stream for RowData<'_, '_> {
    type Item = Result<CellData, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct CellData {
    protocol_data: Vec<u8>,
}

impl CellData {
    fn get_value<RustType>() -> RustType {
        todo!()
    }

    fn into_byte_stream(self) -> bytes::Bytes {
        todo!()
    }

    fn get_bytes(&self) -> &[u8] {
        self.protocol_data.as_ref()
    }
}
