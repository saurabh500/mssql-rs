// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use bytes::Bytes;

use crate::error::Result;

/// Buffered binary stream for large column values (FR-017, research R4).
///
/// Current implementation is fully buffered — the entire column value is
/// read into memory by `mssql-tds` before being yielded. True PLP
/// (Partially Length-Prefixed) streaming requires upstream changes to
/// `mssql-tds`'s `read_plp_bytes()`.
pub struct ColumnStream {
    data: Bytes,
    pos: usize,
    chunk_size: usize,
}

impl ColumnStream {
    /// Create a stream over a fully-buffered binary column.
    #[allow(dead_code)]
    pub(crate) fn new(data: Vec<u8>, chunk_size: usize) -> Self {
        Self {
            data: Bytes::from(data),
            pos: 0,
            chunk_size: chunk_size.max(1),
        }
    }

    /// Total byte length of the column value.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the column value is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Yield the next chunk of bytes, or `None` when exhausted.
    pub fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.pos >= self.data.len() {
            return Ok(None);
        }
        let end = (self.pos + self.chunk_size).min(self.data.len());
        let chunk = self.data.slice(self.pos..end);
        self.pos = end;
        Ok(Some(chunk))
    }

    /// Consume the stream and return the entire value as `Bytes`.
    pub fn into_bytes(self) -> Bytes {
        self.data
    }
}
