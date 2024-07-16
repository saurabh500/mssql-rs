use super::{Encode, ALL_HEADERS_LEN_TX};
use bytes::{BufMut, BytesMut};

pub struct SqlRequest {
    query: Vec<u16>,
}

impl SqlRequest {
    pub fn new(query: &str) -> Self {
        Self {
            query: query.encode_utf16().collect(),
        }
    }
}

impl<'a> Encode<BytesMut> for SqlRequest {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        dst.put_u32_le(ALL_HEADERS_LEN_TX);
        dst.put_u32_le(ALL_HEADERS_LEN_TX - 4);
        dst.put_u16_le(2u16); // Transaction descriptor
        dst.put_slice(&[0u8; 8]);
        dst.put_u32_le(1);

        for c in self.query {
            dst.put_u16_le(c);
        }

        Ok(())
    }
}
