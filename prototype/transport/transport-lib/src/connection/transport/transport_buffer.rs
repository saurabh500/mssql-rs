use crate::Result;
use bytes::BytesMut;

pub(crate) trait TransportBuffer {
    fn get_u8(&mut self) -> Result<u8>;

    fn get_u16_le(&mut self) -> Result<u16>;

    fn get_u32(&mut self) -> Result<u32>;

    fn get_u32_le(&mut self) -> Result<u32>;

    fn split_to(&mut self, size: usize) -> Result<BytesMut>;

    fn advance(&mut self, size: usize) -> Result<()>;

    fn is_eof(&self) -> bool;
}
