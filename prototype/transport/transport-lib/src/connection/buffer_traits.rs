use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::BytesMut;
use crate::Result;

pub(crate) trait BufferStringDecode {
    /// Reads a UTF-16 string of specified `length` from the buffer.
    fn get_utf16_string(&mut self, length: usize) -> Result<String>;
}

impl BufferStringDecode for Cursor<BytesMut> {

    fn get_utf16_string(&mut self, string_len: usize) -> Result<String> {

        let mut bytes: Vec<u16> = vec![0; string_len];

        for item in bytes.iter_mut().take(string_len) {
            *item = self.read_u16::<LittleEndian>()?;
        }

        let new_value = String::from_utf16(&bytes[..])?;
        Ok(new_value)
    }
}
