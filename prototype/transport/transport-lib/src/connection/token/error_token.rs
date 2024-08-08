use std::io::Cursor;
use byteorder::{LittleEndian, ReadBytesExt};
use crate::connection::buffer_traits::BufferStringDecode;

use super::super::TransportBuffer;
use tracing::{event, Level};

pub(crate) struct TokenError {
    pub(crate) error_number: u32,
    pub(crate) error_state: u8,
    pub(crate) error_class: u8,
    pub(crate) error_message: String,
    pub(crate) server: String,
    pub(crate) procedure: String,
    pub(crate) line: u32,
}

/// Represents the Error token in the TDS protocol.
impl TokenError {
    pub(crate) fn decode<T>(src: &mut T) -> crate::Result<Self>
    where
        T: TransportBuffer,
    {
        let mut token_error: TokenError = TokenError {
            error_number: 0,
            error_state: 0,
            error_class: 0,
            error_message: String::from(""),
            server: String::from(""),
            procedure: String::from(""),
            line: 0,
        };
        let token_length = src.get_u16_le()? as usize;
        event!(Level::DEBUG, "The length of error token  {:?}", token_length);

        let token_payload = src.split_to(token_length)?;
        let mut payload_cursor = Cursor::new(token_payload);
        token_error.error_number = payload_cursor.read_u32::<LittleEndian>()?;
        
        token_error.error_state = payload_cursor.read_u8()?;
        token_error.error_class = payload_cursor.read_u8()?;
        let message_len = payload_cursor.read_u16::<LittleEndian>()?;
        token_error.error_message = payload_cursor.get_utf16_string(message_len as usize)?;

        let mut byte_len = payload_cursor.read_u8()?;
        // Check if the server name is available
        if byte_len != 0 {
            token_error.server = payload_cursor.get_utf16_string(byte_len as usize)?;
        }

        byte_len = payload_cursor.read_u8()?;
        // Check if the procedure name is available
        if byte_len != 0 {
            let procedure = payload_cursor.get_utf16_string(byte_len as usize)?;
            token_error.procedure = procedure;
        }

        token_error.line = payload_cursor.read_u32::<LittleEndian>()?;
        
        Ok(token_error)
    }
}
