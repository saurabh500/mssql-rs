use super::super::TransportBuffer;
use super::column_metadata::TokenColumnMetadata;
use tracing::{event, Level};

pub(crate) struct TokenRowData {}

impl TokenRowData {
    pub(crate) fn decode<T>(
        src: &mut T,
        column_metadata: &TokenColumnMetadata,
    ) -> crate::Result<TokenRowData>
    where
        T: TransportBuffer,
    {
        for i in 0..column_metadata.columns() {
            let row_size = src.get_u16_le()? as usize;
            event!(Level::INFO, "Column {} value size: {}", i, row_size);
            let mut chars = vec![0u8; row_size];
            for char in chars.iter_mut().take(row_size) {
                *char = src.get_u8()?;
            }

            let row_value = String::from_utf8(chars[..].to_vec()).expect("Found invalid UTF-8");
            event!(Level::INFO, "Column {} value: {}", i, row_value);
        }

        Ok(TokenRowData {})
    }
}
