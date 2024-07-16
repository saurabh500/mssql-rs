use tracing::{event, Level};
use super::super::TransportBuffer;
use crate::TdsError;

pub(crate) struct TokenColumnMetadata {
    column_count: u16,
    column_types: Vec<ColumnMetadata>,
}

struct ColumnMetadata {
    _column_type: u8,
    _column_name: String,
}

impl TokenColumnMetadata {
    pub(crate) fn columns(&self) -> u16 {
        self.column_count
    }

    pub(crate) fn decode<T>(src: &mut T) -> crate::Result<Self>
    where
        T: TransportBuffer,
    {
        let column_count = src.get_u16_le()?;
        let mut column_metadata = TokenColumnMetadata {
            column_count,
            column_types: Vec::new(),

        };

        if column_count > 0 && column_count < 0xffff {
            for i in 0..column_count {
                
                let _user_ty = src.get_u32_le()?;
                let _flag = src.get_u16_le()?;
                let ty = src.get_u8()?;

                // The code can only handle SQL results with VarChar SQL type.
                // It is not a extensive implementation, but it is a proof of concept to parse query result.
                if ty != 0xA7 {
                    return Err(TdsError::Message(format!("Unsuported column type {:#X}", ty)));
                }

                let shiloh_collation_size = 7;
                src.advance(shiloh_collation_size)?;
                let col_name_size = src.get_u8()? as usize;
                let mut wchars = vec![0; col_name_size];
                for wchar in wchars.iter_mut().take(col_name_size) {
                    *wchar = src.get_u16_le()?;
                }

                let col_name = String::from_utf16(&wchars[..])?;
                event!(Level::INFO, "Column {} : name {}", i, col_name);

                column_metadata.column_types.push(
                ColumnMetadata {
                    _column_type: ty,
                    _column_name: col_name,
                });
            }
        }


        Ok(column_metadata)
    }
}
