mod column_metadata;
mod env_change;
mod error_token;
pub(crate) mod login;
mod login_ack;
pub(crate) mod pre_login;
mod row_data;

use super::TransportBuffer;
use crate::Result;
use crate::TdsError;
use column_metadata::TokenColumnMetadata;
use env_change::TokenEnvChange;
use error_token::TokenError;
use login_ack::TokenLoginAck;
use row_data::TokenRowData;
use tracing::{event, Level};

uint_enum! {
    /// TokenType is a single byte identifier that is used to describe the data.
    pub enum TokenType {
        ReturnStatus = 0x79,
        ColMetaData = 0x81,
        Error = 0xAA,
        Info = 0xAB,
        Order = 0xA9,
        ColInfo = 0xA5,
        ReturnValue = 0xAC,
        LoginAck = 0xAD,
        Row = 0xD1,
        NbcRow = 0xD2,
        Sspi = 0xED,
        EnvChange = 0xE3,
        Done = 0xFD,
        DoneProc = 0xFE,
        DoneInProc = 0xFF,
        FeatureExtAck = 0xAE,
    }
}

pub(crate) fn decode_token<T>(src: &mut T) -> Result<()>
where
    T: TransportBuffer,
{
    let mut column_metadata: Option<TokenColumnMetadata> = None;
    while !src.is_eof() {
        let ty_byte = src.get_u8()?;
        let ty = TokenType::try_from(ty_byte)
            .map_err(|_| TdsError::Message(format!("invalid token type {:x}", ty_byte)))?;
        let size;
        match ty {
            TokenType::Done | TokenType::DoneInProc | TokenType::DoneProc => {
                let status = src.get_u16_le()?;
                event!(Level::INFO, "Server token type: {:?}, flags {}", ty, status);
                size = 10;
            }
            TokenType::LoginAck => {
                size = 0;
                let login_ack = TokenLoginAck::decode(src)?;
                event!(Level::INFO, "TokenLoginAck: {:?}", login_ack);
                event!(Level::INFO, "Login acknowledged!");
            }
            TokenType::EnvChange => {
                size = 0;

                let env_change = TokenEnvChange::decode(src)?;
                event!(Level::INFO, "TokenEnvChange: {}", env_change);
            }
            TokenType::ColMetaData => {
                size = 0;
                column_metadata = Some(TokenColumnMetadata::decode(src)?);
            }
            TokenType::Row => {
                size = 0;
                match column_metadata {
                    Some(ref metadata) => TokenRowData::decode(src, metadata)?,
                    None => return Err(TdsError::Message("Getting row before column".into())),
                };
            }
            TokenType::Error => {
                size = 0;
                let server_error = TokenError::decode(src)?;
                event!(
                    Level::ERROR,
                    "Error token with message {:?} {:?} {:?} {:?}",
                    server_error.error_number,
                    server_error.error_state,
                    server_error.error_class,
                    server_error.error_message
                );
            }
            _ => {
                size = src.get_u16_le()?;
                event!(Level::INFO, "Server token type: {:?}, length {}", ty, size);
            }
        }

        src.advance(size as usize)?;
    }

    Ok(())
}
