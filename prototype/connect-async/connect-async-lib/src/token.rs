use bytes::{Buf, BytesMut};
use tracing::{event, Level};
use crate::error::Error;
use crate::Result;

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

pub(crate) fn decode_token(src: &mut BytesMut) -> Result<()> {

    while src.len() > 3 {
        let ty_byte = src.get_u8();
        let ty = TokenType::try_from(ty_byte).map_err(|_| Error::Protocol(format!("invalid token type {:x}", ty_byte).into()))?;
        let size;
        match ty {
            TokenType::Done | TokenType::DoneInProc | TokenType::DoneProc => {
                let status = src.get_u16_le();
                event!(Level::INFO, "Server token type: {:?}, flags {}", ty, status);
                size = 10;
            }
            TokenType::LoginAck => {
                size = src.get_u16_le();
                event!(Level::INFO, "Server token type: {:?}", ty);
                event!(Level::INFO, "Login acknowledged!");
            }
            _ => {
                size = src.get_u16_le();
                event!(Level::INFO, "Server token type: {:?}, length {}", ty, size);
            }
        }

        if src.len() < size as usize {
            return Err(Error::Protocol(format!("Invalid token {:?}, expected size {} but only {} bytes left", ty, size, src.len()).into()));
        }
        src.advance(size as usize);
    }

    if src.len() > 0 {
        return Err(Error::Protocol(format!("Invalid packet. There are still {} bytes left", src.len()).into()));
    }
    Ok(())
}
