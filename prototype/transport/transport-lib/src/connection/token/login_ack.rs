use crate::TdsError;
use bytes::{Buf, BytesMut};
use std::convert::TryFrom;

uint_enum! {
    #[repr(u32)]
    #[derive(PartialOrd)]
    pub enum FeatureLevel {
        SqlServerV7 = 0x70000000,
        SqlServer2000 = 0x71000000,
        SqlServer2000Sp1 = 0x71000001,
        SqlServer2005 = 0x72090002,
        SqlServer2008 = 0x730A0003,
        SqlServer2008R2 = 0x730B0003,
        /// 2012, 2014, 2016
        SqlServerN = 0x74000004,
    }
}

#[allow(dead_code)] // we might want to debug the values
#[derive(Debug)]
pub struct TokenLoginAck {
    /// The type of interface with which the server will accept client requests
    /// 0: SQL_DFLT (server confirms that whatever is sent by the client is acceptable. If the client
    ///    requested SQL_DFLT, SQL_TSQL will be used)
    /// 1: SQL_TSQL (TSQL is accepted)
    pub(crate) interface: u8,
    pub(crate) tds_version: FeatureLevel,
    pub(crate) prog_name: String,
    /// major.minor.buildhigh.buildlow
    pub(crate) version: u32,
}

impl TokenLoginAck {
    pub(crate) fn decode(src: &mut BytesMut) -> crate::Result<Self>
    {
        let _length = src.get_u16_le();

        let interface = src.get_u8();

        let tds_version = FeatureLevel::try_from(src.get_u32())
            .map_err(|_| TdsError::Message("Login ACK: Invalid TDS version".into()))?;

        let len = src.get_u8() as usize;
        let mut bytes = vec![0; len];

        for item in bytes.iter_mut().take(len) {
            *item = src.get_u16_le();
        }

        let prog_name = String::from_utf16(&bytes[..])?;
        let version = src.get_u32_le();

        Ok(TokenLoginAck {
            interface,
            tds_version,
            prog_name,
            version,
        })
    }
}
