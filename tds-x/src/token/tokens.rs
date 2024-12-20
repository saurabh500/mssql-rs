use crate::read_write::packet_reader::PacketReader;
use async_trait::async_trait;

pub enum TokenType {
    AltMetadata = 0x88,
    AltRow = 0xD3,
    ColMetadata = 0x81,
    ColInfo = 0xA5,
    Done = 0xFD,
    DoneProc = 0xFE,
    DoneInProc = 0xFF,
    EnvCange = 0xE3,
    Error = 0xAA,
    FeatureExtAck = 0xAE,
    FedAuthInfo = 0xEE,
    Info = 0xAB,
    LoginAck = 0xAD,
    NbcRow = 0xD2,
    Offset = 0x78,
    Order = 0xA9,
    ReturnStatus = 0x79,
    ReturnValue = 0xAC,
    Row = 0xD1,
    SSPI = 0xED,
    TabName = 0xA4,
}

pub trait Token {
    fn token_type(&self) -> TokenType;
}

pub struct TokenEvent<'a> {
    pub token: &'a dyn Token,
    pub exit: bool,
}

#[async_trait]
pub trait TokenParser {
    async fn parse(&self, token_type: TokenType, packet_reader: &PacketReader) -> Box<dyn Token>;
}
