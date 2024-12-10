mod sql_request;

use super::Result;
use crate::connection::packet::PacketType;
use crate::connection::token::decode_token;
use crate::Config;
use crate::Connection;
use crate::ConnectionBuilder;
use bytes::{Buf, BufMut};
use sql_request::SqlRequest;
use tracing::{event, Level};

const ALL_HEADERS_LEN_TX: u32 = 22;

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> Result<()>;
}

pub(crate) trait Decode<B: Buf> {
    fn decode(src: &mut B) -> Result<Self>
    where
        Self: Sized;
}

pub struct Parser {
    _config: Config,
    connection: Connection,
}

impl Parser {
    pub fn connect(config: Config) -> crate::Result<Self> {
        let builder = ConnectionBuilder::new();
        let connection = builder.build(&config)?;
        Ok(Self {
            _config: config,
            connection,
        })
    }

    pub fn execute_sql(&mut self, query: &str) -> crate::Result<()> {
        let request = SqlRequest::new(query);
        event!(Level::TRACE, "Runnig SQL query");
        self.connection.send(PacketType::SQLBatch, request)?;

        self.connection.collect_token_packet()?;
        decode_token(&mut self.connection)?;
        Ok(())
    }
}
