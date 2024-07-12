
use crate::Connection;
use crate::Config;
use crate::ConnectionBuilder;

pub struct Parser {
    _config: Config,
    _connection: Connection,
}

impl Parser {
    pub fn connect(config: Config) -> crate::Result<Self> {
        let builder = ConnectionBuilder::new();
        let connection = builder.build(&config)?;
        Ok(Self {
            _config: config,
            _connection: connection,
        })
    }
}
