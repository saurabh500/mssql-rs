use core::convert::From;
use std::collections::HashMap;
use std::io::Error;

use tracing::event;

use crate::token::parsers::{
    DoneInProcTokenParser, DoneProcTokenParser, DoneTokenParser, EnvChangeTokenParser,
    ErrorTokenParser, FeatureExtAckTokenParser, FedAuthInfoTokenParser, InfoTokenParser,
    LoginAckTokenParser, TokenParser,
};
use crate::token::tokens::{TokenType, Tokens};

use super::packet_reader::PacketReader;

pub(crate) struct TokenStreamReader<'a> {
    pub(crate) packet_reader: PacketReader<'a>,
    pub(crate) parser_registry: Box<dyn TokenParserRegistry>,
}

impl TokenStreamReader<'_> {
    pub(crate) async fn receive_token(&mut self) -> Result<Tokens, Error> {
        let token_type_byte = self.packet_reader.read_byte().await?;
        let token_type = TokenType::from(token_type_byte);
        if !self.parser_registry.has_parser(&token_type) {
            panic!("No parser found for token type: {:?}", token_type);
        }

        let parser = self
            .parser_registry
            .get_parser(&token_type)
            .expect("Parser not found");
        event!(
            tracing::Level::DEBUG,
            "Parsing token type: {:?}",
            &token_type
        );

        match parser {
            TokenParsers::EnvChange(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::LoginAck(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::Done(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::DoneInProc(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::DoneProc(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::Info(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::Error(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::FedAuthInfo(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
            TokenParsers::FeatureExtAck(parser) => {
                return parser.parse(&mut self.packet_reader).await;
            }
        }
    }
}

pub(crate) trait TokenParserRegistry {
    fn has_parser(&self, token_type: &TokenType) -> bool;
    fn get_parser(&self, token_type: &TokenType) -> Option<&TokenParsers>;
}

struct GenericTokenParserRegistry {
    parsers: HashMap<TokenType, TokenParsers>,
}

impl TokenParserRegistry for GenericTokenParserRegistry {
    fn has_parser(&self, token_type: &TokenType) -> bool {
        self.parsers.contains_key(token_type)
    }

    fn get_parser(&self, token_type: &TokenType) -> Option<&TokenParsers> {
        // Unwrap will throw an error when the parser is not found.
        // This would be an implementation error and would need to be fixed with Code change.
        self.parsers.get(token_type)
    }
}

pub enum TokenParsers {
    EnvChange(EnvChangeTokenParser),
    LoginAck(LoginAckTokenParser),
    Done(DoneTokenParser),
    DoneInProc(DoneInProcTokenParser),
    DoneProc(DoneProcTokenParser),
    Info(InfoTokenParser),
    Error(ErrorTokenParser),
    FedAuthInfo(FedAuthInfoTokenParser),
    FeatureExtAck(FeatureExtAckTokenParser),
}

macro_rules! impl_from_token_parser {
    ($($parser:ty => $variant:ident),*) => {
        $(
            impl From<$parser> for TokenParsers {
                fn from(parser: $parser) -> Self {
                    TokenParsers::$variant(parser)
                }
            }
        )*
    };
}

impl_from_token_parser!(
    EnvChangeTokenParser => EnvChange,
    LoginAckTokenParser => LoginAck,
    DoneTokenParser => Done,
    DoneInProcTokenParser => DoneInProc,
    DoneProcTokenParser => DoneProc,
    InfoTokenParser => Info,
    ErrorTokenParser => Error,
    FedAuthInfoTokenParser => FedAuthInfo,
    FeatureExtAckTokenParser => FeatureExtAck
);

pub struct LoginTokenRegistry {
    internal_registry: HashMap<TokenType, TokenParsers>,
}

impl Default for LoginTokenRegistry {
    fn default() -> Self {
        let mut internal_registry: HashMap<TokenType, TokenParsers> = HashMap::new();
        internal_registry.insert(
            TokenType::EnvChange,
            TokenParsers::from(EnvChangeTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::LoginAck,
            TokenParsers::from(LoginAckTokenParser::default()),
        );
        internal_registry.insert(TokenType::Done, TokenParsers::from(DoneTokenParser {}));
        internal_registry.insert(
            TokenType::DoneInProc,
            TokenParsers::from(DoneInProcTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::DoneProc,
            TokenParsers::from(DoneProcTokenParser::default()),
        );
        internal_registry.insert(TokenType::Info, TokenParsers::from(InfoTokenParser {}));
        internal_registry.insert(TokenType::Error, TokenParsers::from(ErrorTokenParser {}));
        internal_registry.insert(
            TokenType::FeatureExtAck,
            TokenParsers::from(FeatureExtAckTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::FedAuthInfo,
            TokenParsers::from(FedAuthInfoTokenParser::default()),
        );
        Self { internal_registry }
    }
}

impl TokenParserRegistry for LoginTokenRegistry {
    fn has_parser(&self, token_type: &TokenType) -> bool {
        self.internal_registry.contains_key(token_type)
    }

    fn get_parser(&self, token_type: &TokenType) -> Option<&TokenParsers> {
        // Unwrap will throw an error when the parser is not found.
        // This would be an implementation error and would need to be fixed with Code change.
        self.internal_registry.get(token_type)
    }
}
