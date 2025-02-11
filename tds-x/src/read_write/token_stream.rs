use core::convert::From;
use std::collections::HashMap;
use std::io::Error;

use tracing::event;

use crate::datatypes::decoder::GenericDecoder;
use crate::token::parsers::{
    ColMetadataTokenParser, DoneInProcTokenParser, DoneProcTokenParser, DoneTokenParser,
    EnvChangeTokenParser, ErrorTokenParser, FeatureExtAckTokenParser, FedAuthInfoTokenParser,
    InfoTokenParser, LoginAckTokenParser, OrderTokenParser, ReturnStatusTokenParser,
    RowTokenParser, TokenParser,
};
use crate::token::tokens::{ColMetadataToken, TokenType, Tokens};

use super::packet_reader::PacketReader;

pub(crate) struct TokenStreamReader<'a> {
    pub(crate) packet_reader: PacketReader<'a>,
    pub(crate) parser_registry: Box<dyn TokenParserRegistry>,
}

/// `ParserContext` is used to add additional context, which can be leveraged by the token parsers.
/// One of the usecase is passing the metadata for the columns, to the row parser and to the
/// NBC row token parser.
/// The consumer of the TokenStreamReader is supposed to set/reset this context.
/// Incorrectly managing this context, can lead to bad context being used for subsequent operations.
#[derive(Debug)]
pub(crate) enum ParserContext {
    ColumnMetadata(ColMetadataToken),
    None(()),
}

impl Default for ParserContext {
    fn default() -> Self {
        ParserContext::None(())
    }
}

impl TokenStreamReader<'_> {
    pub(crate) fn new(
        packet_reader: PacketReader,
        parser_registry: Box<dyn TokenParserRegistry>,
    ) -> TokenStreamReader {
        TokenStreamReader {
            packet_reader,
            parser_registry,
        }
    }

    pub(crate) async fn receive_token(&mut self, context: &ParserContext) -> Result<Tokens, Error> {
        let token_type_byte = self.packet_reader.read_byte().await?;
        let token_type = TokenType::from(token_type_byte);
        if !self.parser_registry.has_parser(&token_type) {
            unimplemented!("No parser implemented for token type: {:?}", token_type);
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
            TokenParsers::EnvChange(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::LoginAck(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::Done(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::DoneInProc(parser) => {
                parser.parse(&mut self.packet_reader, context).await
            }
            TokenParsers::DoneProc(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::Info(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::Error(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::FedAuthInfo(parser) => {
                parser.parse(&mut self.packet_reader, context).await
            }
            TokenParsers::FeatureExtAck(parser) => {
                parser.parse(&mut self.packet_reader, context).await
            }
            TokenParsers::ColMetadata(parser) => {
                parser.parse(&mut self.packet_reader, context).await
            }
            TokenParsers::Row(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::Order(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::ReturnStatus(parser) => {
                parser.parse(&mut self.packet_reader, context).await
            }
        }
    }
}

pub(crate) trait TokenParserRegistry: Send + Sync {
    fn has_parser(&self, token_type: &TokenType) -> bool;
    fn get_parser(&self, token_type: &TokenType) -> Option<&TokenParsers>;
}

pub(crate) struct GenericTokenParserRegistry {
    parsers: HashMap<TokenType, TokenParsers>,
}

impl Default for GenericTokenParserRegistry {
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
        internal_registry.insert(
            TokenType::ColMetadata,
            TokenParsers::from(ColMetadataTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::Row,
            TokenParsers::from(RowTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::Order,
            TokenParsers::from(OrderTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::ReturnStatus,
            TokenParsers::from(ReturnStatusTokenParser::default()),
        );
        Self {
            parsers: internal_registry,
        }
    }
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
    ColMetadata(ColMetadataTokenParser),
    Row(RowTokenParser<GenericDecoder>),
    Order(OrderTokenParser),
    ReturnStatus(ReturnStatusTokenParser),
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
    FeatureExtAckTokenParser => FeatureExtAck,
    ColMetadataTokenParser => ColMetadata,
    RowTokenParser<GenericDecoder> => Row,
    OrderTokenParser => Order,
    ReturnStatusTokenParser => ReturnStatus
);
