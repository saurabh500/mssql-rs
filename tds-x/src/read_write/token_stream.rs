use std::collections::HashMap;
use std::io::Error;

use tracing::event;

use crate::token::parsers::{
    DoneInProcTokenParser, DoneProcTokenParser, DoneTokenParser, EnvChangeTokenParser,
    ErrorTokenParser, FeatureExtAckTokenParser, FedAuthInfoTokenParser, InfoTokenParser,
    LoginAckTokenParser, TokenParser,
};
use crate::token::tokens::{Token, TokenType};

use super::packet_reader::PacketReader;

pub(crate) struct TokenStreamReader<'a> {
    pub(crate) packet_reader: PacketReader<'a>,
    pub(crate) parser_registry: Box<dyn TokenParserRegistry>,
}

impl TokenStreamReader<'_> {
    pub(crate) async fn receive_token(&mut self) -> Result<Box<dyn Token>, Error> {
        let token_type_byte = self.packet_reader.read_byte().await?;
        let token_type = TokenType::from(token_type_byte);
        if !self.parser_registry.has_parser(&token_type) {
            panic!("No parser found for token type: {:?}", token_type);
        }

        let parser = self.parser_registry.get_parser(&token_type);
        event!(
            tracing::Level::DEBUG,
            "Parsing token type: {:?}",
            &token_type
        );
        parser.parse(&mut self.packet_reader).await
    }
}

pub(crate) trait TokenParserRegistry {
    fn has_parser(&self, token_type: &TokenType) -> bool;
    fn get_parser(&self, token_type: &TokenType) -> &dyn TokenParser;
}

struct GenericTokenParserRegistry {
    parsers: HashMap<TokenType, Box<dyn TokenParser>>,
}

impl TokenParserRegistry for GenericTokenParserRegistry {
    fn has_parser(&self, token_type: &TokenType) -> bool {
        self.parsers.contains_key(token_type)
    }

    fn get_parser(&self, token_type: &TokenType) -> &dyn TokenParser {
        // Unwrap will throw an error when the parser is not found.
        // This would be an implementation error and would need to be fixed with Code change.
        self.parsers.get(token_type).unwrap().as_ref()
    }
}

pub struct LoginTokenRegistry {
    internal_registry: HashMap<TokenType, Box<dyn TokenParser>>,
}

impl Default for LoginTokenRegistry {
    fn default() -> Self {
        let mut internal_registry: HashMap<TokenType, Box<dyn TokenParser>> = HashMap::new();
        internal_registry.insert(TokenType::EnvChange, Box::new(EnvChangeTokenParser {}));
        internal_registry.insert(TokenType::LoginAck, Box::new(LoginAckTokenParser {}));
        internal_registry.insert(TokenType::Done, Box::new(DoneTokenParser {}));
        internal_registry.insert(TokenType::DoneInProc, Box::new(DoneInProcTokenParser {}));
        internal_registry.insert(TokenType::DoneProc, Box::new(DoneProcTokenParser {}));
        internal_registry.insert(TokenType::Info, Box::new(InfoTokenParser {}));
        internal_registry.insert(TokenType::Error, Box::new(ErrorTokenParser {}));
        internal_registry.insert(
            TokenType::FeatureExtAck,
            Box::new(FeatureExtAckTokenParser {}),
        );
        internal_registry.insert(TokenType::FedAuthInfo, Box::new(FedAuthInfoTokenParser {}));

        Self { internal_registry }
    }
}

impl TokenParserRegistry for LoginTokenRegistry {
    fn has_parser(&self, token_type: &TokenType) -> bool {
        self.internal_registry.contains_key(token_type)
    }

    fn get_parser(&self, token_type: &TokenType) -> &dyn TokenParser {
        // Unwrap will throw an error when the parser is not found.
        // This would be an implementation error and would need to be fixed with Code change.
        self.internal_registry.get(token_type).unwrap().as_ref()
    }
}
