// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::core::{CancelHandle, TdsResult};
use crate::datatypes::decoder::GenericDecoder;
use crate::error::Error::{OperationCancelledError, TimeoutError};
use crate::error::TimeoutErrorType;
use crate::read_write::packet_reader::TdsPacketReader;
use crate::token::parsers::{
    ColMetadataTokenParser, DoneInProcTokenParser, DoneProcTokenParser, DoneTokenParser,
    EnvChangeTokenParser, ErrorTokenParser, FeatureExtAckTokenParser, FedAuthInfoTokenParser,
    InfoTokenParser, LoginAckTokenParser, NbcRowTokenParser, OrderTokenParser,
    ReturnStatusTokenParser, ReturnValueTokenParser, RowTokenParser, TokenParser,
};
use crate::token::tokens::{ColMetadataToken, DoneStatus, TokenType, Tokens};
use async_trait::async_trait;
use core::convert::From;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::timeout;
use tracing::event;

#[async_trait]
#[cfg(not(fuzzing))]
pub(crate) trait TdsTokenStreamReader {
    async fn receive_token(
        &mut self,
        context: &ParserContext,
        remaining_request_timeout: Option<Duration>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<Tokens>;
}

#[async_trait]
#[cfg(fuzzing)]
pub trait TdsTokenStreamReader {
    async fn receive_token(
        &mut self,
        context: &ParserContext,
        remaining_request_timeout: Option<Duration>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<Tokens>;
}

#[cfg(not(fuzzing))]
pub(crate) struct TokenStreamReader<T, R>
where
    T: TdsPacketReader + Send + Sync,
    R: TokenParserRegistry + Send + Sync,
{
    pub(crate) packet_reader: T,
    pub(crate) parser_registry: Box<R>,
}

#[cfg(fuzzing)]
pub struct TokenStreamReader<T, R>
where
    T: TdsPacketReader + Send + Sync,
    R: TokenParserRegistry + Send + Sync,
{
    pub packet_reader: T,
    pub parser_registry: Box<R>,
}

/// `ParserContext` is used to add additional context, which can be leveraged by the token parsers.
/// One of the usecase is passing the metadata for the columns, to the row parser and to the
/// NBC row token parser.
/// The consumer of the TokenStreamReader is supposed to set/reset this context.
/// Incorrectly managing this context, can lead to bad context being used for subsequent operations.
#[derive(Debug)]
#[cfg(not(fuzzing))]
pub(crate) enum ParserContext {
    ColumnMetadata(ColMetadataToken),
    None(()),
}

#[derive(Debug)]
#[cfg(fuzzing)]
pub enum ParserContext {
    ColumnMetadata(ColMetadataToken),
    None(()),
}

impl Default for ParserContext {
    fn default() -> Self {
        ParserContext::None(())
    }
}

impl<T, R> TokenStreamReader<T, R>
where
    T: TdsPacketReader + Send + Sync,
    R: TokenParserRegistry + Send + Sync,
{
    #[cfg(not(fuzzing))]
    pub(crate) fn new(packet_reader: T, parser_registry: Box<R>) -> TokenStreamReader<T, R> {
        TokenStreamReader {
            packet_reader,
            parser_registry,
        }
    }

    #[cfg(fuzzing)]
    pub fn new(packet_reader: T, parser_registry: Box<R>) -> TokenStreamReader<T, R> {
        TokenStreamReader {
            packet_reader,
            parser_registry,
        }
    }

    async fn receive_token_internal(&mut self, context: &ParserContext) -> TdsResult<Tokens> {
        // Read the token type so that we can get the right parser for this token.
        // The first byte of the token is the token type.
        let token_type_byte = self.packet_reader.read_byte().await?;
        let token_type = TokenType::from(token_type_byte);

        // We should always have a parser for the token type.
        // If we don't, then we have a bug in the code.
        if !self.parser_registry.has_parser(&token_type) {
            unreachable!(
                "No parser implemented for token type: {:?}. This is an internal implementation error.",
                token_type
            );
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
            TokenParsers::NbcRow(parser) => parser.parse(&mut self.packet_reader, context).await,
            TokenParsers::ReturnValue(parser) => {
                parser.parse(&mut self.packet_reader, context).await
            }
        }
    }

    /// Tells the server to stop sending tokens for the token stream being read and waits for
    /// an acknowledgement.
    async fn cancel_read_stream_and_wait(&mut self) -> TdsResult<()> {
        self.packet_reader.cancel_read_stream().await?;
        let dummy_context = ParserContext::None(());
        // This method is intended to be called from receive_token(). We enforce only one level
        // of recursion by preventing timeout and cancellation on the internal receive_token() call.
        while let Ok(token) = self.receive_token_internal(&dummy_context).await {
            if let Tokens::Done(done_token) = token {
                if done_token.status.contains(DoneStatus::ATTN) {
                    break;
                }
                // Discard any other token.
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<T, R> TdsTokenStreamReader for TokenStreamReader<T, R>
where
    T: TdsPacketReader + Send + Sync,
    R: TokenParserRegistry + Send + Sync,
{
    async fn receive_token(
        &mut self,
        context: &ParserContext,
        remaining_request_timeout: Option<Duration>,
        cancel_handle: Option<&CancelHandle>,
    ) -> TdsResult<Tokens> {
        let cancellable_receive_token =
            CancelHandle::run_until_cancelled(cancel_handle, self.receive_token_internal(context));
        let token_result = match remaining_request_timeout.as_ref() {
            Some(remaining_request_timeout) => {
                match timeout(*remaining_request_timeout, cancellable_receive_token).await {
                    Ok(result) => result,
                    Err(elapsed) => Err(TimeoutError(TimeoutErrorType::Elapsed(elapsed))),
                }
            }
            None => cancellable_receive_token.await,
        };

        match &token_result {
            Ok(_) => {}
            Err(err) => match err {
                OperationCancelledError(_) | TimeoutError(_) => {
                    self.cancel_read_stream_and_wait().await?;
                }
                _ => {}
            },
        }
        token_result
    }
}
#[cfg(not(fuzzing))]
pub(crate) trait TokenParserRegistry: Send + Sync {
    fn has_parser(&self, token_type: &TokenType) -> bool;
    fn get_parser(&self, token_type: &TokenType) -> Option<&TokenParsers>;
}

#[cfg(fuzzing)]
pub trait TokenParserRegistry: Send + Sync {
    fn has_parser(&self, token_type: &TokenType) -> bool;
    fn get_parser(&self, token_type: &TokenType) -> Option<&TokenParsers>;
}

#[cfg(not(fuzzing))]
pub(crate) struct GenericTokenParserRegistry {
    parsers: HashMap<TokenType, TokenParsers>,
}

#[cfg(fuzzing)]
pub struct GenericTokenParserRegistry {
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
        internal_registry.insert(
            TokenType::NbcRow,
            TokenParsers::from(NbcRowTokenParser::default()),
        );
        internal_registry.insert(
            TokenType::ReturnValue,
            TokenParsers::from(ReturnValueTokenParser::default()),
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
    NbcRow(NbcRowTokenParser<GenericDecoder>),
    ReturnValue(ReturnValueTokenParser<GenericDecoder>),
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
    ReturnStatusTokenParser => ReturnStatus,
    NbcRowTokenParser<GenericDecoder> => NbcRow,
    ReturnValueTokenParser<GenericDecoder> => ReturnValue
);
