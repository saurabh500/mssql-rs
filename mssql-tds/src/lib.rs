// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(dead_code)]
pub mod connection;
pub mod connection_provider;
pub mod core;
pub mod datatypes;
pub mod error;
pub mod handler;
pub mod message;
pub mod query;
pub mod read_write;
pub mod token;

// Expose internal APIs for fuzzing
#[cfg(fuzzing)]
pub mod fuzz_support {
    // Re-export types that are needed for fuzzing
    // These are pub(crate) in the main crate but exposed here for fuzz targets
    pub use crate::connection::tds_client::TdsClient;
    pub use crate::read_write::packet_reader::TdsPacketReader;
    pub use crate::read_write::token_stream::{
        GenericTokenParserRegistry, ParserContext, TdsTokenStreamReader, TokenParserRegistry,
        TokenStreamReader,
    };
    pub use crate::token::parsers::{DoneTokenParser, EnvChangeTokenParser, TokenParser};
    pub use crate::token::tokens::{SqlCollation, Tokens};

    // Import types we need internally
    use crate::connection::transport::tds_transport::TdsTransport;
    use crate::core::NegotiatedEncryptionSetting;
    use crate::handler::handler_factory::NegotiatedSettings;
    use crate::read_write::reader_writer::NetworkWriter;

    use crate::core::{CancelHandle, TdsResult};
    use async_trait::async_trait;
    use std::time::Duration;

    /// MockWriter captures all writes without sending over network
    #[derive(Debug)]
    pub struct MockWriter {
        buffer: Vec<u8>,
        packet_size: u32,
    }

    impl MockWriter {
        pub fn new(packet_size: u32) -> Self {
            Self {
                buffer: Vec::new(),
                packet_size,
            }
        }

        #[allow(dead_code)]
        pub fn get_buffer(&self) -> &[u8] {
            &self.buffer
        }
    }

    #[async_trait]
    impl crate::connection::transport::network_transport::TransportSslHandler for MockWriter {
        async fn enable_ssl(&mut self) -> TdsResult<()> {
            // No-op for mock transport
            Ok(())
        }

        async fn disable_ssl(&mut self) -> TdsResult<()> {
            // No-op for mock transport
            Ok(())
        }
    }

    #[async_trait]
    impl NetworkWriter for MockWriter {
        async fn send(&mut self, data: &[u8]) -> TdsResult<()> {
            self.buffer.extend_from_slice(data);
            Ok(())
        }

        fn packet_size(&self) -> u32 {
            self.packet_size
        }

        fn get_encryption_setting(&self) -> NegotiatedEncryptionSetting {
            NegotiatedEncryptionSetting::NoEncryption
        }
    }

    /// MockTransport simulates a transport layer for fuzzing
    pub struct MockTransport {
        token_stream_reader:
            TokenStreamReader<Box<dyn TdsPacketReader + Send + Sync>, GenericTokenParserRegistry>,
        mock_writer: MockWriter,
        packet_size: u32,
    }

    impl std::fmt::Debug for MockTransport {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockTransport")
                .field("packet_size", &self.packet_size)
                .field("mock_writer", &self.mock_writer)
                .finish()
        }
    }

    impl MockTransport {
        pub fn new(
            packet_reader: Box<dyn TdsPacketReader + Send + Sync>,
            packet_size: u32,
        ) -> Self {
            let parser_registry = Box::new(GenericTokenParserRegistry::default());
            let token_stream_reader = TokenStreamReader::new(packet_reader, parser_registry);

            Self {
                token_stream_reader,
                mock_writer: MockWriter::new(packet_size),
                packet_size,
            }
        }
    }

    #[async_trait]
    impl TdsTokenStreamReader for MockTransport {
        async fn receive_token(
            &mut self,
            context: &ParserContext,
            remaining_request_timeout: Option<Duration>,
            cancel_handle: Option<&CancelHandle>,
        ) -> TdsResult<Tokens> {
            self.token_stream_reader
                .receive_token(context, remaining_request_timeout, cancel_handle)
                .await
        }
    }

    #[async_trait]
    impl TdsTransport for MockTransport {
        fn as_writer(&mut self) -> &mut dyn NetworkWriter {
            &mut self.mock_writer
        }

        fn reset_reader(&mut self) {
            self.token_stream_reader.packet_reader.reset_reader();
        }

        fn packet_size(&self) -> u32 {
            self.packet_size
        }

        async fn close_transport(&mut self) -> TdsResult<()> {
            // No-op for mock transport
            Ok(())
        }
    }

    /// Helper function to create test NegotiatedSettings
    /// Uses internal API for testing purposes only
    pub fn create_test_negotiated_settings() -> NegotiatedSettings {
        // Create a simple negotiated settings for fuzzing
        // We need to access internal structures, so we'll use an internal helper
        crate::handler::handler_factory::create_test_negotiated_settings_internal()
    }

    /// Helper function to create test ExecutionContext
    pub fn create_test_execution_context() -> crate::connection::execution_context::ExecutionContext
    {
        crate::connection::execution_context::ExecutionContext::new()
    }

    /// Helper function to create TdsClient for fuzzing
    pub fn create_fuzz_tds_client(
        packet_reader: Box<dyn TdsPacketReader + Send + Sync>,
        packet_size: u32,
    ) -> TdsClient {
        let mock_transport = MockTransport::new(packet_reader, packet_size);
        let negotiated_settings = create_test_negotiated_settings();
        let execution_context = create_test_execution_context();

        TdsClient::new(
            Box::new(mock_transport),
            negotiated_settings,
            execution_context,
        )
    }
}
