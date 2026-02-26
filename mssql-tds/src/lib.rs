// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#![allow(dead_code)]
pub mod connection;
pub mod connection_provider;
pub mod core;
pub mod datatypes;
pub mod error;
pub mod handler;
pub mod io;
pub mod message;
pub mod query;
pub mod security;
pub mod sql_identifier;
pub mod ssrp;
pub mod token;

// Expose internal APIs for fuzzing
#[cfg(fuzzing)]
pub mod fuzz_support {
    // Re-export types that are needed for fuzzing
    // These are pub(crate) in the main crate but exposed here for fuzz targets
    pub use crate::connection::tds_client::TdsClient;
    pub use crate::connection_provider::tds_connection_provider::TdsConnectionProvider;
    pub use crate::io::packet_reader::TdsPacketReader;
    pub use crate::io::token_stream::{
        GenericTokenParserRegistry, ParserContext, TdsTokenStreamReader, TokenParserRegistry,
        TokenStreamReader,
    };
    pub use crate::token::parsers::common::TokenParser;
    pub use crate::token::parsers::{
        FuzzDoneTokenParser as DoneTokenParser, FuzzEnvChangeTokenParser as EnvChangeTokenParser,
    };
    pub use crate::token::tokens::{SqlCollation, Tokens};

    // Import types we need internally
    use crate::connection::transport::tds_transport::TdsTransport;
    use crate::core::NegotiatedEncryptionSetting;
    use crate::handler::handler_factory::{NegotiatedSettings, SessionSettings};
    use crate::io::reader_writer::{NetworkReader, NetworkReaderWriter, NetworkWriter};

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
        encryption_setting: NegotiatedEncryptionSetting,
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
                encryption_setting: NegotiatedEncryptionSetting::NoEncryption,
            }
        }
    }

    #[async_trait]
    impl NetworkReader for MockTransport {
        async fn receive(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
            // For fuzzing, we don't actually receive data
            // Just fill with zeros and return the buffer length
            buffer.fill(0);
            Ok(buffer.len())
        }

        fn packet_size(&self) -> u32 {
            self.packet_size
        }
    }

    #[async_trait]
    impl crate::connection::transport::network_transport::TransportSslHandler for MockTransport {
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
    impl NetworkWriter for MockTransport {
        async fn send(&mut self, data: &[u8]) -> TdsResult<()> {
            self.mock_writer.send(data).await
        }

        fn packet_size(&self) -> u32 {
            self.packet_size
        }

        fn get_encryption_setting(&self) -> NegotiatedEncryptionSetting {
            self.encryption_setting
        }
    }

    #[async_trait]
    impl NetworkReaderWriter for MockTransport {
        fn notify_encryption_setting_change(&mut self, setting: NegotiatedEncryptionSetting) {
            self.encryption_setting = setting;
        }

        fn notify_session_setting_change(&mut self, _settings: &SessionSettings) {
            // No-op for mock transport
        }

        fn as_writer(&mut self) -> &mut dyn NetworkWriter {
            self
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

        async fn receive_row_into(
            &mut self,
            context: &ParserContext,
            remaining_request_timeout: Option<Duration>,
            cancel_handle: Option<&CancelHandle>,
            writer: &mut (dyn datatypes::row_writer::RowWriter + Send),
        ) -> TdsResult<io::token_stream::RowReadResult> {
            self.token_stream_reader
                .receive_row_into(context, remaining_request_timeout, cancel_handle, writer)
                .await
        }
    }

    #[async_trait]
    impl TdsTransport for MockTransport {
        fn as_writer(&mut self) -> &mut dyn NetworkWriter {
            self
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

        async fn send_attention_with_timeout(
            &mut self,
            _timeout: std::time::Duration,
        ) -> TdsResult<bool> {
            // For mock transport, just return success (ACK received)
            Ok(true)
        }
    }

    // Implement TdsPacketReader by delegating to the inner packet reader
    #[async_trait]
    impl TdsPacketReader for MockTransport {
        async fn read_byte(&mut self) -> TdsResult<u8> {
            self.token_stream_reader.packet_reader.read_byte().await
        }

        async fn read_int16_big_endian(&mut self) -> TdsResult<i16> {
            self.token_stream_reader
                .packet_reader
                .read_int16_big_endian()
                .await
        }

        async fn read_int32_big_endian(&mut self) -> TdsResult<i32> {
            self.token_stream_reader
                .packet_reader
                .read_int32_big_endian()
                .await
        }

        async fn read_int64_big_endian(&mut self) -> TdsResult<i64> {
            self.token_stream_reader
                .packet_reader
                .read_int64_big_endian()
                .await
        }

        async fn read_uint40(&mut self) -> TdsResult<u64> {
            self.token_stream_reader.packet_reader.read_uint40().await
        }

        async fn read_float32(&mut self) -> TdsResult<f32> {
            self.token_stream_reader.packet_reader.read_float32().await
        }

        async fn read_float64(&mut self) -> TdsResult<f64> {
            self.token_stream_reader.packet_reader.read_float64().await
        }

        async fn read_int16(&mut self) -> TdsResult<i16> {
            self.token_stream_reader.packet_reader.read_int16().await
        }

        async fn read_uint16(&mut self) -> TdsResult<u16> {
            self.token_stream_reader.packet_reader.read_uint16().await
        }

        async fn read_int24(&mut self) -> TdsResult<i32> {
            self.token_stream_reader.packet_reader.read_int24().await
        }

        async fn read_uint24(&mut self) -> TdsResult<u32> {
            self.token_stream_reader.packet_reader.read_uint24().await
        }

        async fn read_int32(&mut self) -> TdsResult<i32> {
            self.token_stream_reader.packet_reader.read_int32().await
        }

        async fn read_uint32(&mut self) -> TdsResult<u32> {
            self.token_stream_reader.packet_reader.read_uint32().await
        }

        async fn read_int64(&mut self) -> TdsResult<i64> {
            self.token_stream_reader.packet_reader.read_int64().await
        }

        async fn read_uint64(&mut self) -> TdsResult<u64> {
            self.token_stream_reader.packet_reader.read_uint64().await
        }

        async fn read_bytes(&mut self, buffer: &mut [u8]) -> TdsResult<usize> {
            self.token_stream_reader
                .packet_reader
                .read_bytes(buffer)
                .await
        }

        async fn read_u8_varbyte(&mut self) -> TdsResult<Vec<u8>> {
            self.token_stream_reader
                .packet_reader
                .read_u8_varbyte()
                .await
        }

        async fn read_u16_varbyte(&mut self) -> TdsResult<Vec<u8>> {
            self.token_stream_reader
                .packet_reader
                .read_u16_varbyte()
                .await
        }

        async fn read_varchar_u16_length(&mut self) -> TdsResult<Option<String>> {
            self.token_stream_reader
                .packet_reader
                .read_varchar_u16_length()
                .await
        }

        async fn read_varchar_u8_length(&mut self) -> TdsResult<String> {
            self.token_stream_reader
                .packet_reader
                .read_varchar_u8_length()
                .await
        }

        async fn read_varchar_byte_len(&mut self) -> TdsResult<String> {
            self.token_stream_reader
                .packet_reader
                .read_varchar_byte_len()
                .await
        }

        async fn read_unicode(&mut self, string_length: usize) -> TdsResult<String> {
            self.token_stream_reader
                .packet_reader
                .read_unicode(string_length)
                .await
        }

        async fn read_unicode_with_byte_length(&mut self, byte_length: usize) -> TdsResult<String> {
            self.token_stream_reader
                .packet_reader
                .read_unicode_with_byte_length(byte_length)
                .await
        }

        async fn skip_bytes(&mut self, skip_count: usize) -> TdsResult<()> {
            self.token_stream_reader
                .packet_reader
                .skip_bytes(skip_count)
                .await
        }

        async fn cancel_read_stream(&mut self) -> TdsResult<()> {
            self.token_stream_reader
                .packet_reader
                .cancel_read_stream()
                .await
        }

        fn reset_reader(&mut self) {
            self.token_stream_reader.packet_reader.reset_reader();
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
