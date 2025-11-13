// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use async_trait::async_trait;

use crate::core::TdsResult;
use crate::io::packet_writer::{PacketWriter, TdsPacketWriter};

use crate::message::login::{Feature, FeatureExtension};

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct JsonFeature {
    acknowledged: bool,
}

impl JsonFeature {
    pub const VERSION: u8 = 1;
}

#[async_trait]
impl Feature for JsonFeature {
    fn feature_identifier(&self) -> FeatureExtension {
        FeatureExtension::Json
    }

    fn is_requested(&self) -> bool {
        true
    }

    fn data_length(&self) -> i32 {
        // 1 byte for feature identifier, 4 bytes for length, 1 byte for version
        (size_of::<u8>() + size_of::<i32>() + size_of::<u8>()) as i32
    }

    async fn serialize(&self, packet_writer: &mut PacketWriter) -> TdsResult<()> {
        packet_writer
            .write_byte_async(self.feature_identifier().as_u8())
            .await?;
        packet_writer.write_i32_async(1).await?;
        packet_writer.write_byte_async(Self::VERSION).await?;
        Ok(())
    }

    fn deserialize(&self, data: &[u8]) {
        if data.len() != 1 {
            // Log warning but don't panic - server may send unexpected data
            tracing::warn!(
                "Invalid data length {} for JSON feature, expected 1 byte",
                data.len()
            );
            return;
        }
        let server_supported_version = data[0];

        // Validate that the server supports the expected version or is 0 (indicating no support)
        if server_supported_version != Self::VERSION && server_supported_version != 0 {
            tracing::warn!(
                "Unsupported JSON feature version: {}, expected {} or 0",
                server_supported_version,
                Self::VERSION
            );
        }
    }

    fn is_acknowledged(&self) -> bool {
        self.acknowledged
    }

    fn set_acknowledged(&mut self, acknowledged: bool) {
        self.acknowledged = acknowledged;
    }

    fn clone_box(&self) -> Box<dyn Feature> {
        Box::new(*self)
    }
}
