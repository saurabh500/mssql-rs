use crate::core::EncryptionSetting;
use crate::message::messages::{PacketType, Request, Response, TypedResponse};
use crate::read_write::writer::{NetworkReader, NetworkWriter};
use crate::{
    core::{SQLServerVersion, Version},
    read_write::packet_writer::PacketWriter,
};
use async_trait::async_trait;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use std::thread;
use std::thread::ThreadId;
use uuid::Uuid;

#[derive(PartialEq)]
pub enum EncryptionType {
    Off = 0x00,
    On = 0x01,
    NotSupported = 0x02,
    Required = 0x03,
}

pub enum FederationType {
    Off = 0x00,
    On = 0x01,
}

pub enum MarsType {
    Off = 0x00,
    On = 0x01,
}

pub enum OptionType {
    Version = 0x00,
    Encryption = 0x01,
    InstOpt = 0x02,
    ThreadId = 0x03,
    Mars = 0x04,
    TraceId = 0x05,
    FedAuthRequired = 0x06,
    Nounce = 0x07,
    Terminator = 0xff,
}

pub struct PreloginRequestModel {
    pub sdk_version: Version,
    pub connection_id: Uuid,
    pub activity_id: Uuid,
    pub activity_sequence_number: i32,
    pub mars_enabled: bool,
    pub thread_id: ThreadId,
    pub encryption_setting: EncryptionSetting,
    pub database_instance: String,
    pub fed_auth: bool,
}

static REQUEST_COUNT: AtomicI32 = AtomicI32::new(0);
impl PreloginRequestModel {
    pub fn new(
        connection_id: Uuid,
        mars_enabled: Option<bool>,
        encryption_setting: Option<EncryptionSetting>,
        database_instance: Option<&str>,
    ) -> Self {
        let mars_enabled = mars_enabled.unwrap_or(false);
        let encryption_setting = encryption_setting.unwrap_or(EncryptionSetting::Strict);
        let database_instance = database_instance.unwrap_or("MSSQLServer").to_string();
        PreloginRequestModel {
            sdk_version: Version {
                major: 0,
                minor: 0,
                build: 0,
                revision: 1,
            },
            connection_id,
            activity_id: Uuid::new_v4(),
            activity_sequence_number: REQUEST_COUNT.fetch_add(1, Relaxed),
            mars_enabled,
            thread_id: thread::current().id(),
            encryption_setting,
            database_instance: database_instance.to_string(),
            fed_auth: true, // This is forced to true to allow detecting if federated auth is supported.
        }
    }
}

pub struct PreloginResponseModel {
    pub encryption: EncryptionType,
    pub federated_auth_supported: bool,
    pub dbinstance_valid: Option<bool>,
    pub mars_enabled: Option<bool>,
    pub server_version: Version,
    pub sql_server_version: SQLServerVersion,
}

pub struct PreloginRequest<'a> {
    pub model: &'a PreloginRequestModel,
}

#[async_trait(?Send)]
impl<'a> Request<'a> for PreloginRequest<'a> {
    fn packet_type(&self) -> PacketType {
        PacketType::PreLogin
    }

    fn create_packet_writer(&self, writer: &'a mut dyn NetworkWriter) -> PacketWriter<'a> {
        PacketWriter::new(self.packet_type(), writer)
    }

    async fn serialize(&self, writer: &mut dyn NetworkWriter) {
        let mut packet_writer = self.create_packet_writer(writer);
        let mut serializer = Serializer::new(self.model, &mut packet_writer);
        serializer.serialize().await
    }
}

pub struct PreloginResponse {
    pub model: PreloginResponseModel,
}

#[async_trait(?Send)]
impl Response for PreloginResponse {
    async fn deserialize(&self, _transport: &dyn NetworkReader) {
        panic!()
    }
}

#[async_trait(?Send)]
impl TypedResponse<PreloginResponseModel> for PreloginResponse {
    async fn deserialize(&self, _transport: &dyn NetworkReader) -> PreloginResponseModel {
        PreloginResponseModel {
            encryption: EncryptionType::Off,
            federated_auth_supported: false,
            dbinstance_valid: None,
            mars_enabled: None,
            server_version: Version::new(0, 0, 0, 0),
            sql_server_version: SQLServerVersion::SqlServerNotsupported,
        }
    }
}

struct Serializer<'a, 'n> {
    model: &'a PreloginRequestModel,
    payload_writer: &'a mut PacketWriter<'n>,
    content_next_offset: u32,
}

impl<'a, 'n> Serializer<'a, 'n> {
    pub fn new(
        model: &'a PreloginRequestModel,
        payload_writer: &'a mut PacketWriter<'n>,
    ) -> Serializer<'a, 'n> {
        Serializer {
            model,
            payload_writer,
            // In total there are 7 DataModel (Version, Encryption, Instance, ThreadID, MARS, FEDAUTH, TRACEID) + Terminator
            // Each option has a size of 5 bytes (1 byte for TokenType, 2 bytes for offset, 2 bytes for length)
            // so content section will start at 35 + 1 (terminator byte) = 36
            // The content section hence starts from position 45.but the offset is 0 based so we start from 44
            content_next_offset: 36,
        }
    }
    async fn serialize(&mut self) {
        self.write_version().await;
        self.write_encryption().await;
        self.write_inst_opt().await;
        self.write_thread_id().await;
        self.write_mars().await;
        self.write_trace_id().await;
        self.write_fed_auth_required().await;
        self.write_terminator().await;

        self.payload_writer.finalize().await;
    }

    async fn write_version(&mut self) {
        self.write_option_metadata(OptionType::Version, 6).await;

        self.payload_writer
            .write_byte_async(self.model.sdk_version.major)
            .await;
        self.payload_writer
            .write_byte_async(self.model.sdk_version.minor)
            .await;
        self.payload_writer
            .write_i16_be_async(self.model.sdk_version.build as i16)
            .await;
        self.payload_writer
            .write_i16_be_async(self.model.sdk_version.revision as i16)
            .await;
    }

    async fn write_encryption(&mut self) {
        self.write_option_metadata(OptionType::Encryption, 1).await;

        self.payload_writer
            .write_byte_async(self.model.encryption_setting as u8)
            .await;
    }

    async fn write_inst_opt(&mut self) {
        let instance_bytes = self.model.database_instance.as_bytes();
        self.write_option_metadata(OptionType::InstOpt, (instance_bytes.len() + 1) as u16)
            .await;

        self.payload_writer.write_async(instance_bytes).await;
        self.payload_writer.write_byte_async(0).await;
    }

    async fn write_thread_id(&mut self) {
        self.write_option_metadata(OptionType::ThreadId, 4).await;

        // Revisit because Rust's ThreadId is not the same numerically as the OS-level thread id.
        self.payload_writer.write_i32_be_async(0).await;
    }

    async fn write_mars(&mut self) {
        self.write_option_metadata(OptionType::Mars, 1).await;

        self.payload_writer
            .write_byte_async(match self.model.mars_enabled {
                true => MarsType::On as u8,
                false => MarsType::Off as u8,
            })
            .await;
    }

    async fn write_trace_id(&mut self) {
        let length = (16 + 16 + 4) as u16; // two GUIDs and one 32-bit integer.
        self.write_option_metadata(OptionType::TraceId, length)
            .await;

        let activity_id_bytes = self.model.activity_id.as_bytes();
        let connection_id_bytes = self.model.connection_id.as_bytes();
        self.payload_writer.write_async(activity_id_bytes).await;
        self.payload_writer.write_async(connection_id_bytes).await;
        self.payload_writer
            .write_i32_async(self.model.activity_sequence_number)
            .await
    }

    async fn write_fed_auth_required(&mut self) {
        self.write_option_metadata(OptionType::FedAuthRequired, 1)
            .await;

        self.payload_writer
            .write_byte_async(match self.model.fed_auth {
                true => FederationType::On as u8,
                false => FederationType::Off as u8,
            })
            .await;
    }

    async fn write_terminator(&mut self) {
        self.write_option_metadata(OptionType::Terminator, 1).await;
        self.content_next_offset += 1;
    }

    async fn write_option_metadata(&mut self, option: OptionType, length: u16) {
        self.payload_writer.write_byte_async(option as u8).await;
        self.payload_writer
            .write_i16_be_async(self.content_next_offset as i16)
            .await;
        self.payload_writer.write_i16_be_async(length as i16).await;
        self.content_next_offset += length as u32;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::core::EncryptionSetting;
    use crate::message::messages::PacketType;
    use crate::message::prelogin::{OptionType, PreloginRequestModel, Serializer};
    use crate::read_write::packet_writer::tests::MockNetworkWriter;
    use crate::read_write::packet_writer::PacketWriter;
    use byteorder::{BigEndian, ReadBytesExt};
    use futures::executor::block_on;
    use uuid::Uuid;

    #[test]
    fn test_default_model() {
        let model = PreloginRequestModel::new(
            Uuid::new_v4(),
            Option::from(false),
            Option::from(EncryptionSetting::Required),
            Option::from("MSSQLServer"),
        );
        let mut mock = MockNetworkWriter { size: 1024 };
        let mut packet_writer = PacketWriter::new(PacketType::PreLogin, &mut mock);

        let mut serializer = Serializer::new(&model, &mut packet_writer);
        block_on(serializer.serialize());

        let mut cursor = packet_writer.get_payload();

        // Just validate that the version was serialized correctly to start with.

        // Validate the header.
        assert_eq!(cursor.read_u8().unwrap(), OptionType::Version as u8);
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 36); // Initial content_next_offset.
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 6);

        // Validate the version (0.0.0.1).
        assert_eq!(cursor.read_u8().unwrap(), 0);
        assert_eq!(cursor.read_u8().unwrap(), 0);
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 0);
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 1);
    }
}
