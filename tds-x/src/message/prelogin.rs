use crate::core::EncryptionSetting;
use crate::message::messages::{PacketType, Request, TypedResponse};
use crate::read_write::packet_reader::PacketReader;
use crate::read_write::reader_writer::{NetworkReader, NetworkWriter};
use crate::{
    core::{SQLServerVersion, Version},
    read_write::packet_writer::PacketWriter,
};
use async_trait::async_trait;
use std::collections::VecDeque;
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

impl std::convert::From<u8> for EncryptionType {
    fn from(v: u8) -> Self {
        match v {
            0x00 => EncryptionType::Off,
            0x01 => EncryptionType::On,
            0x02 => EncryptionType::NotSupported,
            0x03 => EncryptionType::Required,
            _ => panic!("unknown encryption type {}", v),
        }
    }
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
    Nonce = 0x07,
    Terminator = 0xff,
}

impl std::convert::From<u8> for OptionType {
    fn from(v: u8) -> Self {
        match v {
            0x00 => OptionType::Version,
            0x01 => OptionType::Encryption,
            0x02 => OptionType::InstOpt,
            0x03 => OptionType::ThreadId,
            0x04 => OptionType::Mars,
            0x05 => OptionType::TraceId,
            0x06 => OptionType::FedAuthRequired,
            0x07 => OptionType::Nonce,
            0xff => OptionType::Terminator,
            _ => {
                panic!("invalid option type {:x}", v);
            }
        }
    }
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

impl PreloginResponseModel {
    fn new() -> Self {
        PreloginResponseModel {
            server_version: Version::new(0, 0, 0, 0),
            sql_server_version: SQLServerVersion::SqlServerNotsupported,
            encryption: EncryptionType::Off,
            dbinstance_valid: Option::from(false),
            mars_enabled: Option::from(false),
            federated_auth_supported: false,
        }
    }
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

pub struct PreloginResponse {}

#[async_trait(?Send)]
impl TypedResponse<PreloginResponseModel> for PreloginResponse {
    async fn deserialize(&self, reader: &mut dyn NetworkReader) -> PreloginResponseModel {
        let mut packet_reader = PacketReader::new(reader);
        struct OptionContext {
            option: OptionType,
            length: usize,
        }
        let mut contexts = VecDeque::new();

        loop {
            let token = packet_reader.read_byte().await.unwrap();
            if token == 0xFF {
                break;
            }

            let option = OptionType::from(token);
            let _ = packet_reader.read_int16_big_endian().await; // offset.
            let length = packet_reader.read_int16_big_endian().await.unwrap();

            // Record the length and option type for later deserialization of the value.
            contexts.push_back(OptionContext {
                option,
                length: length as usize,
            });
        }

        let mut result = PreloginResponseModel::new();
        for context in contexts {
            if context.length == 0 {
                continue;
            }

            match context.option {
                OptionType::Version => {
                    let major = packet_reader.read_byte().await.unwrap();
                    let minor = packet_reader.read_byte().await;
                    let build = packet_reader.read_int16_big_endian().await;
                    let revision = packet_reader.read_int16_big_endian().await;
                    result.server_version = Version::new(
                        major,
                        minor.unwrap(),
                        build.unwrap() as u16,
                        revision.unwrap() as u16,
                    );
                    result.sql_server_version = SQLServerVersion::from(major);
                }
                OptionType::Encryption => {
                    result.encryption =
                        EncryptionType::from(packet_reader.read_byte().await.unwrap());
                    // encryption type.
                }
                OptionType::InstOpt => {
                    result.dbinstance_valid =
                        Option::from(packet_reader.read_byte().await.unwrap() == 0);
                }
                OptionType::Mars => {
                    result.mars_enabled =
                        Option::from(packet_reader.read_byte().await.unwrap() == 1);
                }
                OptionType::FedAuthRequired => {
                    result.federated_auth_supported = packet_reader.read_byte().await.unwrap() == 1;
                }
                _ => {
                    // Todo: Logging that this is being skipped.
                    packet_reader.skip_forward(context.length).await.unwrap();
                }
            };
        }
        result
    }
}

struct Serializer<'a, 'n> {
    model: &'a PreloginRequestModel,
    payload_writer: &'a mut PacketWriter<'n>,
    content_next_offset: u32,
    instance_bytes: &'a [u8],
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
            instance_bytes: model.database_instance.as_bytes(),
        }
    }
    async fn serialize(&mut self) {
        // Write headers then terminate the header table.
        self.write_headers().await;
        self.write_terminator().await;

        // Write data values. Must be the same order was what's in write_headers.
        self.write_version().await;
        self.write_encryption().await;
        self.write_inst_opt().await;
        self.write_thread_id().await;
        self.write_mars().await;
        self.write_trace_id().await;
        self.write_fed_auth_required().await;

        self.payload_writer.finalize().await;
    }

    async fn write_headers(&mut self) {
        self.write_option_metadata(OptionType::Version, 6).await;
        self.write_option_metadata(OptionType::Encryption, 1).await;
        self.write_option_metadata(OptionType::InstOpt, (self.instance_bytes.len() + 1) as u16)
            .await;
        self.write_option_metadata(OptionType::ThreadId, 4).await;
        self.write_option_metadata(OptionType::Mars, 1).await;

        let length = (16 + 16 + 4) as u16; // two GUIDs and one 32-bit integer.
        self.write_option_metadata(OptionType::TraceId, length)
            .await;
        self.write_option_metadata(OptionType::FedAuthRequired, 1)
            .await;
    }

    async fn write_version(&mut self) {
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
        match self.model.encryption_setting {
            EncryptionSetting::Optional => {
                self.payload_writer
                    .write_byte_async(EncryptionType::On as u8)
                    .await;
            }
            EncryptionSetting::Required => {
                self.payload_writer
                    .write_byte_async(EncryptionType::Required as u8)
                    .await;
            }
            _ => {
                self.payload_writer
                    .write_byte_async(EncryptionType::NotSupported as u8)
                    .await;
            }
        }
    }

    async fn write_inst_opt(&mut self) {
        self.payload_writer.write_async(self.instance_bytes).await;
        self.payload_writer.write_byte_async(0).await;
    }

    async fn write_thread_id(&mut self) {
        // Revisit because Rust's ThreadId is not the same numerically as the OS-level thread id.
        self.payload_writer.write_i32_be_async(0).await;
    }

    async fn write_mars(&mut self) {
        self.payload_writer
            .write_byte_async(match self.model.mars_enabled {
                true => MarsType::On as u8,
                false => MarsType::Off as u8,
            })
            .await;
    }

    async fn write_trace_id(&mut self) {
        let activity_id_bytes = self.model.activity_id.as_bytes();
        let connection_id_bytes = self.model.connection_id.as_bytes();
        self.payload_writer.write_async(activity_id_bytes).await;
        self.payload_writer.write_async(connection_id_bytes).await;
        self.payload_writer
            .write_i32_async(self.model.activity_sequence_number)
            .await
    }

    async fn write_fed_auth_required(&mut self) {
        self.payload_writer
            .write_byte_async(match self.model.fed_auth {
                true => FederationType::On as u8,
                false => FederationType::Off as u8,
            })
            .await;
    }

    async fn write_terminator(&mut self) {
        self.payload_writer
            .write_byte_async(OptionType::Terminator as u8)
            .await;
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
    use crate::core::{EncryptionSetting, SQLServerVersion, Version};
    use crate::message::messages::{PacketType, TypedResponse};
    use crate::message::prelogin::{
        OptionType, PreloginRequestModel, PreloginResponse, Serializer,
    };
    use crate::read_write::packet_reader::tests::MockNetworkReader;
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

        // Validate a few headers.
        assert_eq!(cursor.read_u8().unwrap(), OptionType::Version as u8);
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 36); // Initial content_next_offset.
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 6);

        assert_eq!(cursor.read_u8().unwrap(), OptionType::Encryption as u8);
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 42); // Add the length of the previous header to the content_next_offset.
        assert_eq!(cursor.read_i16::<BigEndian>().unwrap(), 1);
    }

    #[test]
    fn test_deserialize_model() {
        let mut mock = MockNetworkWriter { size: 1024 };
        let mut packet_writer = PacketWriter::new(PacketType::PreLogin, &mut mock);
        // Write headers.
        block_on(packet_writer.write_byte_async(OptionType::Version as u8));
        block_on(packet_writer.write_i16_be_async(0)); // offset - unused
        block_on(packet_writer.write_i16_be_async(6)); // length.

        block_on(packet_writer.write_byte_async(OptionType::Mars as u8));
        block_on(packet_writer.write_i16_be_async(0)); // offset - unused
        block_on(packet_writer.write_i16_be_async(6)); // length.

        // Write terminator.
        block_on(packet_writer.write_byte_async(0xFF));

        // Write values
        // Version 15.2.3.4
        block_on(packet_writer.write_byte_async(15));
        block_on(packet_writer.write_byte_async(2));
        block_on(packet_writer.write_i16_be_async(3));
        block_on(packet_writer.write_i16_be_async(4));

        // Mars - enabled.
        block_on(packet_writer.write_byte_async(1));

        let cursor = packet_writer.get_payload();

        // Move the contents written to a reader.
        let mut mock_reader = MockNetworkReader {
            data: cursor.into_inner(),
            position: 0,
        };

        let response = PreloginResponse {};
        let response_model = block_on(response.deserialize(&mut mock_reader));

        // Compare the guid, which is auto-generated.
        assert_eq!(response_model.mars_enabled, Option::from(true));
        assert_eq!(
            response_model.sql_server_version,
            SQLServerVersion::SqlServer2019
        );
        assert_eq!(response_model.server_version, Version::new(15, 2, 3, 4));
    }
}
