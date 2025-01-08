use crate::connection::transport::network_transport::NetworkTransport;
use crate::core::EncryptionSetting;
use crate::message::messages::{PacketType, Request, Response, TypedResponse};
use crate::read_write::writer::{NetworkReader, NetworkWriter};
use crate::{
    core::{SQLServerVersion, Version},
    read_write::packet_writer::PacketWriter,
};
use async_trait::async_trait;
use std::thread::ThreadId;
use uuid::Uuid;

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

pub struct PreloginResponseModel {
    pub encryption: EncryptionType,
    pub federated_auth_supported: bool,
    pub dbinstance_valid: Option<bool>,
    pub mars_enabled: Option<bool>,
    pub server_version: Version,
    pub sql_server_version: SQLServerVersion,
}

pub struct PreloginRequest<'a> {
    pub packet_writer: &'a PacketWriter<'a>,
    pub model: &'a PreloginRequestModel,
}

impl<'a> PreloginRequest<'a> {
    fn serialize(&self, _transport: &NetworkTransport) {}
}

#[async_trait(?Send)]
impl<'a> Request for PreloginRequest<'a> {
    fn packet_type(&self) -> PacketType {
        todo!()
    }

    fn create_packet_writer(&self, _writer: &dyn NetworkWriter) -> PacketWriter {
        todo!()
    }

    async fn serialize(&self, _transport: &dyn NetworkWriter) {
        todo!()
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
            server_version: Version {},
            sql_server_version: SQLServerVersion::SqlServerNotsupported,
        }
    }
}
