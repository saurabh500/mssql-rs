use crate::message::login_options::{
    LoginOptions, OptionFlags1, OptionFlags2, OptionFlags3, TdsVersion, TypeFlags,
};
use crate::message::messages::{PacketType, Request, Response, TdsError, TypedResponse};
use crate::read_write::packet_writer::PacketWriter;
use crate::read_write::writer::{NetworkReader, NetworkWriter};
use async_trait::async_trait;
use std::collections::HashMap;

pub struct SqlCollation {
    //TODO:
}

pub struct RoutingInfo {
    //TODO:
}

pub struct EnvChangeProperties {
    pub database_collation: SqlCollation,
    pub packet_size: i32,
    pub language: String,
    pub database: String,
    pub char_set: Option<String>,
    pub routing_information: RoutingInfo,
}

pub enum FeatureExtension {
    SRecovery = 0x01,
    FedAuth = 0x02,
    AlwaysEncrypted = 0x04,
    GlobalTransactions = 0x05,
    AzureSqlSupport = 0x08,
    DataClassification = 0x09,
    Utf8Support = 0x0A,
    SqlDnsCaching = 0x0B,
    Terminator = 0xFF,
}

#[async_trait]
pub trait Feature {
    fn feature_identifier(&self) -> FeatureExtension;
    fn is_requested(&self) -> bool;
    fn data_length(&self) -> i32;
    async fn serialize(&self, packet_writer: PacketWriter);
    fn deserialize(&self, data: &[u8]);
    fn is_acknowledged(&self) -> bool;
}

pub struct FeaturesRequest {
    pub features: HashMap<FeatureExtension, Box<dyn Feature>>,
}

impl FeaturesRequest {
    pub fn features(&self) -> Vec<&dyn Feature> {
        self.features
            .values()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn Feature>>()
    }

    pub fn is_acknowledged(&self, _feature_extension: FeatureExtension) -> Option<&dyn Feature> {
        todo!()
    }

    pub fn set_acknowledged(&mut self, _feature_extension: FeatureExtension, _data: &[u8]) {
        todo!()
    }

    pub fn get_requested_features(&self) -> Vec<&dyn Feature> {
        todo!()
    }

    pub fn get_acknowledged_features(&self) -> Vec<&dyn Feature> {
        todo!()
    }

    pub fn is_feature_acknowledged(&self, _feature_extension: FeatureExtension) -> bool {
        todo!()
    }
}

pub struct PhysicalAddress {
    // TODO:
}

pub struct LoginRequestModel {
    pub option_flags1: OptionFlags1,
    pub option_flags2: OptionFlags2,
    pub option_flags3: OptionFlags3,
    pub type_flags: TypeFlags,
    pub tds_version: TdsVersion,
    pub user_input: LoginOptions,
    pub features_request: FeaturesRequest,
    pub client_prog_ver: i32,
    pub client_process_id: i32,
    pub connection_id_deprecated: i32,
    pub client_time_zone_deprecated: i32,
    pub client_lcid_deprecated: i32,
    pub client_id: PhysicalAddress,
}

pub struct LoginResponseModel {
    pub change_properties: EnvChangeProperties,
    pub features: FeaturesRequest,
    pub tds_error: Option<TdsError>,
    pub login_ack_token: i32,
}

pub struct LoginRequest<'a> {
    pub packet_generator: &'a PacketWriter<'a>,
    pub model: LoginRequestModel,
}

#[async_trait(?Send)]
impl<'a> Request for LoginRequest<'a> {
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

pub struct LoginResponse {
    pub model: LoginResponseModel,
}

#[async_trait(?Send)]
impl Response for LoginResponse {
    async fn deserialize(&self, _transport: &dyn NetworkReader) {
        todo!()
    }
}

#[async_trait(?Send)]
impl TypedResponse<LoginResponseModel> for LoginResponse {
    async fn deserialize(&self, _transport: &dyn NetworkReader) -> LoginResponseModel {
        todo!()
    }
}
