use crate::connection::client_context::{ClientContext, TdsAuthenticationMethod, TransportContext};
use crate::message::features::jsonfeature::JsonFeature;
use crate::message::login_options::{
    OptionFlags1, OptionFlags2, OptionFlags3, OptionsValue, TdsVersion, TypeFlags,
};
use crate::message::messages::{PacketType, Request, TdsError};
use crate::read_write::packet_writer::PacketWriter;
use crate::read_write::reader_writer::NetworkReader;
use crate::token::fed_auth_info::FedAuthInfoToken;
use crate::token::login_ack::LoginAckToken;
use crate::token::tokens::{
    EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType, SqlCollation, Token, Tokens,
};
use async_trait::async_trait;
use std::collections::HashMap;

use super::features::fedauth::FedAuthFeature;
use super::features::utf8::Utf8Feature;
use crate::core::TdsResult;
use crate::read_write::token_stream::{
    GenericTokenParserRegistry, ParserContext, TokenStreamReader,
};
use tracing::{debug, event, info, trace, Level};

pub(crate) const FIXED_LOGIN_RECORD_LENGTH: i32 = 94;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RoutingInfo {
    pub protocol: u8,
    pub port: u16,
    pub server: String,
}

#[derive(Default)]
pub(crate) struct EnvChangeProperties {
    pub database_collation: Option<SqlCollation>,
    pub packet_size: i32,
    pub language: Option<String>,
    pub database: Option<String>,
    pub char_set: Option<String>,
    pub routing_information: Option<RoutingInfo>,
}

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone)]
#[repr(u8)]
pub(crate) enum FeatureExtension {
    SRecovery = 0x01,
    FedAuth = 0x02,
    AlwaysEncrypted = 0x04,
    GlobalTransactions = 0x05,
    AzureSqlSupport = 0x08,
    DataClassification = 0x09,
    Utf8Support = 0x0A,
    SqlDnsCaching = 0x0B,
    Json = 0x0D,
    Terminator = 0xFF,
}

impl From<u8> for FeatureExtension {
    fn from(value: u8) -> Self {
        match value {
            0x01 => FeatureExtension::SRecovery,
            0x02 => FeatureExtension::FedAuth,
            0x04 => FeatureExtension::AlwaysEncrypted,
            0x05 => FeatureExtension::GlobalTransactions,
            0x08 => FeatureExtension::AzureSqlSupport,
            0x09 => FeatureExtension::DataClassification,
            0x0A => FeatureExtension::Utf8Support,
            0x0B => FeatureExtension::SqlDnsCaching,
            0x0D => FeatureExtension::Json,
            0xFF => FeatureExtension::Terminator,
            _ => unreachable!("Invalid Feature Extension."),
        }
    }
}

#[async_trait]
pub(crate) trait Feature: Send + Sync {
    fn feature_identifier(&self) -> FeatureExtension;
    fn is_requested(&self) -> bool;
    fn data_length(&self) -> i32;
    async fn serialize(&self, packet_writer: &mut PacketWriter) -> TdsResult<()>;
    fn deserialize(&self, data: &[u8]);
    fn is_acknowledged(&self) -> bool;
    fn set_acknowledged(&mut self, _acknowledged: bool);
    fn clone_box(&self) -> Box<dyn Feature>;
}

// Implement Clone for Box<dyn Feature>
impl Clone for Box<dyn Feature> {
    fn clone(&self) -> Box<dyn Feature> {
        self.clone_box()
    }
}

// Implement Clone for FeaturesRequest
impl Clone for FeaturesRequest {
    fn clone(&self) -> Self {
        FeaturesRequest {
            features: self.features.iter().map(|(k, v)| (*k, v.clone())).collect(),
        }
    }
}

pub(crate) struct FeaturesRequest {
    pub features: HashMap<FeatureExtension, Box<dyn Feature>>,
}

impl FeaturesRequest {
    pub fn build(
        authentication_options: TdsAuthenticationMethod,
        access_token: Option<String>,
        prelogin_fedauth_response: bool,
    ) -> Self {
        let mut features: HashMap<FeatureExtension, Box<dyn Feature>> = HashMap::new();
        features.insert(
            FeatureExtension::Utf8Support,
            Box::new(Utf8Feature::default()),
        );

        features.insert(FeatureExtension::Json, Box::new(JsonFeature::default()));

        if authentication_options != TdsAuthenticationMethod::SSPI
            && authentication_options != TdsAuthenticationMethod::Password
        {
            features.insert(
                FeatureExtension::FedAuth,
                Box::new(FedAuthFeature::new(
                    authentication_options,
                    access_token,
                    prelogin_fedauth_response,
                )),
            );
        }
        FeaturesRequest { features }
    }

    pub fn features(&self) -> Vec<&dyn Feature> {
        self.features
            .values()
            .map(|f| f.as_ref())
            .collect::<Vec<&dyn Feature>>()
    }

    pub fn is_acknowledged(&self, _feature_extension: FeatureExtension) -> Option<&dyn Feature> {
        let feature = self.features.get(&_feature_extension);
        match feature {
            Some(f) => {
                if f.is_acknowledged() {
                    Some(f.as_ref())
                } else {
                    None
                }
            }
            None => {
                unreachable!("Feature not found in the features request.");
            }
        }
    }

    pub fn set_acknowledged(&mut self, _feature_extension: FeatureExtension, _data: &[u8]) {
        let feature = self.features.get_mut(&_feature_extension);
        match feature {
            Some(f) => {
                f.set_acknowledged(true);
                f.deserialize(_data);
            }
            None => {
                unreachable!("Feature not found in the features request.");
            }
        }
    }

    pub fn get_requested_features(&self) -> Vec<&dyn Feature> {
        self.features
            .values()
            .filter(|f| f.is_requested())
            .map(|f| f.as_ref())
            .collect()
    }

    pub fn get_acknowledged_features(&self) -> Vec<&dyn Feature> {
        self.features
            .values()
            .filter(|f| f.is_acknowledged())
            .map(|f| f.as_ref())
            .collect()
    }
}

impl From<(&ClientContext, bool)> for FeaturesRequest {
    fn from(context_and_prelogin_fedauth_flag: (&ClientContext, bool)) -> Self {
        let context = context_and_prelogin_fedauth_flag.0;
        FeaturesRequest::build(
            context.tds_authentication_method.clone(),
            context.access_token.clone(),
            context_and_prelogin_fedauth_flag.1,
        )
    }
}

#[derive(Default)]
pub struct PhysicalAddress {
    address_bytes: [u8; 6],
}

pub(crate) struct LoginRequestModel<'context> {
    pub option_flags1: OptionFlags1,
    pub option_flags2: OptionFlags2,
    pub option_flags3: OptionFlags3,
    pub type_flags: TypeFlags,
    pub tds_version: TdsVersion,
    // TODO: user_input needs to be login specific user_input. Need to understand how to replicate the C# concept here.
    pub user_input: &'context ClientContext,
    // This is a transport context, which will be used to get the authoritative server name.
    pub transport_context: &'context TransportContext,
    pub features_request: FeaturesRequest,
    pub client_prog_ver: i32,
    pub client_process_id: i32,
    pub connection_id_deprecated: i32,
    pub client_time_zone_deprecated: i32,
    pub client_lcid_deprecated: i32,
    pub client_id: PhysicalAddress,
}

impl LoginRequestModel<'_> {
    pub(crate) fn from_context<'a, 'b>(
        context: &'a ClientContext,
        pre_login_fedauth_response: bool,
        transport_context: &'b TransportContext,
    ) -> LoginRequestModel<'b>
    where
        'a: 'b,
    {
        LoginRequestModel {
            option_flags1: OptionFlags1::default(),
            option_flags2: context.into(),
            option_flags3: context.into(),
            type_flags: context.into(),
            tds_version: context.tds_version(),
            features_request: (context, pre_login_fedauth_response).into(),
            user_input: context,
            transport_context,
            client_prog_ver: 0,
            client_process_id: 0,
            connection_id_deprecated: 0,
            client_time_zone_deprecated: 0,
            client_lcid_deprecated: 0,
            client_id: PhysicalAddress::default(),
        }
    }
}

pub(crate) struct LoginResponseModel {
    pub change_properties: EnvChangeProperties,
    pub features: FeaturesRequest,
    pub tds_error: Option<TdsError>,
    pub success_token: Option<LoginAckToken>,
    pub fed_auth_info: Option<FedAuthInfoToken>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum LoginResponseStatus {
    NoResponse = 0x00,
    Success = 0x01,
    Error = 0x02,
    WaitingForFedAuth = 0x03,
    Rerouting = 0x04,
}

impl LoginResponseModel {
    fn new(features: FeaturesRequest) -> Self {
        LoginResponseModel {
            change_properties: EnvChangeProperties::default(),
            features,
            tds_error: None,
            success_token: None,
            fed_auth_info: None,
        }
    }

    fn capture_change_property(&mut self, change_token: EnvChangeToken) -> TdsResult<()> {
        let sub_type = change_token.sub_type;

        match change_token.change_type {
            EnvChangeContainer::String(string_change) => match sub_type {
                EnvChangeTokenSubType::Database => {
                    self.change_properties.database =
                        Option::from(string_change.new_value().clone());
                    Ok(())
                }
                EnvChangeTokenSubType::Language => {
                    self.change_properties.language =
                        Option::from(string_change.new_value().clone());
                    Ok(())
                }
                EnvChangeTokenSubType::CharacterSet => {
                    self.change_properties.char_set = Some(string_change.new_value().clone());
                    Ok(())
                }
                _ => {
                    event!(
                        Level::ERROR,
                        "Env change token of type string, but implementation doesn't account for this property type: {:?}",
                        sub_type
                    );
                    Err(crate::error::Error::ProtocolError(
                        "Env change token of type string, but implementation doesn't account for this property type".to_string(),
                    ))
                }
            },
            EnvChangeContainer::SqlCollation(collation_change) => {
                self.change_properties.database_collation = *collation_change.new_value();
                Ok(())
            }
            EnvChangeContainer::UInt32(numeric_change) => match sub_type {
                EnvChangeTokenSubType::PacketSize => {
                    self.change_properties.packet_size = *numeric_change.new_value() as i32;
                    Ok(())
                }
                _ => {
                    event!(
                        Level::DEBUG,
                        "Unaccounted numeric change property type: {:?}",
                        sub_type
                    );
                    Err(crate::error::Error::ProtocolError(format!(
                        "Unaccounted numeric change property type: {:?}",
                        sub_type
                    )))
                }
            },
            EnvChangeContainer::RoutingType(routing_change) => {
                self.change_properties.routing_information = routing_change.new_value().clone();
                Err(crate::error::Error::Redirection {
                    host: routing_change.new_value().as_ref().unwrap().server.clone(),
                    port: routing_change.new_value().as_ref().unwrap().port,
                })
            }
            _ => {
                event!(
                    Level::ERROR,
                    "Unknown change property type: {:?}",
                    change_token.change_type
                );
                Err(crate::error::Error::ProtocolError(
                    "Unknown change property type".to_string(),
                ))
            }
        }
    }

    pub(crate) fn get_status(&self) -> LoginResponseStatus {
        if self.change_properties.routing_information.is_some() {
            return LoginResponseStatus::Rerouting;
        }

        if self.success_token.is_some() {
            return LoginResponseStatus::Success;
        }

        if self.tds_error.is_some() {
            return LoginResponseStatus::Error;
        }

        if self.fed_auth_info.is_some() {
            return LoginResponseStatus::WaitingForFedAuth;
        }

        LoginResponseStatus::Error
    }
}

pub(crate) struct LoginRequest<'context> {
    pub model: LoginRequestModel<'context>,
}

#[async_trait]
impl Request for LoginRequest<'_> {
    fn packet_type(&self) -> PacketType {
        PacketType::Login7
    }

    async fn serialize<'a, 'b>(&'a self, writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        Serializer::new(&self.model, writer).serialize().await
    }
}

pub(crate) struct FedAuthTokenRequest {
    pub access_token_bytes: Vec<u8>,
}

#[async_trait]
impl Request for FedAuthTokenRequest {
    fn packet_type(&self) -> PacketType {
        PacketType::FedAuthToken
    }

    async fn serialize<'a, 'b>(&'a self, writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        // The token bytes as well as the length of the token bytes are written to the packet.
        let len = self.access_token_bytes.len() + size_of::<u32>();
        writer.write_u32_async(len as u32).await?;
        writer
            .write_u32_async(self.access_token_bytes.len() as u32)
            .await?;
        writer.write_async(&self.access_token_bytes).await?;
        writer.finalize().await?;
        Ok(())
    }
}

pub(crate) struct LoginResponse {
    // pub model: LoginResponseModel,
    // pub requested_features: Pin<Box<FeaturesRequest>>,
}

impl LoginResponse {
    pub fn new() -> Self {
        LoginResponse {}
    }

    pub(crate) async fn deserialize(
        &self,
        reader: &mut dyn NetworkReader,
        requested_features: FeaturesRequest,
    ) -> TdsResult<LoginResponseModel> {
        let packet_reader = reader.get_packet_reader();
        let login_token_registry = GenericTokenParserRegistry::default();
        let mut token_stream_reader = TokenStreamReader {
            packet_reader,
            parser_registry: Box::new(login_token_registry),
        };

        let mut response_model = LoginResponseModel::new(requested_features);
        let parser_context = ParserContext::default();
        loop {
            match token_stream_reader
                .receive_token(&parser_context, None, None)
                .await
            {
                Ok(token) => {
                    let token_type = token.token_type();
                    event!(
                        Level::DEBUG,
                        "Received token: {:?} during login response parsing",
                        token_type
                    );

                    match token {
                        Tokens::EnvChange(env_change_token) => {
                            event!(
                                Level::INFO,
                                "Received {:?} during login response parsing.",
                                token_type
                            );
                            // If the redirection is received, then the error will be unwrapped and surfaced
                            // to the caller.
                            response_model.capture_change_property(env_change_token)?;
                        }
                        Tokens::LoginAck(login_ack_token) => {
                            response_model.success_token = Some(login_ack_token);
                        }
                        Tokens::Done(_t) => {
                            // Once we receive a Done token, exit the loop.
                            break;
                        }
                        Tokens::DoneProc(_t) => {
                            // ...
                        }
                        Tokens::DoneInProc(_t) => {
                            // ...
                        }
                        Tokens::Error(error_token) => {
                            event!(
                                Level::ERROR,
                                "Received Error token during login response parsing."
                            );
                            response_model.tds_error = Some(TdsError::new(error_token));
                            // Decide if you want to break here, or keep looping.
                        }
                        Tokens::FeatureExtAck(_t) => {
                            _t.acknowledged_features().iter().for_each(|f| {
                                response_model
                                    .features
                                    .set_acknowledged(f.0, f.1.as_slice());
                            });
                        }
                        Tokens::FedAuthInfo(fed_auth_info_token) => {
                            response_model.fed_auth_info = Some(fed_auth_info_token);
                            break;
                        }
                        Tokens::Info(_t) => {
                            event!(
                                Level::INFO,
                                "Received {:?} during login response parsing.",
                                token_type
                            );
                        }
                        Tokens::Sspi(_t) => {
                            todo!()
                        }
                        _ => {
                            event!(
                                Level::ERROR,
                                "Received unexpected token during login response parsing. \
                        Check that all tokens from the registry are handled."
                            );
                        }
                    };
                }
                Err(e) => {
                    event!(
                        Level::ERROR,
                        "Failed to receive token during login response parsing. Error: {:?}",
                        e
                    );
                    // if Io error then create the Io error
                    return Err(crate::error::Error::ProtocolError(
                        "Failed to receive token during login response parsing.".to_string(),
                    ));
                }
            }
        }
        Ok(response_model)
    }
}

struct Serializer<'a, 'n, 'context> {
    model: &'a LoginRequestModel<'context>,
    payload_writer: &'a mut PacketWriter<'n>,
    features_request: &'a FeaturesRequest,
    content_next_offset: i32,
    deferred_actions_indicator: Vec<LoginDeferredPayload>,
}

impl<'a, 'n, 'context> Serializer<'a, 'n, 'context> {
    pub fn new(
        model: &'a LoginRequestModel<'context>,
        payload_writer: &'a mut PacketWriter<'n>,
    ) -> Serializer<'a, 'n, 'context> {
        Serializer {
            model,
            payload_writer,
            features_request: &model.features_request,
            content_next_offset: FIXED_LOGIN_RECORD_LENGTH,
            deferred_actions_indicator: Vec::new(),
        }
    }

    /// Calculate the length of the login record.
    /// This includes the fixed length of the login record, the length of the variable length fields,
    /// and the length of the feature extension data.
    fn calculate_login_record_length(&self) -> (i32, i32) {
        let mut login_record_length = FIXED_LOGIN_RECORD_LENGTH;
        login_record_length += self.model.user_input.len_bytes();
        login_record_length += self.model.transport_context.len_bytes();
        login_record_length += 4; // Feature extension offset size.

        // We write the feature extension at the end of the login record. This is not necessary, but makes reading
        // packets easier. Hence we add the length of the feature extensions data at the end, and save the offset,
        // for the feature extension.
        let feature_extension_offset = login_record_length;
        login_record_length += self.features_request.len_bytes();
        (login_record_length, feature_extension_offset)
    }

    pub(crate) async fn serialize(&mut self) -> TdsResult<()> {
        let (login_record_length, feature_extension_offset) = self.calculate_login_record_length();
        trace!(login_record_length);
        self.payload_writer
            .write_i32_async(login_record_length)
            .await?;
        self.payload_writer
            .write_u32_async(self.model.tds_version as u32)
            .await?;

        self.payload_writer
            .write_i32_async(self.model.user_input.packet_size as i32)
            .await?;

        self.payload_writer
            .write_i32_async(self.model.client_prog_ver)
            .await?;

        self.payload_writer
            .write_i32_async(self.model.client_process_id)
            .await?;

        self.payload_writer
            .write_i32_async(self.model.connection_id_deprecated)
            .await?;

        self.payload_writer
            .write_byte_async(self.model.option_flags1.value())
            .await?;

        self.payload_writer
            .write_byte_async(self.model.option_flags2.value())
            .await?;

        self.payload_writer
            .write_byte_async(self.model.type_flags.value())
            .await?;

        self.payload_writer
            .write_byte_async(self.model.option_flags3.value())
            .await?;

        self.payload_writer
            .write_i32_async(self.model.client_time_zone_deprecated)
            .await?;

        self.payload_writer
            .write_i32_async(self.model.client_lcid_deprecated)
            .await?;

        self.write_variable_length_section(feature_extension_offset)
            .await?;

        Ok(())
    }

    async fn write_variable_length_section(
        &mut self,
        feature_extension_offset: i32,
    ) -> TdsResult<()> {
        /* Writing variable-length meta data section
            Fixed-Length Metadata (58 bytes)
            HostNameOffset: 2 bytes
            HostNameLength: 2 bytes
            UserNameOffset: 2 bytes
            UserNameLength: 2 bytes
            PasswordOffset: 2 bytes
            PasswordLength: 2 bytes
            AppNameOffset: 2 bytes
            AppNameLength: 2 bytes
            ServerNameOffset: 2 bytes
            ServerNameLength: 2 bytes
            FeatureExtOffset: 2 bytes
            FeatureExtLength: 2 bytes
            LibraryOffset: 2 bytes
            LibraryLength: 2 bytes
            LanguageOffset: 2 bytes
            LanguageLength: 2 bytes
            DatabaseOffset: 2 bytes
            DatabaseLength: 2 bytes
            ClientID: 6 bytes (fixed length)
            SSPIOffset: 2 bytes
            SSPILength: 2 bytes
            AttachDBFileOffset: 2 bytes
            AttachDBFileLength: 2 bytes
            ChangePasswordOffset: 2 bytes
            ChangePasswordLength: 2 bytes
            cbSSPILong: 4 bytes (DWORD)
        */

        self.write_hostname().await?;
        self.write_username().await?; //
        self.write_password().await?; //
        self.write_app_name().await?;
        self.write_server_name().await?;
        self.write_feature_ext().await?;
        self.write_library_name().await?;
        self.write_language().await?;
        self.write_database().await?;
        self.write_client_id().await?;
        self.write_sspi_short().await?;
        self.write_attach_db_file().await?;
        self.write_change_password().await?;
        self.write_cb_sspi_long().await?;

        // Write all the variable length data.
        for indicator in &self.deferred_actions_indicator {
            match indicator {
                LoginDeferredPayload::HostName => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.workstation_id)
                        .await?;
                }
                LoginDeferredPayload::UserName => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.user_name)
                        .await?;
                }
                LoginDeferredPayload::Password => {
                    let mut password_utf16_bytes = self
                        .model
                        .user_input
                        .password
                        .encode_utf16()
                        .flat_map(|f| f.to_le_bytes())
                        .collect::<Vec<u8>>();
                    scramble_password(&mut password_utf16_bytes);
                    self.payload_writer
                        .write_async(&password_utf16_bytes)
                        .await?;
                }
                LoginDeferredPayload::AppName => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.application_name)
                        .await?;
                }
                LoginDeferredPayload::ServerName => {
                    let server_name = match &self.model.transport_context {
                        TransportContext::Tcp { host, port: _ } => host,
                        _ => {
                            unimplemented!("Transport type not supported")
                        }
                    };
                    // let server_name = self
                    //     .model
                    //     .user_input
                    //     .transport
                    //     .get_servername()
                    //     .to_string();
                    info!("Login Server name: {}", server_name);
                    self.payload_writer
                        .write_string_unicode_async(server_name.as_str())
                        .await?;
                }
                LoginDeferredPayload::FeatureExtOffset => {
                    self.payload_writer
                        .write_i32_async(feature_extension_offset)
                        .await?;
                }
                LoginDeferredPayload::Library => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.library_name)
                        .await?;
                }
                LoginDeferredPayload::Language => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.language)
                        .await?;
                }
                LoginDeferredPayload::Database => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.database)
                        .await?;
                }
                LoginDeferredPayload::AttachDbFile => {
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.attach_db_file)
                        .await?;
                }
                LoginDeferredPayload::ChangePassword => {
                    let mut password_utf16_bytes = self
                        .model
                        .user_input
                        .change_password
                        .encode_utf16()
                        .flat_map(|f| f.to_le_bytes())
                        .collect::<Vec<u8>>();
                    scramble_password(&mut password_utf16_bytes);
                    self.payload_writer
                        .write_async(&password_utf16_bytes)
                        .await?;
                }
            }
        }

        self.write_feature_extension_data().await?;

        debug!(?self.content_next_offset);

        self.payload_writer.finalize().await?;
        Ok(())
    }

    /// Serializes the Feature extension data for each of the requested features.
    async fn write_feature_extension_data(&mut self) -> TdsResult<()> {
        for feature in self.features_request.get_requested_features() {
            feature.serialize(self.payload_writer).await?;
        }
        // Write the terminator
        self.payload_writer.write_byte_async(0xff).await?;
        Ok(())
    }

    /// Writes the value of the hostname of the client to the login packet.
    async fn write_hostname(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.workstation_id.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::HostName);
        }
        Ok(())
    }

    /// Writes the value of the username of the client to the login packet if the authentication method is Password.
    async fn write_username(&mut self) -> TdsResult<()> {
        if matches!(
            self.model.user_input.tds_authentication_method,
            TdsAuthenticationMethod::Password
        ) {
            if self
                .write_metadata(self.model.user_input.user_name.len() as i16)
                .await?
            {
                self.deferred_actions_indicator
                    .push(LoginDeferredPayload::UserName);
            }
        } else {
            self.payload_writer.write_i16_async(0).await?;
            self.payload_writer.write_i16_async(0).await?;
        }
        Ok(())
    }

    /// Writes the value of the password of the client to the login packet if the authentication method is Password.
    async fn write_password(&mut self) -> TdsResult<()> {
        if self.model.user_input.tds_authentication_method == TdsAuthenticationMethod::Password {
            if self
                .write_metadata(self.model.user_input.password.len() as i16)
                .await?
            {
                self.deferred_actions_indicator
                    .push(LoginDeferredPayload::Password);
            }
        } else {
            self.payload_writer.write_i16_async(0).await?;
            self.payload_writer.write_i16_async(0).await?;
        }
        Ok(())
    }

    /// Writes the value of the application name provided in the client context to the login packet.
    async fn write_app_name(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.application_name.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::AppName);
        }
        Ok(())
    }

    /// Writes the value of the target sql server to the login packet.
    async fn write_server_name(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.transport_context.get_server_name().len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::ServerName);
        }
        Ok(())
    }

    async fn write_feature_ext(&mut self) -> TdsResult<()> {
        // The offset at which to read the feature extension length.
        self.payload_writer
            .write_i16_async(self.content_next_offset as i16)
            .await?;

        // Length of the size of feature extension offset data, which is a DWORD.
        self.payload_writer.write_i16_async(4).await?;

        self.content_next_offset += size_of::<i32>() as i32;

        self.deferred_actions_indicator
            .push(LoginDeferredPayload::FeatureExtOffset);

        Ok(())
    }

    /// Writes the value of the library name to the login packet.
    /// This is also called the Client interface name.
    async fn write_library_name(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.library_name.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::Library);
        }
        Ok(())
    }

    async fn write_language(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.language.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::Language);
        }
        Ok(())
    }

    /// Writes the value of the database name, which we are connecting to.
    async fn write_database(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.database.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::Database);
        }
        Ok(())
    }

    /// Writes the network interface address of the client.
    async fn write_client_id(&mut self) -> TdsResult<()> {
        self.payload_writer
            .write_async(&self.model.client_id.address_bytes)
            .await?;
        Ok(())
    }

    async fn write_sspi_short(&mut self) -> TdsResult<()> {
        let _ = self.write_metadata(0).await?;
        Ok(())
    }

    async fn write_attach_db_file(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.attach_db_file.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::AttachDbFile);
        }
        Ok(())
    }

    async fn write_change_password(&mut self) -> TdsResult<()> {
        if self
            .write_metadata(self.model.user_input.change_password.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::ChangePassword);
        }
        Ok(())
    }

    async fn write_cb_sspi_long(&mut self) -> TdsResult<()> {
        self.payload_writer.write_i32_async(0).await?;
        Ok(())
    }

    async fn write_metadata(&mut self, char_length: i16) -> TdsResult<bool> {
        if char_length == 0 {
            self.payload_writer
                .write_i16_async(self.content_next_offset as i16)
                .await?;
            self.payload_writer.write_i16_async(0).await?;
            return Ok(false);
        }

        self.payload_writer
            .write_i16_async(self.content_next_offset as i16)
            .await?;
        self.payload_writer.write_i16_async(char_length).await?;

        self.content_next_offset += (char_length * 2) as i32;
        Ok(true)
    }
}

#[allow(clippy::manual_rotate)]
fn scramble_password(password_utf16_bytes: &mut [u8]) {
    for b in password_utf16_bytes.iter_mut() {
        *b = (*b >> 4) | (*b << 4); // Swap the nibbles
        *b ^= 0xA5; // XOR with 0xA5
    }
}

/// The deferred actions that are not yet serialized.
/// These values are used to point out the actual payload, that needs to be serialized.
#[derive(Clone, Copy)]
enum LoginDeferredPayload {
    HostName,
    UserName,
    Password,
    AppName,
    ServerName,
    FeatureExtOffset,
    Library,
    Language,
    Database,
    AttachDbFile,
    ChangePassword,
}

/// Trait to calculate the size of the fields for the login records in bytes.
trait SizedLoginItem {
    fn len_bytes(&self) -> i32;
}

impl SizedLoginItem for TransportContext {
    fn len_bytes(&self) -> i32 {
        self.get_server_name().len_bytes()
    }
}

impl SizedLoginItem for String {
    fn len_bytes(&self) -> i32 {
        (self.len() * 2) as i32
    }
}

impl SizedLoginItem for FeaturesRequest {
    fn len_bytes(&self) -> i32 {
        let mut length = 0;
        for feature in self.get_requested_features() {
            length += feature.data_length();
        }

        length += 1; // Feature extension terminator.

        length
    }
}

impl SizedLoginItem for ClientContext {
    fn len_bytes(&self) -> i32 {
        // Do not consider the length of the transport context. That
        // may be overridden by the redirected endpoint.
        let mut client_context_item_length = 0;
        client_context_item_length += self.workstation_id.len_bytes();
        client_context_item_length += self.application_name.len_bytes();
        client_context_item_length += self.library_name.len_bytes();
        client_context_item_length += self.language.len_bytes();
        client_context_item_length += self.database.len_bytes();
        client_context_item_length += self.attach_db_file.len_bytes();

        client_context_item_length += self.calculate_byte_length_for_authentication();
        client_context_item_length
    }
}

impl ClientContext {
    /// Calculate the length of the fields to be persisted for the authentication data.
    /// This would involve the length of the username and password for [TdsAuthenticationMethod::Password].
    /// For TdsAuthenticationMethod::SSPI, we need to add the length of the SSPI information to be sent to the server.
    /// FedAuth or AccessToken authentication is accounted for, in the Feature extension data.
    fn calculate_byte_length_for_authentication(&self) -> i32 {
        let mut length = 0;
        if matches!(
            self.tds_authentication_method,
            TdsAuthenticationMethod::Password
        ) {
            length += self.password.len_bytes();
            length += self.user_name.len_bytes();
        } else if matches!(
            self.tds_authentication_method,
            TdsAuthenticationMethod::SSPI
        ) {
            todo!("SSPI authentication not implemented yet. Add logic to compute the length of the SSPI information to be sent to the server.");
        }
        length
    }
}
