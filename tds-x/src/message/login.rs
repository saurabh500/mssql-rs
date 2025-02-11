use crate::connection::client_context::{ClientContext, TdsAuthenticationMethod};
use crate::message::login_options::{
    OptionFlags1, OptionFlags2, OptionFlags3, OptionsValue, TdsVersion, TypeFlags,
};
use crate::message::messages::{PacketType, Request, TdsError};
use crate::read_write::packet_writer::PacketWriter;
use crate::read_write::reader_writer::{NetworkReader, NetworkWriter};
use crate::token::fed_auth_info::FedAuthInfoToken;
use crate::token::login_ack::LoginAckToken;
use crate::token::tokens::{
    EnvChangeContainer, EnvChangeToken, EnvChangeTokenSubType, SqlCollation, Token, Tokens,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::io::Error;

use super::features::utf8::Utf8Feature;
use super::login_options::{
    OptionChangePassword, OptionInitLang, OptionIntegratedSecurity, OptionOdbc, OptionOleDb,
    OptionSqlType, OptionUser,
};
use tracing::{event, Level};

use crate::read_write::token_stream::{
    GenericTokenParserRegistry, ParserContext, TokenStreamReader,
};

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
            0xFF => FeatureExtension::Terminator,
            _ => unreachable!("Invalid Feature Extension."),
        }
    }
}

#[async_trait(?Send)]
pub(crate) trait Feature {
    fn feature_identifier(&self) -> FeatureExtension;
    fn is_requested(&self) -> bool;
    fn data_length(&self) -> i32;
    async fn serialize(&self, packet_writer: &mut PacketWriter) -> Result<(), Error>;
    fn deserialize(&self, data: &[u8]);
    fn is_acknowledged(&self) -> bool;
    fn set_acknowledged(&mut self, _acknowledged: bool);
}

pub(crate) struct FeaturesRequest {
    pub features: HashMap<FeatureExtension, Box<dyn Feature>>,
}

/// Default implementation for Feature Request, which will add all the features.
impl Default for FeaturesRequest {
    fn default() -> Self {
        let mut features: HashMap<FeatureExtension, Box<dyn Feature>> = HashMap::new();
        features.insert(
            FeatureExtension::Utf8Support,
            Box::new(Utf8Feature::default()),
        );
        FeaturesRequest { features }
    }
}

impl FeaturesRequest {
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

#[derive(Default)]
pub struct PhysicalAddress {
    address_bytes: [u8; 6],
}

pub(crate) struct LoginRequestModel<'a> {
    pub option_flags1: OptionFlags1,
    pub option_flags2: OptionFlags2,
    pub option_flags3: OptionFlags3,
    pub type_flags: TypeFlags,
    pub tds_version: TdsVersion,
    // TODO: user_input needs to be login specific user_input. Need to understand how to replicate the C# concept here.
    pub user_input: &'a ClientContext,
    pub features_request: FeaturesRequest,
    pub client_prog_ver: i32,
    pub client_process_id: i32,
    pub connection_id_deprecated: i32,
    pub client_time_zone_deprecated: i32,
    pub client_lcid_deprecated: i32,
    pub client_id: PhysicalAddress,
}

impl LoginRequestModel<'_> {
    pub(crate) fn from_context(
        context: &crate::connection::client_context::ClientContext,
    ) -> LoginRequestModel {
        let replication_option = match context.replication {
            true => OptionUser::ReplicationLogin,
            false => OptionUser::Normal,
        };

        let integrated_security = match context.integrated_security() {
            true => OptionIntegratedSecurity::On,
            false => OptionIntegratedSecurity::Off,
        };

        let option_flags2 = OptionFlags2 {
            init_lang: OptionInitLang::Fatal,
            odbc: OptionOdbc::On,
            user: replication_option,
            integrated_security,
        };

        let change_password_option = match context.change_password.is_empty() {
            true => OptionChangePassword::No,
            false => OptionChangePassword::Yes,
        };
        let option_flags3 = OptionFlags3 {
            change_password: change_password_option,
            binary_xml: false,
            spawn_user_instance: context.user_instance,
            unknown_collation_handling: false,
            extension_used: true,
        };
        let type_flags = TypeFlags {
            sql_type: OptionSqlType::Default,
            ole_db: OptionOleDb::Off,
            access_intent: context.application_intent,
        };

        LoginRequestModel {
            option_flags1: OptionFlags1::default(),
            option_flags2,
            option_flags3,
            type_flags,
            tds_version: context.tds_version(),
            features_request: FeaturesRequest::default(),
            user_input: context,
            client_prog_ver: 0,
            client_process_id: 0,
            connection_id_deprecated: 0,
            client_time_zone_deprecated: 0,
            client_lcid_deprecated: 0,
            client_id: PhysicalAddress::default(),
        }
    }
}

#[derive(Default)]
pub(crate) struct LoginResponseModel {
    pub change_properties: EnvChangeProperties,
    pub features: FeaturesRequest,
    pub tds_error: Option<TdsError>,
    pub success_token: Option<LoginAckToken>,
    pub fed_auth_info: Option<FedAuthInfoToken>,
}

#[repr(u8)]
pub(crate) enum LoginResponseStatus {
    NoResponse = 0x00,
    Success = 0x01,
    Error = 0x02,
    WaitingForFedAuth = 0x03,
    Rerouting = 0x04,
}

impl LoginResponseModel {
    fn capture_change_property(&mut self, change_token: EnvChangeToken) {
        let sub_type = change_token.sub_type;

        match change_token.change_type {
            EnvChangeContainer::String(string_change) => match sub_type {
                EnvChangeTokenSubType::Database => {
                    self.change_properties.database =
                        Option::from(string_change.new_value().clone());
                }
                EnvChangeTokenSubType::Language => {
                    self.change_properties.language =
                        Option::from(string_change.new_value().clone());
                }
                EnvChangeTokenSubType::CharacterSet => {
                    self.change_properties.char_set = Some(string_change.new_value().clone());
                }
                _ => {
                    event!(
                        Level::DEBUG,
                        "Unaccounted change property type: {:?}",
                        sub_type
                    );
                }
            },
            EnvChangeContainer::SqlCollation(collation_change) => {
                self.change_properties.database_collation = *collation_change.new_value();
            }
            EnvChangeContainer::UInt32(numeric_change) => match sub_type {
                EnvChangeTokenSubType::PacketSize => {
                    self.change_properties.packet_size = *numeric_change.new_value() as i32;
                }
                _ => {
                    event!(
                        Level::DEBUG,
                        "Unaccounted numeric change property type: {:?}",
                        sub_type
                    );
                }
            },
            EnvChangeContainer::RoutingType(routing_change) => {
                self.change_properties.routing_information = routing_change.new_value().clone();
            }
            _ => {
                event!(
                    Level::DEBUG,
                    "Unknown change property type: {:?}",
                    change_token.change_type
                );
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

pub(crate) struct LoginRequest<'a> {
    pub model: LoginRequestModel<'a>,
}

#[async_trait(?Send)]
impl<'a> Request<'a> for LoginRequest<'a> {
    fn packet_type(&self) -> PacketType {
        PacketType::Login7
    }

    fn create_packet_writer(&self, writer: &'a mut dyn NetworkWriter) -> PacketWriter<'a> {
        writer.get_packet_writer(self.packet_type())
    }

    async fn serialize(&self, transport: &mut dyn NetworkWriter) -> Result<(), Error> {
        // TODO: Log the datamodel.
        let mut packet_writer = self.create_packet_writer(transport);
        let _ = Serializer::new(&self.model, &mut packet_writer)
            .serialize()
            .await?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct LoginResponse {
    pub model: LoginResponseModel,
}

impl LoginResponse {
    pub(crate) async fn deserialize(&self, reader: &mut dyn NetworkReader) -> LoginResponseModel {
        let packet_reader = reader.get_packet_reader();
        let login_token_registry = GenericTokenParserRegistry::default();
        let mut token_stream_reader = TokenStreamReader {
            packet_reader,
            parser_registry: Box::new(login_token_registry),
        };

        let mut response_model = LoginResponseModel::default();
        let parser_context = ParserContext::default();
        while let Ok(token) = token_stream_reader.receive_token(&parser_context).await {
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
                    response_model.capture_change_property(env_change_token);
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
            }
        }
        // If receive_token() returns Err(e), the while let loop ends automatically.

        response_model
    }
}

struct Serializer<'a> {
    model: &'a LoginRequestModel<'a>,
    payload_writer: &'a mut PacketWriter<'a>,
    features_request: &'a FeaturesRequest,
    content_next_offset: i32,
    deferred_actions_indicator: Vec<LoginDeferredPayload>,
}

impl Serializer<'_> {
    pub fn new<'a>(
        model: &'a LoginRequestModel<'a>,
        payload_writer: &'a mut PacketWriter<'a>,
    ) -> Serializer<'a> {
        Serializer {
            model,
            payload_writer,
            features_request: &model.features_request,
            content_next_offset: 94,
            deferred_actions_indicator: Vec::new(),
            // TODO: Deferred actions.
        }
    }

    pub(crate) async fn serialize(&mut self) -> Result<(), Error> {
        // This is the place holder for the login packet. We will come back and repopulate it after constructing the login packet.
        self.payload_writer.write_i32_async(0).await?;

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

        self.write_variable_length_section().await?;

        Ok(())
    }

    async fn write_variable_length_section(&mut self) -> Result<(), Error> {
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
        self.write_username().await?;
        self.write_password().await?;
        self.write_app_name().await?;
        self.write_server_name().await?;
        self.write_feature_ext().await?;
        self.write_library().await?;
        self.write_language().await?;
        self.write_database().await?;
        self.write_client_id().await?;
        self.write_sspi_short().await?;
        self.write_attach_db_file().await?;
        self.write_change_password().await?;
        self.write_cb_sspi_long().await?;

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
                    self.payload_writer
                        .write_string_unicode_async(&self.model.user_input.server_name)
                        .await?;
                }
                LoginDeferredPayload::FeatureExt => {
                    self.payload_writer
                        .write_i32_async(size_of::<i32>() as i32 + self.payload_writer.position())
                        .await?;

                    for feature in self.features_request.features() {
                        if feature.is_requested() {
                            feature.serialize(self.payload_writer).await?;
                        }
                    }

                    self.payload_writer.write_byte_async(0xff).await?;
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

        self.payload_writer
            .write_i32_at_index(0, self.content_next_offset);

        self.payload_writer.finalize().await?;
        Ok(())
    }

    async fn write_hostname(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.workstation_id.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::HostName);
        }
        Ok(())
    }

    async fn write_username(&mut self) -> Result<(), Error> {
        if self.model.user_input.tds_authentication_method == TdsAuthenticationMethod::Password {
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

    async fn write_password(&mut self) -> Result<(), Error> {
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

    async fn write_app_name(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.application_name.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::AppName);
        }
        Ok(())
    }

    async fn write_server_name(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.server_name.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::ServerName);
        }
        Ok(())
    }

    async fn write_feature_ext(&mut self) -> Result<(), Error> {
        self.payload_writer
            .write_i16_async(self.content_next_offset as i16)
            .await?;

        self.payload_writer.write_i16_async(4).await?;

        let mut feature_data_length = 0;

        // TODO: Uncomment when features are implemented.
        for feature in self.features_request.get_requested_features() {
            feature_data_length += feature.data_length();
        }

        self.content_next_offset +=
            (size_of::<i32>() as i32) + feature_data_length + (size_of::<u8>() as i32);

        self.deferred_actions_indicator
            .push(LoginDeferredPayload::FeatureExt);

        Ok(())
    }

    async fn write_library(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.library_name.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::Library);
        }
        Ok(())
    }

    async fn write_language(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.language.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::Language);
        }
        Ok(())
    }

    async fn write_database(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.database.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::Database);
        }
        Ok(())
    }

    async fn write_client_id(&mut self) -> Result<(), Error> {
        self.payload_writer
            .write_async(&self.model.client_id.address_bytes)
            .await?;
        Ok(())
    }

    async fn write_sspi_short(&mut self) -> Result<(), Error> {
        let _ = self.write_metadata(0).await?;
        Ok(())
    }

    async fn write_attach_db_file(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.attach_db_file.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::AttachDbFile);
        }
        Ok(())
    }

    async fn write_change_password(&mut self) -> Result<(), Error> {
        if self
            .write_metadata(self.model.user_input.change_password.len() as i16)
            .await?
        {
            self.deferred_actions_indicator
                .push(LoginDeferredPayload::ChangePassword);
        }
        Ok(())
    }

    async fn write_cb_sspi_long(&mut self) -> Result<(), Error> {
        self.payload_writer.write_i32_async(0).await?;
        Ok(())
    }

    async fn write_metadata(&mut self, char_length: i16) -> Result<bool, Error> {
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
    FeatureExt,
    Library,
    Language,
    Database,
    AttachDbFile,
    ChangePassword,
}
