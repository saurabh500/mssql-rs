use crate::connection::client_context::ClientContext;
use crate::core::{EncryptionSetting, SQLServerVersion, Version};
use crate::message::login::{
    EnvChangeProperties, FeaturesRequest, LoginResponse, LoginResponseModel, RoutingInfo,
};
use crate::message::messages::Request;
use crate::message::prelogin::{
    EncryptionType, PreloginRequest, PreloginRequestModel, PreloginResponse, PreloginResponseModel,
};
use crate::read_write::writer::NetworkReaderWriter;
use crate::token::tokens::SqlCollation;
use uuid::Uuid;

pub(crate) struct HandlerFactory<'a> {
    pub(crate) context: &'a ClientContext,
}

impl HandlerFactory<'_> {
    pub(crate) fn prelogin_handler(&self) -> PreloginHandler<'_, '_> {
        PreloginHandler { factory: self }
    }

    pub(crate) fn login_handler(&self) -> LoginHandler<'_, '_> {
        LoginHandler { factory: self }
    }

    pub(crate) fn session_handler(&self) -> SessionHandler<'_, '_> {
        SessionHandler { factory: self }
    }
}

pub(crate) struct SessionHandler<'a, 'n> {
    pub(crate) factory: &'a HandlerFactory<'n>,
}

pub(crate) struct SessionSettings {
    // TODO
}

impl SessionHandler<'_, '_> {
    pub(crate) async fn execute(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> SessionSettings {
        let _ = self.factory.prelogin_handler().execute(reader_writer).await;
        SessionSettings {}
    }
}

pub struct PreloginResult {
    pub encryption_setting: EncryptionSetting,
    pub is_fed_auth_supported: bool,
}

impl PreloginResult {}

pub struct PreloginHandler<'a, 'n> {
    factory: &'a HandlerFactory<'n>,
}

impl<'a> PreloginHandler<'_, '_> {
    async fn execute(&self, reader_writer: &mut impl NetworkReaderWriter) -> PreloginResult {
        // Create the request.
        let request_model = PreloginRequestModel::new(
            Uuid::new_v4(),
            Option::from(self.factory.context.mars_enabled),
            Option::from(self.factory.context.encryption),
            Option::from(self.factory.context.database.as_str()),
        );
        let prelogin_request = PreloginRequest {
            model: &request_model,
        };

        // Serialize it.
        prelogin_request.serialize(reader_writer).await;

        // Return result (which contains data model).
        let response = PreloginResponse {
            model: PreloginResponseModel {
                encryption: EncryptionType::Off,
                federated_auth_supported: false,
                dbinstance_valid: None,
                mars_enabled: None,
                server_version: Version::new(0, 0, 0, 0),
                sql_server_version: SQLServerVersion::SqlServerNotsupported,
            },
        };

        // TODO: Convert panics to Error objects.
        let response_model = &response.model;
        if request_model.mars_enabled && !response_model.mars_enabled.unwrap() {
            panic!("Server does not support MARS.")
        }

        if !response_model.dbinstance_valid.unwrap() {
            panic!("Database instance validation failed.")
        }

        if request_model.encryption_setting == EncryptionSetting::Strict {
            return PreloginResult {
                encryption_setting: EncryptionSetting::Strict,
                is_fed_auth_supported: response_model.federated_auth_supported,
            };
        }

        if response_model.encryption == EncryptionType::NotSupported {
            panic!("Encryption is not supported.")
        }

        if request_model.encryption_setting == EncryptionSetting::Optional
            && response_model.encryption == EncryptionType::Off
        {
            return PreloginResult {
                encryption_setting: EncryptionSetting::LoginOnly,
                is_fed_auth_supported: response_model.federated_auth_supported,
            };
        }

        PreloginResult {
            encryption_setting: EncryptionSetting::Required,
            is_fed_auth_supported: response_model.federated_auth_supported,
        }
    }
}

pub struct LoginHandler<'a, 'n> {
    factory: &'a HandlerFactory<'n>,
}

impl LoginHandler<'_, '_> {
    fn execute(&self) -> LoginResponse {
        LoginResponse {
            model: LoginResponseModel {
                change_properties: EnvChangeProperties {
                    database_collation: SqlCollation::new(&Vec::new()).unwrap(),
                    packet_size: 0,
                    language: "".to_string(),
                    database: "".to_string(),
                    char_set: None,
                    routing_information: RoutingInfo {},
                },
                features: FeaturesRequest {
                    features: Default::default(),
                },
                tds_error: None,
                login_ack_token: 0,
            },
        }
    }
}
