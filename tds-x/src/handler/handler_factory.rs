use std::io::Error;

use crate::connection::client_context::ClientContext;
use crate::core::EncryptionSetting;
use crate::message::login::{
    EnvChangeProperties, Feature, LoginRequest, LoginRequestModel, LoginResponse,
    LoginResponseModel, LoginResponseStatus,
};
use crate::message::messages::{Request, TypedResponse};
use crate::message::prelogin::{
    EncryptionType, PreloginRequest, PreloginRequestModel, PreloginResponse,
};
use crate::read_write::reader_writer::NetworkReaderWriter;
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
        LoginHandler {
            factory: self,
            encryption: self.context.encryption,
        }
    }

    pub(crate) fn session_handler(&self) -> SessionHandler<'_, '_> {
        SessionHandler { factory: self }
    }

    fn create_login_request(&self) -> LoginRequest {
        let model = self.create_login_model();
        LoginRequest { model }
    }

    fn create_login_model(&self) -> LoginRequestModel {
        LoginRequestModel::from_context(self.context)
    }

    fn create_login_response(&self) -> LoginResponse {
        LoginResponse::default()
    }
}

pub(crate) struct NegotiatedSettings {
    pub session_settings: SessionSettings,
    pub encryption: EncryptionSetting,
}

pub(crate) struct SessionSettings {
    pub database_collation: SqlCollation,
    pub packet_size: u32,
    pub language: String,
    pub database: String,
    pub char_set: String,
    pub server_name: String,
    pub user_name: String,
    supported_features: Vec<Box<dyn Feature>>,
    mars_enabled: bool,
}

impl SessionSettings {
    // Note that this destructively consumes the features list.
    fn new(context: &ClientContext, feautures: &mut Vec<Box<dyn Feature>>) -> Self {
        let mut result = SessionSettings {
            database_collation: Default::default(),
            packet_size: context.packet_size as u32,
            language: context.language.clone(),
            database: context.database.clone(),
            char_set: "".to_string(),
            server_name: context.server_name.clone(),
            user_name: context.user_name.clone(),
            supported_features: vec![],
            mars_enabled: context.mars_enabled,
        };
        result.supported_features.append(feautures);
        result
    }

    fn update_settings(&mut self, change_properties: &EnvChangeProperties) {
        if change_properties.char_set.is_some() {
            self.char_set = change_properties.char_set.clone().unwrap();
        }

        if change_properties.database_collation.is_some() {
            self.database_collation = change_properties.database_collation.clone().unwrap();
        }

        if change_properties.language.is_some() {
            self.language = change_properties.language.clone().unwrap();
        }

        if change_properties.database.is_some() {
            self.database = change_properties.database.clone().unwrap();
        }

        if change_properties.packet_size > 0 {
            self.packet_size = change_properties.packet_size as u32;
        }
    }
}

pub(crate) struct SessionHandler<'a, 'n> {
    pub(crate) factory: &'a HandlerFactory<'n>,
}

impl SessionHandler<'_, '_> {
    pub(crate) async fn execute(
        &mut self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> Result<NegotiatedSettings, Error> {
        let pre_login_result = self.get_pre_login_result(reader_writer).await?;
        self.validate_prelogin_result(&pre_login_result)?;

        // Note: This must happen before login because the login process can use the negotiated
        // encryption setting.
        reader_writer.notify_encryption_setting_change(pre_login_result.encryption_setting);

        let mut login_result = self.get_login_result(reader_writer).await;
        self.validate_login_result(&login_result)?;

        let negotiated_settings = self.negotiate_settings(&pre_login_result, &mut login_result);

        Ok(negotiated_settings)
    }

    async fn get_pre_login_result(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> Result<PreloginResult, Error> {
        let result = self
            .factory
            .prelogin_handler()
            .execute(reader_writer)
            .await?;
        Ok(result)
    }

    fn validate_prelogin_result(&self, _result: &PreloginResult) -> Result<(), Error> {
        // TBDif sever does not support fed auth and client expects fed auth then throw an exception.
        Ok(())
    }

    fn validate_login_result(&self, _result: &LoginResult) -> Result<(), Error> {
        // No validation currently.
        Ok(())
    }

    fn negotiate_settings(
        &self,
        prelogin_result: &PreloginResult,
        login_result: &mut LoginResult,
    ) -> NegotiatedSettings {
        let change_props = &login_result.change_properties;

        let mut session_settings =
            SessionSettings::new(self.factory.context, &mut login_result.supported_features);
        session_settings.update_settings(change_props);

        NegotiatedSettings {
            session_settings,
            encryption: prelogin_result.encryption_setting,
        }
    }

    async fn get_login_result(
        &mut self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> LoginResult {
        self.factory.login_handler().execute(reader_writer).await
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

impl PreloginHandler<'_, '_> {
    async fn execute(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> Result<PreloginResult, Error> {
        // Create the request.
        let request_model = PreloginRequestModel::new(
            Uuid::new_v4(),
            Option::from(self.factory.context.mars_enabled),
            Option::from(self.factory.context.encryption),
            Option::from(self.factory.context.database_instance.as_str()),
        );
        let prelogin_request = PreloginRequest {
            model: &request_model,
        };

        // Serialize it.
        prelogin_request.serialize(reader_writer).await?;

        // Return result (which contains data model).
        let response = PreloginResponse {};

        // TODO: Convert panics to Error objects.
        let response_model = &response.deserialize(reader_writer).await;
        if request_model.mars_enabled && !response_model.mars_enabled.unwrap() {
            panic!("Server does not support MARS.")
        }

        if !response_model.dbinstance_valid.unwrap() {
            // Non-fatal behaviour.
            eprintln!("Database instance validation failed.")
        }

        if request_model.encryption_setting == EncryptionSetting::Strict {
            return Ok(PreloginResult {
                encryption_setting: EncryptionSetting::Strict,
                is_fed_auth_supported: response_model.federated_auth_supported,
            });
        }

        if response_model.encryption == EncryptionType::NotSupported {
            panic!("Encryption is not supported.")
        }

        if request_model.encryption_setting == EncryptionSetting::Optional
            && response_model.encryption == EncryptionType::Off
        {
            return Ok(PreloginResult {
                encryption_setting: EncryptionSetting::LoginOnly,
                is_fed_auth_supported: response_model.federated_auth_supported,
            });
        }

        Ok(PreloginResult {
            encryption_setting: EncryptionSetting::Required,
            is_fed_auth_supported: response_model.federated_auth_supported,
        })
    }
}

struct LoginResult {
    supported_features: Vec<Box<dyn Feature>>,
    change_properties: EnvChangeProperties,
    status: LoginResponseStatus,
}

pub struct LoginHandler<'a, 'n> {
    factory: &'a HandlerFactory<'n>,
    encryption: EncryptionSetting,
}

impl LoginHandler<'_, '_> {
    async fn execute(&self, reader_writer: &mut impl NetworkReaderWriter) -> LoginResult {
        if self.encryption != EncryptionSetting::Strict
            && self.encryption != EncryptionSetting::NotSupported
        {
            todo!("Handle TDS 7.4 login");
        }

        let _request_model = self.send_login7_request(reader_writer).await;
        let login_response = self.get_login_response(reader_writer).await;

        // TODO Handle the response.
        let response_status = login_response.get_status();

        LoginResult {
            supported_features: vec![],
            change_properties: login_response.change_properties,
            status: response_status,
        }
    }

    async fn send_login7_request(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> Result<LoginRequestModel, Error> {
        let request = self.factory.create_login_request();
        let request_model = &request.model;

        if request_model.user_input.integrated_security() {
            todo!("Integrated security is not supported yet");
        }

        if self.encryption == EncryptionSetting::LoginOnly {
            todo!("TDS 7.4 implementation");
        } else {
            request.serialize(reader_writer).await?;
        }
        Ok(request.model)
    }

    async fn get_login_response(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> LoginResponseModel {
        let response = self.factory.create_login_response();
        response.deserialize(reader_writer).await
    }
}
