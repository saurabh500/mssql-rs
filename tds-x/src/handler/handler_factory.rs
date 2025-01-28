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

    fn create_session_settings(
        &self,
        _supported_features: Vec<Box<dyn Feature>>,
    ) -> SessionSettings {
        SessionSettings {}
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

#[derive(Clone)]
pub(crate) struct SessionSettings {
    //TODO
    // supported_features: Vec<&'a dyn Feature>,
}

impl SessionSettings {
    fn update_settings(&self, _change_properties: &EnvChangeProperties) {
        // todo!()
        // Need to populate this information.
    }
}

pub(crate) struct SessionHandler<'a, 'n> {
    pub(crate) factory: &'a HandlerFactory<'n>,
}

impl SessionHandler<'_, '_> {
    pub(crate) async fn execute(
        &mut self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> SessionSettings {
        let pre_login_result = self.get_pre_login_result(reader_writer).await;
        self.validate_and_apply_prelogin_result(pre_login_result);
        let login_result = self.get_login_result(reader_writer).await;
        self.validate_and_apply_login_result(&login_result);

        let settings = self
            .factory
            .create_session_settings(login_result.supported_features);
        settings.update_settings(&login_result.change_properties);

        settings
    }

    async fn get_pre_login_result(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> PreloginResult {
        self.factory.prelogin_handler().execute(reader_writer).await
    }

    fn validate_and_apply_login_result(&self, _login_result: &LoginResult) {
        // todo!("save the results from login response");
        // var changeProps = loginResult.ChangeProperties;

        // if (changeProps.PacketSize > 0)
        // {
        //     this.clientContext.PacketSize = (short)loginResult.ChangeProperties.PacketSize;
        // }

        // if (!string.IsNullOrEmpty(changeProps.Language))
        // {
        //     this.clientContext.Language = changeProps.Language;
        // }

        // if (!string.IsNullOrEmpty(changeProps.Database))
        // {
        //     this.clientContext.Database = changeProps.Database;
        // }
    }

    fn validate_and_apply_prelogin_result(&self, _prelogin_result: PreloginResult) {
        // todo!("save the encryption settings");
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
    async fn execute(&self, reader_writer: &mut impl NetworkReaderWriter) -> PreloginResult {
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
        prelogin_request.serialize(reader_writer).await;

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
            request.serialize(reader_writer).await;
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
