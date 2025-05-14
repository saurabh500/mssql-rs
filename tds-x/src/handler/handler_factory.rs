use crate::connection::client_context::{ClientContext, TransportContext};
use crate::core::{EncryptionSetting, NegotiatedEncryptionSetting, TdsResult};
use crate::error::Error;
use crate::message::login::{
    EnvChangeProperties, Feature, FeaturesRequest, LoginRequest, LoginRequestModel, LoginResponse,
    LoginResponseModel, LoginResponseStatus,
};
use crate::message::messages::Request;
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

    pub(crate) fn login_handler(&self, prelogin_fedauth_supported: bool) -> LoginHandler<'_, '_> {
        LoginHandler {
            factory: self,
            prelogin_fedauth_supported,
        }
    }

    pub(crate) fn session_handler<'a>(
        &'a self,
        transport_context: &'a TransportContext,
    ) -> SessionHandler<'a, 'a, 'a> {
        SessionHandler {
            factory: self,
            transport_context,
        }
    }

    fn create_login_request<'a, 'b>(
        &'a self,
        prelogin_fedauth_supported: bool,
        transport_context: &'b TransportContext,
    ) -> LoginRequest<'a>
    where
        'b: 'a,
    {
        let model = self.create_login_model(prelogin_fedauth_supported, transport_context);
        LoginRequest { model }
    }

    fn create_login_model<'a, 'b>(
        &'a self,
        prelogin_fedauth_supported: bool,
        transport_context: &'b TransportContext,
    ) -> LoginRequestModel<'a>
    where
        'b: 'a,
    {
        LoginRequestModel::from_context(self.context, prelogin_fedauth_supported, transport_context)
    }

    fn create_login_response(&self) -> LoginResponse {
        LoginResponse::new()
    }
}

// The settings that can be negotiated during and after the login process as well.
pub(crate) struct NegotiatedSettings {
    pub session_settings: SessionSettings,
    pub database_collation: SqlCollation,
    pub language: String,
    pub database: String,
    pub char_set: Option<String>,
}

impl NegotiatedSettings {
    fn new(
        session_settings: SessionSettings,
        database_collation: SqlCollation,
        language: String,
        database: String,
        char_set: Option<String>,
    ) -> Self {
        NegotiatedSettings {
            session_settings,
            database_collation,
            language,
            database,
            char_set,
        }
    }

    fn update_settings(&mut self, change_properties: &EnvChangeProperties) {
        if change_properties.char_set.is_some() {
            self.char_set = change_properties.char_set.clone();
        }

        if change_properties.database_collation.is_some() {
            self.database_collation = change_properties.database_collation.unwrap();
        }

        if change_properties.language.is_some() {
            self.language = change_properties.language.clone().unwrap();
        }

        if change_properties.database.is_some() {
            self.database = change_properties.database.clone().unwrap();
        }
    }
}

// The settings of the session that are negotiated during the login process. They dont change after login.
pub(crate) struct SessionSettings {
    pub packet_size: u32,
    pub user_name: String,
    supported_features: Vec<Box<dyn Feature>>,
    mars_enabled: bool,
    pub pre_login_has_fedauth_supported: bool,
    pub encryption: NegotiatedEncryptionSetting,
}

impl SessionSettings {
    // Note that this destructively consumes the features list.
    fn new(
        context: &ClientContext,
        pre_login_has_fedauth_supported: bool,
        packet_size: u32,
        encryption: NegotiatedEncryptionSetting,
        features: &mut Vec<Box<dyn Feature>>,
    ) -> Self {
        let mut result = SessionSettings {
            packet_size,
            user_name: context.user_name.clone(),
            supported_features: vec![],
            mars_enabled: context.mars_enabled,
            pre_login_has_fedauth_supported,
            encryption,
        };
        result.supported_features.append(features);
        result
    }
}

pub(crate) struct SessionHandler<'a, 'b, 'n> {
    pub(crate) factory: &'a HandlerFactory<'n>,
    pub(crate) transport_context: &'b TransportContext,
}

impl<'a, 'b, 'n> SessionHandler<'a, 'b, 'n> {
    fn new(factory: &'a HandlerFactory<'n>, transport_context: &'b TransportContext) -> Self {
        SessionHandler {
            factory,
            transport_context,
        }
    }

    pub(crate) async fn execute(
        &mut self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> TdsResult<NegotiatedSettings> {
        let pre_login_result = self.get_pre_login_result(reader_writer).await?;
        self.validate_prelogin_result(&pre_login_result)?;

        // Note: This must happen before login because the login process can use the negotiated
        // encryption setting.
        reader_writer.notify_encryption_setting_change(pre_login_result.encryption_setting);

        let mut login_result = self
            .get_login_result(reader_writer, pre_login_result.is_fed_auth_supported)
            .await?;
        self.validate_login_result(&login_result)?;

        let negotiated_settings = self.negotiate_settings(&pre_login_result, &mut login_result);
        reader_writer.notify_session_setting_change(&negotiated_settings.session_settings);
        Ok(negotiated_settings)
    }

    async fn get_pre_login_result(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> TdsResult<PreloginResult> {
        let result = self
            .factory
            .prelogin_handler()
            .execute(reader_writer)
            .await?;
        Ok(result)
    }

    fn validate_prelogin_result(&self, _result: &PreloginResult) -> TdsResult<()> {
        // TBDif sever does not support fed auth and client expects fed auth then throw an exception.
        Ok(())
    }

    fn validate_login_result(&self, _result: &LoginResult) -> TdsResult<()> {
        // No validation currently.
        Ok(())
    }

    fn negotiate_settings(
        &self,
        prelogin_result: &PreloginResult,
        login_result: &mut LoginResult,
    ) -> NegotiatedSettings {
        let change_props = &login_result.change_properties;
        let packet_size = change_props.packet_size as u32;
        let session_settings = SessionSettings::new(
            self.factory.context,
            prelogin_result.is_fed_auth_supported,
            packet_size,
            prelogin_result.encryption_setting,
            &mut login_result.supported_features,
        );

        let database_collation = match change_props.database_collation {
            Some(ref collation) => *collation,
            None => unreachable!("Database collation shouldn't be empty after login."),
        };

        let database = match change_props.database {
            Some(ref db) => db.clone(),
            None => unreachable!("Database shouldn't be empty after login."),
        };

        NegotiatedSettings::new(
            session_settings,
            database_collation,
            "".to_string(),
            database,
            change_props.char_set.clone(),
        )
    }

    async fn get_login_result(
        &mut self,
        reader_writer: &mut impl NetworkReaderWriter,
        prelogin_fedauth_supported: bool,
    ) -> TdsResult<LoginResult> {
        self.factory
            .login_handler(prelogin_fedauth_supported)
            .execute(reader_writer, self.transport_context)
            .await
    }
}

struct PreloginResult {
    encryption_setting: NegotiatedEncryptionSetting,
    is_fed_auth_supported: bool,
}

impl PreloginResult {}

pub(crate) struct PreloginHandler<'a, 'n> {
    factory: &'a HandlerFactory<'n>,
}

impl PreloginHandler<'_, '_> {
    async fn execute(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
    ) -> TdsResult<PreloginResult> {
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

        // Serialize it. Note that the login process uses a timeout at a higher level than the request.
        let mut packet_writer = prelogin_request.create_packet_writer(reader_writer, None);
        prelogin_request.serialize(&mut packet_writer).await?;

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
            // If strict is used, the user is using TDS 8. The server's encryption response is
            // unused and the stream is already (and stays) encrypted.
            return Ok(PreloginResult {
                encryption_setting: NegotiatedEncryptionSetting::Strict,
                is_fed_auth_supported: response_model.federated_auth_supported,
            });
        }

        match &response_model.encryption {
            EncryptionType::Off => {
                // The _only_ way the server would have sent Off would be if the
                // client asked for it to be off. If the server supports
                // encryption it will return On and if not returns Unsupported.
                if request_model.encryption_setting == EncryptionSetting::PreferOff {
                    Ok(PreloginResult {
                        encryption_setting: NegotiatedEncryptionSetting::LoginOnly,
                        is_fed_auth_supported: response_model.federated_auth_supported,
                    })
                } else {
                    // For other user encryption settings, the server would have
                    assert!(request_model.encryption_setting == EncryptionSetting::Required);
                    panic!("Server disallowed encryption but client requires it.");
                }
            }
            EncryptionType::NotSupported => {
                if request_model.encryption_setting != EncryptionSetting::PreferOff {
                    Ok(PreloginResult {
                        encryption_setting: NegotiatedEncryptionSetting::Mandatory,
                        is_fed_auth_supported: response_model.federated_auth_supported,
                    })
                } else {
                    // For other user encryption settings, the server would have
                    assert!(request_model.encryption_setting == EncryptionSetting::Required);
                    panic!("Server does not support encryption but client requires it.");
                }
            }
            _ => Ok(PreloginResult {
                encryption_setting: NegotiatedEncryptionSetting::Mandatory,
                is_fed_auth_supported: response_model.federated_auth_supported,
            }),
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
    prelogin_fedauth_supported: bool,
}

impl LoginHandler<'_, '_> {
    async fn execute(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
        transport_context: &TransportContext,
    ) -> TdsResult<LoginResult> {
        let encryption = reader_writer.get_encryption_setting();
        if encryption != NegotiatedEncryptionSetting::Strict
            && encryption != NegotiatedEncryptionSetting::NoEncryption
        {
            // Note: We should not toggle encryption on if Strict is used because it is already,
            // and we shouldn't alter the streams.
            reader_writer.enable_ssl().await?;
        }

        let _request_model = self
            .send_login7_request(reader_writer, transport_context)
            .await?;
        let requested_features = _request_model.features_request;
        let login_response = self
            .get_login_response(reader_writer, requested_features)
            .await?;

        // TODO Handle the response.
        let response_status = login_response.get_status();

        Ok(LoginResult {
            supported_features: vec![],
            change_properties: login_response.change_properties,
            status: response_status,
        })
    }

    async fn send_login7_request<'a, 'b>(
        &'a self,
        reader_writer: &mut impl NetworkReaderWriter,
        transport_context: &'b TransportContext,
    ) -> TdsResult<LoginRequestModel<'a>>
    where
        'b: 'a,
    {
        let request = self
            .factory
            .create_login_request(self.prelogin_fedauth_supported, transport_context);
        let request_model: &LoginRequestModel = &request.model;

        if request_model.user_input.integrated_security() {
            todo!("Integrated security is not supported yet");
        }

        // Note that the login process uses a timeout at a higher level than the request.
        let mut packet_writer = request.create_packet_writer(reader_writer, None);
        request.serialize(&mut packet_writer).await?;
        Ok(request.model)
    }

    async fn get_login_response(
        &self,
        reader_writer: &mut impl NetworkReaderWriter,
        requested_features: FeaturesRequest,
    ) -> TdsResult<LoginResponseModel> {
        let response = self.factory.create_login_response();
        let response_model = response
            .deserialize(reader_writer, requested_features)
            .await?;
        if response_model.tds_error.is_some() {
            let tds_error = response_model.tds_error.unwrap();
            Err(Error::SqlServerError {
                message: tds_error.get_message(),
                state: tds_error.error_token.state,
                class: tds_error.error_token.severity as i32,
                number: tds_error.error_token.number,
                server_name: None,
                proc_name: None,
                line_number: None,
            })
        } else {
            Ok(response_model)
        }
    }
}
