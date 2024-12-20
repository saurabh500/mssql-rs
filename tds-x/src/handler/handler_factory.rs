use crate::connection::client_context::ClientContext;
use crate::connection::transport::network_transport::NetworkTransport;
use crate::core::{SQLServerVersion, Version};
use crate::message::login::{
    EnvChangeProperties, FeaturesRequest, LoginResponse, LoginResponseModel, RoutingInfo,
    SqlCollation,
};
use crate::message::prelogin::{EncryptionType, PreloginResponse, PreloginResponseModel};

pub struct HandlerFactory<'a> {
    context: &'a ClientContext,
    transport: &'a dyn NetworkTransport,
}

impl<'a> HandlerFactory<'a> {
    fn prelogin_handler(&self) -> PreloginHandler {
        PreloginHandler { factory: self }
    }

    fn login_handler(&self) -> LoginHandler {
        LoginHandler { factory: self }
    }
}

pub struct PreloginHandler<'a> {
    factory: &'a HandlerFactory<'a>,
}

impl<'a> PreloginHandler<'a> {
    fn execute(&self) -> PreloginResponse {
        // Create the request.
        // Serialize it.
        // Return result (which contains data model).
        PreloginResponse {
            model: PreloginResponseModel {
                encryption: EncryptionType::Off,
                federated_auth_supported: false,
                dbinstance_valid: None,
                mars_enabled: None,
                server_version: Version {},
                sql_server_version: SQLServerVersion::SqlServerNotsupported,
            },
        }
    }
}

pub struct LoginHandler<'a> {
    factory: &'a HandlerFactory<'a>,
}

impl<'a> LoginHandler<'a> {
    fn execute(&self) -> LoginResponse {
        LoginResponse {
            model: LoginResponseModel {
                change_properties: EnvChangeProperties {
                    database_collation: SqlCollation {},
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
