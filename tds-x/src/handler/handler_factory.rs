use crate::connection::client_context::ClientContext;
use crate::connection::transport::network_transport::NetworkTransport;
use crate::connection::transport::packet::PacketWriter;

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
            model: &PreloginResponseModel {},
        }
    }
}

pub struct LoginHandler<'a> {
    factory: &'a HandlerFactory<'a>,
}

impl<'a> LoginHandler<'a> {
    fn execute(&self) -> LoginResponse {
        LoginResponse {
            model: &LoginResponseModel {},
        }
    }
}

pub struct PreloginRequestModel {}

pub struct PreloginResponseModel {}

pub struct PreloginRequest<'a> {
    pub packet_generator: &'a PacketWriter,
    pub model: &'a PreloginRequestModel,
}

impl<'a> PreloginRequest<'a> {
    fn serialize(&self, _transport: &dyn NetworkTransport) {}
}

pub struct PreloginResponse<'a> {
    pub model: &'a PreloginResponseModel,
}

impl<'a> PreloginResponse<'a> {
    fn deserialize(_transport: &dyn NetworkTransport) -> PreloginResponseModel {
        PreloginResponseModel {}
    }
}

pub struct LoginRequestModel {}

pub struct LoginResponseModel {}

pub struct LoginRequest<'a> {
    pub packet_generator: &'a PacketWriter,
    pub model: &'a LoginRequestModel,
}

impl<'a> LoginRequest<'a> {
    fn serialize(&self, _transport: &dyn NetworkTransport) {}
}

pub struct LoginResponse<'a> {
    pub model: &'a LoginResponseModel,
}

impl<'a> LoginResponse<'a> {
    fn deserialize(&self, _transport: &dyn NetworkTransport) -> LoginResponseModel {
        LoginResponseModel {}
    }
}
