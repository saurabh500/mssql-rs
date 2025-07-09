use tds_x::{
    connection::client_context::{ClientContext, TransportContext},
    core::{EncryptionOptions, EncryptionSetting},
};

#[napi(object)]
#[derive(Clone)]
pub struct JsClientContext {
    pub server_name: String,
    pub port: u16,
    pub user_name: String,
    pub password: String,
    pub database: String,
    pub trust_server_certificate: bool,
}

impl From<JsClientContext> for ClientContext {
    fn from(js_ctx: JsClientContext) -> Self {
        let encryption_options = EncryptionOptions {
            mode: EncryptionSetting::Required,
            trust_server_certificate: js_ctx.trust_server_certificate,
            host_name_in_cert: None,
        };
        println!("Creating ClientContext with server_name: {}, port: {}, user_name: {}, database: {}, trust_server_certificate: {}",
      js_ctx.server_name, js_ctx.port, js_ctx.user_name, js_ctx.database, encryption_options.trust_server_certificate);
        ClientContext {
            transport_context: TransportContext::Tcp {
                host: js_ctx.server_name,
                port: js_ctx.port,
            },
            user_name: js_ctx.user_name,
            password: js_ctx.password,
            database: js_ctx.database,
            encryption_options,
            ..Default::default()
        }
    }
}
