use crate::core::EncryptionSetting;
use crate::message::login_options::{ApplicationIntent, TdsVersion};
use hostname;

#[derive(PartialEq, Copy, Clone)]
pub enum IPAddressPreference {
    IPv4First = 0,
    IPv6First = 1,
    UsePlatformDefault = 2,
}

pub enum NetworkTracerOutput {
    File = 0,
    Console = 1,
}

#[derive(PartialEq, Copy, Clone)]
pub enum TdsAuthenticationMethod {
    Password,
    SSPI, // Integrated Authentication with AD.
    ActiveDirectoryPassword,
    ActiveDirectoryInteractive,
    ActiveDirectoryDeviceCodeFlow,
    ActiveDirectoryServicePrincipal,
    ActiveDirectoryManagedIdentity,
    ActiveDirectoryMSI,
    ActiveDirectoryDefault,
    ActiveDirectoryWorkloadIdentity,
    ActiveDirectoryIntegrated,
    AccessToken,
}

pub struct ClientContext {
    pub application_intent: ApplicationIntent,
    pub application_name: String,
    pub attach_db_file: String,
    pub change_password: String,
    pub connect_retry_count: u32,
    pub connect_timeout: u32,
    pub database: String,
    pub database_instance: String,
    pub enlist: bool,
    pub encryption: EncryptionSetting,
    pub failover_partner: String,
    pub ipaddress_preference: IPAddressPreference,
    pub language: String,
    pub library_name: String,
    pub mars_enabled: bool,
    pub network_tracer_enabled: bool,
    pub network_tracer_output: NetworkTracerOutput,
    pub new_password: String,
    pub packet_size: i16,
    pub password: String,
    pub pooling: bool,
    pub replication: bool,
    pub tds_authentication_method: TdsAuthenticationMethod,
    pub user_instance: bool,
    pub user_name: String,
    pub workstation_id: String,
    pub access_token: Option<String>,
    pub transport_context: TransportContext,
}

impl ClientContext {
    pub fn new() -> ClientContext {
        ClientContext {
            application_intent: ApplicationIntent::ReadWrite,
            application_name: "TDSX Rust Client".to_string(),
            attach_db_file: "".to_string(),
            change_password: "".to_string(),
            connect_retry_count: 0,
            connect_timeout: 15,
            database: "".to_string(),
            database_instance: "MSSQLServer".to_string(),
            enlist: false,
            encryption: EncryptionSetting::Strict,
            failover_partner: "".to_string(),
            ipaddress_preference: IPAddressPreference::UsePlatformDefault,
            language: "us_english".to_string(),
            library_name: "TdsX".to_string(),
            mars_enabled: false,
            network_tracer_enabled: false,
            network_tracer_output: NetworkTracerOutput::Console,
            new_password: "".to_string(),
            packet_size: 8000,
            password: "".to_string(),
            pooling: false,
            replication: false,
            tds_authentication_method: TdsAuthenticationMethod::Password,
            user_instance: false,
            user_name: "".to_string(),
            workstation_id: ClientContext::default_workstation_id(hostname::get),
            access_token: None,
            transport_context: TransportContext::Tcp {
                host: "localhost".to_string(),
                port: 1433,
            },
        }
    }

    pub fn integrated_security(&self) -> bool {
        matches!(
            self.tds_authentication_method,
            TdsAuthenticationMethod::SSPI
        )
    }

    pub fn tds_version(&self) -> TdsVersion {
        if matches!(self.encryption, EncryptionSetting::Strict) {
            TdsVersion::V8_0
        } else {
            TdsVersion::V7_4
        }
    }
}

impl Default for ClientContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientContext {
    /// Generates a default workstation ID based on the hostname.
    /// If the hostname is longer than 128 characters, it truncates it to 128 characters.
    /// This function is used to ensure that the workstation ID does not exceed the maximum length
    /// allowed by the server.
    fn default_workstation_id<F>(get_hostname: F) -> String
    where
        F: Fn() -> Result<std::ffi::OsString, std::io::Error>,
    {
        let hostname = get_hostname()
            .unwrap_or_else(|_| "".into())
            .to_string_lossy()
            .to_string();
        if hostname.len() > 128 {
            hostname[..128].to_string()
        } else {
            hostname
        }
    }
}

#[derive(PartialEq, Clone)]
pub enum TransportContext {
    Tcp { host: String, port: u16 },
    NamedPipe { pipe_name: String },
    SharedMemory,
}

impl TransportContext {
    pub fn get_server_name(&self) -> &String {
        match self {
            TransportContext::Tcp { host, .. } => host,
            _ => {
                unimplemented!("Transport is not TCP");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_workstation_id_truncation() {
        // Simulate a long hostname
        let long_hostname = "a".repeat(150);
        let truncated_hostname = long_hostname[..128].to_string();

        // Test the default_workstation_id function with a mock closure
        let result = ClientContext::default_workstation_id(|| {
            Ok(std::ffi::OsString::from(long_hostname.clone()))
        });
        assert_eq!(result, truncated_hostname);
    }
}
