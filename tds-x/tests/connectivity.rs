#[cfg(test)]
mod common;

mod connectivity {

    use std::{collections::HashMap, env};

    use azure_core::credentials::TokenCredential;

    use crate::common::{create_context, get_scalar_value, init_tracing};
    use azure_identity::{
        DefaultAzureCredential, TokenCredentialOptions, VirtualMachineManagedIdentityCredential,
    };
    use dotenv::dotenv;
    use futures::StreamExt;
    use tds_x::{
        connection::client_context::{ClientContext, EntraIdTokenFactory, TdsAuthenticationMethod},
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        core::{EncryptionSetting, TdsResult},
        datatypes::decoder::ColumnValues,
        message::login_options::ApplicationIntent,
        query::result::QueryResultType,
    };

    // The scope we want an access token for.
    // For Azure SQL Database, the usual resource is "https://database.windows.net/.default".
    const SCOPE: &str = "https://database.windows.net/.default";

    async fn generate_access_token() -> String {
        // let azclicred = AzureCliCredential::new();

        let mut credential_options = TokenCredentialOptions::default();
        credential_options.set_authority_host(
            "https://login.windows.net/E8F4741A-817A-403A-B28F-200D2B07D656".to_string(),
        );

        let credential = DefaultAzureCredential::new();

        let token_response = credential.unwrap().get_token(&[SCOPE]).await;

        let secret = token_response.as_ref().unwrap().token.secret();
        print!("{}", secret);
        secret.to_string()
    }

    async fn generate_access_token_with_sts_and_resource(
        spn: String,
        sts: String,
        auth_method: &TdsAuthenticationMethod,
    ) -> String {
        // let azclicred = AzureCliCredential::new();
        let scopes = &[spn.as_ref()];
        let mut credential_options = TokenCredentialOptions::default();
        credential_options.set_authority_host(sts);
        let token_response = match auth_method {
            TdsAuthenticationMethod::Password => todo!(),
            TdsAuthenticationMethod::SSPI => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryPassword => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryInteractive => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryDeviceCodeFlow => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryServicePrincipal => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryManagedIdentity => {
                let vm_credential = VirtualMachineManagedIdentityCredential::new(
                    azure_identity::ImdsId::SystemAssigned,
                    credential_options,
                )
                .unwrap();
                vm_credential.get_token(scopes).await
            }
            TdsAuthenticationMethod::ActiveDirectoryDefault => {
                let credential = DefaultAzureCredential::new();
                credential.unwrap().get_token(scopes).await
            }
            TdsAuthenticationMethod::ActiveDirectoryMSI => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryWorkloadIdentity => todo!(),
            TdsAuthenticationMethod::ActiveDirectoryIntegrated => todo!(),
            TdsAuthenticationMethod::AccessToken => todo!(),
        };

        let secret = token_response.as_ref().unwrap().token.secret();
        print!("{}", secret);
        secret.to_string()
    }

    pub fn create_context_with_accesstoken(access_token: String) -> ClientContext {
        dotenv().ok();
        println!("This test expects that `az login --tenant E8F4741A-817A-403A-B28F-200D2B07D656` was run to get a token.");
        init_tracing();

        ClientContext {
            transport_context: tds_x::connection::client_context::TransportContext::Tcp {
                host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
                port: 1433,
            },
            // user_name: env::var("DB_USERNAME").expect("DB_USERNAME environment variable not set"),
            // password: env::var("SQL_PASSWORD").expect("SQL_PASSWORD environment variable not set"),
            database: "master".to_string(),
            encryption: EncryptionSetting::On,
            tds_authentication_method: TdsAuthenticationMethod::AccessToken,
            access_token: Some(access_token),
            ..Default::default()
        }
    }

    pub fn create_context_with_auth_method(auth_method: TdsAuthenticationMethod) -> ClientContext {
        dotenv().ok();
        init_tracing();
        let mut auth_method_map = HashMap::new();

        auth_method_map.insert(
            TdsAuthenticationMethod::ActiveDirectoryDefault,
            Box::new(DefaultEntraIdTokenFactory {}) as Box<dyn EntraIdTokenFactory>,
        );

        auth_method_map.insert(
            TdsAuthenticationMethod::ActiveDirectoryManagedIdentity,
            Box::new(DefaultEntraIdTokenFactory {}) as Box<dyn EntraIdTokenFactory>,
        );

        ClientContext {
            transport_context: tds_x::connection::client_context::TransportContext::Tcp {
                host: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
                port: 1433,
            },
            database: "master".to_string(),
            encryption: EncryptionSetting::On,
            tds_authentication_method: auth_method,
            auth_method_map,
            connect_timeout: 3600,
            ..Default::default()
        }
    }

    #[tokio::test]
    pub async fn select_1() {
        let access_token = generate_access_token().await;
        let context = create_context_with_accesstoken(access_token);
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(&context, None).await;
        let mut connection = connection_result.unwrap();
        let command = "select 1".to_string();
        let result = connection.execute(command, None, None).await.unwrap();
        let mut stream = result.stream_results();
        while let Some(qrt) = stream.next().await {
            let res = qrt.unwrap();
            match res {
                QueryResultType::ResultSet(rs) => {
                    let mut row_stream = rs.into_row_stream().unwrap();
                    while let Some(row) = row_stream.next().await {
                        let mut unwrapped_row = row.unwrap();
                        while let Some(cell) = unwrapped_row.next().await {
                            print!("{:?},", cell.unwrap().get_value());
                        }
                    }
                }
                _ => {
                    unreachable!("Shouldn't have reached here");
                }
            }
        }
    }

    #[tokio::test]
    pub async fn test_authentication_provider() {
        let context =
            create_context_with_auth_method(TdsAuthenticationMethod::ActiveDirectoryDefault);
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(&context).await;
        let mut connection = connection_result.unwrap();
        let command = "select 1".to_string();
        let result = connection.execute(command, None).await.unwrap();
        let mut stream = result.stream_results();
        while let Some(qrt) = stream.next().await {
            let res = qrt.unwrap();
            match res {
                QueryResultType::ResultSet(rs) => {
                    let mut row_stream = rs.into_row_stream().unwrap();
                    while let Some(row) = row_stream.next().await {
                        let mut unwrapped_row = row.unwrap();
                        while let Some(cell) = unwrapped_row.next().await {
                            print!("{:?},", cell.unwrap().get_value());
                        }
                    }
                }
                _ => {
                    unreachable!("Shouldn't have reached here");
                }
            }
        }
    }

    struct DefaultEntraIdTokenFactory {}

    #[async_trait::async_trait]
    impl EntraIdTokenFactory for DefaultEntraIdTokenFactory {
        async fn create_token(
            &self,
            _spn: String,
            _sts_url: String,
            auth_method: TdsAuthenticationMethod,
        ) -> TdsResult<Vec<u8>> {
            let spn = if !_spn.ends_with("/.default") {
                if _spn.ends_with('/') {
                    format!("{}.default", _spn)
                } else {
                    format!("{}/.default", _spn)
                }
            } else {
                _spn.clone()
            };
            let token =
                generate_access_token_with_sts_and_resource(spn, _sts_url, &auth_method).await;
            let utf16: Vec<u16> = token.encode_utf16().collect();
            let bytes: Vec<u8> = utf16.iter().flat_map(|u| u.to_le_bytes()).collect();
            Ok(bytes)
        }
    }

    #[tokio::test]
    pub async fn validate_host_name() {
        let context = create_context();
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(&context, None).await;
        let mut connection = connection_result.unwrap();
        let command =
            "select host_name from sys.dm_exec_sessions where client_interface_name = 'TdsX'"
                .to_string();
        let result = connection.execute(command, None, None).await.unwrap();
        let col_hostname = get_scalar_value(result).await.unwrap();
        if let Some(column_value) = col_hostname {
            match column_value {
                ColumnValues::String(value) => {
                    assert_eq!(value.to_utf8_string(), context.workstation_id);
                }
                _ => unreachable!("Expected a string value"),
            }
        } else {
            unreachable!("Expected a string value");
        }
    }

    #[tokio::test]
    pub async fn validate_app_intent_doesnt_cause_problems() {
        let mut context = create_context();
        context.application_intent = ApplicationIntent::ReadOnly;
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(&context, None).await;
        let mut connection = connection_result.unwrap();
        let command = "select 1".to_string();
        let result = connection.execute(command, None, None).await.unwrap();
        let col_hostname = get_scalar_value(result).await.unwrap();
        if let Some(column_value) = col_hostname {
            match column_value {
                ColumnValues::Int(value) => {
                    assert_eq!(value, 1);
                }
                _ => unreachable!("Expected a int value"),
            }
        } else {
            unreachable!("Expected a int value");
        }
    }
}
