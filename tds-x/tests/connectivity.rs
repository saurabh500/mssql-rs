#[cfg(test)]
mod common;

mod connectivity {

    use std::env;

    use azure_core::credentials::TokenCredential;

    use crate::common::{create_context, get_scalar_value};
    use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
    use dotenv::dotenv;
    use futures::StreamExt;
    use tds_x::{
        connection::client_context::{ClientContext, TdsAuthenticationMethod},
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        core::EncryptionSetting,
        datatypes::decoder::ColumnValues,
        query::result::QueryResultType,
    };
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    // The scope we want an access token for.
    // For Azure SQL Database, the usual resource is "https://database.windows.net/.default".
    const SCOPE: &str = "https://database.windows.net/.default";

    // #[tokio::test]
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

    pub fn create_context_with_accesstoken(access_token: String) -> ClientContext {
        dotenv().ok();
        println!("This test expects that `az login --tenant E8F4741A-817A-403A-B28F-200D2B07D656` was run to get a token.");
        let trace_level = Level::DEBUG;
        let subscriber = FmtSubscriber::builder()
            .with_max_level(trace_level)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Setting default subscriber failed");

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

    #[tokio::test]
    pub async fn select_1() {
        let access_token = generate_access_token().await;
        let context = create_context_with_accesstoken(access_token);
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

    #[tokio::test]
    pub async fn validate_host_name() {
        let context = create_context();
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(&context).await;
        let mut connection = connection_result.unwrap();
        let command =
            "select host_name from sys.dm_exec_sessions where client_interface_name = 'TdsX'"
                .to_string();
        let result = connection.execute(command).await.unwrap();
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
}
