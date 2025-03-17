#[cfg(test)]
mod connectivity {

    use std::env;

    use azure_core::credentials::TokenCredential;

    use azure_identity::{DefaultAzureCredential, TokenCredentialOptions};
    use dotenv::dotenv;
    use futures::StreamExt;
    use tds_x::{
        connection::client_context::{ClientContext, TdsAuthenticationMethod},
        connection_provider::tds_connection_provider::TdsConnectionProvider,
        core::EncryptionSetting,
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
        // let azclicreds = AzureCliCredential::new();

        // Request the token. get_token expects a slice of scopes (strings).
        let token_response = credential.unwrap().get_token(&[SCOPE]).await;

        // println!(
        //     "Token response: {:?}",
        //     token_response.unwrap().token.secret()
        // );

        let secret = token_response.as_ref().unwrap().token.secret();
        print!("{}", secret);
        secret.to_string()
        // "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayIsImtpZCI6ImltaTBZMnowZFlLeEJ0dEFxS19UdDVoWUJUayJ9.eyJhdWQiOiJodHRwczovL2RhdGFiYXNlLndpbmRvd3MubmV0LyIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0L2U4ZjQ3NDFhLTgxN2EtNDAzYS1iMjhmLTIwMGQyYjA3ZDY1Ni8iLCJpYXQiOjE3NDEzMTI2OTEsIm5iZiI6MTc0MTMxMjY5MSwiZXhwIjoxNzQxMzE2ODc4LCJhY3IiOiIxIiwiYWlvIjoiQWNRQU8vOFpBQUFBVUV0K0N1UlRyODkzZjFEK1BvaFg2K0ZSWEtJdWR4T2pFMDlzMHliUUw5alpRaE53MGptN1l4TXk4enJHclBnSjJSNmJzNUpNSnRaYnM4dEZ5dStKNjNNZGxLS0ZacXUveFQzaGh5d2VHQzV4MDVkZksrQW1aVTI2VzZDZ0ljb25od0wrSzBBb1I0Z2R5QlhYSTU5eGZISTBDclhSUWFqTk4xc0M2ZTF3Q014dFdyLy8rU0JrbG5sZUFpMnY1RHR5eUkycU9TRlZGanp5M2NETnAxQmdBQzZxZFR0eDFNQkpVV1IrOVFyc1U5NFB6NGFXa2NJL21rbFBrbVlmYTMrbyIsImFsdHNlY2lkIjoiNTo6MTAwMzIwMDM2ODE2MTZFMiIsImFtciI6WyJmaWRvIiwicnNhIiwibWZhIl0sImFwcGlkIjoiMmZkOTA4YWQtMDY2NC00MzQ0LWI5YmUtY2QzZThiNTc0YzM4IiwiYXBwaWRhY3IiOiIwIiwiZW1haWwiOiJzaW5naHNhdXJhQG1pY3Jvc29mdC5jb20iLCJmYW1pbHlfbmFtZSI6IlNpbmdoIE1TRlQiLCJnaXZlbl9uYW1lIjoiU2F1cmFiaCIsImdyb3VwcyI6WyI3M2EwY2YyZC1iMjczLTQwZjEtOWZmZi02MGFmYmE2ZjE3ZjMiXSwiaG9tZV9vaWQiOiJkMDcxN2VjMi1mODRjLTQ0ODktOGYzMi1jMDg3YzZmY2U3ZjAiLCJpZHAiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDcvIiwiaWR0eXAiOiJ1c2VyIiwiaXBhZGRyIjoiMjAuMy4xOTAuMTE4IiwibmFtZSI6IlNhdXJhYmggU2luZ2giLCJvaWQiOiJkMDA4MjU4NC1kZWIwLTQxZTItYmI3Ny04ZDYwZGZmN2QwNjEiLCJwdWlkIjoiMTAwMzIwMDM2RUQ0MkU1QyIsInJoIjoiMS5BY29BR25UMDZIcUJPa0N5anlBTkt3ZldWdE1IS1FJYkRfZEl1dHdicHF1cmJXYjZBQnpLQUEuIiwic2NwIjoiVXNlci5SZWFkIiwic2lkIjoiYzQxOWI2MWItNGRlNi00MTMwLTllMjQtM2JhY2ViNmNiZWFiIiwic3ViIjoiMFBqWXhab3c1cWtkMGkwNnpZRm9iZVJFaHE3b2VPeFBleXB5MEZkM21fTSIsInRpZCI6ImU4ZjQ3NDFhLTgxN2EtNDAzYS1iMjhmLTIwMGQyYjA3ZDY1NiIsInVuaXF1ZV9uYW1lIjoic2luZ2hzYXVyYUBtaWNyb3NvZnQuY29tIiwidXRpIjoiYVM1ZkNTV05jVW13bnMtMGIwMUVBQSIsInZlciI6IjEuMCIsInhtc19pZHJlbCI6IjMyIDEifQ.L55j7j7Go9E75PzDIP7-xFmzELMfW70fyD2mc0xmRziq0Ip9lz-0SAxuJa5ScQW2EFHzinMUGDXy9L9YhaWJ3P4JcI8i7iVI5EQkC_PhdbJ5rTT7ROTaA65qZIdOVRLiGY7vix4BoHmXIEiIgb47xEw6blKMrAXIwB_g45rI47PXXcfGQ2nE4nPl-_sxLipj2Wtm-okKjgoY99IQDJAQyFWDUjw912x8HK0tRE8kjvOq9B-Wrs0Qv0-VG-LpjlPQJU3p5b9K0n5KkexIIuI4JQkoTJCHVT2x1577hPFnuM7u5_25w2J2EG5anqBqFXcpDgP3hM84EevGnALi1G62tg".to_string()
    }

    pub fn create_context(access_token: String) -> ClientContext {
        dotenv().ok();
        let trace_level = Level::DEBUG;
        let subscriber = FmtSubscriber::builder()
            .with_max_level(trace_level)
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("Setting default subscriber failed");

        ClientContext {
            server_name: env::var("DB_HOST").expect("DB_HOST environment variable not set"),
            port: 1433,
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
        let context = create_context(access_token);
        let provider = TdsConnectionProvider {};
        let connection_result = provider.create_connection(&context).await;
        let mut connection = connection_result.unwrap();
        let command = "select 1".to_string();
        let result = connection.execute(command).await.unwrap();
        let mut stream = result.stream_results();
        while let Some(qrt) = stream.next().await {
            let res = qrt.unwrap();
            match res {
                QueryResultType::ResultSet(rs) => {
                    let mut row_stream = rs.into_row_stream().await.unwrap();
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
}
