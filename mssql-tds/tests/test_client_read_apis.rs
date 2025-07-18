// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod client_based_iterators {
    use crate::common::{create_context, init_tracing};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test]
    async fn test_multiquery_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let query = "SELECT TOP(2) * FROM sys.databases; SELECT 1";

        client.execute(query.to_string(), None, None).await?;
        let mut row_count = 0;
        loop {
            while client.next_row().await?.is_some() {
                row_count += 1;
            }

            if !client.move_to_next().await? {
                break;
            }
        }
        assert_eq!(
            row_count, 3,
            "Expected 3 rows from the multi-query execution"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_incomplete_resultset_iteration() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, None).await?;
        let query = "SELECT TOP(2) * FROM sys.databases; SELECT 1";

        client.execute(query.to_string(), None, None).await?;
        let mut row_count = 0;

        if client.next_row().await?.is_some() {
            row_count += 1;
        }
        client.close_query().await?;

        assert_eq!(
            row_count, 1,
            "Expected 1 row from the incomplete result set execution"
        );
        let mut row_count = 0;
        client.execute(query.to_string(), None, None).await?;
        loop {
            while client.next_row().await?.is_some() {
                row_count += 1;
            }
            if !client.move_to_next().await? {
                break;
            }
        }

        client.close_query().await?;
        assert_eq!(
            row_count, 3,
            "Expected 3 rows from the multi-query execution on connection reuse."
        );

        Ok(())
    }
}
