// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod client_based_iterators {
    use crate::common::{build_tcp_datasource, create_context, init_tracing};
    use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
    use mssql_tds::message::transaction_management::TransactionIsolationLevel;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test]
    async fn test_transaction_begin() -> Result<(), Box<dyn std::error::Error>> {
        let context = create_context();

        let provider = TdsConnectionProvider {};
        let mut client = provider.create_client(context, &build_tcp_datasource(), None).await?;
        client
            .begin_transaction(TransactionIsolationLevel::ReadCommitted, None)
            .await?;

        client.commit_transaction(None, None).await?;
        Ok(())
    }
}
