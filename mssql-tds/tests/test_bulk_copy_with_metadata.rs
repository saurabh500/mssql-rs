// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod bulk_copy_with_metadata_tests {
    use crate::common::{begin_connection, create_context, init_tracing};
    use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient};
    use mssql_tds::datatypes::column_values::ColumnValues;
    use mssql_tds::datatypes::sql_string::SqlString;
    use mssql_tds::message::bulk_load::BulkLoadMessage;
    use mssql_tds::connection::bulk_copy::BulkCopyOptions;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_bulk_copy_with_server_metadata() {
        let context = create_context();
        let mut client = begin_connection(context).await;

        // Drop table if exists
        client
            .execute(
                "IF OBJECT_ID('dbo.BulkCopyMetadataTest', 'U') IS NOT NULL DROP TABLE dbo.BulkCopyMetadataTest"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to drop test table");
        client.close_query().await.expect("Failed to close query");

        // Create real table (not temp table for better debugging)
        client
            .execute(
                "CREATE TABLE dbo.BulkCopyMetadataTest (
                    id INT NOT NULL,
                    name NVARCHAR(100) NOT NULL,
                    age SMALLINT NOT NULL,
                    active BIT NOT NULL
                )"
                .to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to create test table");

        // Close the query to free up the connection
        client.close_query().await.expect("Failed to close query");

        // Fetch metadata from the server
        let table_metadata = client.fetch_table_metadata("dbo.BulkCopyMetadataTest", None, None)
            .await
            .expect("Failed to fetch table metadata");

        println!("DEBUG: Fetched metadata for {} columns", table_metadata.columns.len());
        
        // Convert server metadata to bulk copy metadata
        let bulk_copy_metadata: Vec<_> = table_metadata
            .columns
            .iter()
            .map(|col| col.into())
            .collect();

        println!("DEBUG: Converted to bulk copy metadata:");
        for (i, meta) in bulk_copy_metadata.iter().enumerate() {
            println!("  Column {}: name='{}', tds_type=0x{:02X}, sql_type={:?}", 
                i, meta.column_name, meta.tds_type, meta.sql_type);
        }

        // Prepare test data
        let rows = vec![
            vec![
                ColumnValues::Int(1),
                ColumnValues::String(SqlString::from_utf8_string("Alice".to_string())),
                ColumnValues::SmallInt(30),
                ColumnValues::Bit(true),
            ],
            vec![
                ColumnValues::Int(2),
                ColumnValues::String(SqlString::from_utf8_string("Bob".to_string())),
                ColumnValues::SmallInt(25),
                ColumnValues::Bit(false),
            ],
            vec![
                ColumnValues::Int(3),
                ColumnValues::String(SqlString::from_utf8_string("Charlie".to_string())),
                ColumnValues::SmallInt(35),
                ColumnValues::Bit(true),
            ],
        ];

        // Create bulk load message with server-provided metadata
        let message = BulkLoadMessage::new(
            "dbo.BulkCopyMetadataTest".to_string(),
            bulk_copy_metadata,
            rows,
            BulkCopyOptions::default(),
        );

        // Execute bulk copy
        let rows_affected = client.execute_bulk_load(message, None, None)
            .await
            .expect("Bulk copy failed");

        println!("DEBUG: Bulk copy completed, rows_affected: {}", rows_affected);
        
        // Check actual row count in database before assertion
        client
            .execute(
                "SELECT COUNT(*) as cnt FROM dbo.BulkCopyMetadataTest".to_string(),
                None,
                None,
            )
            .await
            .expect("Failed to count rows");
        
        if let Some(resultset) = client.get_current_resultset() {
            if let Some(row) = resultset.next_row().await.expect("Failed to read count") {
                println!("DEBUG: Actual rows in database: {:?}", row[0]);
            }
        }
        client.close_query().await.expect("Failed to close count query");
        
        assert_eq!(rows_affected, 3, "Expected 3 rows to be inserted");
    }
}
