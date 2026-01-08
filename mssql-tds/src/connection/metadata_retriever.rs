// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Metadata retrieval for bulk copy operations.
//!
//! This module provides traits and implementations for retrieving table metadata
//! from SQL Server. The metadata is used to automatically map columns and validate
//! types during bulk copy operations.
//!
//! # Implementations
//!
//! - [`FmtOnlyMetadataRetriever`]: Uses `SET FMTONLY ON` for fast metadata retrieval
//!
//! # Custom Retrievers
//!
//! You can implement the [`MetadataRetriever`] trait to provide custom metadata
//! retrieval strategies, such as caching or alternative sources.

use crate::connection::tds_client::TdsClient;
use crate::core::{CancelHandle, TdsResult};
use crate::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
use crate::error::Error;
use crate::sql_identifier::{
    CATALOG_INDEX, SCHEMA_INDEX, TABLE_INDEX, build_multipart_name, escape_identifier,
    escape_string_literal, parse_multipart_identifier,
};
use crate::token::tokens::ColMetadataToken;
use async_trait::async_trait;
use tracing::{debug, instrument, trace};

/// Result of fetching table metadata, including collation information.
#[derive(Debug, Clone)]
pub(crate) struct TableMetadataResult {
    /// Column metadata from the COLMETADATA token
    pub col_metadata: ColMetadataToken,
    /// Collation names for each column (index matches column order)
    /// Only populated for character type columns
    pub collation_names: Vec<Option<String>>,
}

/// Trait for retrieving destination table metadata.
///
/// This trait allows different strategies for retrieving table metadata,
/// such as querying system catalogs, caching, or using alternative sources.
///
/// # Example
///
/// ```rust,ignore
/// use mssql_tds::connection::metadata_retriever::{MetadataRetriever};
/// use mssql_tds::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
///
/// struct CachedMetadataRetriever {
///     cache: HashMap<String, Vec<BulkCopyColumnMetadata>>,
/// }
///
/// #[async_trait]
/// impl MetadataRetriever for CachedMetadataRetriever {
///     async fn retrieve_metadata(
///         &mut self,
///         client: &mut TdsClient,
///         table_name: &str,
///         timeout_sec: u32,
///     ) -> TdsResult<Vec<BulkCopyColumnMetadata>> {
///         if let Some(metadata) = self.cache.get(table_name) {
///             return Ok(metadata.clone());
///         }
///         // Fallback to default retrieval...
///     }
/// }
/// ```
#[async_trait]
pub trait MetadataRetriever: Send {
    /// Retrieve metadata for the specified table.
    ///
    /// # Arguments
    ///
    /// * `client` - Mutable reference to the TDS client for executing queries
    /// * `table_name` - Name of the destination table (may include schema)
    /// * `timeout_sec` - Query timeout in seconds
    ///
    /// # Returns
    ///
    /// A vector of `BulkCopyColumnMetadata` containing column information
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table does not exist
    /// - No permission to access metadata
    /// - Network errors during query execution
    async fn retrieve_metadata(
        &mut self,
        client: &mut TdsClient,
        table_name: &str,
        timeout_sec: u32,
    ) -> TdsResult<Vec<BulkCopyColumnMetadata>>;
}

/// Metadata retriever using SET FMTONLY ON query.
///
/// This implementation retrieves table metadata by executing `SET FMTONLY ON; SELECT * FROM table`
/// which returns only the COLMETADATA token without any rows. This is faster than
/// querying system catalog views and provides complete metadata including:
/// - Column names, types, nullability
/// - Identity and computed column flags (from TDS COLMETADATA flags)
/// - Precision, scale, and collation information
///
/// # Advantages
///
/// - Fast: Single round-trip with SET FMTONLY
/// - Works with any user permissions that allow SELECT on the table
/// - Handles temporal tables and SQL Graph columns correctly (via dynamic column list)
///
/// # When to use
///
/// Use this retriever when:
/// - You need fast metadata retrieval
/// - You have SELECT permission but not sys.columns access
/// - Performance is critical for bulk operations
///
/// # Example
///
/// ```rust,ignore
/// let retriever = FmtOnlyMetadataRetriever::new();
/// let bulk_copy = BulkCopy::with_retriever(
///     &mut client,
///     "MyTable",
///     Box::new(retriever)
/// );
/// ```
#[derive(Debug, Default)]
pub struct FmtOnlyMetadataRetriever;

impl FmtOnlyMetadataRetriever {
    /// Create a new FMTONLY metadata retriever.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl MetadataRetriever for FmtOnlyMetadataRetriever {
    async fn retrieve_metadata(
        &mut self,
        client: &mut TdsClient,
        table_name: &str,
        timeout_sec: u32,
    ) -> TdsResult<Vec<BulkCopyColumnMetadata>> {
        // Fetch metadata using SET FMTONLY ON and sp_tablecollations_100
        let metadata_result =
            fetch_table_metadata(client, table_name, Some(timeout_sec), None).await?;

        // Convert using TryFrom trait - directly to BulkCopyColumnMetadata
        Vec::<BulkCopyColumnMetadata>::try_from(metadata_result)
    }
}

impl TryFrom<TableMetadataResult> for Vec<BulkCopyColumnMetadata> {
    type Error = Error;

    fn try_from(result: TableMetadataResult) -> Result<Self, Self::Error> {
        if result.col_metadata.columns.is_empty() {
            return Err(Error::UsageError(
                "Table not found or has no columns".to_string(),
            ));
        }

        // Convert each column using the existing From<&ColumnMetadata> implementation
        let mut metadata: Vec<BulkCopyColumnMetadata> = result
            .col_metadata
            .columns
            .iter()
            .map(BulkCopyColumnMetadata::from)
            .collect();

        // Add collation names to the metadata
        for (i, col_meta) in metadata.iter_mut().enumerate() {
            if i < result.collation_names.len() {
                col_meta.collation_name = result.collation_names[i].clone();
            }
        }

        Ok(metadata)
    }
}

impl TryFrom<ColMetadataToken> for Vec<BulkCopyColumnMetadata> {
    type Error = Error;

    fn try_from(col_metadata_token: ColMetadataToken) -> Result<Self, Self::Error> {
        if col_metadata_token.columns.is_empty() {
            return Err(Error::UsageError(
                "Table not found or has no columns".to_string(),
            ));
        }

        // Convert each column using the existing From<&ColumnMetadata> implementation
        let metadata = col_metadata_token
            .columns
            .iter()
            .map(BulkCopyColumnMetadata::from)
            .collect();

        Ok(metadata)
    }
}

/// Fetch table metadata using SET FMTONLY ON query.
///
/// This function retrieves column metadata for a table by executing a
/// query with `SET FMTONLY ON`, which returns only the COLMETADATA
/// token without any row data. It also fetches collation information
/// by calling sp_tablecollations_100.
///
/// # Arguments
///
/// * `client` - Mutable reference to the TDS client
/// * `table_name` - Name of the table, supporting multipart names:
///   - 1-part: "table" (uses current database and default schema)
///   - 2-part: "schema.table"
///   - 3-part: "database.schema.table"
///   - 4-part: "server.database.schema.table" (server part ignored in query context)
/// * `timeout_sec` - Query timeout in seconds
/// * `cancel_handle` - Optional cancellation handle
///
/// # Returns
///
/// A `TableMetadataResult` containing column metadata and collation names.
///
/// # Errors
///
/// Returns an error if:
/// - Table does not exist
/// - No permission to access the table
/// - Network errors during query execution
/// - Invalid multipart identifier format
///
/// # Implementation Notes
///
/// This function is designed for bulk copy operations where we need table schema
/// without the overhead of querying system catalog views. It dynamically builds the column
/// list to:
/// - Support hidden columns in temporal tables
/// - Exclude SQL Graph columns that cannot be selected
/// - Handle multipart table names with proper catalog prefix
/// - Retrieve collation names for character columns via sp_tablecollations_100
#[instrument(skip(client), level = "info")]
pub(crate) async fn fetch_table_metadata(
    client: &mut TdsClient,
    table_name: &str,
    timeout_sec: Option<u32>,
    cancel_handle: Option<&CancelHandle>,
) -> TdsResult<TableMetadataResult> {
    // Parse the multipart identifier
    let parts = parse_multipart_identifier(table_name, false)?;

    // Validate table name exists
    let table_part = parts[TABLE_INDEX]
        .as_ref()
        .ok_or_else(|| Error::UsageError(format!("Invalid table name: {}", table_name)))?;

    // Check if temp table
    let is_temp_table = table_part.starts_with('#');

    // Determine catalog
    let catalog = if is_temp_table && parts[CATALOG_INDEX].is_none() {
        "tempdb".to_string()
    } else if let Some(cat) = &parts[CATALOG_INDEX] {
        escape_identifier(cat)
    } else {
        // No catalog specified, don't prefix
        String::new()
    };

    // Build full object name for OBJECT_ID
    let full_name = build_multipart_name(&parts);
    let escaped_full_name = escape_string_literal(&full_name);

    // Build query with catalog prefix
    let catalog_prefix = if !catalog.is_empty() {
        format!("{}.", catalog)
    } else {
        String::new()
    };

    // Prepare schema and table names for sp_tablecollations_100
    // Match C# behavior: escape for use in TSQL literal block
    let schema_name = parts[SCHEMA_INDEX]
        .as_ref()
        .map(|s| escape_identifier(&escape_string_literal(s)))
        .unwrap_or_else(|| "dbo".to_string());

    let table_name_escaped = escape_identifier(&escape_string_literal(table_part));

    // Use SET FMTONLY ON to get metadata without query execution overhead.
    // This matches .NET SqlBulkCopy behavior and is more efficient than SELECT TOP 0.
    // It also dynamically builds the column list to:
    // - Support hidden columns in temporal tables
    // - Exclude SQL Graph columns that cannot be selected
    // - Use catalog prefix to query correct database
    //
    // Additionally, we call sp_tablecollations_100 to retrieve collation names
    // for character type columns (matches .NET SqlBulkCopy behavior).
    let query = format!(
        r#"DECLARE @Column_Names NVARCHAR(MAX) = NULL;
IF EXISTS (SELECT TOP 1 * FROM sys.all_columns WHERE [object_id] = OBJECT_ID('sys.all_columns') AND [name] = 'graph_type')
BEGIN
    SELECT @Column_Names = COALESCE(@Column_Names + ', ', '') + QUOTENAME([name]) 
    FROM {catalog_prefix}sys.all_columns 
    WHERE [object_id] = OBJECT_ID('{escaped_full_name}') 
    AND COALESCE([graph_type], 0) NOT IN (1, 3, 4, 6, 7) 
    ORDER BY [column_id] ASC;
END
ELSE
BEGIN
    SELECT @Column_Names = COALESCE(@Column_Names + ', ', '') + QUOTENAME([name]) 
    FROM {catalog_prefix}sys.all_columns 
    WHERE [object_id] = OBJECT_ID('{escaped_full_name}') 
    ORDER BY [column_id] ASC;
END

SELECT @Column_Names = COALESCE(@Column_Names, '*');

SET FMTONLY ON;
EXEC(N'SELECT ' + @Column_Names + N' FROM {escaped_full_name}');
SET FMTONLY OFF;

EXEC {catalog_prefix}sp_tablecollations_100 N'{schema_name}.{table_name_escaped}';"#
    );

    debug!("Fetching table metadata with FMTONLY and collations");

    // Execute the query
    client.execute(query, timeout_sec, cancel_handle).await?;

    // Get the metadata from the first result set (FMTONLY result)
    let col_metadata = client
        .get_current_metadata()
        .ok_or_else(|| {
            Error::UsageError(format!("Failed to fetch metadata for table {table_name}"))
        })?
        .clone();

    debug!(
        "Fetched {} columns from table metadata",
        col_metadata.columns.len()
    );
    for (i, col) in col_metadata.columns.iter().enumerate() {
        trace!(
            "Column {}: name='{}', tds_type=0x{:02X}, nullable={}",
            i,
            col.column_name,
            col.data_type as u8,
            col.is_nullable()
        );
    }

    // Move to the next result set (collations from sp_tablecollations_100)
    // This will consume the DONE token from the first result set and read until
    // the next COLMETADATA token is encountered
    client.move_to_column_metadata().await?;

    // Read collation names from the result set
    // sp_tablecollations_100 returns columns: TableName, ColumnName, column_id, collation_name
    // We need the collation_name (column index 3)
    let mut collation_names: Vec<Option<String>> = Vec::new();

    while let Some(row) = client.get_next_row().await? {
        // Column 3 (0-indexed) is the collation_name
        if row.len() > 3 {
            let collation_value = &row[3];
            let collation_name = match collation_value {
                crate::datatypes::column_values::ColumnValues::String(s) => Some(s.to_string()),
                crate::datatypes::column_values::ColumnValues::Null => None,
                _ => {
                    trace!("Unexpected collation value type: {:?}", collation_value);
                    None
                }
            };

            if let Some(name) = &collation_name {
                trace!("Column {} collation: {}", collation_names.len(), name);
            }

            collation_names.push(collation_name);
        } else {
            // If row doesn't have enough columns, push None
            collation_names.push(None);
        }
    }

    debug!("Fetched {} collation names", collation_names.len());

    // Ensure we have the same number of collations as columns
    // If not, pad with None
    while collation_names.len() < col_metadata.columns.len() {
        collation_names.push(None);
    }

    // Close the query to free up the connection
    client.close_query().await?;

    Ok(TableMetadataResult {
        col_metadata,
        collation_names,
    })
}
