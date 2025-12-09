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
//! - [`SystemCatalogRetriever`]: Queries `sys.columns` and `sys.objects` system views
//! - [`SelectTop0Retriever`]: Uses `SET FMTONLY ON` for faster metadata retrieval
//!
//! # Custom Retrievers
//!
//! You can implement the [`MetadataRetriever`] trait to provide custom metadata
//! retrieval strategies, such as caching or alternative sources.

use crate::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
use crate::core::{CancelHandle, TdsResult};
use crate::datatypes::bulk_copy_metadata::{
    BulkCopyColumnMetadata, SqlDbType, SystemTypeId, TypeLength,
};
use crate::datatypes::column_values::ColumnValues;
use crate::datatypes::sqldatatypes::{TdsDataType, TypeInfo, TypeInfoVariant};
use crate::error::Error;
use crate::token::tokens::{ColMetadataToken, SqlCollation};
use async_trait::async_trait;
use tracing::{debug, instrument, trace};

/// Trait for retrieving destination table metadata.
///
/// This trait allows different strategies for retrieving table metadata,
/// such as querying system catalogs, caching, or using alternative sources.
///
/// # Example
///
/// ```rust,ignore
/// use mssql_tds::connection::metadata_retriever::{MetadataRetriever, DestinationColumnMetadata};
///
/// struct CachedMetadataRetriever {
///     cache: HashMap<String, Vec<DestinationColumnMetadata>>,
/// }
///
/// #[async_trait]
/// impl MetadataRetriever for CachedMetadataRetriever {
///     async fn retrieve_metadata(
///         &mut self,
///         client: &mut TdsClient,
///         table_name: &str,
///         timeout_sec: u32,
///     ) -> TdsResult<Vec<DestinationColumnMetadata>> {
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
    /// A vector of `DestinationColumnMetadata` containing column information
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
    ) -> TdsResult<Vec<DestinationColumnMetadata>>;
}

/// Default metadata retriever using SQL Server system catalog views.
///
/// This implementation queries `sys.columns` and `sys.objects` to retrieve
/// table metadata. It handles both regular tables and temporary tables.
#[derive(Debug, Default)]
pub struct SystemCatalogRetriever;

impl SystemCatalogRetriever {
    /// Create a new system catalog retriever.
    pub fn new() -> Self {
        Self
    }

    /// Parse table name into schema and table components.
    pub fn parse_table_name(table_name: &str) -> (String, String) {
        if let Some(dot_pos) = table_name.rfind('.') {
            let schema = table_name[..dot_pos].to_string();
            let table = table_name[dot_pos + 1..].to_string();
            (schema, table)
        } else {
            ("dbo".to_string(), table_name.to_string())
        }
    }
}

#[async_trait]
impl MetadataRetriever for SystemCatalogRetriever {
    async fn retrieve_metadata(
        &mut self,
        client: &mut TdsClient,
        table_name: &str,
        timeout_sec: u32,
    ) -> TdsResult<Vec<DestinationColumnMetadata>> {
        // Parse table name to extract schema and table
        let (schema, table) = Self::parse_table_name(table_name);

        // Query sys.columns for table metadata
        // Handle temp tables (starting with #) which are in tempdb
        let query = if table.starts_with('#') {
            // Temp tables are in tempdb.sys.objects
            format!(
                "SELECT \
                    c.name, \
                    c.column_id, \
                    c.system_type_id, \
                    c.max_length, \
                    c.precision, \
                    c.scale, \
                    c.is_nullable, \
                    c.is_identity, \
                    c.is_computed, \
                    c.collation_name \
                FROM tempdb.sys.columns c \
                INNER JOIN tempdb.sys.objects o ON c.object_id = o.object_id \
                WHERE o.name LIKE '{}%' \
                ORDER BY c.column_id",
                table.replace('\'', "''").replace("%", "[%]") // Escape wildcards for LIKE
            )
        } else {
            // Regular tables
            format!(
                "SELECT \
                    c.name, \
                    c.column_id, \
                    c.system_type_id, \
                    c.max_length, \
                    c.precision, \
                    c.scale, \
                    c.is_nullable, \
                    c.is_identity, \
                    c.is_computed, \
                    c.collation_name \
                FROM sys.columns c \
                INNER JOIN sys.objects o ON c.object_id = o.object_id \
                WHERE o.name = '{}' AND SCHEMA_NAME(o.schema_id) = '{}' \
                ORDER BY c.column_id",
                table.replace('\'', "''"), // Escape single quotes
                schema.replace('\'', "''")
            )
        };

        // Execute the query
        client
            .execute(query, Some(timeout_sec), None)
            .await?;

        // Read the results
        let mut metadata = Vec::new();

        if let Some(resultset) = client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await? {
                if row.len() < 10 {
                    return Err(Error::UsageError(
                        "Unexpected number of columns in metadata query result".to_string(),
                    ));
                }

                // Extract column values
                let name = match &row[0] {
                    ColumnValues::String(s) => s.to_utf8_string(),
                    _ => {
                        return Err(Error::UsageError(
                            "Expected string for column name".to_string(),
                        ));
                    }
                };

                let column_id = match &row[1] {
                    ColumnValues::Int(i) => *i as usize,
                    _ => return Err(Error::UsageError("Expected int for column_id".to_string())),
                };

                let system_type_id = match &row[2] {
                    ColumnValues::TinyInt(t) => *t,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected tinyint for system_type_id".to_string(),
                        ));
                    }
                };

                let max_length = match &row[3] {
                    ColumnValues::SmallInt(s) => *s,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected smallint for max_length".to_string(),
                        ));
                    }
                };

                let precision = match &row[4] {
                    ColumnValues::TinyInt(p) => *p,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected tinyint for precision".to_string(),
                        ));
                    }
                };

                let scale = match &row[5] {
                    ColumnValues::TinyInt(s) => *s,
                    _ => return Err(Error::UsageError("Expected tinyint for scale".to_string())),
                };

                let is_nullable = match &row[6] {
                    ColumnValues::Bit(b) => *b,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected bit for is_nullable".to_string(),
                        ));
                    }
                };

                let is_identity = match &row[7] {
                    ColumnValues::Bit(b) => *b,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected bit for is_identity".to_string(),
                        ));
                    }
                };

                let is_computed = match &row[8] {
                    ColumnValues::Bit(b) => *b,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected bit for is_computed".to_string(),
                        ));
                    }
                };

                // collation_name can be NULL for non-string types
                let _collation_name = match &row[9] {
                    ColumnValues::String(s) => Some(s.to_utf8_string()),
                    ColumnValues::Null => None,
                    _ => {
                        return Err(Error::UsageError(
                            "Expected string or NULL for collation_name".to_string(),
                        ));
                    }
                };

                // Map system_type_id to SqlDbType using TryFrom trait with SystemTypeId wrapper
                let sql_type = SqlDbType::try_from(SystemTypeId(system_type_id))?;

                // TODO: Parse collation_name to create SqlCollation
                // For now, use None (will be enhanced in future)
                let collation = None;

                metadata.push(DestinationColumnMetadata {
                    name,
                    ordinal: column_id - 1, // SQL Server is 1-based, we use 0-based
                    system_type_id,
                    sql_type,
                    max_length,
                    precision,
                    scale,
                    is_nullable,
                    is_identity,
                    is_computed,
                    collation,
                });
            }
        }

        // Close the query
        client.close_query().await?;

        if metadata.is_empty() {
            return Err(Error::UsageError(format!(
                "Table '{}' not found or has no columns",
                table_name
            )));
        }

        Ok(metadata)
    }
}

/// Metadata retriever using SET FMTONLY ON query.
///
/// This implementation retrieves table metadata by executing `SET FMTONLY ON; SELECT * FROM table`
/// which returns only the COLMETADATA token without any rows. This is faster than
/// querying sys.columns and provides complete metadata including:
/// - Column names, types, nullability
/// - Identity and computed column flags (from TDS COLMETADATA flags)
/// - Precision, scale, and collation information
///
/// # Advantages over SystemCatalogRetriever
///
/// - Faster: Single round-trip with SET FMTONLY instead of sys.columns query
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
/// let retriever = SelectTop0Retriever::new();
/// let bulk_copy = BulkCopy::with_retriever(
///     &mut client,
///     "MyTable",
///     Box::new(retriever)
/// );
/// ```
#[derive(Debug, Default)]
pub struct SelectTop0Retriever;

impl SelectTop0Retriever {
    /// Create a new SELECT TOP 0 metadata retriever.
    pub fn new() -> Self {
        Self
    }

    /// Convert TDS type ID to SqlDbType.
    ///
    /// This maps the TDS data type from COLMETADATA token to our SqlDbType enum.
    fn map_tds_type_to_sql_type(tds_type: TdsDataType, type_info: &TypeInfo) -> TdsResult<SqlDbType> {
        
        match tds_type {
            TdsDataType::Int1 => Ok(SqlDbType::TinyInt),
            TdsDataType::Int2 => Ok(SqlDbType::SmallInt),
            TdsDataType::Int4 => Ok(SqlDbType::Int),
            TdsDataType::Int8 => Ok(SqlDbType::BigInt),
            TdsDataType::Bit => Ok(SqlDbType::Bit),
            TdsDataType::Flt4 => Ok(SqlDbType::Real),
            TdsDataType::Flt8 => Ok(SqlDbType::Float),
            TdsDataType::Money => Ok(SqlDbType::Money),
            TdsDataType::Money4 => Ok(SqlDbType::SmallMoney),
            TdsDataType::DateTime => Ok(SqlDbType::DateTime),
            TdsDataType::DateTim4 => Ok(SqlDbType::SmallDateTime),
            TdsDataType::DateN => Ok(SqlDbType::Date),
            TdsDataType::TimeN => Ok(SqlDbType::Time),
            TdsDataType::DateTime2N => Ok(SqlDbType::DateTime2),
            TdsDataType::DateTimeOffsetN => Ok(SqlDbType::DateTimeOffset),
            TdsDataType::Guid => Ok(SqlDbType::UniqueIdentifier),
            TdsDataType::BigBinary | TdsDataType::BigVarBinary => Ok(SqlDbType::VarBinary),
            TdsDataType::Image => Ok(SqlDbType::Image),
            TdsDataType::BigChar => Ok(SqlDbType::Char),
            TdsDataType::BigVarChar => Ok(SqlDbType::VarChar),
            TdsDataType::Text => Ok(SqlDbType::Text),
            TdsDataType::NChar => Ok(SqlDbType::NChar),
            TdsDataType::NVarChar => Ok(SqlDbType::NVarChar),
            TdsDataType::NText => Ok(SqlDbType::NText),
            TdsDataType::Xml => Ok(SqlDbType::Xml),
            
            // Nullable variants - map to underlying type
            TdsDataType::IntN => {
                match type_info.length {
                    1 => Ok(SqlDbType::TinyInt),
                    2 => Ok(SqlDbType::SmallInt),
                    4 => Ok(SqlDbType::Int),
                    8 => Ok(SqlDbType::BigInt),
                    _ => Err(Error::UsageError(format!(
                        "Invalid IntN length: {}",
                        type_info.length
                    ))),
                }
            }
            TdsDataType::FltN => {
                match type_info.length {
                    4 => Ok(SqlDbType::Real),
                    8 => Ok(SqlDbType::Float),
                    _ => Err(Error::UsageError(format!(
                        "Invalid FltN length: {}",
                        type_info.length
                    ))),
                }
            }
            TdsDataType::MoneyN => {
                match type_info.length {
                    4 => Ok(SqlDbType::SmallMoney),
                    8 => Ok(SqlDbType::Money),
                    _ => Err(Error::UsageError(format!(
                        "Invalid MoneyN length: {}",
                        type_info.length
                    ))),
                }
            }
            TdsDataType::DateTimeN => {
                match type_info.length {
                    4 => Ok(SqlDbType::SmallDateTime),
                    8 => Ok(SqlDbType::DateTime),
                    _ => Err(Error::UsageError(format!(
                        "Invalid DateTimeN length: {}",
                        type_info.length
                    ))),
                }
            }
            TdsDataType::DecimalN => Ok(SqlDbType::Decimal),
            TdsDataType::NumericN => Ok(SqlDbType::Numeric),
            
            _ => Err(Error::UsageError(format!(
                "Unsupported TDS data type: {:?}",
                tds_type
            ))),
        }
    }

    /// Get max_length from TypeInfo.
    fn get_max_length(type_info: &TypeInfo) -> i16 {
        match &type_info.type_info_variant {
            TypeInfoVariant::FixedLen(_) => type_info.length as i16,
            TypeInfoVariant::VarLen(_, _) => type_info.length as i16,
            TypeInfoVariant::VarLenString(_, _, _) => type_info.length as i16,
            TypeInfoVariant::VarLenScale(_, _) => type_info.length as i16,
            TypeInfoVariant::VarLenPrecisionScale(_, _, _, _) => type_info.length as i16,
            TypeInfoVariant::PartialLen(_, _, _, _, _) => -1, // PLP types use -1
        }
    }

    /// Get precision from TypeInfo.
    fn get_precision(type_info: &TypeInfo) -> u8 {
        match &type_info.type_info_variant {
            TypeInfoVariant::VarLenPrecisionScale(_, _, precision, _) => *precision,
            _ => 0,
        }
    }

    /// Get scale from TypeInfo.
    fn get_scale(type_info: &TypeInfo) -> u8 {
        match &type_info.type_info_variant {
            TypeInfoVariant::VarLenScale(_, scale) => *scale,
            TypeInfoVariant::VarLenPrecisionScale(_, _, _, scale) => *scale,
            _ => 0,
        }
    }
}

#[async_trait]
impl MetadataRetriever for SelectTop0Retriever {
    async fn retrieve_metadata(
        &mut self,
        client: &mut TdsClient,
        table_name: &str,
        timeout_sec: u32,
    ) -> TdsResult<Vec<DestinationColumnMetadata>> {
        // Fetch metadata using SET FMTONLY ON
        let col_metadata_token = fetch_table_metadata(client, table_name, Some(timeout_sec), None).await?;

        // Convert ColMetadataToken to Vec<DestinationColumnMetadata>
        let mut metadata = Vec::with_capacity(col_metadata_token.columns.len());

        for (ordinal, col) in col_metadata_token.columns.iter().enumerate() {
            // Map TDS type to SqlDbType
            let sql_type = Self::map_tds_type_to_sql_type(col.data_type, &col.type_info)?;

            // Get system_type_id from SqlDbType
            // Note: This is an approximation - SELECT TOP 0 doesn't give us the exact system_type_id
            let system_type_id = match sql_type {
                SqlDbType::TinyInt => 48,
                SqlDbType::SmallInt => 52,
                SqlDbType::Int => 56,
                SqlDbType::BigInt => 127,
                SqlDbType::Bit => 104,
                SqlDbType::Real => 59,
                SqlDbType::Float => 62,
                SqlDbType::Money => 60,
                SqlDbType::SmallMoney => 122,
                SqlDbType::DateTime => 61,
                SqlDbType::SmallDateTime => 58,
                SqlDbType::Date => 40,
                SqlDbType::Time => 41,
                SqlDbType::DateTime2 => 42,
                SqlDbType::DateTimeOffset => 43,
                SqlDbType::UniqueIdentifier => 36,
                SqlDbType::VarBinary | SqlDbType::Binary => 165,
                SqlDbType::Image => 34,
                SqlDbType::VarChar | SqlDbType::Char => 167,
                SqlDbType::Text => 35,
                SqlDbType::NVarChar | SqlDbType::NChar => 231,
                SqlDbType::NText => 99,
                SqlDbType::Xml => 241,
                SqlDbType::Decimal | SqlDbType::Numeric => 106,
                _ => 0, // Unknown
            };

            let max_length = Self::get_max_length(&col.type_info);
            let precision = Self::get_precision(&col.type_info);
            let scale = Self::get_scale(&col.type_info);
            let is_nullable = col.is_nullable();

            // Extract identity and computed flags from TDS COLMETADATA token
            // These flags are part of the TDS protocol response
            let is_identity = col.is_identity();
            let is_computed = col.is_computed();

            // Get collation for string types
            let collation = col.get_collation();

            metadata.push(DestinationColumnMetadata {
                name: col.column_name.clone(),
                ordinal,
                system_type_id,
                sql_type,
                max_length,
                precision,
                scale,
                is_nullable,
                is_identity,
                is_computed,
                collation,
            });
        }

        if metadata.is_empty() {
            return Err(Error::UsageError(format!(
                "Table '{}' not found or has no columns",
                table_name
            )));
        }

        Ok(metadata)
    }
}

/// Metadata about a destination table column.
///
/// This is retrieved from SQL Server's system tables and used for
/// automatic column mapping and type validation.
#[derive(Debug, Clone)]
pub struct DestinationColumnMetadata {
    /// Column name
    pub name: String,

    /// Column ordinal (0-based position in table)
    pub ordinal: usize,

    /// SQL Server type ID (from sys.columns.system_type_id)
    pub system_type_id: u8,

    /// SqlDbType mapped from system_type_id
    pub sql_type: SqlDbType,

    /// Maximum length in bytes (-1 for MAX types)
    pub max_length: i16,

    /// Precision (for numeric/decimal types)
    pub precision: u8,

    /// Scale (for numeric/decimal types)
    pub scale: u8,

    /// Whether the column allows NULL values
    pub is_nullable: bool,

    /// Whether the column is an identity column
    pub is_identity: bool,

    /// Whether the column is computed
    pub is_computed: bool,

    /// Collation (for string types)
    pub collation: Option<SqlCollation>,
}

impl DestinationColumnMetadata {
    /// Convert destination metadata to BulkCopyColumnMetadata for protocol serialization.
    pub fn to_bulk_copy_metadata(&self) -> BulkCopyColumnMetadata {
        // Use fixed-length types for non-nullable columns, nullable types for nullable columns
        let tds_type = if self.is_nullable {
            self.sql_type.to_tds_type()
        } else {
            self.sql_type.to_tds_type_fixed()
        };

        let type_length = match self.sql_type {
            SqlDbType::BigInt
            | SqlDbType::Int
            | SqlDbType::SmallInt
            | SqlDbType::TinyInt
            | SqlDbType::Bit
            | SqlDbType::Real
            | SqlDbType::Float
            | SqlDbType::Date
            | SqlDbType::SmallDateTime
            | SqlDbType::Money
            | SqlDbType::SmallMoney => TypeLength::Fixed(self.max_length as i32),
            SqlDbType::VarChar | SqlDbType::NVarChar | SqlDbType::VarBinary => {
                if self.max_length == -1 {
                    TypeLength::Plp
                } else {
                    TypeLength::Variable(self.max_length as i32)
                }
            }
            SqlDbType::Char | SqlDbType::NChar | SqlDbType::Binary => {
                TypeLength::Fixed(self.max_length as i32)
            }
            SqlDbType::Text
            | SqlDbType::NText
            | SqlDbType::Image
            | SqlDbType::Xml
            | SqlDbType::Json => TypeLength::Plp,
            _ => TypeLength::Variable(self.max_length as i32),
        };

        let mut metadata = BulkCopyColumnMetadata::new(&self.name, self.sql_type, tds_type)
            .with_length(self.max_length as i32, type_length)
            .with_nullable(self.is_nullable);

        if matches!(self.sql_type, SqlDbType::Decimal | SqlDbType::Numeric) {
            metadata = metadata.with_precision_scale(self.precision, self.scale);
        }

        if let Some(collation) = self.collation {
            metadata = metadata.with_collation(collation);
        }

        if self.is_identity {
            metadata = metadata.with_identity(true);
        }

        // Note: Computed columns are typically skipped in bulk copy operations
        // The metadata doesn't need to track this flag for serialization

        metadata
    }
}

/// Fetch table metadata using SET FMTONLY ON query.
///
/// This function retrieves column metadata for a table by executing a
/// query with `SET FMTONLY ON`, which returns only the COLMETADATA
/// token without any row data.
///
/// # Arguments
///
/// * `client` - Mutable reference to the TDS client
/// * `table_name` - Name of the table (can include schema: "dbo.Users")
/// * `timeout_sec` - Query timeout in seconds
/// * `cancel_handle` - Optional cancellation handle
///
/// # Returns
///
/// A `ColMetadataToken` containing the column metadata from the server.
///
/// # Errors
///
/// Returns an error if:
/// - Table does not exist
/// - No permission to access the table
/// - Network errors during query execution
///
/// # Implementation Notes
///
/// This function is designed for bulk copy operations where we need table schema
/// without the overhead of querying sys.columns. It dynamically builds the column
/// list to:
/// - Support hidden columns in temporal tables
/// - Exclude SQL Graph columns that cannot be selected
///
/// For full metadata including computed/identity flags, use SystemCatalogRetriever instead.
#[instrument(skip(client), level = "info")]
pub(crate) async fn fetch_table_metadata(
    client: &mut TdsClient,
    table_name: &str,
    timeout_sec: Option<u32>,
    cancel_handle: Option<&CancelHandle>,
) -> TdsResult<ColMetadataToken> {
    // Use SET FMTONLY ON to get metadata without query execution overhead.
    // This matches .NET SqlBulkCopy behavior and is more efficient than SELECT TOP 0.
    // It also dynamically builds the column list to:
    // - Support hidden columns in temporal tables
    // - Exclude SQL Graph columns that cannot be selected
    let query = format!(
        r#"DECLARE @Column_Names NVARCHAR(MAX) = NULL;
IF EXISTS (SELECT TOP 1 * FROM sys.all_columns WHERE [object_id] = OBJECT_ID('sys.all_columns') AND [name] = 'graph_type')
BEGIN
    SELECT @Column_Names = COALESCE(@Column_Names + ', ', '') + QUOTENAME([name]) 
    FROM sys.all_columns 
    WHERE [object_id] = OBJECT_ID('{table_name}') 
    AND COALESCE([graph_type], 0) NOT IN (1, 3, 4, 6, 7) 
    ORDER BY [column_id] ASC;
END
ELSE
BEGIN
    SELECT @Column_Names = COALESCE(@Column_Names + ', ', '') + QUOTENAME([name]) 
    FROM sys.all_columns 
    WHERE [object_id] = OBJECT_ID('{table_name}') 
    ORDER BY [column_id] ASC;
END

SELECT @Column_Names = COALESCE(@Column_Names, '*');

SET FMTONLY ON;
EXEC(N'SELECT ' + @Column_Names + N' FROM {table_name}');
SET FMTONLY OFF;"#
    );

    debug!("Fetching table metadata with FMTONLY");

    // Execute the query
    client.execute(query, timeout_sec, cancel_handle).await?;

    // Get the metadata from the result and clone it immediately
    let metadata = client.get_current_metadata().ok_or_else(|| {
        Error::UsageError(format!("Failed to fetch metadata for table {table_name}"))
    })?.clone();

    debug!(
        "Fetched {} columns from table metadata",
        metadata.columns.len()
    );
    for (i, col) in metadata.columns.iter().enumerate() {
        trace!(
            "Column {}: name='{}', tds_type=0x{:02X}, nullable={}",
            i,
            col.column_name,
            col.data_type as u8,
            col.is_nullable()
        );
    }

    // Close the query to free up the connection
    client.close_query().await?;

    Ok(metadata)
}
