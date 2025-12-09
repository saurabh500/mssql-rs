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
use crate::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
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
        TdsDataType::IntN => match type_info.length {
            1 => Ok(SqlDbType::TinyInt),
            2 => Ok(SqlDbType::SmallInt),
            4 => Ok(SqlDbType::Int),
            8 => Ok(SqlDbType::BigInt),
            _ => Err(Error::UsageError(format!(
                "Invalid IntN length: {}",
                type_info.length
            ))),
        },
        TdsDataType::FltN => match type_info.length {
            4 => Ok(SqlDbType::Real),
            8 => Ok(SqlDbType::Float),
            _ => Err(Error::UsageError(format!(
                "Invalid FltN length: {}",
                type_info.length
            ))),
        },
        TdsDataType::MoneyN => match type_info.length {
            4 => Ok(SqlDbType::SmallMoney),
            8 => Ok(SqlDbType::Money),
            _ => Err(Error::UsageError(format!(
                "Invalid MoneyN length: {}",
                type_info.length
            ))),
        },
        TdsDataType::DateTimeN => match type_info.length {
            4 => Ok(SqlDbType::SmallDateTime),
            8 => Ok(SqlDbType::DateTime),
            _ => Err(Error::UsageError(format!(
                "Invalid DateTimeN length: {}",
                type_info.length
            ))),
        },
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

#[async_trait]
impl MetadataRetriever for FmtOnlyMetadataRetriever {
    async fn retrieve_metadata(
        &mut self,
        client: &mut TdsClient,
        table_name: &str,
        timeout_sec: u32,
    ) -> TdsResult<Vec<DestinationColumnMetadata>> {
        // Fetch metadata using SET FMTONLY ON
        let col_metadata_token =
            fetch_table_metadata(client, table_name, Some(timeout_sec), None).await?;

        // Convert using TryFrom trait
        Vec::<DestinationColumnMetadata>::try_from(col_metadata_token)
    }
}

impl TryFrom<ColMetadataToken> for Vec<DestinationColumnMetadata> {
    type Error = Error;

    fn try_from(col_metadata_token: ColMetadataToken) -> Result<Self, Self::Error> {
        if col_metadata_token.columns.is_empty() {
            return Err(Error::UsageError(
                "Table not found or has no columns".to_string(),
            ));
        }

        let mut metadata = Vec::with_capacity(col_metadata_token.columns.len());

        for (ordinal, col) in col_metadata_token.columns.iter().enumerate() {
            // Map TDS type to SqlDbType
            let sql_type = map_tds_type_to_sql_type(col.data_type, &col.type_info)?;

            // Get system_type_id from SqlDbType
            // Note: This is an approximation - FMTONLY doesn't give us the exact system_type_id
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

            let max_length = get_max_length(&col.type_info);
            let precision = get_precision(&col.type_info);
            let scale = get_scale(&col.type_info);
            let is_nullable = col.is_nullable();

            // Extract identity and computed flags from TDS COLMETADATA token
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
/// without the overhead of querying system catalog views. It dynamically builds the column
/// list to:
/// - Support hidden columns in temporal tables
/// - Exclude SQL Graph columns that cannot be selected
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
    let metadata = client
        .get_current_metadata()
        .ok_or_else(|| {
            Error::UsageError(format!("Failed to fetch metadata for table {table_name}"))
        })?
        .clone();

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
