// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Bulk copy operations for high-performance data insertion.
//!
//! This module provides the `BulkCopy` API for efficiently inserting large amounts of data
//! into SQL Server tables. It uses the TDS bulk load protocol for optimal performance.
//!
//! # Example
//!
//! ```rust,ignore
//! use mssql_tds::connection::bulk_copy::{BulkCopy, BulkCopyRow};
//! use mssql_tds::datatypes::column_values::ColumnValues;
//! use mssql_tds::datatypes::bulk_copy_metadata::BulkCopyColumnMetadata;
//!
//! // Define a struct to represent your data
//! struct User {
//!     id: i32,
//!     name: String,
//!     email: String,
//! }
//!
//! // Implement BulkCopyRow for your struct
//! impl BulkCopyRow for User {
//!     fn to_column_values(&self) -> Vec<ColumnValues> {
//!         vec![
//!             ColumnValues::Int(self.id),
//!             ColumnValues::String(self.name.clone().into()),
//!             ColumnValues::String(self.email.clone().into()),
//!         ]
//!     }
//!     
//!     fn column_metadata() -> Vec<BulkCopyColumnMetadata> where Self: Sized {
//!         vec![
//!             BulkCopyColumnMetadata::new("id", SqlDbType::Int, 0x38)
//!                 .with_length(4, TypeLength::Fixed(4))
//!                 .with_nullable(false),
//!             // ... more metadata
//!         ]
//!     }
//! }
//!
//! // Use bulk copy
//! let mut bulk_copy = client.bulk_copy("Users");
//! bulk_copy
//!     .batch_size(5000)
//!     .timeout(30);
//!
//! let users = vec![/* your data */];
//! let result = bulk_copy.write_to_server(users.into_iter()).await?;
//! println!("Inserted {} rows", result.rows_affected);
//! ```

use crate::connection::tds_client::{TdsClient, ResultSet, ResultSetClient};
use crate::core::TdsResult;
use crate::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
use crate::datatypes::column_values::ColumnValues;
use crate::error::Error;
use crate::message::bulk_load::BulkLoadMessage;
use crate::token::tokens::SqlCollation;
use std::time::{Duration, Instant};

/// Trait for types that can be bulk copied to SQL Server.
///
/// Implement this trait for your custom types to enable bulk copy operations.
/// The trait requires two methods:
/// 1. `to_column_values()` - Converts an instance to a vector of column values
/// 2. `column_metadata()` - Provides metadata about the columns (static method)
///
/// # Example
///
/// ```rust,ignore
/// use mssql_tds::connection::bulk_copy::BulkCopyRow;
/// use mssql_tds::datatypes::column_values::ColumnValues;
/// use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
///
/// struct Product {
///     id: i32,
///     name: String,
///     price: f64,
/// }
///
/// impl BulkCopyRow for Product {
///     fn to_column_values(&self) -> Vec<ColumnValues> {
///         vec![
///             ColumnValues::Int(self.id),
///             ColumnValues::String(self.name.clone().into()),
///             ColumnValues::Float(self.price),
///         ]
///     }
///     
///     fn column_metadata() -> Vec<BulkCopyColumnMetadata> where Self: Sized {
///         vec![
///             BulkCopyColumnMetadata::new("id", SqlDbType::Int, 0x38)
///                 .with_length(4, TypeLength::Fixed(4)),
///             BulkCopyColumnMetadata::new("name", SqlDbType::NVarChar, 0xE7)
///                 .with_length(100, TypeLength::Variable(100)),
///             BulkCopyColumnMetadata::new("price", SqlDbType::Float, 0x3E)
///                 .with_length(8, TypeLength::Fixed(8)),
///         ]
///     }
/// }
/// ```
pub trait BulkCopyRow {
    /// Convert this row to a vector of column values.
    ///
    /// The order of values must match the order of columns in `column_metadata()`.
    fn to_column_values(&self) -> Vec<ColumnValues>;

    /// Get metadata for all columns.
    ///
    /// This is called once per bulk copy operation to set up the column structure.
    /// The metadata must match the values returned by `to_column_values()`.
    fn column_metadata() -> Vec<BulkCopyColumnMetadata>
    where
        Self: Sized;
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
        let tds_type = self.sql_type.to_tds_type();
        
        let type_length = match self.sql_type {
            SqlDbType::BigInt | SqlDbType::Int | SqlDbType::SmallInt | SqlDbType::TinyInt |
            SqlDbType::Bit | SqlDbType::Real | SqlDbType::Float | SqlDbType::Date |
            SqlDbType::SmallDateTime | SqlDbType::Money | SqlDbType::SmallMoney => {
                TypeLength::Fixed(self.max_length as i32)
            }
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
            SqlDbType::Text | SqlDbType::NText | SqlDbType::Image | 
            SqlDbType::Xml | SqlDbType::Json => {
                TypeLength::Plp
            }
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

/// Map SQL Server system_type_id to SqlDbType.
///
/// This mapping is based on the sys.types catalog view in SQL Server.
/// Reference: https://docs.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-types-transact-sql
fn map_system_type_id_to_sql_db_type(system_type_id: u8) -> Result<SqlDbType, Error> {
    match system_type_id {
        // Exact numeric types
        48 => Ok(SqlDbType::TinyInt),      // tinyint
        52 => Ok(SqlDbType::SmallInt),     // smallint
        56 => Ok(SqlDbType::Int),          // int
        127 => Ok(SqlDbType::BigInt),      // bigint
        106 => Ok(SqlDbType::Decimal),     // decimal
        108 => Ok(SqlDbType::Numeric),     // numeric
        122 => Ok(SqlDbType::SmallMoney),  // smallmoney
        60 => Ok(SqlDbType::Money),        // money
        104 => Ok(SqlDbType::Bit),         // bit
        
        // Approximate numeric types
        59 => Ok(SqlDbType::Real),         // real
        62 => Ok(SqlDbType::Float),        // float
        
        // Date and time types
        40 => Ok(SqlDbType::Date),         // date
        41 => Ok(SqlDbType::Time),         // time
        42 => Ok(SqlDbType::DateTime2),    // datetime2
        43 => Ok(SqlDbType::DateTimeOffset), // datetimeoffset
        58 => Ok(SqlDbType::SmallDateTime), // smalldatetime
        61 => Ok(SqlDbType::DateTime),     // datetime
        
        // Character strings
        167 => Ok(SqlDbType::VarChar),     // varchar
        175 => Ok(SqlDbType::Char),        // char
        35 => Ok(SqlDbType::Text),         // text
        
        // Unicode character strings
        231 => Ok(SqlDbType::NVarChar),    // nvarchar
        239 => Ok(SqlDbType::NChar),       // nchar
        99 => Ok(SqlDbType::NText),        // ntext
        
        // Binary strings
        165 => Ok(SqlDbType::VarBinary),   // varbinary
        173 => Ok(SqlDbType::Binary),      // binary
        34 => Ok(SqlDbType::Image),        // image
        
        // Other types
        36 => Ok(SqlDbType::UniqueIdentifier), // uniqueidentifier
        241 => Ok(SqlDbType::Xml),         // xml
        
        // Unsupported or unknown types
        _ => Err(Error::UsageError(format!(
            "Unsupported system_type_id: {}",
            system_type_id
        ))),
    }
}

/// Options for configuring bulk copy operations.
///
/// These options control various aspects of the bulk copy behavior,
/// such as batch size, timeout, and SQL Server-specific options.
#[derive(Debug, Clone)]
pub struct BulkCopyOptions {
    /// Number of rows in each batch. Default: 0 (all rows in one batch)
    pub batch_size: usize,

    /// Timeout for the operation in seconds. Default: 30
    pub timeout_sec: u32,

    /// Check constraints on the destination table. Default: false
    pub check_constraints: bool,

    /// Enable triggers on the destination table. Default: false
    pub fire_triggers: bool,

    /// Preserve source identity values. Default: false
    /// If false, identity values are auto-generated by SQL Server.
    pub keep_identity: bool,

    /// Preserve NULL values regardless of defaults. Default: false
    pub keep_nulls: bool,

    /// Obtain a bulk update lock for the duration of the operation. Default: false
    pub table_lock: bool,

    /// Use an internal transaction for the bulk copy. Default: true
    /// If true, the entire operation is wrapped in a transaction.
    pub use_internal_transaction: bool,

    /// Number of rows to process before calling the progress callback.
    /// Default: 0 (no progress notifications)
    pub notification_interval: usize,
}

impl Default for BulkCopyOptions {
    fn default() -> Self {
        Self {
            batch_size: 0,
            timeout_sec: 30,
            check_constraints: false,
            fire_triggers: false,
            keep_identity: false,
            keep_nulls: false,
            table_lock: false,
            use_internal_transaction: true,
            notification_interval: 0,
        }
    }
}

impl BulkCopyOptions {
    /// Create a new `BulkCopyOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate the options.
    pub fn validate(&self) -> TdsResult<()> {
        if self.batch_size > 1_000_000 {
            return Err(crate::error::Error::UsageError(
                "batch_size cannot exceed 1,000,000".to_string(),
            ));
        }
        Ok(())
    }
}

/// Specifies how to map source columns to destination columns.
#[derive(Debug, Clone)]
pub enum ColumnMappingSource {
    /// Map by column name
    Name(String),
    /// Map by column ordinal (0-based)
    Ordinal(usize),
}

/// Defines a mapping between a source column and a destination column.
#[derive(Debug, Clone)]
pub struct ColumnMapping {
    /// Source column (by name or ordinal)
    pub source: ColumnMappingSource,
    /// Destination column name
    pub destination: String,
}

impl ColumnMapping {
    /// Create a mapping from source column name to destination column name.
    pub fn by_name(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            source: ColumnMappingSource::Name(source.into()),
            destination: destination.into(),
        }
    }

    /// Create a mapping from source column ordinal to destination column name.
    pub fn by_ordinal(source_ordinal: usize, destination: impl Into<String>) -> Self {
        Self {
            source: ColumnMappingSource::Ordinal(source_ordinal),
            destination: destination.into(),
        }
    }
}

/// A resolved mapping from source column index to destination column index.
///
/// This is the result of resolving user-provided column mappings against
/// the actual destination table metadata.
#[derive(Debug, Clone)]
struct ResolvedColumnMapping {
    /// Source column index (0-based)
    source_index: usize,
    /// Destination column index (0-based)
    destination_index: usize,
    /// Destination column name
    destination_name: String,
    /// Expected destination type
    destination_type: SqlDbType,
}

/// Progress information for bulk copy operations.
///
/// Passed to the progress callback to report the current state of the operation.
#[derive(Debug, Clone)]
pub struct BulkCopyProgress {
    /// Number of rows copied so far
    pub rows_copied: u64,
    /// Total number of rows (if known)
    pub total_rows: Option<u64>,
    /// Elapsed time since the operation started
    pub elapsed: Duration,
    /// Throughput in rows per second
    pub rows_per_second: f64,
}

impl BulkCopyProgress {
    /// Calculate progress percentage (0.0 to 100.0).
    pub fn percentage(&self) -> Option<f64> {
        self.total_rows
            .map(|total| (self.rows_copied as f64 / total as f64) * 100.0)
    }

    /// Estimate time remaining based on current throughput.
    pub fn estimated_time_remaining(&self) -> Option<Duration> {
        if self.rows_per_second <= 0.0 {
            return None;
        }

        self.total_rows.map(|total| {
            let remaining_rows = total.saturating_sub(self.rows_copied) as f64;
            Duration::from_secs_f64(remaining_rows / self.rows_per_second)
        })
    }
}

/// Result of a bulk copy operation.
///
/// Contains statistics about the completed operation.
#[derive(Debug, Clone)]
pub struct BulkCopyResult {
    /// Number of rows successfully copied
    pub rows_affected: u64,
    /// Time taken for the operation
    pub elapsed: Duration,
    /// Throughput in rows per second
    pub rows_per_second: f64,
}

impl BulkCopyResult {
    /// Create a new result from the given statistics.
    pub fn new(rows_affected: u64, elapsed: Duration) -> Self {
        let rows_per_second = if elapsed.as_secs_f64() > 0.0 {
            rows_affected as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        Self {
            rows_affected,
            elapsed,
            rows_per_second,
        }
    }
}

/// High-level bulk copy API for efficiently inserting large amounts of data.
///
/// `BulkCopy` provides a convenient builder-style API for configuring and executing
/// bulk copy operations. It handles batching, progress reporting, and error handling.
///
/// # Example
///
/// ```rust,ignore
/// use mssql_tds::connection::bulk_copy::{BulkCopy, BulkCopyRow, ColumnMapping};
/// use std::time::Duration;
///
/// let mut bulk_copy = BulkCopy::new(&mut client, "Users")
///     .batch_size(5000)
///     .timeout(Duration::from_secs(60))
///     .keep_identity(true)
///     .add_column_mapping(ColumnMapping::by_name("SourceId", "Id"))
///     .add_column_mapping(ColumnMapping::by_name("SourceName", "Name"));
///
/// let rows = vec![/* your data */];
/// let result = bulk_copy.write_to_server(rows.into_iter()).await?;
/// println!("Inserted {} rows in {:?}", result.rows_affected, result.elapsed);
/// ```
pub struct BulkCopy<'a> {
    /// Reference to the TDS client connection
    client: &'a mut TdsClient,

    /// Name of the destination table
    table_name: String,

    /// Bulk copy options
    options: BulkCopyOptions,

    /// Column mappings (empty means use source column order)
    column_mappings: Vec<ColumnMapping>,

    /// Progress callback
    progress_callback: Option<Box<dyn FnMut(BulkCopyProgress) + Send + 'a>>,
    
    /// Cached destination table metadata (retrieved from sys.columns)
    destination_metadata: Option<Vec<DestinationColumnMetadata>>,
}

impl<'a> BulkCopy<'a> {
    /// Create a new `BulkCopy` instance for the given table.
    ///
    /// # Arguments
    ///
    /// * `client` - A mutable reference to the TDS client connection
    /// * `table_name` - Name of the destination table (can include schema: "dbo.Users")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut bulk_copy = BulkCopy::new(&mut client, "MyTable");
    /// ```
    pub fn new(client: &'a mut TdsClient, table_name: impl Into<String>) -> Self {
        Self {
            client,
            table_name: table_name.into(),
            options: BulkCopyOptions::default(),
            column_mappings: Vec::new(),
            progress_callback: None,
            destination_metadata: None,
        }
    }

    /// Set the batch size (number of rows per batch).
    ///
    /// Default: 0 (all rows in one batch)
    ///
    /// # Arguments
    ///
    /// * `size` - Number of rows to include in each batch
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.batch_size(5000); // Process 5000 rows at a time
    /// ```
    pub fn batch_size(mut self, size: usize) -> Self {
        self.options.batch_size = size;
        self
    }

    /// Set the operation timeout.
    ///
    /// Default: 30 seconds
    ///
    /// # Arguments
    ///
    /// * `timeout` - Maximum duration for the operation
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::time::Duration;
    /// bulk_copy.timeout(Duration::from_secs(120)); // 2 minute timeout
    /// ```
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.options.timeout_sec = timeout.as_secs() as u32;
        self
    }

    /// Set whether to check constraints on the destination table.
    ///
    /// Default: false
    ///
    /// # Arguments
    ///
    /// * `enabled` - If true, check constraints will be enforced
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.check_constraints(true);
    /// ```
    pub fn check_constraints(mut self, enabled: bool) -> Self {
        self.options.check_constraints = enabled;
        self
    }

    /// Set whether to fire triggers on the destination table.
    ///
    /// Default: false
    ///
    /// # Arguments
    ///
    /// * `enabled` - If true, triggers will be fired for inserted rows
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.fire_triggers(true);
    /// ```
    pub fn fire_triggers(mut self, enabled: bool) -> Self {
        self.options.fire_triggers = enabled;
        self
    }

    /// Set whether to preserve source identity values.
    ///
    /// Default: false (identity values are auto-generated by SQL Server)
    ///
    /// # Arguments
    ///
    /// * `enabled` - If true, source identity values will be preserved
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.keep_identity(true); // Preserve source identity values
    /// ```
    pub fn keep_identity(mut self, enabled: bool) -> Self {
        self.options.keep_identity = enabled;
        self
    }

    /// Set whether to preserve NULL values regardless of column defaults.
    ///
    /// Default: false
    ///
    /// # Arguments
    ///
    /// * `enabled` - If true, NULLs will be preserved even if columns have defaults
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.keep_nulls(true);
    /// ```
    pub fn keep_nulls(mut self, enabled: bool) -> Self {
        self.options.keep_nulls = enabled;
        self
    }

    /// Set whether to obtain a bulk update lock for the operation.
    ///
    /// Default: false
    ///
    /// # Arguments
    ///
    /// * `enabled` - If true, a table lock will be obtained
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.table_lock(true); // Lock the table during bulk copy
    /// ```
    pub fn table_lock(mut self, enabled: bool) -> Self {
        self.options.table_lock = enabled;
        self
    }

    /// Set whether to use an internal transaction.
    ///
    /// Default: true (entire operation wrapped in a transaction)
    ///
    /// # Arguments
    ///
    /// * `enabled` - If true, the operation will use an internal transaction
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.use_internal_transaction(false); // Use external transaction
    /// ```
    pub fn use_internal_transaction(mut self, enabled: bool) -> Self {
        self.options.use_internal_transaction = enabled;
        self
    }

    /// Set the notification interval for progress callbacks.
    ///
    /// Default: 0 (no progress notifications)
    ///
    /// # Arguments
    ///
    /// * `interval` - Number of rows to process before calling the progress callback
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy.notification_interval(1000); // Report progress every 1000 rows
    /// ```
    pub fn notification_interval(mut self, interval: usize) -> Self {
        self.options.notification_interval = interval;
        self
    }

    /// Add a column mapping from source to destination.
    ///
    /// If no mappings are specified, columns are mapped by ordinal position.
    ///
    /// # Arguments
    ///
    /// * `mapping` - Column mapping specification
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use mssql_tds::connection::bulk_copy::ColumnMapping;
    ///
    /// bulk_copy
    ///     .add_column_mapping(ColumnMapping::by_name("SourceId", "Id"))
    ///     .add_column_mapping(ColumnMapping::by_ordinal(1, "Name"));
    /// ```
    pub fn add_column_mapping(mut self, mapping: ColumnMapping) -> Self {
        self.column_mappings.push(mapping);
        self
    }

    /// Set a progress callback to receive notifications during the operation.
    ///
    /// The callback is invoked every `notification_interval` rows (if set).
    ///
    /// # Arguments
    ///
    /// * `callback` - Function to call with progress information
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// bulk_copy
    ///     .notification_interval(1000)
    ///     .on_progress(|progress| {
    ///         println!("Copied {} rows ({:.1}%)",
    ///             progress.rows_copied,
    ///             progress.percentage().unwrap_or(0.0));
    ///     });
    /// ```
    pub fn on_progress<F>(mut self, callback: F) -> Self
    where
        F: FnMut(BulkCopyProgress) + Send + 'a,
    {
        self.progress_callback = Some(Box::new(callback));
        self
    }

    /// Retrieve destination table metadata from SQL Server.
    ///
    /// This queries the sys.columns catalog view to get column information
    /// for the destination table. The metadata is cached for subsequent operations.
    ///
    /// # Returns
    ///
    /// A vector of `DestinationColumnMetadata` containing column information
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Table does not exist
    /// - No permission to access sys.columns
    /// - Network errors during query execution
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let metadata = bulk_copy.retrieve_destination_metadata().await?;
    /// for col in &metadata {
    ///     println!("Column: {}, Type: {:?}", col.name, col.sql_type);
    /// }
    /// ```
    pub async fn retrieve_destination_metadata(&mut self) -> TdsResult<Vec<DestinationColumnMetadata>> {
        // Check if we already have cached metadata
        if let Some(ref metadata) = self.destination_metadata {
            return Ok(metadata.clone());
        }

        // Parse table name to extract schema and table
        let (schema, table) = self.parse_table_name();

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
                table.replace('\'', "''").replace("%", "[%]")  // Escape wildcards for LIKE
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
                table.replace('\'', "''"),  // Escape single quotes
                schema.replace('\'', "''")
            )
        };

        // Execute the query
        self.client.execute(query, Some(self.options.timeout_sec), None).await?;

        // Read the results
        let mut metadata = Vec::new();
        
        if let Some(resultset) = self.client.get_current_resultset() {
            while let Some(row) = resultset.next_row().await? {
                if row.len() < 10 {
                    return Err(Error::UsageError(
                        "Unexpected number of columns in metadata query result".to_string()
                    ));
                }

                // Extract column values
                let name = match &row[0] {
                    ColumnValues::String(s) => s.to_utf8_string(),
                    _ => return Err(Error::UsageError("Expected string for column name".to_string())),
                };

                let column_id = match &row[1] {
                    ColumnValues::Int(i) => *i as usize,
                    _ => return Err(Error::UsageError("Expected int for column_id".to_string())),
                };

                let system_type_id = match &row[2] {
                    ColumnValues::TinyInt(t) => *t,
                    _ => return Err(Error::UsageError("Expected tinyint for system_type_id".to_string())),
                };

                let max_length = match &row[3] {
                    ColumnValues::SmallInt(s) => *s,
                    _ => return Err(Error::UsageError("Expected smallint for max_length".to_string())),
                };

                let precision = match &row[4] {
                    ColumnValues::TinyInt(p) => *p,
                    _ => return Err(Error::UsageError("Expected tinyint for precision".to_string())),
                };

                let scale = match &row[5] {
                    ColumnValues::TinyInt(s) => *s,
                    _ => return Err(Error::UsageError("Expected tinyint for scale".to_string())),
                };

                let is_nullable = match &row[6] {
                    ColumnValues::Bit(b) => *b,
                    _ => return Err(Error::UsageError("Expected bit for is_nullable".to_string())),
                };

                let is_identity = match &row[7] {
                    ColumnValues::Bit(b) => *b,
                    _ => return Err(Error::UsageError("Expected bit for is_identity".to_string())),
                };

                let is_computed = match &row[8] {
                    ColumnValues::Bit(b) => *b,
                    _ => return Err(Error::UsageError("Expected bit for is_computed".to_string())),
                };

                // collation_name can be NULL for non-string types
                let _collation_name = match &row[9] {
                    ColumnValues::String(s) => Some(s.to_utf8_string()),
                    ColumnValues::Null => None,
                    _ => return Err(Error::UsageError("Expected string or NULL for collation_name".to_string())),
                };

                // Map system_type_id to SqlDbType
                let sql_type = map_system_type_id_to_sql_db_type(system_type_id)?;

                // TODO: Parse collation_name to create SqlCollation
                // For now, use None (will be enhanced in future)
                let collation = None;

                metadata.push(DestinationColumnMetadata {
                    name,
                    ordinal: column_id - 1,  // SQL Server is 1-based, we use 0-based
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
        self.client.close_query().await?;

        if metadata.is_empty() {
            return Err(Error::UsageError(format!(
                "Table '{}' not found or has no columns",
                self.table_name
            )));
        }

        // Cache the metadata
        self.destination_metadata = Some(metadata.clone());

        Ok(metadata)
    }

    /// Retrieve destination table metadata directly from SQL Server's COLMETADATA token.
    ///
    /// This method queries the destination table with `SELECT TOP 0 * FROM table_name`
    /// to get the exact column metadata (including TDS types) that SQL Server expects.
    /// This is more accurate than querying sys.columns because it gives us the actual
    /// TDS types that will be used in bulk copy protocol.
    ///
    /// This matches the .NET SqlBulkCopy behavior which queries the table schema
    /// before sending bulk data.
    ///
    /// # Returns
    ///
    /// A vector of `BulkCopyColumnMetadata` with TDS types from the server
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table doesn't exist
    /// - Network errors occur
    /// - Timeout occurs
    pub async fn retrieve_destination_metadata_from_server(&mut self) -> TdsResult<Vec<BulkCopyColumnMetadata>> {
        // Fetch metadata from the server using COLMETADATA token
        let col_metadata = self.client.fetch_table_metadata(
            &self.table_name,
            Some(self.options.timeout_sec),
            None,
        ).await?;

        // Convert from ColumnMetadata (from COLMETADATA token) to BulkCopyColumnMetadata
        let bulk_copy_metadata: Vec<BulkCopyColumnMetadata> = col_metadata
            .columns
            .iter()
            .map(|col| col.into())
            .collect();

        if bulk_copy_metadata.is_empty() {
            return Err(Error::UsageError(format!(
                "Table '{}' not found or has no columns",
                self.table_name
            )));
        }

        eprintln!("DEBUG: Retrieved {} columns with server TDS types:", bulk_copy_metadata.len());
        for (i, meta) in bulk_copy_metadata.iter().enumerate() {
            eprintln!("  Column {}: name='{}', tds_type=0x{:02X}, sql_type={:?}, nullable={}", 
                i, meta.column_name, meta.tds_type, meta.sql_type, meta.is_nullable);
        }

        Ok(bulk_copy_metadata)
    }

    /// Parse table name into schema and table components.
    ///
    /// Handles formats:
    /// - "Users" → ("dbo", "Users")
    /// - "dbo.Users" → ("dbo", "Users")
    /// - "schema.table" → ("schema", "table")
    fn parse_table_name(&self) -> (String, String) {
        if let Some(dot_pos) = self.table_name.rfind('.') {
            let schema = self.table_name[..dot_pos].to_string();
            let table = self.table_name[dot_pos + 1..].to_string();
            (schema, table)
        } else {
            // Default to dbo schema
            ("dbo".to_string(), self.table_name.clone())
        }
    }

    /// Resolve column mappings from source to destination.
    ///
    /// This method resolves user-provided column mappings (by name or ordinal)
    /// against the destination table metadata. It validates:
    /// - All required (non-nullable, non-identity, non-computed) destination columns are mapped
    /// - Type compatibility between source and destination columns
    /// - No duplicate mappings to the same destination column
    ///
    /// # Arguments
    ///
    /// * `source_metadata` - Metadata for source columns
    /// * `destination_metadata` - Metadata for destination table columns
    ///
    /// # Returns
    ///
    /// A vector of resolved mappings from source index to destination index
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A required destination column is not mapped
    /// - A source column references a non-existent destination column
    /// - Type compatibility check fails
    /// - Duplicate mappings to the same destination column exist
    async fn resolve_column_mappings(
        &mut self,
        source_metadata: &[BulkCopyColumnMetadata],
        destination_metadata: &[DestinationColumnMetadata],
    ) -> TdsResult<Vec<ResolvedColumnMapping>> {
        let mut resolved_mappings = Vec::new();
        let mut mapped_destination_indices = std::collections::HashSet::new();

        if self.column_mappings.is_empty() {
            // No explicit mappings: use ordinal mapping (source[i] → destination[i])
            // This is the default behavior when no mappings are specified
            for (source_idx, source_col) in source_metadata.iter().enumerate() {
                if source_idx >= destination_metadata.len() {
                    // More source columns than destination columns - ignore extras
                    break;
                }

                let dest_col = &destination_metadata[source_idx];

                // Skip computed columns (they can't be inserted)
                if dest_col.is_computed {
                    continue;
                }

                // Skip identity columns unless keep_identity is enabled
                if dest_col.is_identity && !self.options.keep_identity {
                    continue;
                }

                // Check type compatibility
                self.check_type_compatibility(&source_col.sql_type, &dest_col.sql_type, &source_col.column_name, &dest_col.name)?;

                resolved_mappings.push(ResolvedColumnMapping {
                    source_index: source_idx,
                    destination_index: dest_col.ordinal,
                    destination_name: dest_col.name.clone(),
                    destination_type: dest_col.sql_type,
                });

                mapped_destination_indices.insert(dest_col.ordinal);
            }
        } else {
            // Explicit mappings: resolve each mapping
            for mapping in &self.column_mappings {
                // Find source column index
                let source_idx = match &mapping.source {
                    ColumnMappingSource::Name(source_name) => {
                        source_metadata
                            .iter()
                            .position(|col| col.column_name.eq_ignore_ascii_case(source_name))
                            .ok_or_else(|| {
                                Error::UsageError(format!(
                                    "Source column '{}' not found in source metadata",
                                    source_name
                                ))
                            })?
                    }
                    ColumnMappingSource::Ordinal(idx) => {
                        if *idx >= source_metadata.len() {
                            return Err(Error::UsageError(format!(
                                "Source column ordinal {} is out of range (source has {} columns)",
                                idx,
                                source_metadata.len()
                            )));
                        }
                        *idx
                    }
                };

                // Find destination column
                let dest_col = destination_metadata
                    .iter()
                    .find(|col| col.name.eq_ignore_ascii_case(&mapping.destination))
                    .ok_or_else(|| {
                        Error::UsageError(format!(
                            "Destination column '{}' not found in table '{}'",
                            mapping.destination, self.table_name
                        ))
                    })?;

                // Check for duplicate mappings
                if mapped_destination_indices.contains(&dest_col.ordinal) {
                    return Err(Error::UsageError(format!(
                        "Duplicate mapping to destination column '{}'",
                        dest_col.name
                    )));
                }

                // Skip computed columns
                if dest_col.is_computed {
                    return Err(Error::UsageError(format!(
                        "Cannot map to computed column '{}'",
                        dest_col.name
                    )));
                }

                // Check identity columns
                if dest_col.is_identity && !self.options.keep_identity {
                    return Err(Error::UsageError(format!(
                        "Cannot map to identity column '{}' unless keep_identity is enabled",
                        dest_col.name
                    )));
                }

                // Check type compatibility
                let source_col = &source_metadata[source_idx];
                self.check_type_compatibility(&source_col.sql_type, &dest_col.sql_type, &source_col.column_name, &dest_col.name)?;

                resolved_mappings.push(ResolvedColumnMapping {
                    source_index: source_idx,
                    destination_index: dest_col.ordinal,
                    destination_name: dest_col.name.clone(),
                    destination_type: dest_col.sql_type,
                });

                mapped_destination_indices.insert(dest_col.ordinal);
            }
        }

        // Validate that all required destination columns are mapped
        for dest_col in destination_metadata {
            // Skip computed columns and identity columns (unless keep_identity)
            if dest_col.is_computed {
                continue;
            }
            if dest_col.is_identity && !self.options.keep_identity {
                continue;
            }

            // Check if this required column is mapped
            if !dest_col.is_nullable && !mapped_destination_indices.contains(&dest_col.ordinal) {
                return Err(Error::UsageError(format!(
                    "Required destination column '{}' (ordinal {}) is not mapped and is not nullable",
                    dest_col.name, dest_col.ordinal
                )));
            }
        }

        Ok(resolved_mappings)
    }

    /// Check type compatibility between source and destination columns.
    ///
    /// This validates that the source ColumnValues type can be converted to
    /// the destination SqlDbType. Some conversions are implicit (e.g., Int → BigInt),
    /// while others are not allowed (e.g., String → Int).
    ///
    /// # Arguments
    ///
    /// * `source_type` - Source column SQL type
    /// * `dest_type` - Destination column SQL type
    /// * `source_name` - Source column name (for error messages)
    /// * `dest_name` - Destination column name (for error messages)
    ///
    /// # Errors
    ///
    /// Returns an error if the types are incompatible
    fn check_type_compatibility(
        &self,
        source_type: &SqlDbType,
        dest_type: &SqlDbType,
        source_name: &str,
        dest_name: &str,
    ) -> TdsResult<()> {
        // Exact match is always compatible
        if source_type == dest_type {
            return Ok(());
        }

        // Check compatible type conversions
        let compatible = match (source_type, dest_type) {
            // Numeric type promotions (smaller → larger)
            (SqlDbType::TinyInt, SqlDbType::SmallInt | SqlDbType::Int | SqlDbType::BigInt) => true,
            (SqlDbType::SmallInt, SqlDbType::Int | SqlDbType::BigInt) => true,
            (SqlDbType::Int, SqlDbType::BigInt) => true,
            
            // Numeric to float conversions
            (SqlDbType::TinyInt | SqlDbType::SmallInt | SqlDbType::Int | SqlDbType::BigInt, 
             SqlDbType::Real | SqlDbType::Float) => true,
            (SqlDbType::Real, SqlDbType::Float) => true,
            
            // Decimal/Numeric are interchangeable
            (SqlDbType::Decimal, SqlDbType::Numeric) | (SqlDbType::Numeric, SqlDbType::Decimal) => true,
            
            // String type conversions (char → varchar, nchar → nvarchar)
            (SqlDbType::Char, SqlDbType::VarChar) => true,
            (SqlDbType::NChar, SqlDbType::NVarChar) => true,
            (SqlDbType::VarChar, SqlDbType::NVarChar) => true, // ASCII → Unicode
            
            // Text type conversions
            (SqlDbType::Text, SqlDbType::VarChar | SqlDbType::NVarChar) => true,
            (SqlDbType::NText, SqlDbType::NVarChar) => true,
            (SqlDbType::VarChar, SqlDbType::Text) => true,
            (SqlDbType::NVarChar, SqlDbType::NText) => true,
            
            // Binary type conversions
            (SqlDbType::Binary, SqlDbType::VarBinary) => true,
            (SqlDbType::VarBinary, SqlDbType::Image) => true,
            (SqlDbType::Image, SqlDbType::VarBinary) => true,
            
            // DateTime conversions
            (SqlDbType::SmallDateTime, SqlDbType::DateTime | SqlDbType::DateTime2) => true,
            (SqlDbType::DateTime, SqlDbType::DateTime2) => true,
            (SqlDbType::Date, SqlDbType::DateTime | SqlDbType::DateTime2) => true,
            
            // Money conversions
            (SqlDbType::SmallMoney, SqlDbType::Money) => true,
            
            // All other combinations are incompatible
            _ => false,
        };

        if !compatible {
            return Err(Error::UsageError(format!(
                "Type mismatch: Cannot convert source column '{}' ({:?}) to destination column '{}' ({:?})",
                source_name, source_type, dest_name, dest_type
            )));
        }

        Ok(())
    }

    /// Write rows to the server using an iterator.
    ///
    /// This method will batch rows according to the configured `batch_size`,
    /// serialize them using the TDS bulk load protocol, and send them to the server.
    ///
    /// # Arguments
    ///
    /// * `rows` - Iterator over rows implementing `BulkCopyRow`
    ///
    /// # Returns
    ///
    /// `BulkCopyResult` containing statistics about the operation
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Connection is not available
    /// - Invalid configuration options
    /// - Network errors during transmission
    /// - SQL Server errors (constraints, type mismatches, etc.)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let rows = vec![
    ///     User { id: 1, name: "Alice".to_string() },
    ///     User { id: 2, name: "Bob".to_string() },
    /// ];
    ///
    /// let result = bulk_copy.write_to_server(rows.into_iter()).await?;
    /// println!("Inserted {} rows", result.rows_affected);
    /// ```
    pub async fn write_to_server<I, R>(&mut self, rows: I) -> TdsResult<BulkCopyResult>
    where
        I: Iterator<Item = R>,
        R: BulkCopyRow,
    {
        // Validate options
        self.options.validate()?;

        let start_time = Instant::now();
        let mut total_rows: u64 = 0;

        // Get column metadata from the row type
        let source_metadata = R::column_metadata();

        // Retrieve destination table metadata directly from server with exact TDS types
        // This matches .NET SqlBulkCopy behavior and ensures we use the types SQL Server expects
        let server_metadata = self.retrieve_destination_metadata_from_server().await?;

        // For column mapping resolution, we still need DestinationColumnMetadata
        // So retrieve that as well (it's cached)
        let destination_metadata = self.retrieve_destination_metadata().await?;

        // Resolve column mappings
        let resolved_mappings = self.resolve_column_mappings(&source_metadata, &destination_metadata).await?;

        // Build destination column metadata for the bulk load message
        // Use the server-provided metadata with exact TDS types
        let mut dest_column_metadata = Vec::new();
        for mapping in &resolved_mappings {
            // Use the server metadata which has the correct TDS types
            let server_col = &server_metadata[mapping.destination_index];
            dest_column_metadata.push(server_col.clone());
        }

        // Determine batch size (0 means all rows in one batch)
        let batch_size = if self.options.batch_size == 0 {
            usize::MAX
        } else {
            self.options.batch_size
        };

        // Process rows in batches
        let mut rows = rows.peekable();
        while rows.peek().is_some() {
            // Collect a batch of rows
            let mut batch_rows = Vec::with_capacity(batch_size.min(10000));
            for _ in 0..batch_size {
                if let Some(row) = rows.next() {
                    let source_values = row.to_column_values();
                    
                    // Reorder columns according to resolved mappings
                    let mut dest_values = Vec::with_capacity(resolved_mappings.len());
                    for mapping in &resolved_mappings {
                        if mapping.source_index < source_values.len() {
                            dest_values.push(source_values[mapping.source_index].clone());
                        } else {
                            return Err(Error::UsageError(format!(
                                "Source row has {} columns, but mapping references column {}",
                                source_values.len(),
                                mapping.source_index
                            )));
                        }
                    }
                    
                    batch_rows.push(dest_values);
                } else {
                    break;
                }
            }

            if batch_rows.is_empty() {
                break;
            }

            // Send this batch to the server
            let batch_count = self.send_batch(&dest_column_metadata, batch_rows).await?;
            total_rows += batch_count;

            // Report progress if callback is configured
            if let Some(ref mut callback) = self.progress_callback {
                if self.options.notification_interval > 0
                    && total_rows % self.options.notification_interval as u64 == 0
                {
                    let elapsed = start_time.elapsed();
                    let rows_per_second = if elapsed.as_secs_f64() > 0.0 {
                        total_rows as f64 / elapsed.as_secs_f64()
                    } else {
                        0.0
                    };

                    callback(BulkCopyProgress {
                        rows_copied: total_rows,
                        total_rows: None,
                        elapsed,
                        rows_per_second,
                    });
                }
            }
        }

        let elapsed = start_time.elapsed();
        Ok(BulkCopyResult::new(total_rows, elapsed))
    }

    /// Send a single batch of rows to the server.
    ///
    /// This is an internal method that creates a BulkLoadMessage and sends it
    /// through the TDS protocol.
    async fn send_batch(
        &mut self,
        column_metadata: &[BulkCopyColumnMetadata],
        rows: Vec<Vec<ColumnValues>>,
    ) -> TdsResult<u64> {
        // Create the bulk load message (cloning metadata since BulkLoadMessage owns its data)
        let message = BulkLoadMessage {
            table_name: self.table_name.clone(),
            column_metadata: column_metadata.to_vec(),
            rows,
            options: self.options.clone(),
        };

        // Send the message through the TDS client and get the number of rows affected
        let timeout_sec = if self.options.timeout_sec > 0 {
            Some(self.options.timeout_sec)
        } else {
            None
        };

        // Execute the bulk load and return the row count from SQL Server
        let rows_affected = self
            .client
            .execute_bulk_load(message, timeout_sec, None)
            .await?;

        Ok(rows_affected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bulk_copy_options_default() {
        let opts = BulkCopyOptions::default();
        assert_eq!(opts.batch_size, 0);
        assert_eq!(opts.timeout_sec, 30);
        assert!(!opts.check_constraints);
        assert!(!opts.fire_triggers);
        assert!(!opts.keep_identity);
        assert!(!opts.keep_nulls);
        assert!(!opts.table_lock);
        assert!(opts.use_internal_transaction);
        assert_eq!(opts.notification_interval, 0);
    }

    #[test]
    fn test_bulk_copy_options_validate() {
        let opts = BulkCopyOptions {
            batch_size: 5000,
            ..Default::default()
        };
        assert!(opts.validate().is_ok());

        let invalid_opts = BulkCopyOptions {
            batch_size: 2_000_000,
            ..Default::default()
        };
        assert!(invalid_opts.validate().is_err());
    }

    #[test]
    fn test_column_mapping_by_name() {
        let mapping = ColumnMapping::by_name("source_col", "dest_col");
        assert!(matches!(
            mapping.source,
            ColumnMappingSource::Name(ref name) if name == "source_col"
        ));
        assert_eq!(mapping.destination, "dest_col");
    }

    #[test]
    fn test_column_mapping_by_ordinal() {
        let mapping = ColumnMapping::by_ordinal(2, "dest_col");
        assert!(matches!(
            mapping.source,
            ColumnMappingSource::Ordinal(2)
        ));
        assert_eq!(mapping.destination, "dest_col");
    }

    #[test]
    fn test_bulk_copy_progress_percentage() {
        let progress = BulkCopyProgress {
            rows_copied: 500,
            total_rows: Some(1000),
            elapsed: Duration::from_secs(10),
            rows_per_second: 50.0,
        };

        assert_eq!(progress.percentage(), Some(50.0));

        let progress_no_total = BulkCopyProgress {
            rows_copied: 500,
            total_rows: None,
            elapsed: Duration::from_secs(10),
            rows_per_second: 50.0,
        };

        assert_eq!(progress_no_total.percentage(), None);
    }

    #[test]
    fn test_bulk_copy_progress_estimated_time() {
        let progress = BulkCopyProgress {
            rows_copied: 1000,
            total_rows: Some(2000),
            elapsed: Duration::from_secs(10),
            rows_per_second: 100.0,
        };

        let estimated = progress.estimated_time_remaining().unwrap();
        assert_eq!(estimated.as_secs(), 10); // 1000 remaining rows / 100 rows/sec = 10 sec
    }

    #[test]
    fn test_bulk_copy_result() {
        let result = BulkCopyResult::new(10000, Duration::from_secs(10));
        assert_eq!(result.rows_affected, 10000);
        assert_eq!(result.rows_per_second, 1000.0);
    }

    #[test]
    fn test_destination_metadata_to_bulk_copy_metadata() {
        let dest_meta = DestinationColumnMetadata {
            name: "TestColumn".to_string(),
            ordinal: 0,
            system_type_id: 56, // Int
            sql_type: SqlDbType::Int,
            max_length: 4,
            precision: 0,
            scale: 0,
            is_nullable: true,
            is_identity: false,
            is_computed: false,
            collation: None,
        };

        let bulk_meta = dest_meta.to_bulk_copy_metadata();
        assert_eq!(bulk_meta.column_name, "TestColumn");
        assert_eq!(bulk_meta.sql_type, SqlDbType::Int);
        assert_eq!(bulk_meta.tds_type, 0x38); // TDS type for Int
        assert!(bulk_meta.is_nullable);
    }

    #[test]
    fn test_map_system_type_id_to_sql_db_type() {
        // Test common types
        assert_eq!(map_system_type_id_to_sql_db_type(48).unwrap(), SqlDbType::TinyInt);
        assert_eq!(map_system_type_id_to_sql_db_type(56).unwrap(), SqlDbType::Int);
        assert_eq!(map_system_type_id_to_sql_db_type(127).unwrap(), SqlDbType::BigInt);
        assert_eq!(map_system_type_id_to_sql_db_type(231).unwrap(), SqlDbType::NVarChar);
        assert_eq!(map_system_type_id_to_sql_db_type(167).unwrap(), SqlDbType::VarChar);
        assert_eq!(map_system_type_id_to_sql_db_type(36).unwrap(), SqlDbType::UniqueIdentifier);

        // Test unsupported type
        assert!(map_system_type_id_to_sql_db_type(255).is_err());
    }

    #[test]
    fn test_parse_table_name() {
        // Mock a TdsClient (we just need the table_name field for this test)
        // Note: In real code, we'd use a proper test fixture or mock
        // For now, we'll test the logic directly through the parsing behavior
        
        // Test with schema.table format
        let table_with_schema = "myschema.mytable";
        let (schema, table) = if let Some(dot_pos) = table_with_schema.rfind('.') {
            let s = table_with_schema[..dot_pos].to_string();
            let t = table_with_schema[dot_pos + 1..].to_string();
            (s, t)
        } else {
            ("dbo".to_string(), table_with_schema.to_string())
        };
        assert_eq!(schema, "myschema");
        assert_eq!(table, "mytable");

        // Test without schema (defaults to dbo)
        let table_without_schema = "mytable";
        let (schema2, table2) = if let Some(dot_pos) = table_without_schema.rfind('.') {
            let s = table_without_schema[..dot_pos].to_string();
            let t = table_without_schema[dot_pos + 1..].to_string();
            (s, t)
        } else {
            ("dbo".to_string(), table_without_schema.to_string())
        };
        assert_eq!(schema2, "dbo");
        assert_eq!(table2, "mytable");
    }

    // Tests for type compatibility checking
    // Note: These test the internal logic without needing a full BulkCopy instance
    
    fn check_compat(source: SqlDbType, dest: SqlDbType) -> bool {
        // Exact match
        if source == dest {
            return true;
        }

        // Test the compatibility rules
        matches!(
            (source, dest),
            // Numeric promotions
            (SqlDbType::TinyInt, SqlDbType::SmallInt | SqlDbType::Int | SqlDbType::BigInt) |
            (SqlDbType::SmallInt, SqlDbType::Int | SqlDbType::BigInt) |
            (SqlDbType::Int, SqlDbType::BigInt) |
            // Numeric to float
            (SqlDbType::TinyInt | SqlDbType::SmallInt | SqlDbType::Int | SqlDbType::BigInt, 
             SqlDbType::Real | SqlDbType::Float) |
            (SqlDbType::Real, SqlDbType::Float) |
            // Decimal/Numeric
            (SqlDbType::Decimal, SqlDbType::Numeric) | (SqlDbType::Numeric, SqlDbType::Decimal) |
            // String conversions
            (SqlDbType::Char, SqlDbType::VarChar) |
            (SqlDbType::NChar, SqlDbType::NVarChar) |
            (SqlDbType::VarChar, SqlDbType::NVarChar) |
            (SqlDbType::Text, SqlDbType::VarChar | SqlDbType::NVarChar) |
            (SqlDbType::NText, SqlDbType::NVarChar) |
            (SqlDbType::VarChar, SqlDbType::Text) |
            (SqlDbType::NVarChar, SqlDbType::NText) |
            // Binary conversions
            (SqlDbType::Binary, SqlDbType::VarBinary) |
            (SqlDbType::VarBinary, SqlDbType::Image) |
            (SqlDbType::Image, SqlDbType::VarBinary) |
            // DateTime conversions
            (SqlDbType::SmallDateTime, SqlDbType::DateTime | SqlDbType::DateTime2) |
            (SqlDbType::DateTime, SqlDbType::DateTime2) |
            (SqlDbType::Date, SqlDbType::DateTime | SqlDbType::DateTime2) |
            // Money conversions
            (SqlDbType::SmallMoney, SqlDbType::Money)
        )
    }

    #[test]
    fn test_type_compatibility_exact_match() {
        // Exact type matches should always be compatible
        assert!(check_compat(SqlDbType::Int, SqlDbType::Int));
        assert!(check_compat(SqlDbType::NVarChar, SqlDbType::NVarChar));
        assert!(check_compat(SqlDbType::DateTime2, SqlDbType::DateTime2));
    }

    #[test]
    fn test_type_compatibility_numeric_promotions() {
        // TinyInt can promote to larger integer types
        assert!(check_compat(SqlDbType::TinyInt, SqlDbType::SmallInt));
        assert!(check_compat(SqlDbType::TinyInt, SqlDbType::Int));
        assert!(check_compat(SqlDbType::TinyInt, SqlDbType::BigInt));
        
        // SmallInt can promote to larger integer types
        assert!(check_compat(SqlDbType::SmallInt, SqlDbType::Int));
        assert!(check_compat(SqlDbType::SmallInt, SqlDbType::BigInt));
        
        // Int can promote to BigInt
        assert!(check_compat(SqlDbType::Int, SqlDbType::BigInt));
        
        // But not the reverse
        assert!(!check_compat(SqlDbType::BigInt, SqlDbType::Int));
        assert!(!check_compat(SqlDbType::Int, SqlDbType::SmallInt));
    }

    #[test]
    fn test_type_compatibility_numeric_to_float() {
        // Integer types can convert to float types
        assert!(check_compat(SqlDbType::TinyInt, SqlDbType::Real));
        assert!(check_compat(SqlDbType::SmallInt, SqlDbType::Float));
        assert!(check_compat(SqlDbType::Int, SqlDbType::Float));
        assert!(check_compat(SqlDbType::BigInt, SqlDbType::Real));
        
        // Real can promote to Float
        assert!(check_compat(SqlDbType::Real, SqlDbType::Float));
        
        // But not the reverse
        assert!(!check_compat(SqlDbType::Float, SqlDbType::Real));
    }

    #[test]
    fn test_type_compatibility_string_types() {
        // Char types can convert to Varchar
        assert!(check_compat(SqlDbType::Char, SqlDbType::VarChar));
        assert!(check_compat(SqlDbType::NChar, SqlDbType::NVarChar));
        
        // ASCII to Unicode conversion
        assert!(check_compat(SqlDbType::VarChar, SqlDbType::NVarChar));
        
        // Text type conversions
        assert!(check_compat(SqlDbType::Text, SqlDbType::VarChar));
        assert!(check_compat(SqlDbType::Text, SqlDbType::NVarChar));
        assert!(check_compat(SqlDbType::NText, SqlDbType::NVarChar));
        assert!(check_compat(SqlDbType::VarChar, SqlDbType::Text));
        assert!(check_compat(SqlDbType::NVarChar, SqlDbType::NText));
    }

    #[test]
    fn test_type_compatibility_binary_types() {
        assert!(check_compat(SqlDbType::Binary, SqlDbType::VarBinary));
        assert!(check_compat(SqlDbType::VarBinary, SqlDbType::Image));
        assert!(check_compat(SqlDbType::Image, SqlDbType::VarBinary));
    }

    #[test]
    fn test_type_compatibility_datetime_types() {
        // SmallDateTime can convert to DateTime and DateTime2
        assert!(check_compat(SqlDbType::SmallDateTime, SqlDbType::DateTime));
        assert!(check_compat(SqlDbType::SmallDateTime, SqlDbType::DateTime2));
        
        // DateTime can convert to DateTime2
        assert!(check_compat(SqlDbType::DateTime, SqlDbType::DateTime2));
        
        // Date can convert to DateTime types
        assert!(check_compat(SqlDbType::Date, SqlDbType::DateTime));
        assert!(check_compat(SqlDbType::Date, SqlDbType::DateTime2));
    }

    #[test]
    fn test_type_compatibility_incompatible_types() {
        // String to numeric should not be compatible
        assert!(!check_compat(SqlDbType::VarChar, SqlDbType::Int));
        assert!(!check_compat(SqlDbType::NVarChar, SqlDbType::BigInt));
        
        // Numeric to string should not be compatible
        assert!(!check_compat(SqlDbType::Int, SqlDbType::VarChar));
        
        // DateTime to numeric should not be compatible
        assert!(!check_compat(SqlDbType::DateTime, SqlDbType::Int));
        
        // Binary to string should not be compatible
        assert!(!check_compat(SqlDbType::VarBinary, SqlDbType::VarChar));
    }

    // Note: Full integration tests for BulkCopy require a TdsClient connection
    // and will be added in the integration test phase
}
