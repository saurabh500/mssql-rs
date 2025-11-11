// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Bulk copy column metadata structures for BulkCopy operations.
//!
//! This module provides the metadata structures needed to properly transmit
//! column information during bulk copy operations, matching the .NET SqlBulkCopy
//! implementation's metadata handling.

use crate::token::tokens::SqlCollation;
use tracing::{trace, warn};

/// SQL Database types supported in bulk copy operations.
///
/// This enum represents the SQL Server data types that can be used in BulkCopy.
/// It aligns with SQL Server's type system and TDS protocol requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDbType {
    // Integer types
    BigInt,
    Int,
    SmallInt,
    TinyInt,
    Bit,

    // Floating point types
    Float,
    Real,

    // Exact numeric types
    Decimal,
    Numeric,
    Money,
    SmallMoney,

    // Date and time types
    Date,
    DateTime,
    DateTime2,
    DateTimeOffset,
    SmallDateTime,
    Time,

    // Character types
    Char,
    VarChar,
    Text,
    NChar,
    NVarChar,
    NText,

    // Binary types
    Binary,
    VarBinary,
    Image,

    // Other types
    UniqueIdentifier,
    Xml,
    Variant,
    Udt,

    // SQL Server 2019+ types
    Json,

    // SQL Server 2025+ types (future)
    Vector,
}

impl SqlDbType {
    /// Map SqlDbType to TDS protocol type byte for bulk copy.
    ///
    /// Returns the TDS type identifier used in the COLMETADATA token.
    /// For bulk copy, we use nullable variant types (e.g., INTN instead of INT4)
    /// to support nullable columns, matching .NET SqlBulkCopy behavior.
    /// Reference: TDS protocol specification and .NET MetaType.NullableType
    pub fn to_tds_type(&self) -> u8 {
        match self {
            // Integer types - use INTN (0x26) for nullable int types
            SqlDbType::TinyInt => 0x26,  // TdsDataType::IntN (length 1)
            SqlDbType::SmallInt => 0x26, // TdsDataType::IntN (length 2)
            SqlDbType::Int => 0x26,      // TdsDataType::IntN (length 4)
            SqlDbType::BigInt => 0x26,   // TdsDataType::IntN (length 8)
            SqlDbType::Bit => 0x68,      // TdsDataType::BitN

            // Floating point types - use FLTN (0x6D) for nullable float types
            SqlDbType::Real => 0x6D,  // TdsDataType::FltN (length 4)
            SqlDbType::Float => 0x6D, // TdsDataType::FltN (length 8)

            // Exact numeric types
            SqlDbType::Decimal => 0x6A,    // TdsDataType::DecimalN
            SqlDbType::Numeric => 0x6C,    // TdsDataType::NumericN
            SqlDbType::Money => 0x6E,      // TdsDataType::MoneyN (length 8)
            SqlDbType::SmallMoney => 0x6E, // TdsDataType::MoneyN (length 4)

            // Date and time types
            SqlDbType::Date => 0x28,           // TdsDataType::DateN
            SqlDbType::Time => 0x29,           // TdsDataType::TimeN
            SqlDbType::DateTime => 0x6F,       // TdsDataType::DateTimeN (length 8)
            SqlDbType::DateTime2 => 0x2A,      // TdsDataType::DateTime2N
            SqlDbType::DateTimeOffset => 0x2B, // TdsDataType::DateTimeOffsetN
            SqlDbType::SmallDateTime => 0x6F,  // TdsDataType::DateTimeN (length 4)

            // Character types
            SqlDbType::Char => 0xAF,     // TdsDataType::BigChar
            SqlDbType::VarChar => 0xA7,  // TdsDataType::BigVarChar
            SqlDbType::Text => 0x23,     // TdsDataType::Text
            SqlDbType::NChar => 0xEF,    // TdsDataType::NChar
            SqlDbType::NVarChar => 0xE7, // TdsDataType::NVarChar
            SqlDbType::NText => 0x63,    // TdsDataType::NText

            // Binary types
            SqlDbType::Binary => 0xAD,    // TdsDataType::BigBinary
            SqlDbType::VarBinary => 0xA5, // TdsDataType::BigVarBinary
            SqlDbType::Image => 0x22,     // TdsDataType::Image

            // Other types
            SqlDbType::UniqueIdentifier => 0x24, // TdsDataType::Guid
            SqlDbType::Xml => 0xF1,              // TdsDataType::Xml
            SqlDbType::Json => 0xF4,             // TdsDataType::Json
            SqlDbType::Variant => 0x62,          // TdsDataType::SsVariant
            SqlDbType::Udt => 0xF0,              // TdsDataType::Udt
            SqlDbType::Vector => 0x00,           // Future type, placeholder
        }
    }

    /// Convert SqlDbType to TDS type byte using fixed-length types (non-nullable).
    ///
    /// Use this for NOT NULL columns to generate more compact wire format.
    /// For nullable columns, use `to_tds_type()` instead.
    pub fn to_tds_type_fixed(&self) -> u8 {
        match self {
            // Integer types - use fixed-length types
            SqlDbType::TinyInt => 0x30,  // TdsDataType::Int1
            SqlDbType::SmallInt => 0x34, // TdsDataType::Int2
            SqlDbType::Int => 0x38,      // TdsDataType::Int4
            SqlDbType::BigInt => 0x7F,   // TdsDataType::Int8
            SqlDbType::Bit => 0x32,      // TdsDataType::Bit

            // Floating point types - use fixed-length types
            SqlDbType::Real => 0x3B,  // TdsDataType::Flt4
            SqlDbType::Float => 0x3E, // TdsDataType::Flt8

            // For other types, use the nullable variant (they don't have fixed versions)
            _ => self.to_tds_type(),
        }
    }
}

/// Encoding types for character data.
///
/// Represents different character encodings that can be used for string columns,
/// determined by the column's collation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum EncodingType {
    /// UTF-8 encoding
    #[default]
    Utf8,
    /// UTF-16 Little Endian (used for NCHAR/NVARCHAR)
    Utf16Le,
    /// Latin-1 (ISO-8859-1)
    Latin1,
    /// Windows code page encoding
    CodePage(u16),
}

impl EncodingType {
    /// Encode a Rust string to bytes using this encoding.
    pub fn encode(&self, s: &str) -> Vec<u8> {
        match self {
            EncodingType::Utf8 => s.as_bytes().to_vec(),
            EncodingType::Utf16Le => s.encode_utf16().flat_map(|c| c.to_le_bytes()).collect(),
            EncodingType::Latin1 => {
                // Latin-1 is the first 256 Unicode code points
                s.chars()
                    .map(|c| {
                        let code = c as u32;
                        if code < 256 {
                            code as u8
                        } else {
                            b'?' // Replacement character
                        }
                    })
                    .collect()
            }
            EncodingType::CodePage(cp) => {
                // For now, fallback to UTF-8 for non-Latin1 code pages
                // A full implementation would use encoding_rs or similar
                warn!("Code page {} not fully supported, using UTF-8", cp);
                s.as_bytes().to_vec()
            }
        }
    }

    /// Calculate the byte length of a string in this encoding.
    pub fn byte_length(&self, s: &str) -> usize {
        match self {
            EncodingType::Utf8 => s.len(),
            EncodingType::Utf16Le => s.len() * 2,
            EncodingType::Latin1 => s.len(),
            EncodingType::CodePage(_) => s.len(), // Approximation
        }
    }

    /// Get the encoding for NCHAR/NVARCHAR types (always UTF-16LE).
    pub fn unicode() -> Self {
        EncodingType::Utf16Le
    }

    /// Get the default encoding for CHAR/VARCHAR types (UTF-8).
    pub fn default_ansi() -> Self {
        EncodingType::Utf8
    }
}

/// Type length specification for SQL types.
///
/// Different SQL types have different length semantics:
/// - Fixed: Types with a fixed size (e.g., INT, BIGINT)
/// - Variable: Variable-length types with a maximum (e.g., VARCHAR(100))
/// - Plp: Partial Length Prefix types (MAX types: VARCHAR(MAX), VARBINARY(MAX))
/// - Unknown: For streaming data where length is not known in advance
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeLength {
    /// Fixed-length type (e.g., INT is always 4 bytes)
    Fixed(i32),
    /// Variable-length type with maximum (e.g., VARCHAR(100))
    Variable(i32),
    /// Partial Length Prefix (MAX types)
    Plp,
    /// Unknown length (for streaming)
    Unknown,
}

impl TypeLength {
    /// Check if this is a PLP (MAX) type.
    pub fn is_plp(&self) -> bool {
        matches!(self, TypeLength::Plp)
    }

    /// Check if this is a fixed-length type.
    pub fn is_fixed(&self) -> bool {
        matches!(self, TypeLength::Fixed(_))
    }

    /// Get the maximum length, if applicable.
    pub fn max_length(&self) -> Option<i32> {
        match self {
            TypeLength::Fixed(len) | TypeLength::Variable(len) => Some(*len),
            TypeLength::Plp | TypeLength::Unknown => None,
        }
    }
}

/// Column metadata for bulk copy operations.
///
/// This structure contains all the metadata needed to properly serialize
/// a column's data in the TDS bulk load protocol. It closely mirrors the
/// .NET SqlBulkCopy implementation's `_SqlMetaData` structure.
///
/// # Example
///
/// ```rust,ignore
/// use mssql_tds::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
///
/// let metadata = BulkCopyColumnMetadata {
///     column_name: "id".to_string(),
///     sql_type: SqlDbType::Int,
///     tds_type: 0x38, // SQLINT4
///     length: 4,
///     length_type: TypeLength::Fixed(4),
///     precision: 0,
///     scale: 0,
///     collation: None,
///     encoding: None,
///     is_nullable: false,
///     is_identity: true,
///     is_encrypted: false,
///     table_name: None,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct BulkCopyColumnMetadata {
    /// Column name
    pub column_name: String,

    /// SQL data type
    pub sql_type: SqlDbType,

    /// TDS wire protocol type byte
    pub tds_type: u8,

    /// Maximum length for the column
    pub length: i32,

    /// Length type classification
    pub length_type: TypeLength,

    /// Precision (for Decimal/Numeric types)
    pub precision: u8,

    /// Scale (for Decimal/Numeric/Time/DateTime2/DateTimeOffset types)
    pub scale: u8,

    /// Collation information (for character types)
    pub collation: Option<SqlCollation>,

    /// Character encoding (for character types)
    pub encoding: Option<EncodingType>,

    /// Whether the column accepts NULL values
    pub is_nullable: bool,

    /// Whether the column is an identity column
    pub is_identity: bool,

    /// Whether the column is encrypted (Always Encrypted)
    pub is_encrypted: bool,

    /// Table name (for long/LOB types in some TDS versions)
    pub table_name: Option<String>,
}

impl BulkCopyColumnMetadata {
    /// Create a new metadata instance with required fields.
    pub fn new(column_name: impl Into<String>, sql_type: SqlDbType, tds_type: u8) -> Self {
        Self {
            column_name: column_name.into(),
            sql_type,
            tds_type,
            length: 0,
            length_type: TypeLength::Fixed(0),
            precision: 0,
            scale: 0,
            collation: None,
            encoding: None,
            is_nullable: true,
            is_identity: false,
            is_encrypted: false,
            table_name: None,
        }
    }

    /// Set the column length.
    pub fn with_length(mut self, length: i32, length_type: TypeLength) -> Self {
        self.length = length;
        self.length_type = length_type;
        self
    }

    /// Set precision and scale (for Decimal/Numeric).
    pub fn with_precision_scale(mut self, precision: u8, scale: u8) -> Self {
        self.precision = precision;
        self.scale = scale;
        self
    }

    /// Set scale only (for Time/DateTime2/DateTimeOffset).
    pub fn with_scale(mut self, scale: u8) -> Self {
        self.scale = scale;
        self
    }

    /// Set collation (for character types).
    pub fn with_collation(mut self, collation: SqlCollation) -> Self {
        self.collation = Some(collation);
        self
    }

    /// Set encoding (for character types).
    pub fn with_encoding(mut self, encoding: EncodingType) -> Self {
        self.encoding = Some(encoding);
        self
    }

    /// Set nullable flag.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.is_nullable = nullable;
        self
    }

    /// Set identity flag.
    pub fn with_identity(mut self, identity: bool) -> Self {
        self.is_identity = identity;
        self
    }

    /// Set encrypted flag.
    pub fn with_encrypted(mut self, encrypted: bool) -> Self {
        self.is_encrypted = encrypted;
        self
    }

    /// Set table name (for LOB types).
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = Some(table_name.into());
        self
    }

    /// Check if this is a character type that needs collation.
    pub fn needs_collation(&self) -> bool {
        matches!(
            self.sql_type,
            SqlDbType::Char
                | SqlDbType::VarChar
                | SqlDbType::Text
                | SqlDbType::NChar
                | SqlDbType::NVarChar
                | SqlDbType::NText
        )
    }

    /// Check if this is a numeric type that needs precision/scale.
    pub fn needs_precision_scale(&self) -> bool {
        matches!(self.sql_type, SqlDbType::Decimal | SqlDbType::Numeric)
    }

    /// Check if this is a PLP (MAX) type.
    pub fn is_plp(&self) -> bool {
        self.length_type.is_plp()
    }

    /// Check if this is a long type (legacy LOB types).
    pub fn is_long(&self) -> bool {
        matches!(
            self.sql_type,
            SqlDbType::Text | SqlDbType::NText | SqlDbType::Image
        )
    }

    /// Get the SQL type definition string for this column.
    ///
    /// This generates a SQL type definition string similar to what's used in
    /// CREATE TABLE statements and sp_executesql parameter lists.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let meta = BulkCopyColumnMetadata::new("id", SqlDbType::Int, 0x38);
    /// assert_eq!(meta.get_sql_type_definition(), "int");
    ///
    /// let meta = BulkCopyColumnMetadata::new("name", SqlDbType::NVarChar, 0xE7)
    ///     .with_length(100, TypeLength::Variable(100));
    /// assert_eq!(meta.get_sql_type_definition(), "nvarchar(100)");
    /// ```
    pub fn get_sql_type_definition(&self) -> String {
        match self.sql_type {
            SqlDbType::Int => "int".to_string(),
            SqlDbType::BigInt => "bigint".to_string(),
            SqlDbType::SmallInt => "smallint".to_string(),
            SqlDbType::TinyInt => "tinyint".to_string(),
            SqlDbType::Bit => "bit".to_string(),
            SqlDbType::Decimal | SqlDbType::Numeric => {
                format!("decimal({}, {})", self.precision, self.scale)
            }
            SqlDbType::Float => "float".to_string(),
            SqlDbType::Real => "real".to_string(),
            SqlDbType::Money => "money".to_string(),
            SqlDbType::SmallMoney => "smallmoney".to_string(),
            SqlDbType::NVarChar => {
                if self.is_plp() {
                    "nvarchar(max)".to_string()
                } else {
                    format!("nvarchar({})", self.length)
                }
            }
            SqlDbType::NChar => format!("nchar({})", self.length),
            SqlDbType::VarChar => {
                if self.is_plp() {
                    "varchar(max)".to_string()
                } else {
                    format!("varchar({})", self.length)
                }
            }
            SqlDbType::Char => format!("char({})", self.length),
            SqlDbType::VarBinary => {
                if self.is_plp() {
                    "varbinary(max)".to_string()
                } else {
                    format!("varbinary({})", self.length)
                }
            }
            SqlDbType::Binary => format!("binary({})", self.length),
            SqlDbType::UniqueIdentifier => "uniqueidentifier".to_string(),
            SqlDbType::DateTime => "datetime".to_string(),
            SqlDbType::SmallDateTime => "smalldatetime".to_string(),
            SqlDbType::Date => "date".to_string(),
            SqlDbType::Time => format!("time({})", self.scale),
            SqlDbType::DateTime2 => format!("datetime2({})", self.scale),
            SqlDbType::DateTimeOffset => format!("datetimeoffset({})", self.scale),
            SqlDbType::Text => "text".to_string(),
            SqlDbType::NText => "ntext".to_string(),
            SqlDbType::Image => "image".to_string(),
            SqlDbType::Xml => "xml".to_string(),
            SqlDbType::Udt => format!("varbinary({})", self.length),
            SqlDbType::Variant => "sql_variant".to_string(),
            SqlDbType::Json => "nvarchar(max)".to_string(),
            SqlDbType::Vector => format!("vector({})", self.length),
        }
    }
}

impl Default for BulkCopyColumnMetadata {
    fn default() -> Self {
        Self {
            column_name: String::new(),
            sql_type: SqlDbType::Int,
            tds_type: 0x38, // SQLINT4
            length: 4,
            length_type: TypeLength::Fixed(4),
            precision: 0,
            scale: 0,
            collation: None,
            encoding: None,
            is_nullable: true,
            is_identity: false,
            is_encrypted: false,
            table_name: None,
        }
    }
}

/// Convert from a TDS ColumnMetadata (from COLMETADATA token) to BulkCopyColumnMetadata.
///
/// This function extracts the TDS type and other metadata directly from the server's
/// COLMETADATA response, ensuring we use the exact types that SQL Server expects.
impl From<&crate::query::metadata::ColumnMetadata> for BulkCopyColumnMetadata {
    fn from(col: &crate::query::metadata::ColumnMetadata) -> Self {
        use crate::datatypes::sqldatatypes::TdsDataType;
        use crate::datatypes::sqldatatypes::TypeInfoVariant;

        // Extract TDS type byte
        let tds_type = col.data_type as u8;

        // Map TDS type to SqlDbType
        let sql_type = match col.data_type {
            TdsDataType::Int8 => SqlDbType::BigInt,
            TdsDataType::Int4 => SqlDbType::Int,
            TdsDataType::IntN => {
                // INTN can represent TinyInt, SmallInt, Int, or BigInt depending on length
                // For bulk copy, we'll infer from type_info
                match &col.type_info.type_info_variant {
                    TypeInfoVariant::VarLen(_var_type, len) => match *len {
                        1 => SqlDbType::TinyInt,
                        2 => SqlDbType::SmallInt,
                        4 => SqlDbType::Int,
                        8 => SqlDbType::BigInt,
                        _ => SqlDbType::Int,
                    },
                    _ => SqlDbType::Int,
                }
            }
            TdsDataType::Int2 => SqlDbType::SmallInt,
            TdsDataType::Int1 => SqlDbType::TinyInt,
            TdsDataType::Bit => SqlDbType::Bit,
            TdsDataType::BitN => SqlDbType::Bit,
            TdsDataType::Flt8 => SqlDbType::Float,
            TdsDataType::Flt4 => SqlDbType::Real,
            TdsDataType::FltN => {
                // FLTN can be Float or Real
                match &col.type_info.type_info_variant {
                    TypeInfoVariant::VarLen(_var_type, len) => match *len {
                        4 => SqlDbType::Real,
                        8 => SqlDbType::Float,
                        _ => SqlDbType::Float,
                    },
                    _ => SqlDbType::Float,
                }
            }
            TdsDataType::Money => SqlDbType::Money,
            TdsDataType::Money4 => SqlDbType::SmallMoney,
            TdsDataType::MoneyN => match &col.type_info.type_info_variant {
                TypeInfoVariant::VarLen(_var_type, len) => match *len {
                    4 => SqlDbType::SmallMoney,
                    8 => SqlDbType::Money,
                    _ => SqlDbType::Money,
                },
                _ => SqlDbType::Money,
            },
            TdsDataType::DateTime => SqlDbType::DateTime,
            TdsDataType::DateTim4 => SqlDbType::SmallDateTime,
            TdsDataType::DateTimeN => match &col.type_info.type_info_variant {
                TypeInfoVariant::VarLen(_var_type, len) => match *len {
                    4 => SqlDbType::SmallDateTime,
                    8 => SqlDbType::DateTime,
                    _ => SqlDbType::DateTime,
                },
                _ => SqlDbType::DateTime,
            },
            TdsDataType::DateN => SqlDbType::Date,
            TdsDataType::TimeN => SqlDbType::Time,
            TdsDataType::DateTime2N => SqlDbType::DateTime2,
            TdsDataType::DateTimeOffsetN => SqlDbType::DateTimeOffset,
            TdsDataType::BigChar => SqlDbType::Char,
            TdsDataType::BigVarChar => SqlDbType::VarChar,
            TdsDataType::VarChar => SqlDbType::VarChar,
            TdsDataType::Text => SqlDbType::Text,
            TdsDataType::NChar => SqlDbType::NChar,
            TdsDataType::NVarChar => SqlDbType::NVarChar,
            TdsDataType::NText => SqlDbType::NText,
            TdsDataType::Binary => SqlDbType::Binary,
            TdsDataType::BigBinary => SqlDbType::Binary,
            TdsDataType::BigVarBinary => SqlDbType::VarBinary,
            TdsDataType::VarBinary => SqlDbType::VarBinary,
            TdsDataType::Image => SqlDbType::Image,
            TdsDataType::Guid => SqlDbType::UniqueIdentifier,
            TdsDataType::DecimalN | TdsDataType::Decimal => SqlDbType::Decimal,
            TdsDataType::NumericN | TdsDataType::Numeric => SqlDbType::Numeric,
            TdsDataType::Xml => SqlDbType::Xml,
            TdsDataType::Json => SqlDbType::Json,
            TdsDataType::Udt => SqlDbType::Udt,
            TdsDataType::SsVariant => SqlDbType::Variant,
            _ => SqlDbType::VarChar, // Default fallback
        };

        // Extract length, precision, scale from TypeInfo
        let (length, type_length, precision, scale) = match &col.type_info.type_info_variant {
            TypeInfoVariant::FixedLen(len) => {
                let actual_len = len.get_len() as i32;
                (actual_len, TypeLength::Fixed(actual_len), 0, 0)
            }
            TypeInfoVariant::VarLen(_var_type, len) => {
                (*len as i32, TypeLength::Variable(*len as i32), 0, 0)
            }
            TypeInfoVariant::VarLenString(_len, max_len, _collation) => {
                trace!(
                    "VarLenString - max_len={}, col_name={}",
                    max_len, col.column_name
                );
                if *max_len == 0xFFFF {
                    (-1, TypeLength::Plp, 0, 0)
                } else {
                    // Use max_len for both length and type_length (column definition size)
                    (*max_len as i32, TypeLength::Variable(*max_len as i32), 0, 0)
                }
            }
            TypeInfoVariant::VarLenScale(len, scale) => {
                (*len as i32, TypeLength::Variable(*len as i32), 0, *scale)
            }
            TypeInfoVariant::VarLenPrecisionScale(len, _max_len, precision, scale) => (
                *len as i32,
                TypeLength::Variable(*len as i32),
                *precision,
                *scale,
            ),
            TypeInfoVariant::PartialLen(
                _plp_type,
                unknown_len,
                _collation,
                _chunk_size,
                _plp_null,
            ) => {
                // PLP types
                if unknown_len.is_none() {
                    (-1, TypeLength::Plp, 0, 0)
                } else {
                    (unknown_len.unwrap() as i32, TypeLength::Plp, 0, 0)
                }
            }
        };

        let mut metadata = BulkCopyColumnMetadata::new(&col.column_name, sql_type, tds_type)
            .with_length(length, type_length)
            .with_nullable(col.is_nullable());

        if precision > 0 {
            metadata = metadata.with_precision_scale(precision, scale);
        }

        if let Some(collation) = col.get_collation() {
            metadata = metadata.with_collation(collation);
        }

        if col.is_identity() {
            metadata = metadata.with_identity(true);
        }

        metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_utf8() {
        let enc = EncodingType::Utf8;
        let bytes = enc.encode("Hello");
        assert_eq!(bytes, b"Hello");
        assert_eq!(enc.byte_length("Hello"), 5);
    }

    #[test]
    fn test_encoding_utf16le() {
        let enc = EncodingType::Utf16Le;
        let bytes = enc.encode("Hi");
        assert_eq!(bytes, vec![b'H', 0, b'i', 0]);
        assert_eq!(enc.byte_length("Hi"), 4);
    }

    #[test]
    fn test_encoding_latin1() {
        let enc = EncodingType::Latin1;
        let bytes = enc.encode("Café");
        // é is code point 233, which fits in Latin-1
        assert_eq!(bytes.len(), 4);
    }

    #[test]
    fn test_type_length_plp() {
        let tl = TypeLength::Plp;
        assert!(tl.is_plp());
        assert!(!tl.is_fixed());
        assert_eq!(tl.max_length(), None);
    }

    #[test]
    fn test_type_length_fixed() {
        let tl = TypeLength::Fixed(4);
        assert!(!tl.is_plp());
        assert!(tl.is_fixed());
        assert_eq!(tl.max_length(), Some(4));
    }

    #[test]
    fn test_type_length_variable() {
        let tl = TypeLength::Variable(100);
        assert!(!tl.is_plp());
        assert!(!tl.is_fixed());
        assert_eq!(tl.max_length(), Some(100));
    }

    #[test]
    fn test_metadata_builder() {
        let meta = BulkCopyColumnMetadata::new("test_col", SqlDbType::VarChar, 0xA7)
            .with_length(100, TypeLength::Variable(100))
            .with_nullable(false);

        assert_eq!(meta.column_name, "test_col");
        assert_eq!(meta.sql_type, SqlDbType::VarChar);
        assert_eq!(meta.length, 100);
        assert!(!meta.is_nullable);
    }

    #[test]
    fn test_metadata_needs_collation() {
        let meta = BulkCopyColumnMetadata::new("str_col", SqlDbType::NVarChar, 0xE7);
        assert!(meta.needs_collation());

        let meta2 = BulkCopyColumnMetadata::new("int_col", SqlDbType::Int, 0x38);
        assert!(!meta2.needs_collation());
    }

    #[test]
    fn test_metadata_needs_precision_scale() {
        let meta = BulkCopyColumnMetadata::new("dec_col", SqlDbType::Decimal, 0x6A)
            .with_precision_scale(18, 2);

        assert!(meta.needs_precision_scale());
        assert_eq!(meta.precision, 18);
        assert_eq!(meta.scale, 2);
    }

    #[test]
    fn test_metadata_is_plp() {
        let meta = BulkCopyColumnMetadata::new("max_col", SqlDbType::VarChar, 0xA7)
            .with_length(-1, TypeLength::Plp);

        assert!(meta.is_plp());
    }

    #[test]
    fn test_metadata_is_long() {
        let meta = BulkCopyColumnMetadata::new("text_col", SqlDbType::Text, 0x23);
        assert!(meta.is_long());

        let meta2 = BulkCopyColumnMetadata::new("varchar_col", SqlDbType::VarChar, 0xA7);
        assert!(!meta2.is_long());
    }
}
