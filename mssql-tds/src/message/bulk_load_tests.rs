// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Additional unit tests for bulk load message module
//! These tests focus on command generation and options handling.

#[cfg(test)]
mod tests {

    use crate::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
    use crate::token::tokens::SqlCollation;

    // Helper function to create a simple int column
    fn create_int_column(name: &str) -> BulkCopyColumnMetadata {
        BulkCopyColumnMetadata::new(name, SqlDbType::Int, 0x26)
            .with_nullable(false)
            .with_length(4, TypeLength::Fixed(4))
    }

    // Helper function to create a varchar column
    fn create_varchar_column(name: &str, length: i32) -> BulkCopyColumnMetadata {
        BulkCopyColumnMetadata::new(name, SqlDbType::VarChar, 0xA7)
            .with_nullable(true)
            .with_length(length, TypeLength::Variable(length))
            .with_collation(SqlCollation::default())
    }

    // Helper function to create an nvarchar column
    fn create_nvarchar_column(name: &str, length: i32) -> BulkCopyColumnMetadata {
        BulkCopyColumnMetadata::new(name, SqlDbType::NVarChar, 0xE7)
            .with_nullable(true)
            .with_length(length, TypeLength::Variable(length))
            .with_collation(SqlCollation::default())
    }

    #[test]
    fn test_tds_type_mapping_comprehensive() {
        // Test all major TDS type mappings
        assert_eq!(SqlDbType::TinyInt.to_tds_type(), 0x26);
        assert_eq!(SqlDbType::SmallInt.to_tds_type(), 0x26);
        assert_eq!(SqlDbType::Int.to_tds_type(), 0x26);
        assert_eq!(SqlDbType::BigInt.to_tds_type(), 0x26);
        assert_eq!(SqlDbType::Bit.to_tds_type(), 0x68);
        assert_eq!(SqlDbType::Real.to_tds_type(), 0x6D);
        assert_eq!(SqlDbType::Float.to_tds_type(), 0x6D);
        assert_eq!(SqlDbType::Decimal.to_tds_type(), 0x6A);
        assert_eq!(SqlDbType::Numeric.to_tds_type(), 0x6C);
        assert_eq!(SqlDbType::Money.to_tds_type(), 0x6E);
        assert_eq!(SqlDbType::SmallMoney.to_tds_type(), 0x6E);
        assert_eq!(SqlDbType::Date.to_tds_type(), 0x28);
        assert_eq!(SqlDbType::Time.to_tds_type(), 0x29);
        assert_eq!(SqlDbType::DateTime.to_tds_type(), 0x6F);
        assert_eq!(SqlDbType::DateTime2.to_tds_type(), 0x2A);
        assert_eq!(SqlDbType::DateTimeOffset.to_tds_type(), 0x2B);
        assert_eq!(SqlDbType::SmallDateTime.to_tds_type(), 0x6F);
        assert_eq!(SqlDbType::Char.to_tds_type(), 0xAF);
        assert_eq!(SqlDbType::VarChar.to_tds_type(), 0xA7);
        assert_eq!(SqlDbType::Text.to_tds_type(), 0x23);
        assert_eq!(SqlDbType::NChar.to_tds_type(), 0xEF);
        assert_eq!(SqlDbType::NVarChar.to_tds_type(), 0xE7);
        assert_eq!(SqlDbType::NText.to_tds_type(), 0x63);
        assert_eq!(SqlDbType::Binary.to_tds_type(), 0xAD);
        assert_eq!(SqlDbType::VarBinary.to_tds_type(), 0xA5);
        assert_eq!(SqlDbType::Image.to_tds_type(), 0x22);
        assert_eq!(SqlDbType::UniqueIdentifier.to_tds_type(), 0x24);
        assert_eq!(SqlDbType::Xml.to_tds_type(), 0xF1);
        assert_eq!(SqlDbType::Json.to_tds_type(), 0xF4);
        assert_eq!(SqlDbType::Variant.to_tds_type(), 0x62);
        assert_eq!(SqlDbType::Udt.to_tds_type(), 0xF0);
    }

    #[test]
    fn test_tds_type_fixed_mapping() {
        // Test fixed-length type mappings
        assert_eq!(SqlDbType::TinyInt.to_tds_type_fixed(), 0x30);
        assert_eq!(SqlDbType::SmallInt.to_tds_type_fixed(), 0x34);
        assert_eq!(SqlDbType::Int.to_tds_type_fixed(), 0x38);
        assert_eq!(SqlDbType::BigInt.to_tds_type_fixed(), 0x7F);
        assert_eq!(SqlDbType::Bit.to_tds_type_fixed(), 0x32);
        assert_eq!(SqlDbType::Real.to_tds_type_fixed(), 0x3B);
        assert_eq!(SqlDbType::Float.to_tds_type_fixed(), 0x3E);

        // Types without fixed versions should return nullable variant
        assert_eq!(
            SqlDbType::Decimal.to_tds_type_fixed(),
            SqlDbType::Decimal.to_tds_type()
        );
        assert_eq!(
            SqlDbType::NVarChar.to_tds_type_fixed(),
            SqlDbType::NVarChar.to_tds_type()
        );
    }

    #[test]
    fn test_bulk_copy_tds_type_mapping() {
        // Test that most types return the same TDS type for bulk copy
        assert_eq!(
            SqlDbType::Int.to_bulk_copy_tds_type(),
            SqlDbType::Int.to_tds_type()
        );
        assert_eq!(
            SqlDbType::VarChar.to_bulk_copy_tds_type(),
            SqlDbType::VarChar.to_tds_type()
        );
        assert_eq!(
            SqlDbType::NVarChar.to_bulk_copy_tds_type(),
            SqlDbType::NVarChar.to_tds_type()
        );
        assert_eq!(
            SqlDbType::DateTime.to_bulk_copy_tds_type(),
            SqlDbType::DateTime.to_tds_type()
        );

        // Test that JSON is properly identified as 0xF4
        assert_eq!(
            SqlDbType::Json.to_tds_type(),
            0xF4,
            "JSON should return 0xF4 (TdsDataType::Json) from to_tds_type()"
        );

        // Test that JSON returns NVARCHAR for bulk copy
        assert_eq!(
            SqlDbType::Json.to_bulk_copy_tds_type(),
            0xE7,
            "JSON should return 0xE7 (TdsDataType::NVarChar) from to_bulk_copy_tds_type() for bulk copy operations"
        );

        // Verify JSON is treated differently than NVARCHAR
        assert_ne!(
            SqlDbType::Json.to_tds_type(),
            SqlDbType::NVarChar.to_tds_type(),
            "JSON type identifier (0xF4) should differ from NVARCHAR (0xE7)"
        );

        // Verify JSON uses NVARCHAR for bulk copy
        assert_eq!(
            SqlDbType::Json.to_bulk_copy_tds_type(),
            SqlDbType::NVarChar.to_tds_type(),
            "JSON should use NVARCHAR encoding (0xE7) for bulk copy"
        );
    }
}
