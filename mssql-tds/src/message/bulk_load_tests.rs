// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Additional unit tests for bulk load message module
//! These tests focus on command generation and options handling.

#[cfg(test)]
mod tests {
    use crate::connection::bulk_copy::BulkCopyOptions;
    use crate::datatypes::bulk_copy_metadata::{BulkCopyColumnMetadata, SqlDbType, TypeLength};
    use crate::message::bulk_load::BulkLoadMessage;
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
    fn test_build_insert_bulk_command_with_all_options() {
        let metadata = vec![create_int_column("ID")];

        let options = BulkCopyOptions {
            keep_nulls: true,
            table_lock: true,
            check_constraints: true,
            fire_triggers: true,
            keep_identity: true,
            ..Default::default()
        };

        let message = BulkLoadMessage::new("TestTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("WITH ("));
        assert!(command.contains("KEEP_NULLS"));
        assert!(command.contains("TABLOCK"));
        assert!(command.contains("CHECK_CONSTRAINTS"));
        assert!(command.contains("FIRE_TRIGGERS"));
        assert!(command.contains("KEEP_IDENTITY"));
    }

    #[test]
    fn test_build_insert_bulk_command_with_partial_options() {
        let metadata = vec![create_int_column("ID")];

        let options = BulkCopyOptions {
            table_lock: true,
            fire_triggers: true,
            ..Default::default()
        };

        let message = BulkLoadMessage::new("PartialTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("WITH ("));
        assert!(command.contains("TABLOCK"));
        assert!(command.contains("FIRE_TRIGGERS"));
        assert!(!command.contains("KEEP_NULLS"));
        assert!(!command.contains("CHECK_CONSTRAINTS"));
        assert!(!command.contains("KEEP_IDENTITY"));
    }

    #[test]
    fn test_build_insert_bulk_command_no_options() {
        let metadata = vec![create_int_column("ID")];
        let options = BulkCopyOptions::default();

        let message = BulkLoadMessage::new("NoOptionsTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        // Should not contain WITH clause when no options are set
        assert!(!command.contains("WITH ("));
    }

    #[test]
    fn test_build_insert_bulk_command_multiple_columns() {
        let metadata = vec![
            create_int_column("Col1"),
            create_nvarchar_column("Col2", 50),
            create_int_column("Col3"),
        ];

        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("MultiColTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[Col1] int"));
        assert!(command.contains("[Col2] nvarchar(50)"));
        assert!(command.contains("[Col3] int"));
        // Verify commas between columns
        assert!(command.contains(", [Col2]"));
        assert!(command.contains(", [Col3]"));
    }

    #[test]
    fn test_build_insert_bulk_command_varchar_max() {
        let meta_varchar_max = BulkCopyColumnMetadata::new("LargeText", SqlDbType::VarChar, 0xA7)
            .with_nullable(true)
            .with_length(-1, TypeLength::Plp) // -1 indicates MAX
            .with_collation(SqlCollation::default());

        let metadata = vec![meta_varchar_max];
        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("MaxTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[LargeText] varchar(max)"));
    }

    #[test]
    fn test_build_insert_bulk_command_nvarchar_max() {
        let meta_nvarchar_max =
            BulkCopyColumnMetadata::new("LargeNText", SqlDbType::NVarChar, 0xE7)
                .with_nullable(true)
                .with_length(-1, TypeLength::Plp)
                .with_collation(SqlCollation::default());

        let metadata = vec![meta_nvarchar_max];
        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("NMaxTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[LargeNText] nvarchar(max)"));
    }

    #[test]
    fn test_build_insert_bulk_command_with_identity() {
        let id_meta = BulkCopyColumnMetadata::new("ID", SqlDbType::Int, 0x26)
            .with_nullable(false)
            .with_identity(true)
            .with_length(4, TypeLength::Fixed(4));

        let metadata = vec![id_meta, create_nvarchar_column("Name", 50)];

        let options = BulkCopyOptions {
            keep_identity: true,
            ..Default::default()
        };

        let message = BulkLoadMessage::new("IdentityTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[ID] int"));
        assert!(command.contains("KEEP_IDENTITY"));
    }

    #[test]
    fn test_build_insert_bulk_command_decimal_with_precision() {
        let dec_meta = BulkCopyColumnMetadata::new("Price", SqlDbType::Decimal, 0x6A)
            .with_nullable(true)
            .with_precision_scale(18, 2);

        let metadata = vec![dec_meta];
        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("DecimalTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[Price] decimal(18, 2)"));
    }

    #[test]
    fn test_build_insert_bulk_command_datetime2_with_scale() {
        let dt2_meta = BulkCopyColumnMetadata::new("Timestamp", SqlDbType::DateTime2, 0x2A)
            .with_nullable(true)
            .with_scale(7);

        let metadata = vec![dt2_meta];
        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("TimeTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[Timestamp] datetime2(7)"));
    }

    #[test]
    fn test_build_insert_bulk_command_binary_types() {
        let bin_meta = BulkCopyColumnMetadata::new("Data", SqlDbType::VarBinary, 0xA5)
            .with_nullable(true)
            .with_length(50, TypeLength::Variable(50));

        let metadata = vec![bin_meta];
        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("BinaryTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[Data] varbinary(50)"));
    }

    #[test]
    fn test_build_insert_bulk_command_uniqueidentifier() {
        let guid_meta = BulkCopyColumnMetadata::new("ID", SqlDbType::UniqueIdentifier, 0x24)
            .with_nullable(false)
            .with_length(16, TypeLength::Fixed(16));

        let metadata = vec![guid_meta];
        let options = BulkCopyOptions::default();
        let message = BulkLoadMessage::new("GuidTable".to_string(), metadata, vec![], options);
        let command = message.build_insert_bulk_command();

        assert!(command.contains("[ID] uniqueidentifier"));
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
}
