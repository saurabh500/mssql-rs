// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod rpc_datatypes {
    use std::str::FromStr;

    use crate::common::{
        begin_connection, build_tcp_datasource, create_client, get_first_row, init_tracing,
    };
    use mssql_tds::core::TdsResult;
    use mssql_tds::datatypes::column_values::{
        SqlDate, SqlDateTime, SqlDateTime2, SqlMoney, SqlSmallMoney, SqlTime,
    };
    use mssql_tds::datatypes::decoder::DecimalParts;
    use mssql_tds::datatypes::sql_string::SqlString;
    use mssql_tds::datatypes::sql_vector::SqlVector;
    use mssql_tds::datatypes::sqldatatypes::VectorBaseType;
    use mssql_tds::{
        datatypes::{column_values::ColumnValues, sqltypes::SqlType},
        message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
    };
    use uuid::Uuid;

    #[ctor::ctor]
    fn init() {
        init_tracing();
    }

    #[tokio::test]
    async fn test_sp_execute_multi_data_types() {
        let int_value = 1;
        let tinyint_value = 2;
        let nvarchar_value = SqlString::from_utf8_string("Name".to_string());
        let bigint_value = 30;
        let bit_value = true;
        let varbinary_value = vec![1, 2, 3, 4];
        let binary_value = vec![1, 2, 3, 4];
        let float_value = 10.14;
        let real_value = 3.144567;
        let xml_value = "<root>Test</root>".to_string();
        let days = 200;

        let dayssincebeginning = 300;
        let timesincebeginning = 1000;

        let guid = "123e4567-e89b-12d3-a456-426614174000".to_string();

        let decimal = DecimalParts {
            is_positive: true,
            int_parts: vec![-123, 0, 0, 0],
            scale: 38,
            precision: 38,
        };

        let columns = vec![
            ("int", SqlType::Int(Some(int_value))),
            ("tinyint", SqlType::TinyInt(Some(tinyint_value))),
            (
                "nvarchar",
                SqlType::NVarchar(Some(nvarchar_value.clone()), 100),
            ),
            ("bigint", SqlType::BigInt(Some(bigint_value))),
            ("bit", SqlType::Bit(Some(bit_value))),
            (
                "varbinary",
                SqlType::VarBinary(Some(varbinary_value.clone()), 100),
            ),
            (
                "varbinarymax",
                SqlType::VarBinaryMax(Some(varbinary_value.clone())),
            ),
            (
                "binary",
                SqlType::Binary(Some(binary_value.clone()), binary_value.len() as u16),
            ),
            (
                "binary8000",
                SqlType::Binary(Some(binary_value.clone()), binary_value.len() as u16),
            ),
            ("float", SqlType::Float(Some(float_value))),
            ("real", SqlType::Real(Some(real_value))),
            ("xml", SqlType::Xml(Some(xml_value.clone().into()))),
            (
                "varchar",
                SqlType::NVarchar(Some(nvarchar_value.clone()), 200),
            ),
            ("date", SqlType::Date(Some(SqlDate::create(200).unwrap()))),
            (
                "datetime",
                SqlType::DateTime(Some(SqlDateTime {
                    days: dayssincebeginning,
                    time: timesincebeginning,
                })),
            ),
            (
                "datetime2",
                SqlType::DateTime2(Some(SqlDateTime2 {
                    days: dayssincebeginning as u32,
                    time: SqlTime {
                        time_nanoseconds: timesincebeginning as u64,
                        scale: 6,
                    },
                })),
            ),
            ("guid", SqlType::Uuid(Some(Uuid::from_str(&guid).unwrap()))),
            ("decimal", SqlType::Decimal(Some(decimal.clone()))),
            (
                "smallmoney",
                SqlType::SmallMoney(Some(SqlSmallMoney { int_val: 12345 })),
            ),
            (
                "money",
                SqlType::Money(Some(SqlMoney {
                    lsb_part: 1234,
                    msb_part: 5678,
                })),
            ),
        ];

        let query = generate_select_statement(&columns);

        let col_count = columns.len();
        let mut named_parameters = Vec::new();
        for column in columns.into_iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, column.1);
            named_parameters.push(param);
        }

        let mut connection = begin_connection(&build_tcp_datasource()).await;

        connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let (metadata, first_row_columns) = get_first_row(&mut connection).await.unwrap();

        for (i, column) in metadata.iter().enumerate() {
            println!("Column {i}: {column:?}");
        }

        assert_eq!(first_row_columns.len(), col_count);
        for (i, column) in first_row_columns.iter().enumerate() {
            match &column {
                ColumnValues::Int(value) => {
                    assert_eq!(*value, int_value);
                }
                ColumnValues::TinyInt(value) => {
                    assert_eq!(*value, tinyint_value);
                }
                ColumnValues::String(value) => {
                    assert_eq!(value, &SqlString::from_utf8_string("Name".to_string()));
                }
                ColumnValues::BigInt(value) => {
                    assert_eq!(*value, bigint_value);
                }
                ColumnValues::Bit(value) => {
                    assert_eq!(*value, bit_value);
                }
                ColumnValues::Bytes(value) => {
                    let col_name = metadata[i].column_name.clone();
                    assert_eq!(
                        value,
                        &vec![1, 2, 3, 4],
                        "Binary value mismatch for column {i} {column:?} {col_name}"
                    );
                }
                ColumnValues::Float(value) => {
                    assert_eq!(*value, float_value);
                }
                ColumnValues::Real(value) => {
                    assert_eq!(*value, real_value);
                }
                ColumnValues::Xml(value) => {
                    assert_eq!(*value, xml_value.clone().into());
                }
                ColumnValues::Date(value) => {
                    assert_eq!(value.get_days(), days);
                }
                ColumnValues::DateTime(value) => {
                    assert_eq!(value.days, dayssincebeginning);
                    assert_eq!(value.time, timesincebeginning);
                }
                ColumnValues::DateTime2(value) => {
                    assert_eq!(value.days, dayssincebeginning as u32);
                    assert_eq!(value.time.scale, 6);
                    assert_eq!(value.time.time_nanoseconds, timesincebeginning as u64);
                }
                ColumnValues::Uuid(value) => {
                    assert_eq!(value.to_string(), guid);
                }
                ColumnValues::Decimal(value) => {
                    assert_eq!(value, &decimal);
                }
                ColumnValues::SmallMoney(value) => {
                    assert_eq!(value.int_val, 12345);
                }
                ColumnValues::Money(value) => {
                    assert_eq!(value.lsb_part, 1234);
                    assert_eq!(value.msb_part, 5678);
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_sp_execute_null_for_data_types() {
        let columns = vec![
            ("nvarchar", SqlType::NVarchar(None, 100)),
            ("nvarcharmax", SqlType::NVarcharMax(None)),
            ("varbinary", SqlType::VarBinary(None, 100)),
            ("varbinarymax", SqlType::VarBinaryMax(None)),
            ("int", SqlType::Int(None)),
            ("tinyint", SqlType::TinyInt(None)),
            ("bigint", SqlType::BigInt(None)),
            ("bit", SqlType::Bit(None)),
            ("float", SqlType::Float(None)),
            ("real", SqlType::Real(None)),
            ("xml", SqlType::Xml(None)),
            ("varchar", SqlType::Varchar(None, 100)),
            ("varcharmax", SqlType::VarcharMax(None)),
            ("date", SqlType::Date(None)),
            ("datetime", SqlType::DateTime(None)),
            ("datetime2", SqlType::DateTime2(None)),
            ("guid", SqlType::Uuid(None)),
            ("decimal", SqlType::Decimal(None)),
            ("smallmoney", SqlType::SmallMoney(None)),
            ("binary8000", SqlType::Binary(None, 8000)),
            ("money", SqlType::Money(None)),
        ];

        let query = generate_select_statement(&columns);

        let col_count = columns.len();
        let mut named_parameters = Vec::new();
        for column in columns.into_iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, column.1);
            named_parameters.push(param);
        }

        let mut connection = begin_connection(&build_tcp_datasource()).await;

        connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let (_, first_row_columns) = get_first_row(&mut connection).await.unwrap();

        assert_eq!(first_row_columns.len(), col_count);
        for column in first_row_columns.iter() {
            match &column {
                ColumnValues::Null => {
                    // Expecting null values for all columns
                }
                _ => {
                    panic!("Expected null values, but got: {column:?}");
                }
            }
        }
    }

    // This test exists so that it can exclusively be skipped or run for SQL servers which support the
    // new capability.
    #[allow(clippy::single_match)]
    #[tokio::test]
    async fn test_sp_execute_new_data_types() {
        let json_value = "[\"abc\",\"ghi\",\"def\"]".to_string();
        let vector_value = SqlVector::try_from_f32(vec![1.0, 2.5, 3.2, -0.5]).unwrap();
        let columns = vec![
            ("json", SqlType::Json(Some(json_value.clone().into()))),
            (
                "vector",
                SqlType::Vector(Some(vector_value.clone()), 4, VectorBaseType::Float32),
            ),
        ];

        let col_count = columns.len();
        let query = generate_select_statement(&columns);
        println!("{query}");
        let mut named_parameters = Vec::new();
        for column in columns.into_iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, column.1);
            named_parameters.push(param);
        }

        let mut connection = begin_connection(&build_tcp_datasource()).await;

        connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let (_, first_row_columns) = get_first_row(&mut connection).await.unwrap();

        assert_eq!(first_row_columns.len(), col_count);
        for column in first_row_columns.iter() {
            match &column {
                ColumnValues::Json(value) => {
                    assert_eq!(*value, json_value.clone().into());
                }
                ColumnValues::Vector(value) => {
                    assert_eq!(value, &vector_value);
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_bad_sql_statement_with_trailing_comma() -> TdsResult<()> {
        let mut client = create_client(&build_tcp_datasource()).await?;

        let query = "SELECT @bit AS bit,;".to_string();

        let mut named_parameters = Vec::new();
        let param = RpcParameter::new(
            Some("@bit".to_string()),
            StatusFlags::NONE,
            SqlType::Bit(Some(false)),
        );
        named_parameters.push(param);
        let result = client
            .execute_sp_executesql(query, named_parameters, None, None)
            .await;

        assert!(
            result.is_err(),
            "Expected an error due to trailing comma in SQL statement"
        );
        Ok(())
    }

    pub fn generate_select_statement(columns: &Vec<(&str, SqlType)>) -> String {
        let mut select_statement = String::from("SELECT\n");

        for (i, column) in columns.iter().enumerate() {
            select_statement.push_str(&format!("    @{} AS {} ", column.0, column.0));
            if i < columns.len() - 1 {
                select_statement.push_str(",\n");
            }
        }
        select_statement
    }

    /// Test that verifies packet size negotiation works correctly with sp_executesql.
    ///
    /// This test reproduces a bug where `notify_session_setting_change` only updated
    /// `self.packet_size` but NOT `self.tds_read_buffer.max_packet_size`. Since
    /// `execute_sp_executesql` doesn't call `reset_reader()` before executing, the
    /// validation check would reject valid packets that exceeded the initial 4096-byte
    /// limit but were within the negotiated size (e.g., 8000 bytes).
    ///
    /// The test executes a parameterized query returning large data that requires
    /// packets at the negotiated size.
    #[tokio::test]
    async fn test_sp_executesql_with_negotiated_packet_size() -> TdsResult<()> {
        let mut client = create_client(&build_tcp_datasource()).await?;

        // Create a large string value that will require negotiated packet size
        let large_string = "X".repeat(6000);
        let large_string2 = "Y".repeat(6000);

        let query =
            "SELECT @large1 AS LargeColumn1, @large2 AS LargeColumn2, @small AS SmallColumn"
                .to_string();

        let named_parameters = vec![
            RpcParameter::new(
                Some("@large1".to_string()),
                StatusFlags::NONE,
                SqlType::NVarcharMax(Some(SqlString::from_utf8_string(large_string.clone()))),
            ),
            RpcParameter::new(
                Some("@large2".to_string()),
                StatusFlags::NONE,
                SqlType::NVarcharMax(Some(SqlString::from_utf8_string(large_string2.clone()))),
            ),
            RpcParameter::new(
                Some("@small".to_string()),
                StatusFlags::NONE,
                SqlType::Int(Some(42)),
            ),
        ];

        // This would fail with "TDS packet length 8000 exceeds negotiated max packet size 4096"
        // if the buffer's max_packet_size wasn't updated in notify_session_setting_change
        client
            .execute_sp_executesql(query, named_parameters, None, None)
            .await?;

        let (metadata, first_row) = get_first_row(&mut client).await?;

        assert_eq!(metadata.len(), 3, "Expected 3 columns");
        assert_eq!(first_row.len(), 3, "Expected 3 column values");

        // Verify LargeColumn1
        match &first_row[0] {
            ColumnValues::String(s) => {
                assert_eq!(s.to_utf8_string().len(), 6000, "Expected 6000 X characters");
            }
            _ => panic!("Expected String value for LargeColumn1"),
        }

        // Verify LargeColumn2
        match &first_row[1] {
            ColumnValues::String(s) => {
                assert_eq!(s.to_utf8_string().len(), 6000, "Expected 6000 Y characters");
            }
            _ => panic!("Expected String value for LargeColumn2"),
        }

        // Verify SmallColumn
        match &first_row[2] {
            ColumnValues::Int(v) => {
                assert_eq!(*v, 42, "Expected SmallColumn to be 42");
            }
            _ => panic!("Expected Int value for SmallColumn"),
        }

        Ok(())
    }
}
