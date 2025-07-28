// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#[cfg(test)]
mod common;

mod rpc_datatypes {
    use std::str::FromStr;

    use crate::common::{
        begin_connection, create_client, create_context, get_first_row, init_tracing,
    };
    use mssql_tds::core::TdsResult;
    use mssql_tds::datatypes::column_values::{SqlDate, SqlDateTime, SqlDateTime2, SqlTime};
    use mssql_tds::datatypes::sql_string::SqlString;
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
        let float_value = 10.14;
        let real_value = 3.144567;
        let xml_value = "<root>Test</root>".to_string();
        let days = 200;

        let dayssincebeginning = 300;
        let timesincebeginning = 1000;

        let guid = "123e4567-e89b-12d3-a456-426614174000".to_string();

        let columns = vec![
            ("int", SqlType::Int(Some(int_value))),
            ("tinyint", SqlType::TinyInt(Some(tinyint_value))),
            (
                "nvarchar",
                SqlType::NVarchar(Some(nvarchar_value.clone()), 100),
            ),
            ("bigint", SqlType::BigInt(Some(bigint_value))),
            ("bit", SqlType::Bit(Some(bit_value))),
            ("varbinary", SqlType::VarBinary(Some(varbinary_value), 100)),
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
        ];

        let query = generate_select_statement(&columns);
        let col_count = columns.len();
        let mut named_parameters = Vec::new();
        for column in columns.into_iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, column.1);
            named_parameters.push(param);
        }

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let batch_result = connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let (metadata, first_row_columns) = get_first_row(batch_result).await.unwrap();

        for (i, column) in metadata.iter().enumerate() {
            println!("Column {i}: {column:?}");
        }

        assert_eq!(first_row_columns.len(), col_count);
        for column in first_row_columns.iter() {
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
                    assert_eq!(value, &vec![1, 2, 3, 4]);
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
                _ => {}
            }
        }
    }

    // This test exists so that it can exclusively be skipped or run for SQL servers which support the
    // new capability.
    #[allow(clippy::single_match)]
    #[tokio::test]
    async fn test_sp_execute_new_data_types() {
        let json_value = "[\"abc\",\"ghi\",\"def\"]".to_string();
        let columns = vec![("json", SqlType::Json(Some(json_value.clone().into())))];

        let col_count = columns.len();
        let query = generate_select_statement(&columns);
        println!("{query}");
        let mut named_parameters = Vec::new();
        for column in columns.into_iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, column.1);
            named_parameters.push(param);
        }

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let batch_result = connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let (_, first_row_columns) = get_first_row(batch_result).await.unwrap();

        assert_eq!(first_row_columns.len(), col_count);
        for column in first_row_columns.iter() {
            match &column {
                ColumnValues::Json(value) => {
                    assert_eq!(*value, json_value.clone().into());
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_bad_sql_statement_with_trailing_comma() -> TdsResult<()> {
        let context = create_context();
        let mut client = create_client(context).await?;

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
            select_statement.push_str(&format!("    @{} AS {}", column.0, column.0));
            if i < columns.len() - 1 {
                select_statement.push_str(",\n");
            }
        }
        select_statement
    }
}
