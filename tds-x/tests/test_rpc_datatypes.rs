#[cfg(test)]
mod common;

mod rpc_datatypes {
    use crate::common::{begin_connection, create_context, get_first_row, init_tracing};
    use tds_x::datatypes::sql_string::SqlString;
    use tds_x::{
        datatypes::{column_values::ColumnValues, sqltypes::SqlType},
        message::parameters::rpc_parameters::{RpcParameter, StatusFlags},
    };

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
        let columns = vec![
            ("int", SqlType::Int(Some(int_value))),
            ("tinyint", SqlType::TinyInt(Some(tinyint_value))),
            ("nvarchar", SqlType::NVarchar(Some(nvarchar_value), 100)),
            ("bigint", SqlType::BigInt(Some(bigint_value))),
            ("bit", SqlType::Bit(Some(bit_value))),
            ("varbinary", SqlType::VarBinary(Some(varbinary_value), 100)),
            ("float", SqlType::Float(Some(float_value))),
            ("real", SqlType::Real(Some(real_value))),
            ("xml", SqlType::Xml(Some(xml_value.clone().into()))),
        ];

        let query = generate_select_statement(&columns);
        println!("{}", query);
        let mut named_parameters = Vec::new();
        for column in columns.iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, &column.1);
            named_parameters.push(param);
        }

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let batch_result = connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let first_row_columns = get_first_row(batch_result).await.unwrap();

        assert_eq!(first_row_columns.len(), columns.len());
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

        let query = generate_select_statement(&columns);
        println!("{}", query);
        let mut named_parameters = Vec::new();
        for column in columns.iter() {
            let param =
                RpcParameter::new(Some(format!("@{}", column.0)), StatusFlags::NONE, &column.1);
            named_parameters.push(param);
        }

        let context = create_context();
        let mut connection = begin_connection(context).await;

        let batch_result = connection
            .execute_sp_executesql(query.to_string(), named_parameters, None, None)
            .await
            .unwrap();

        let first_row_columns = get_first_row(batch_result).await.unwrap();

        assert_eq!(first_row_columns.len(), columns.len());
        for column in first_row_columns.iter() {
            match &column {
                ColumnValues::Json(value) => {
                    assert_eq!(*value, json_value.clone().into());
                }
                _ => {}
            }
        }
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
