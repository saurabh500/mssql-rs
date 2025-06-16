use bitflags::bitflags;

use crate::datatypes::column_values::ColumnValues;
use crate::{
    core::TdsResult,
    datatypes::{encoder::Encoder, sqldatatypes::TdsDataType},
    error::Error,
    read_write::packet_writer::PacketWriter,
    token::tokens::SqlCollation,
};

bitflags! {
    pub struct StatusFlags: u8 {
        const NONE = 0b0000_0000;
        const BY_REF_VALUE = 0b0000_0001;
        const DEFAULT_VALUE = 0b0000_0010;
        const RESERVED_BIT_1 = 0b0000_0100;
        const ENCRYPTED = 0b0000_1000;
        const RESERVED_BIT_4 = 0b0001_0000;
    }
}

/// Represents a parameter in an RPC (Remote Procedure Call) message.
pub struct RpcParameter<'a> {
    /// The name of the parameter, if applicable. For positional
    /// parameters, this will be `None`.
    pub(crate) name: Option<String>,
    /// Options for the parameter. This is a bitmask
    /// represents whether the parameter is input, output, or both, as well as the encryption setting.
    options: StatusFlags,

    /// The data type of the parameter.
    pub(crate) data_type: &'a TdsDataType,

    /// is the parameter null?
    is_null: bool,

    /// Is this the correct datatype?
    value: &'a ColumnValues,
}

impl<'b> RpcParameter<'b> {
    pub fn new(
        name: Option<String>,
        options: StatusFlags,
        data_type: &'b TdsDataType,
        is_null: bool,
        value: &'b ColumnValues,
    ) -> Self {
        Self {
            name,
            options,
            data_type,
            is_null,
            value,
        }
    }

    pub(crate) fn get_sql_name(&self) -> &str {
        // For nullable types, we need to check the actual datatype to derive the name.
        match self.data_type {
            TdsDataType::IntN => match self.value {
                ColumnValues::Int(_) => TdsDataType::Int4.get_meta_type_name(),
                ColumnValues::BigInt(_) => TdsDataType::Int8.get_meta_type_name(),
                ColumnValues::SmallInt(_) => TdsDataType::Int2.get_meta_type_name(),
                ColumnValues::TinyInt(_) => TdsDataType::Int1.get_meta_type_name(),
                _ => unreachable!("Unexpected value type for IntN"),
            },
            _ => self.data_type.get_meta_type_name(),
        }
    }

    /// Serializes the RPC parameter into the provided `PacketWriter`.
    /// The `encoder` is used to encode the parameter value based on its data type.
    /// The `db_collation` is used for string types to determine the collation.
    /// The `is_positional` flag indicates whether the parameter is positional or named.
    pub(crate) async fn serialize<T: Encoder>(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        db_collation: &SqlCollation,
        is_positional: bool,
        encoder: &T,
    ) -> TdsResult<()> {
        // If the parameter is positional, then we dont need to write the name.
        if is_positional {
            // Indicates that the parameter name is 0 length, since this is
            // a positional parameter.
            packet_writer.write_byte_async(0).await?;
        } else {
            match self.name {
                Some(ref name) => {
                    if name.len() > 0xFF {
                        return Err(Error::UsageError(
                            "Parameter name is too long. Maximum length is 255 characters."
                                .to_string(),
                        ));
                    }
                    let name_length = name.len() as u8;
                    // We can only send byte length.
                    packet_writer.write_byte_async(name_length).await?;
                    packet_writer.write_string_unicode_async(name).await?;
                }
                None => {
                    // Since this is not a positional parameter,
                    // we expect that a name is provided.
                    // If it is not provided, then the called made a mistake.
                    // Since this is a programming error, we can panic.
                    unreachable!("Parameter name is None for a non-positional parameter. Unexpected implemetation path");
                }
            }
        }

        // Write the options byte.
        packet_writer.write_byte_async(self.options.bits()).await?;

        encoder
            .encode(packet_writer, *self.data_type, self.value, db_collation)
            .await?;
        Ok(())
    }
}

// Builds a comma-separated list of parameter names and types for the RPC call.
pub(crate) fn build_parameter_list_string(
    named_params: &Vec<RpcParameter>,
    params_list: &mut String,
) {
    let mut first_param = true;
    for param in named_params {
        if let Some(param_name) = &param.name {
            // TODO: while persisting types with length, we need to compute the length and
            // add the length after the type name. e.g. Nvarchar(200), varchar(100) etc.
            let param_type_name = param.get_sql_name();
            if first_param {
                first_param = false;
            } else {
                params_list.push_str(", ");
            }
            params_list.push_str(&format!("{} {} ", param_name, param_type_name));
        }
    }
}
