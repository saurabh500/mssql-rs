use crate::core::TdsResult;
use crate::error::Error;
use crate::{read_write::packet_reader::PacketReader, token::tokens::SqlCollation};
use std::fmt::format;

// TdsDataType is a list of all the datatypes in TDS protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
#[repr(u8)]
pub enum TdsDataType {
    Void = 0x1F,
    Image = 0x22,
    Text = 0x23,
    Guid = 0x24,
    VarBinary = 0x25,
    IntN = 0x26,
    VarChar = 0x27,
    DateN = 0x28,
    TimeN = 0x29,
    DateTime2N = 0x2A,
    DateTimeOffsetN = 0x2B,
    Binary = 0x2D,
    Char = 0x2F,
    Int1 = 0x30,
    Bit = 0x32,
    Int2 = 0x34,
    Decimal = 0x37,
    Int4 = 0x38,
    DateTim4 = 0x3A,
    Flt4 = 0x3B,
    Money = 0x3C,
    DateTime = 0x3D,
    Flt8 = 0x3E,
    Numeric = 0x3F,
    SsVariant = 0x62,
    NText = 0x63,
    BitN = 0x68,
    DecimalN = 0x6A,
    NumericN = 0x6C,
    FltN = 0x6D,
    MoneyN = 0x6E,
    DateTimeN = 0x6F,
    Money4 = 0x7A,
    Int8 = 0x7F,
    BigVarBinary = 0xA5,
    BigVarChar = 0xA7,
    BigBinary = 0xAD,
    BigChar = 0xAF,
    NVarChar = 0xE7,
    NChar = 0xEF,
    Udt = 0xF0,
    Xml = 0xF1,
    Json = 0xF4,

    #[default]
    None = 0x00,
}

impl TdsDataType {
    // Function to return the T-SQL name for the datatype.
    // Will be used to construct the @parameters parameter of the stored procedures.
    pub fn get_meta_type_name(&self) -> &'static str {
        match self {
            TdsDataType::Int8 => "bigint",
            TdsDataType::Flt8 => "float",
            TdsDataType::Flt4 => "real",
            TdsDataType::BigBinary => "binary",
            TdsDataType::BigVarBinary => "varbinary",
            TdsDataType::Image => "image",
            TdsDataType::Bit => "bit",
            TdsDataType::Int1 => "tinyint",
            TdsDataType::Int2 => "smallint",
            TdsDataType::Int4 => "int",
            TdsDataType::BigChar => "char",
            TdsDataType::BigVarChar => "varchar",
            TdsDataType::Text => "text",
            TdsDataType::NChar => "nchar",
            TdsDataType::NVarChar => "nvarchar",
            TdsDataType::NText => "ntext",
            TdsDataType::DecimalN => "decimal",
            TdsDataType::Xml => "xml",
            TdsDataType::DateTime => "datetime",
            TdsDataType::DateTim4 => "smalldatetime",
            TdsDataType::Money => "money",
            TdsDataType::Money4 => "smallmoney",
            TdsDataType::Guid => "uniqueidentifier",
            TdsDataType::SsVariant => "sql_variant",
            TdsDataType::Udt => "udt",
            TdsDataType::Json => "json",
            TdsDataType::DateN => "date",
            TdsDataType::TimeN => "time",
            TdsDataType::DateTime2N => "datetime2",
            TdsDataType::DateTimeOffsetN => "datetimeoffset",
            _ => unreachable!("Unsupported TDS data type: {:?} for meta type name", self),
        }
    }
}

impl TryFrom<u8> for TdsDataType {
    type Error = Error;

    fn try_from(value: u8) -> TdsResult<Self> {
        match value {
            0x1F => Ok(TdsDataType::Void),
            0x22 => Ok(TdsDataType::Image),
            0x23 => Ok(TdsDataType::Text),
            0x24 => Ok(TdsDataType::Guid),
            0x25 => Ok(TdsDataType::VarBinary),
            0x26 => Ok(TdsDataType::IntN),
            0x27 => Ok(TdsDataType::VarChar),
            0x28 => Ok(TdsDataType::DateN),
            0x29 => Ok(TdsDataType::TimeN),
            0x2A => Ok(TdsDataType::DateTime2N),
            0x2B => Ok(TdsDataType::DateTimeOffsetN),
            0x2D => Ok(TdsDataType::Binary),
            0x2F => Ok(TdsDataType::Char),
            0x30 => Ok(TdsDataType::Int1),
            0x32 => Ok(TdsDataType::Bit),
            0x34 => Ok(TdsDataType::Int2),
            0x37 => Ok(TdsDataType::Decimal),
            0x38 => Ok(TdsDataType::Int4),
            0x3A => Ok(TdsDataType::DateTim4),
            0x3B => Ok(TdsDataType::Flt4),
            0x3C => Ok(TdsDataType::Money),
            0x3D => Ok(TdsDataType::DateTime),
            0x3E => Ok(TdsDataType::Flt8),
            0x3F => Ok(TdsDataType::Numeric),
            0x62 => Ok(TdsDataType::SsVariant),
            0x63 => Ok(TdsDataType::NText),
            0x68 => Ok(TdsDataType::BitN),
            0x6A => Ok(TdsDataType::DecimalN),
            0x6C => Ok(TdsDataType::NumericN),
            0x6D => Ok(TdsDataType::FltN),
            0x6E => Ok(TdsDataType::MoneyN),
            0x6F => Ok(TdsDataType::DateTimeN),
            0x7A => Ok(TdsDataType::Money4),
            0x7F => Ok(TdsDataType::Int8),
            0xA5 => Ok(TdsDataType::BigVarBinary),
            0xA7 => Ok(TdsDataType::BigVarChar),
            0xAD => Ok(TdsDataType::BigBinary),
            0xAF => Ok(TdsDataType::BigChar),
            0xE7 => Ok(TdsDataType::NVarChar),
            0xEF => Ok(TdsDataType::NChar),
            0xF0 => Ok(TdsDataType::Udt),
            0xF1 => Ok(TdsDataType::Xml),
            0xF4 => Ok(TdsDataType::Json),
            _ => Err(Error::ProtocolError(format(format_args!(
                "Invalid TDS Type {:?}",
                value
            )))),
        }
    }
}

// Macro to generate TryFrom implementation for a enum from TdsDataType
macro_rules! impl_try_from_tdstypes {
    (
        $(#[doc = $doc:literal])*
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $enum_name:ident {
            $($variant:ident = $tds_type:expr),* $(,)?
        }
    ) => {
        #[repr(u8)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $enum_name {
            $($variant = $tds_type),*
        }

        impl TryFrom<TdsDataType> for $enum_name {
            type Error = Error;

            fn try_from(value: TdsDataType) -> TdsResult<Self> {
                match value {
                    $(TdsDataType::$variant => Ok($enum_name::$variant),)*
                    _ => Err(Error::ProtocolError(format(format_args!("Invalid TDS Type {:?}", value)))),
                }
            }
        }
    };
}

impl_try_from_tdstypes!(
    /// The subset of TdsDataTypes which are categorized as Fixed Length Types.
    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum FixedLengthTypes {
        Int1 = TdsDataType::Int1 as u8,
        Bit = TdsDataType::Bit as u8,
        Int2 = TdsDataType::Int2 as u8,
        Int4 = TdsDataType::Int4 as u8,
        DateTim4 = TdsDataType::DateTim4 as u8,
        Flt4 = TdsDataType::Flt4 as u8,
        Money4 = TdsDataType::Money4 as u8,
        Money = TdsDataType::Money as u8,
        DateTime = TdsDataType::DateTime as u8,
        Flt8 = TdsDataType::Flt8 as u8,
        Int8 = TdsDataType::Int8 as u8,
    }
);

impl FixedLengthTypes {
    /// Returns the number of bytes required to store the length of the data type.
    pub fn get_len(&self) -> usize {
        match self {
            FixedLengthTypes::Int1 | FixedLengthTypes::Bit => size_of::<u8>(),
            FixedLengthTypes::Int2 => size_of::<u16>(),
            FixedLengthTypes::Int4
            | FixedLengthTypes::DateTim4
            | FixedLengthTypes::Flt4
            | FixedLengthTypes::Money4 => size_of::<u32>(),

            FixedLengthTypes::Money
            | FixedLengthTypes::DateTime
            | FixedLengthTypes::Flt8
            | FixedLengthTypes::Int8 => size_of::<u64>(),
        }
    }
}

impl_try_from_tdstypes!(
    /// The subset of TdsDataTypes which are categorized as Variable Length Types.
    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum VariableLengthTypes {
        Guid = TdsDataType::Guid as u8,
        IntN = TdsDataType::IntN as u8,
        BitN = TdsDataType::BitN as u8,
        Decimal = TdsDataType::Decimal as u8,
        Numeric = TdsDataType::Numeric as u8,
        DecimalN = TdsDataType::DecimalN as u8,
        NumericN = TdsDataType::NumericN as u8,
        FltN = TdsDataType::FltN as u8,
        MoneyN = TdsDataType::MoneyN as u8,
        DateTimeN = TdsDataType::DateTimeN as u8,
        DateN = TdsDataType::DateN as u8,
        TimeN = TdsDataType::TimeN as u8,
        DateTime2N = TdsDataType::DateTime2N as u8,
        DateTimeOffsetN = TdsDataType::DateTimeOffsetN as u8,
        Char = TdsDataType::Char as u8,
        VarChar = TdsDataType::VarChar as u8,
        Binary = TdsDataType::Binary as u8,
        VarBinary = TdsDataType::VarBinary as u8,
        BigVarBinary = TdsDataType::BigVarBinary as u8,
        BigVarChar = TdsDataType::BigVarChar as u8,
        BigBinary = TdsDataType::BigBinary as u8,
        BigChar = TdsDataType::BigChar as u8,
        NVarChar = TdsDataType::NVarChar as u8,
        NChar = TdsDataType::NChar as u8,
        Xml = TdsDataType::Xml as u8,
        Text = TdsDataType::Text as u8,
        Image = TdsDataType::Image as u8,
        NText = TdsDataType::NText as u8,
        SsVariant = TdsDataType::SsVariant as u8,
        Json = TdsDataType::Json as u8,
    }
);

impl VariableLengthTypes {
    /// Returns the number of bytes that need to be read off the wire, to determine the length
    /// of the data type. This either 1, 2 or 4 bytes, depending on if this variable length type is a TDS
    /// BYTELEN_TYPE, USHORTLEN_TYPE or LONGLEN_TYPE.
    pub fn get_len_byte_count(&self) -> usize {
        match self {
            VariableLengthTypes::BigVarBinary
            | VariableLengthTypes::BigVarChar
            | VariableLengthTypes::BigBinary
            | VariableLengthTypes::BigChar
            | VariableLengthTypes::NVarChar
            | VariableLengthTypes::NChar => size_of::<u16>(),

            VariableLengthTypes::Guid
            | VariableLengthTypes::IntN
            | VariableLengthTypes::Decimal
            | VariableLengthTypes::Numeric
            | VariableLengthTypes::BitN
            | VariableLengthTypes::DecimalN
            | VariableLengthTypes::NumericN
            | VariableLengthTypes::FltN
            | VariableLengthTypes::MoneyN
            | VariableLengthTypes::DateTimeN
            | VariableLengthTypes::DateN
            | VariableLengthTypes::TimeN
            | VariableLengthTypes::DateTime2N
            | VariableLengthTypes::DateTimeOffsetN
            | VariableLengthTypes::Char
            | VariableLengthTypes::VarChar
            | VariableLengthTypes::Binary
            | VariableLengthTypes::VarBinary => size_of::<u8>(),

            VariableLengthTypes::Image
            | VariableLengthTypes::NText
            | VariableLengthTypes::SsVariant
            | VariableLengthTypes::Text
            | VariableLengthTypes::Xml
            | VariableLengthTypes::Json => size_of::<u32>(),
        }
    }
}

impl_try_from_tdstypes!(
    /// Partial Length types (chunked data types). They do not require the full data length
    /// to be specified before the actual data is streamed out.
    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum PartialLengthType {
        Xml = TdsDataType::Xml as u8,
        BigVarChar = TdsDataType::BigVarChar as u8,
        BigVarBinary = TdsDataType::BigVarBinary as u8,
        NVarChar = TdsDataType::NVarChar as u8,
        Udt = TdsDataType::Udt as u8,
        Json = TdsDataType::Json as u8,
    }
);

/// Represents the TYPE_INFO in the tds spec
#[derive(Debug, Clone)]
pub struct TypeInfo {
    pub tds_type: TdsDataType,
    pub length: Length,
    pub type_info_variant: TypeInfoVariant,
}

impl TypeInfo {
    pub(crate) fn get_collation(&self) -> Option<SqlCollation> {
        match &self.type_info_variant {
            TypeInfoVariant::VarLenString(_, _, collation) => {
                if collation.is_some() {
                    *collation
                } else {
                    None
                }
            }
            TypeInfoVariant::PartialLen(_, _, collation, _, _) => {
                if collation.is_some() {
                    *collation
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

type Precision = u8;
type Scale = u8;
type Length = usize;

/// Represents one of the variants of the TypeInfo from TDS Spec.
#[derive(Debug, Clone)]
pub enum TypeInfoVariant {
    FixedLen(FixedLengthTypes),
    VarLenString(VariableLengthTypes, Length, Option<SqlCollation>),
    VarLenPrecisionScale(VariableLengthTypes, Length, Precision, Scale),
    VarLenScale(VariableLengthTypes, Precision),
    VarLen(VariableLengthTypes, Length),
    PartialLen(
        PartialLengthType,
        Option<Length>,
        Option<SqlCollation>,
        Option<XmlInfo>,
        Option<UdtInfo>,
    ),
}

#[derive(Debug, Clone)]
pub struct XmlInfo {
    schema_present: u8,
    db_name: Option<String>,
    owning_schema: Option<String>,
    xml_schema_collection: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UdtInfoInColMetadata {
    max_byte_size: u16,
    db_name: String,
    schema_name: String,
    type_name: String,
    assembly_qualified_name: UdtMetadata,
}

type UdtMetadata = String;

#[derive(Debug, Clone)]
pub struct UdtInfoInRpc {
    db_name: String,
    schema_name: String,
    type_name: String,
}

#[derive(Debug, Clone)]
pub enum UdtInfo {
    InColMetadata(UdtInfoInColMetadata),
    InRpc(UdtInfoInRpc),
}

pub async fn read_type_info(
    reader: &mut PacketReader<'_>,
    data_type: TdsDataType,
) -> TdsResult<TypeInfo> {
    let fixed_length_type = FixedLengthTypes::try_from(data_type);
    if let Ok(fdt) = fixed_length_type {
        return Ok(TypeInfo {
            tds_type: data_type,
            length: fdt.get_len(),
            type_info_variant: TypeInfoVariant::FixedLen(fdt),
        });
    }

    let var_len_type = VariableLengthTypes::try_from(data_type);

    if let Ok(vdt) = var_len_type {
        let type_info = match vdt {
            VariableLengthTypes::TimeN
            | VariableLengthTypes::DateTime2N
            | VariableLengthTypes::DateTimeOffsetN => {
                let scale = reader.read_byte().await?;
                TypeInfo {
                    tds_type: data_type,
                    length: 0,
                    type_info_variant: TypeInfoVariant::VarLenScale(vdt, scale),
                }
            }
            VariableLengthTypes::MoneyN
            | VariableLengthTypes::DateTimeN
            | VariableLengthTypes::IntN
            | VariableLengthTypes::FltN
            | VariableLengthTypes::Guid
            | VariableLengthTypes::BitN => {
                let length: usize = reader.read_byte().await? as usize;
                TypeInfo {
                    tds_type: data_type,
                    length,
                    type_info_variant: TypeInfoVariant::VarLen(var_len_type?, length),
                }
            }
            VariableLengthTypes::DecimalN
            | VariableLengthTypes::NumericN
            | VariableLengthTypes::Decimal
            | VariableLengthTypes::Numeric => {
                let len_byte_count = vdt.get_len_byte_count();
                let length = match len_byte_count {
                    1 => reader.read_byte().await? as usize,
                    2 => reader.read_uint16().await? as usize,
                    4 => reader.read_int32().await? as usize,
                    _ => {
                        unreachable!(
                            "Invalid tds length {:?} for type: {:?}",
                            len_byte_count, data_type
                        )
                    }
                };
                let precision = reader.read_byte().await?;
                let scale = reader.read_byte().await?;
                TypeInfo {
                    tds_type: data_type,
                    length,
                    type_info_variant: TypeInfoVariant::VarLenPrecisionScale(
                        var_len_type.unwrap(),
                        length,
                        precision,
                        scale,
                    ),
                }
            }
            VariableLengthTypes::BigChar
            | VariableLengthTypes::BigVarChar
            | VariableLengthTypes::Text
            | VariableLengthTypes::NText
            | VariableLengthTypes::NChar
            | VariableLengthTypes::NVarChar => {
                let len_byte_count = vdt.get_len_byte_count();
                let length = match len_byte_count {
                    1 => reader.read_byte().await? as usize,
                    2 => reader.read_uint16().await? as usize,
                    4 => reader.read_int32().await? as usize,
                    _ => {
                        unreachable!(
                            "Invalid tds length {:?} for type: {:?}",
                            len_byte_count, data_type
                        )
                    }
                };

                // BIGCHARTYPE, BIGVARCHARTYPE, TEXTTYPE, NTEXTTYPE,
                // NCHARTYPE, or NVARCHARTYPE
                let collation = {
                    let mut collation_bytes: [u8; 5] = [0; 5];
                    let _ = reader.read_bytes(&mut collation_bytes).await?;

                    if collation_bytes.is_empty() {
                        None
                    } else {
                        Some(SqlCollation::new(&collation_bytes))
                    }
                };

                TypeInfo {
                    tds_type: data_type,
                    length,
                    type_info_variant: TypeInfoVariant::VarLenString(
                        var_len_type.unwrap(),
                        length,
                        collation,
                    ),
                }
            }
            VariableLengthTypes::BigVarBinary | VariableLengthTypes::BigBinary => {
                let len_byte_count = vdt.get_len_byte_count();
                let length = match len_byte_count {
                    1 => reader.read_byte().await? as usize,
                    2 => reader.read_uint16().await? as usize,
                    4 => reader.read_int32().await? as usize,
                    _ => {
                        unreachable!(
                            "Invalid tds length {:?} for type: {:?}",
                            len_byte_count, data_type
                        )
                    }
                };
                TypeInfo {
                    tds_type: data_type,
                    length,
                    type_info_variant: TypeInfoVariant::VarLen(var_len_type.unwrap(), length),
                }
            }
            ty => {
                println!("Unsupported TDS type: {:?}", ty);
                unimplemented!("Unsupported TDS type encountered. Cannot retrieve it's type info");
            }
        };

        // At this point, its possible that we have a data type which could be PLP
        // Check if the data type matches the PLP types, and if so, convert it to PLP
        match data_type {
            TdsDataType::Xml
            | TdsDataType::BigVarChar
            | TdsDataType::BigVarBinary
            | TdsDataType::NVarChar
            | TdsDataType::Json => {
                let plp_type = PartialLengthType::try_from(data_type);
                if type_info.length == 0xFFFF {
                    match type_info.type_info_variant {
                        TypeInfoVariant::VarLenString(_, _, collation) => Ok(TypeInfo {
                            tds_type: data_type,
                            length: type_info.length,
                            type_info_variant: TypeInfoVariant::PartialLen(
                                plp_type.unwrap(),
                                Some(type_info.length),
                                collation,
                                None,
                                None,
                            ),
                        }),
                        _ => {
                            unimplemented!("Other PLP types apart from strings are not implemented")
                        }
                    }
                } else {
                    Ok(type_info)
                }
            }
            _ => Ok(type_info),
        }
    } else {
        unimplemented!(
            "Couldnt find the Variable length equivalent of data_type.
            Is this UDT: {:?}",
            data_type
        )
    }

    // TODO: We are only left with UDT type now
}

pub fn is_unicode_type(data_type: TdsDataType) -> bool {
    matches!(data_type, TdsDataType::NVarChar | TdsDataType::NChar)
}
