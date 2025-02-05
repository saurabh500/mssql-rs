#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TdsDataType {
    // Fixed-Length Data Types
    TinyInt = 0x30,
    SmallInt = 0x34,
    Int = 0x38,
    BigInt = 0x7F,
    Bit = 0x32,
    Real = 0x3B,
    Float = 0x3E,
    Money = 0x3C,
    SmallMoney = 0x7A,

    // Variable-Length Data Types
    Char = 0x2F,
    VarChar = 0xA7,
    NChar = 0xEF,
    NVarChar = 0xE7,
    Binary = 0xAD,
    VarBinary = 0xA5,
    Text = 0x23,
    NText = 0x63,
    Image = 0x22,

    IntN = 0x26,
    BitN = 0x68,
    MoneyN = 0x6E,
    FltN = 0x6D,

    // Date/Time Data Types
    DateTime = 0x3D,
    SmallDateTime = 0x3A,
    Date = 0x28,
    Time = 0x29,
    DateTime2 = 0x2A,
    DateTimeOffset = 0x2B,
    DateTimeN = 0x6F,

    // Exact Numerics
    Decimal = 0x6A,
    Numeric = 0x6C,

    // Special Types
    UniqueIdentifier = 0x24,
    SqlVariant = 0x62,
    Xml = 0xF1,

    // Other Types
    Udt = 0xF0,         // User-defined types
    Geography = 0x11,   // Geography
    Geometry = 0xF9,    // Geometry
    HierarchyId = 0xFD, // HierarchyId
    BigChar = 0xAF,

    #[default]
    None = 0x00,
}

impl TdsDataType {
    /// Converts an integer value to the corresponding SqlDataType, if valid.
    pub fn from_u8(value: u8) -> Option<TdsDataType> {
        match value {
            0x30 => Some(TdsDataType::TinyInt),
            0x34 => Some(TdsDataType::SmallInt),
            0x38 => Some(TdsDataType::Int),
            0x7F => Some(TdsDataType::BigInt),
            0x32 => Some(TdsDataType::Bit),
            0x3B => Some(TdsDataType::Real),
            0x3E => Some(TdsDataType::Float),
            0x3C => Some(TdsDataType::Money),
            0x7A => Some(TdsDataType::SmallMoney),
            0x2F => Some(TdsDataType::Char),
            0xA7 => Some(TdsDataType::VarChar),
            0xEF => Some(TdsDataType::NChar),
            0xE7 => Some(TdsDataType::NVarChar),
            0xAD => Some(TdsDataType::Binary),
            0xA5 => Some(TdsDataType::VarBinary),
            0x23 => Some(TdsDataType::Text),
            0x63 => Some(TdsDataType::NText),
            0x22 => Some(TdsDataType::Image),
            0x26 => Some(TdsDataType::IntN),
            0x68 => Some(TdsDataType::BitN),
            0x3D => Some(TdsDataType::DateTime),
            0x3A => Some(TdsDataType::SmallDateTime),
            0x28 => Some(TdsDataType::Date),
            0x29 => Some(TdsDataType::Time),
            0x2A => Some(TdsDataType::DateTime2),
            0x2B => Some(TdsDataType::DateTimeOffset),
            0x6A => Some(TdsDataType::Decimal),
            0x6C => Some(TdsDataType::Numeric),
            0x24 => Some(TdsDataType::UniqueIdentifier),
            0x62 => Some(TdsDataType::SqlVariant),
            0xF1 => Some(TdsDataType::Xml),
            0xF0 => Some(TdsDataType::Udt),
            0x11 => Some(TdsDataType::Geography),
            0xF9 => Some(TdsDataType::Geometry),
            0xFD => Some(TdsDataType::HierarchyId),
            0xAF => Some(TdsDataType::BigChar),
            0x6E => Some(TdsDataType::MoneyN),
            0x6D => Some(TdsDataType::FltN),
            0x6F => Some(TdsDataType::DateTimeN),
            _ => None,
        }
    }
}
