#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDataType {
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

    // Date/Time Data Types
    DateTime = 0x3D,
    SmallDateTime = 0x3A,
    Date = 0x28,
    Time = 0x29,
    DateTime2 = 0x2A,
    DateTimeOffset = 0x2B,

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
}

impl SqlDataType {
    /// Converts an integer value to the corresponding SqlDataType, if valid.
    pub fn from_u8(value: u8) -> Option<SqlDataType> {
        match value {
            0x30 => Some(SqlDataType::TinyInt),
            0x34 => Some(SqlDataType::SmallInt),
            0x38 => Some(SqlDataType::Int),
            0x7F => Some(SqlDataType::BigInt),
            0x32 => Some(SqlDataType::Bit),
            0x3B => Some(SqlDataType::Real),
            0x3E => Some(SqlDataType::Float),
            0x3C => Some(SqlDataType::Money),
            0x7A => Some(SqlDataType::SmallMoney),
            0x2F => Some(SqlDataType::Char),
            0xA7 => Some(SqlDataType::VarChar),
            0xEF => Some(SqlDataType::NChar),
            0xE7 => Some(SqlDataType::NVarChar),
            0xAD => Some(SqlDataType::Binary),
            0xA5 => Some(SqlDataType::VarBinary),
            0x23 => Some(SqlDataType::Text),
            0x63 => Some(SqlDataType::NText),
            0x22 => Some(SqlDataType::Image),
            0x26 => Some(SqlDataType::IntN),
            0x68 => Some(SqlDataType::BitN),
            0x3D => Some(SqlDataType::DateTime),
            0x3A => Some(SqlDataType::SmallDateTime),
            0x28 => Some(SqlDataType::Date),
            0x29 => Some(SqlDataType::Time),
            0x2A => Some(SqlDataType::DateTime2),
            0x2B => Some(SqlDataType::DateTimeOffset),
            0x6A => Some(SqlDataType::Decimal),
            0x6C => Some(SqlDataType::Numeric),
            0x24 => Some(SqlDataType::UniqueIdentifier),
            0x62 => Some(SqlDataType::SqlVariant),
            0xF1 => Some(SqlDataType::Xml),
            0xF0 => Some(SqlDataType::Udt),
            0x11 => Some(SqlDataType::Geography),
            0xF9 => Some(SqlDataType::Geometry),
            0xFD => Some(SqlDataType::HierarchyId),
            0xAF => Some(SqlDataType::BigChar),
            _ => None,
        }
    }
}
