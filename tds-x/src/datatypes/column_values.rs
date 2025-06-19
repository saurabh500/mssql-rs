use crate::core::TdsResult;
use crate::datatypes::decoder::{DecimalParts, MoneyParts};
use crate::datatypes::sql_json::SqlJson;
use crate::datatypes::sql_string::SqlString;
use crate::error::Error;
use uuid::Uuid;

#[derive(Debug, PartialOrd, PartialEq)]
pub struct SqlXml {
    pub bytes: Vec<u8>,
}

impl SqlXml {
    pub fn as_string(&self) -> String {
        let mut u16_buffer = Vec::with_capacity(self.bytes.len() / 2);
        self.bytes
            .chunks(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .for_each(|item| u16_buffer.push(item));

        String::from_utf16(&u16_buffer).unwrap()
    }
}

#[derive(Debug, PartialEq)]
pub enum ColumnValues {
    TinyInt(u8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Real(f32),
    Float(f64),
    Decimal(DecimalParts),
    Numeric(DecimalParts),
    Bit(bool),
    String(SqlString),
    DateTime(SqlDateTime),
    Date(SqlDate),
    Time(Time),
    DateTime2(DateTime2),
    DateTimeOffset(DateTimeOffset),
    SmallDateTime(SqlSmallDateTime),
    SmallMoney(MoneyParts),
    Money(MoneyParts),
    MoneyN(MoneyParts),
    Bytes(Vec<u8>),
    Xml(SqlXml),
    Null,
    Uuid(Uuid),
    Json(SqlJson),
}

pub const DEFAULT_VARTIME_SCALE: u8 = 7;

#[derive(Debug, PartialEq, Clone)]
pub struct Time {
    pub time_nanoseconds: u64,
    pub scale: u8,
}

impl Time {
    pub(crate) fn get_scale(&self) -> u8 {
        self.scale
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct DateTime2 {
    pub days: u32,
    pub time: Time,
}

#[derive(Debug, PartialEq, Clone)]
pub struct DateTimeOffset {
    pub datetime2: DateTime2,
    pub offset: i16,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SqlSmallDateTime {
    // One 2-byte unsigned integer that represents the number of days since January 1, 1900.
    pub days: u16,

    // One 2-byte unsigned integer that represents the number of minutes elapsed since 12 AM that day.
    pub time: u16,
}

#[derive(Debug, PartialEq, Clone)]
pub struct SqlDateTime {
    // One 4-byte signed integer that represents the number of days since January 1, 1900.
    // Negative numbers are allowed to represent dates since January 1, 1753.
    pub days: i32,

    // One 4-byte unsigned integer that represents the number of one
    // three-hundredths of a second (300 counts per second) elapsed since 12 AM that day.
    pub time: u32,
}

/// Represents a date in SQL Server, which is stored as the number of days since January 1, year 1.
/// This is a 3-byte unsigned integer, allowing for a range of dates from January 1, 0001 to December 31, 9999.
/// The reason for this struct is to evolve it to provide validation for
/// creation of Date type. We dont want a 4 byte u32 assigned to days,
/// which is invalid.
#[derive(Debug, PartialEq, Clone)]
pub struct SqlDate {
    // date is represented as one 3-byte unsigned integer that represents the number of days since January 1, year 1.
    days: u32,
}

impl SqlDate {
    pub fn create(days: u32) -> TdsResult<SqlDate> {
        if days <= 0xFFFFFF {
            Ok(SqlDate { days })
        } else {
            Err(Error::UsageError(
                "Value out of range for SqlDate (must be <= 0xFFFFFF)".to_string(),
            ))
        }
    }

    // This function is unsafe because it allows the creation of a SqlDate without checking the range.
    // It should only be used when you are sure that the value is within the valid range
    // (0 to 0xFFFFFF). This is meant for scenarios where SQL server is sending
    // us the date value and we need to create a SqlDate from it without validation.
    pub(crate) fn unchecked_create(days: u32) -> SqlDate {
        SqlDate { days }
    }

    pub fn get_days(&self) -> u32 {
        self.days
    }
}
