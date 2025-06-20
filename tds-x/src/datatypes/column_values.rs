use crate::core::TdsResult;
use crate::datatypes::decoder::DecimalParts;
use crate::datatypes::sql_json::SqlJson;
use crate::datatypes::sql_string::SqlString;
use crate::error::Error;
use core::fmt;
use std::fmt::Debug;
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
    SmallMoney(SqlSmallMoney),
    Money(SqlMoney),
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

#[derive(PartialEq, Clone, Debug)]
pub struct SqlSmallMoney {
    pub int_val: i32,
}
impl From<i32> for SqlSmallMoney {
    fn from(value: i32) -> Self {
        SqlSmallMoney { int_val: value }
    }
}
// This struct represents the TDS money value.
#[derive(PartialEq, Clone)]
pub struct SqlMoney {
    pub lsb_part: i32, // LSB
    pub msb_part: i32, // MSB - Only populated for Money, 0 for SmallMoney
}
impl Debug for SqlMoney {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Money value: {:?}, int_part_1: {:?}, int_part_2: {:?}",
            TdsResult::<f64>::from(self).unwrap(),
            self.lsb_part,
            self.msb_part
        )
    }
}
impl From<(i32, i32)> for SqlMoney {
    fn from(value: (i32, i32)) -> Self {
        SqlMoney {
            lsb_part: value.0,
            msb_part: value.1,
        }
    }
}
impl From<i32> for SqlMoney {
    fn from(value: i32) -> Self {
        SqlMoney {
            lsb_part: value,
            msb_part: 0,
        }
    }
}
// This function reassembles the two 4-byte integers (in mixed endian format) into a single 8-byte signed integer.
// The resulting value is the TDS money value, which can be divided by 10^4 to get the actual money value.
// (See comments in MoneyParts definition for more details)
impl From<&SqlMoney> for TdsResult<f64> {
    fn from(value: &SqlMoney) -> Self {
        let lsb = value.lsb_part;
        let msb = value.msb_part;
        // -----Example:------
        // While this logic works on both little and big endian machines, this example assumes
        // a little endian machine. Coz big endian case is trivial.
        // 1) Hex representation of an 8-byte int value (MSB to LSB):
        //       - 11 22 33 44 55 66 77 88
        // 2) 8-byte int value stored in LE machine (Low Mem address First (LMF)):
        //       - 88 77 66 55 44 33 22 11
        // 3) This int value stored in TDS wire-format as two 4-byte integers (mixed endian, LMF):
        //       - 44 33 22 11, 88 77 66 55 (MSB = 44 33 22 11, LSB = 88 77 66 55)
        // *** We have (3) in variables msb and lsb. We need to reassemble it into (2) ***
        // - lsb as i64 =
        //       - +ve LSB: 88 77 66 55 00 00 00 00 (LMF)
        //       - -ve LSB: 88 77 66 55 ff ff ff ff (LMF)
        // - (lsb as i64) & 0x00000000FFFFFFFF = lsb_in_i64 = 88 77 66 55 00 00 00 00 (LMF)
        //       - This step is to handle -ve LSB case. We need to convert the ff ff ff ff MSB bytes
        //         to 00 00 00 00. This is done by masking the LSB with 0x00000000FFFFFFFF.
        // - (msb as i64) << 32 = 00 00 00 00 44 33 22 11 (LMF)
        // - (lsb_in_i64) | ((msb as i64) << 32) = 88 77 66 55 44 33 22 11 (LMF)
        let lsb_in_i64 = (lsb as i64) & 0x00000000FFFFFFFF;
        let money_val = lsb_in_i64 | ((msb as i64) << 32);
        // TDS value of money is the value multiplied by 10^4, hence we need to divide while decoding.
        // TODO: (value as f64) can cause precision loss
        Ok((money_val as f64) / 10000.0000)
    }
}
impl From<&SqlMoney> for TdsResult<f32> {
    fn from(value: &SqlMoney) -> Self {
        // TDS value of money is the value multiplied by 10^4, hence we need to divide while decoding.
        let scaled_value = (value.lsb_part as f64) / 10000.0000; // f64 so that we don't lose precision
        Ok(scaled_value as f32) // Post division, money value  must fit in f32
                                // TODO: For max (& min) value of smallmoney (214748.3647), the f32 value is 214748.36, which is not accurate. Debug & fix this.
                                //       See test test_money_no_panic. Trying to query these max values from SSMS or ODBC gives correct value.
    }
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
