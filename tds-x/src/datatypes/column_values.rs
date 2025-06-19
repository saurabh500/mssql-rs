use crate::datatypes::decoder::{DecimalParts, MoneyParts};
use crate::datatypes::sql_json::SqlJson;
use crate::datatypes::sql_string::SqlString;
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
    DateTime((i32, u32)),
    Date(u32),
    Time(Time),
    DateTime2(DateTime2),
    DateTimeOffset(DateTimeOffset),
    SmallDateTime { day: u16, time: u16 },
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
