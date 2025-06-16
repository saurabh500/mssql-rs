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
    // TODO: Will be migrated to the time struct in future
    Time(u64),
    // TODO: Will be migrated to a Datetime2 struct
    DateTime2 {
        days: u32,
        time_nanos: u64,
    },
    // TODO: Will be migrated to a DateTimeOffset struct
    DateTimeOffset {
        days: u32,
        time_nanos: u64,
        offset: i16,
    },
    SmallDateTime {
        day: u16,
        time: u16,
    },
    SmallMoney(MoneyParts),
    Money(MoneyParts),
    MoneyN(MoneyParts),
    Bytes(Vec<u8>),
    Xml(SqlXml),
    Null,
    Uuid(Uuid),
    Json(SqlJson),
}
