use uuid::Uuid;

use crate::{
    core::TdsResult,
    datatypes::{
        decoder::{DecimalParts, MoneyParts, SqlXml},
        sql_string::SqlString,
        sqldatatypes::TdsDataType,
    },
    read_write::packet_writer::PacketWriter,
    token::tokens::SqlCollation,
};

#[derive(Debug, PartialEq)]
pub enum SqlType {
    Bit(Option<bool>),
    TinyInt(Option<u8>),
    SmallInt(Option<i16>),
    Int(Option<i32>),
    BigInt(Option<i64>),
    Real(Option<f32>),
    Float(Option<f64>),
    Decimal(Option<DecimalParts>),
    Numeric(Option<DecimalParts>),
    Money(Option<MoneyParts>),
    SmallMoney(Option<MoneyParts>),

    // TODO: Will be migrated to the time struct in future
    Time(u64),
    DateTime2 {
        days: u32,
        time_nanos: u64,
    },
    DateTimeOffset {
        days: u32,
        time_nanos: u64,
        offset: i16,
    },
    SmallDateTime {
        day: u16,
        time: u16,
    },

    /// Represents a Varchar with a specifiied length.
    NVarchar(Option<SqlString>, u32),

    /// Represents a Varchar with MAX length.
    NVarcharMax(Option<SqlString>),

    Varchar(Option<SqlString>, u32),
    VarcharMax(Option<SqlString>),

    VarBinary(Option<Vec<u8>>, u32),
    VarBinaryMax(Option<Vec<u8>>),

    Binary(Option<Vec<u8>>),
    Char(Option<SqlString>, u32),
    NChar(Option<SqlString>, u32),

    Text(Option<SqlString>),
    NText(Option<SqlString>),

    Json(Option<String>),

    Xml(Option<SqlXml>),
    Uuid(Option<Uuid>),
    // To be added in future
    // Variant
    // TVP
}

type NullableTdsType = TdsDataType;

impl SqlType {
    pub(crate) async fn serialize(
        &self,
        _packet_writer: &mut PacketWriter<'_>,
        _db_collation: &SqlCollation,
    ) -> TdsResult<()> {
        todo!()
    }
}
