use std::fmt;

use crate::{
    datatypes::decoder::ColumnValues, message::login::RoutingInfo, query::metadata::ColumnMetadata,
};

use super::{
    fed_auth_info::{FedAuthInfoToken, SspiToken},
    login_ack::LoginAckToken,
};

#[derive(Eq, PartialEq, Hash, Debug)]
#[repr(u8)]
pub enum TokenType {
    AltMetadata = 0x88,
    AltRow = 0xD3,
    ColMetadata = 0x81,
    ColInfo = 0xA5,
    Done = 0xFD,
    DoneProc = 0xFE,
    DoneInProc = 0xFF,
    EnvChange = 0xE3,
    Error = 0xAA,
    FeatureExtAck = 0xAE,
    FedAuthInfo = 0xEE,
    Info = 0xAB,
    LoginAck = 0xAD,
    NbcRow = 0xD2,
    Offset = 0x78,
    Order = 0xA9,
    ReturnStatus = 0x79,
    ReturnValue = 0xAC,
    Row = 0xD1,
    SSPI = 0xED,
    TabName = 0xA4,
}

impl From<u8> for TokenType {
    fn from(value: u8) -> Self {
        match value {
            0x88 => TokenType::AltMetadata,
            0xD3 => TokenType::AltRow,
            0x81 => TokenType::ColMetadata,
            0xA5 => TokenType::ColInfo,
            0xFD => TokenType::Done,
            0xFE => TokenType::DoneProc,
            0xFF => TokenType::DoneInProc,
            0xE3 => TokenType::EnvChange,
            0xAA => TokenType::Error,
            0xAE => TokenType::FeatureExtAck,
            0xEE => TokenType::FedAuthInfo,
            0xAB => TokenType::Info,
            0xAD => TokenType::LoginAck,
            0xD2 => TokenType::NbcRow,
            0x78 => TokenType::Offset,
            0xA9 => TokenType::Order,
            0x79 => TokenType::ReturnStatus,
            0xAC => TokenType::ReturnValue,
            0xD1 => TokenType::Row,
            0xED => TokenType::SSPI,
            0xA4 => TokenType::TabName,
            _ => panic!("Unknown token type: {:#X}", value),
        }
    }
}

pub trait Token {
    fn token_type(&self) -> TokenType;
}

#[derive(Debug)]
pub(crate) enum Tokens {
    Done(DoneToken),
    DoneInProc(DoneInProcToken),
    DoneProc(DoneProcToken),
    EnvChange(EnvChangeToken),
    Error(ErrorToken),
    Info(InfoToken),
    LoginAck(LoginAckToken),
    FeatureExtAck(FeatureExtAckToken),
    FedAuthInfo(FedAuthInfoToken),
    Sspi(SspiToken),
    Row(RowToken),
    ColMetadata(ColMetadataToken),
    NbcRow(NbcRowToken),
}

macro_rules! impl_from_token {
    ($token_type:ty, $variant:ident) => {
        impl From<$token_type> for Tokens {
            fn from(token: $token_type) -> Self {
                Tokens::$variant(token)
            }
        }
    };
}

impl_from_token!(EnvChangeToken, EnvChange);
impl_from_token!(ErrorToken, Error);
impl_from_token!(InfoToken, Info);
impl_from_token!(DoneToken, Done);
impl_from_token!(DoneInProcToken, DoneInProc);
impl_from_token!(DoneProcToken, DoneProc);
impl_from_token!(LoginAckToken, LoginAck);
impl_from_token!(FeatureExtAckToken, FeatureExtAck);
impl_from_token!(FedAuthInfoToken, FedAuthInfo);
impl_from_token!(SspiToken, Sspi);
impl_from_token!(RowToken, Row);
impl_from_token!(ColMetadataToken, ColMetadata);
impl_from_token!(NbcRowToken, NbcRow);

impl Token for Tokens {
    fn token_type(&self) -> TokenType {
        match self {
            Tokens::Done(token) => token.token_type(),
            Tokens::DoneInProc(token) => token.token_type(),
            Tokens::DoneProc(token) => token.token_type(),
            Tokens::EnvChange(token) => token.token_type(),
            Tokens::Error(token) => token.token_type(),
            Tokens::Info(token) => token.token_type(),
            Tokens::LoginAck(token) => token.token_type(),
            Tokens::FeatureExtAck(token) => token.token_type(),
            Tokens::FedAuthInfo(token) => token.token_type(),
            Tokens::Sspi(token) => token.token_type(),
            Tokens::Row(token) => token.token_type(),
            Tokens::ColMetadata(token) => token.token_type(),
            Tokens::NbcRow(token) => token.token_type(),
        }
    }
}

pub(crate) struct TokenEvent<'a> {
    pub token: &'a dyn Token,
    pub exit: bool,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum EnvChangeContainer {
    String(EnvChangeTokenValuePairs<String>),
    SqlCollation(EnvChangeTokenValuePairs<SqlCollation>),
    UInt32(EnvChangeTokenValuePairs<u32>),
    RoutingType(EnvChangeTokenValuePairs<Option<RoutingInfo>>),
    BytesType(EnvChangeTokenValuePairs<Vec<u8>>),
}

impl From<(String, String)> for EnvChangeContainer {
    fn from(value: (String, String)) -> Self {
        EnvChangeContainer::String(EnvChangeTokenValuePairs::<String>::new(value.0, value.1))
    }
}

impl From<(SqlCollation, SqlCollation)> for EnvChangeContainer {
    fn from(value: (SqlCollation, SqlCollation)) -> Self {
        EnvChangeContainer::SqlCollation(EnvChangeTokenValuePairs::<SqlCollation>::new(
            value.0, value.1,
        ))
    }
}

impl From<(u32, u32)> for EnvChangeContainer {
    fn from(value: (u32, u32)) -> Self {
        EnvChangeContainer::UInt32(EnvChangeTokenValuePairs::<u32>::new(value.0, value.1))
    }
}

impl From<(Option<RoutingInfo>, Option<RoutingInfo>)> for EnvChangeContainer {
    fn from(value: (Option<RoutingInfo>, Option<RoutingInfo>)) -> Self {
        EnvChangeContainer::RoutingType(EnvChangeTokenValuePairs::<Option<RoutingInfo>>::new(
            value.0, value.1,
        ))
    }
}

impl From<(Vec<u8>, Vec<u8>)> for EnvChangeContainer {
    fn from(value: (Vec<u8>, Vec<u8>)) -> Self {
        EnvChangeContainer::BytesType(EnvChangeTokenValuePairs::<Vec<u8>>::new(value.0, value.1))
    }
}

impl fmt::Debug for EnvChangeContainer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EnvChangeContainer::String(value) => write!(f, "String: {:?}", value),
            EnvChangeContainer::SqlCollation(value) => write!(f, "SqlCollation: {:?}", value),
            EnvChangeContainer::UInt32(value) => write!(f, "UInt32: {:?}", value),
            EnvChangeContainer::RoutingType(value) => write!(f, "RoutingType: {:?}", value),
            EnvChangeContainer::BytesType(value) => write!(f, "ByteType: {:?}", value),
        }
    }
}

#[derive(Debug)]
pub(crate) struct EnvChangeToken {
    pub sub_type: EnvChangeTokenSubType,
    pub change_type: EnvChangeContainer,
}

trait EnvChangeSubToken {
    fn sub_type(&self) -> EnvChangeTokenSubType;
}

#[derive(Debug)]
pub struct FeatureExtAckToken {}

impl Token for EnvChangeToken {
    fn token_type(&self) -> TokenType {
        TokenType::EnvChange
    }
}

impl EnvChangeSubToken for EnvChangeToken {
    fn sub_type(&self) -> EnvChangeTokenSubType {
        self.sub_type
    }
}

impl Token for FeatureExtAckToken {
    fn token_type(&self) -> TokenType {
        TokenType::FeatureExtAck
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ColMetadataToken {
    pub column_count: u16,
    pub columns: Vec<ColumnMetadata>,
}

impl Token for ColMetadataToken {
    fn token_type(&self) -> TokenType {
        TokenType::ColMetadata
    }
}

#[derive(Debug, Default)]
pub(crate) struct NbcRowToken {}

impl Token for NbcRowToken {
    fn token_type(&self) -> TokenType {
        TokenType::NbcRow
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlCollation {
    pub info: u32,
    pub lcid: i32,
    pub comparison_style: u8,
    pub sort_id: u8,
    // TODO: Encoding is represented as a String, but
    // this detail needs to be worked out
    pub encoding: Option<String>,
}

impl SqlCollation {
    /// Creates a new SqlCollation from a 5-byte array.
    pub fn new(collation_bytes: &[u8]) -> Self {
        let byte_len = collation_bytes.len();
        if byte_len != 5 && byte_len != 0 {
            panic!("Collation must be exactly 5 bytes long or none.");
        }
        if byte_len == 0 {
            return Self::default();
        }
        let info = u32::from_le_bytes([
            collation_bytes[0],
            collation_bytes[1],
            collation_bytes[2],
            collation_bytes[3],
        ]);
        let lcid = (info & 0x000FFFFF) as i32; // Lower 20 bits
        let comparison_style = ((info >> 20) & 0xFF) as u8; // Next 8 bits
        let sort_id = collation_bytes[4];

        let encoding = Self::get_encoding(lcid, sort_id);

        SqlCollation {
            info,
            lcid,
            comparison_style,
            sort_id,
            encoding,
        }
    }

    /// TODO: Encoding handling needs to be thought of. How do we go from lcid / sort id to encoding?.
    /// Option<String> return type is just a place holder.
    fn get_encoding(_lcid: i32, _sort_id: u8) -> Option<String> {
        None
    }
}

impl fmt::Display for SqlCollation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "INFO: {} LCID: {}, ComparisonStyle: {}, SortID: {}, Encoding: {:?}",
            self.info, self.lcid, self.comparison_style, self.sort_id, self.encoding
        )
    }
}

/// Static lookup table for code pages by SortID
pub static CODE_PAGE_FROM_SORT_ID: [Option<u16>; 256] = [
    None,       // 0
    None,       // 1
    None,       // 2
    None,       // 3
    None,       // 4
    None,       // 5
    None,       // 6
    None,       // 7
    None,       // 8
    None,       // 9
    None,       // 10
    None,       // 11
    None,       // 12
    None,       // 13
    None,       // 14
    None,       // 15
    None,       // 16
    None,       // 17
    None,       // 18
    None,       // 19
    None,       // 20
    None,       // 21
    None,       // 22
    None,       // 23
    None,       // 24
    None,       // 25
    None,       // 26
    None,       // 27
    None,       // 28
    None,       // 29
    Some(437),  // 30
    Some(437),  // 31
    Some(437),  // 32
    Some(437),  // 33
    Some(437),  // 34
    None,       // 35
    None,       // 36
    None,       // 37
    None,       // 38
    None,       // 39
    Some(850),  // 40
    Some(850),  // 41
    Some(850),  // 42
    Some(850),  // 43
    Some(850),  // 44
    None,       // 45
    None,       // 46
    None,       // 47
    None,       // 48
    Some(850),  // 49
    Some(1252), // 50
    Some(1252), // 51
    Some(1252), // 52
    Some(1252), // 53
    Some(1252), // 54
    Some(850),  // 55
    Some(850),  // 56
    Some(850),  // 57
    Some(850),  // 58
    Some(850),  // 59
    Some(850),  // 60
    Some(850),  // 61
    None,       // 62
    None,       // 63
    None,       // 64
    None,       // 65
    None,       // 66
    None,       // 67
    None,       // 68
    None,       // 69
    None,       // 70
    Some(1252), // 71
    Some(1252), // 72
    Some(1252), // 73
    Some(1252), // 74
    Some(1252), // 75
    None,       // 76
    None,       // 77
    None,       // 78
    None,       // 79
    None,       // 80
    None,       // 81
    None,       // 82
    None,       // 83
    None,       // 84
    Some(1250), // 85
    Some(1250), // 86
    Some(1250), // 87
    Some(1250), // 88
    Some(1250), // 89
    Some(1250), // 90
    Some(1250), // 91
    Some(1250), // 92
    Some(1250), // 93
    Some(1250), // 94
    Some(1250), // 95
    Some(1250), // 96
    Some(1250), // 97
    Some(1250), // 98
    Some(1250), // 99
    Some(1250), // 100
    Some(1250), // 101
    Some(1250), // 102
    Some(1250), // 103
    None,       // 104
    None,       // 105
    None,       // 106
    None,       // 107
    None,       // 108
    Some(1251), // 109
    Some(1251), // 110
    Some(1251), // 111
    Some(1251), // 112
    Some(1251), // 113
    None,       // 114
    None,       // 115
    None,       // 116
    Some(1253), // 117
    Some(1253), // 118
    Some(1253), // 119
    None,       // 120
    None,       // 121
    None,       // 122
    None,       // 123
    None,       // 124
    None,       // 125
    Some(1253), // 126
    Some(1253), // 127
    Some(1253), // 128
    None,       // 129
    Some(1253), // 130
    None,       // 131
    None,       // 132
    None,       // 133
    None,       // 134
    Some(1254), // 135
    Some(1254), // 136
    Some(1254), // 137
    None,       // 138
    None,       // 139
    None,       // 140
    None,       // 141
    None,       // 142
    None,       // 143
    Some(1255), // 144
    Some(1255), // 145
    Some(1255), // 146
    None,       // 147
    None,       // 148
    None,       // 149
    None,       // 150
    None,       // 151
    None,       // 152
    Some(1256), // 153
    Some(1256), // 154
    Some(1256), // 155
    None,       // 156
    None,       // 157
    None,       // 158
    None,       // 159
    None,       // 160
    None,       // 161
    Some(1257), // 162
    Some(1257), // 163
    Some(1257), // 164
    Some(1257), // 165
    Some(1257), // 166
    Some(1257), // 167
    Some(1257), // 168
    Some(1257), // 169
    Some(1257), // 170
    None,       // 171
    None,       // 172
    None,       // 173
    None,       // 174
    None,       // 175
    None,       // 176
    None,       // 177
    None,       // 178
    None,       // 179
    None,       // 180
    None,       // 181
    None,       // 182
    None,       // 183
    None,       // 184
    None,       // 185
    None,       // 186
    None,       // 187
    None,       // 188
    None,       // 189
    None,       // 190
    None,       // 191
    None,       // 192
    None,       // 193
    Some(1252), // 194
    Some(1252), // 195
    Some(1252), // 196
    Some(1252), // 197
    None,       // 198
    None,       // 199
    None,       // 200
    None,       // 201
    None,       // 202
    Some(932),  // 203
    Some(932),  // 204
    Some(949),  // 205
    Some(949),  // 206
    Some(950),  // 207
    Some(950),  // 208
    Some(936),  // 209
    Some(936),  // 210
    Some(932),  // 211
    Some(949),  // 212
    Some(950),  // 213
    Some(936),  // 214
    Some(874),  // 215
    Some(874),  // 216
    Some(874),  // 217
    None,       // 218
    None,       // 219
    None,       // 220
    Some(1252), // 221
    Some(1252), // 222
    Some(1252), // 223
    Some(1252), // 224
    Some(1252), // 225
    Some(1252), // 226
    Some(1252), // 227
    None,       // 228
    None,       // 229
    None,       // 230
    None,       // 231
    None,       // 232
    None,       // 233
    None,       // 234
    None,       // 235
    None,       // 236
    None,       // 237
    None,       // 238
    None,       // 239
    None,       // 240
    None,       // 241
    None,       // 242
    None,       // 243
    None,       // 244
    None,       // 245
    None,       // 246
    None,       // 247
    None,       // 248
    None,       // 249
    None,       // 250
    None,       // 251
    None,       // 252
    None,       // 253
    None,       // 254
    None,       // 255
];

#[derive(Debug)]
pub(crate) struct ErrorToken {
    pub number: u32,
    pub state: u8,
    pub severity: u8,
    pub message: String,
    pub server_name: String,
    pub proc_name: String,
    pub line_number: u32,
}

impl Token for ErrorToken {
    fn token_type(&self) -> TokenType {
        TokenType::Error
    }
}

#[derive(Debug)]
pub(crate) struct InfoToken {
    pub number: u32,
    pub state: u8,
    pub severity: u8,
    pub message: String,
    pub server_name: String,
    pub proc_name: String,
    pub line_number: u32,
}

impl Token for InfoToken {
    fn token_type(&self) -> TokenType {
        TokenType::Info
    }
}

#[derive(Debug)]
pub(crate) struct DoneToken {
    pub status: DoneStatus,
    pub cur_cmd: CurrentCommand,
    pub row_count: u64,
}

impl Token for DoneToken {
    fn token_type(&self) -> TokenType {
        TokenType::Done
    }
}

#[derive(Debug)]
pub(crate) struct DoneInProcToken {
    pub status: DoneStatus,
    pub cur_cmd: CurrentCommand,
    pub row_count: u64,
}

impl Token for DoneInProcToken {
    fn token_type(&self) -> TokenType {
        TokenType::DoneInProc
    }
}

#[derive(Debug)]
pub(crate) struct DoneProcToken {
    pub status: DoneStatus,
    pub cur_cmd: CurrentCommand,
    pub row_count: u64,
}

impl Token for DoneProcToken {
    fn token_type(&self) -> TokenType {
        TokenType::DoneProc
    }
}

#[derive(Debug)]
pub struct RowToken {
    pub all_values: Vec<ColumnValues>,
}

impl RowToken {
    pub fn new(all_values: Vec<ColumnValues>) -> Self {
        Self { all_values }
    }
}

impl Token for RowToken {
    fn token_type(&self) -> TokenType {
        TokenType::Row
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct DoneStatus: u16 {
        /// Final.
        const FINAL = 0x0000;

        /// More.
        const MORE = 0x0001;

        /// Error.
        const ERROR = 0x0002;

        /// In Transaction.
        const IN_XACT = 0x0004;

        /// Count.
        const COUNT = 0x0010;

        /// Attention.
        const ATTN = 0x0020;

        /// Server Error.
        const SERVER_ERROR = 0x0100;
    }
}

impl From<u16> for DoneStatus {
    fn from(value: u16) -> Self {
        DoneStatus::from_bits_truncate(value)
    }
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CurrentCommand {
    None = 0x00,
    Select = 0xc1,
    Insert = 0xc3,
    Delete = 0xc4,
    Update = 0xc5,
    Abort = 0xd2,
    BeginXact = 0xd4,
    EndXact = 0xd5,
    BulkInsert = 0xf0,
    OpenCursor = 0x20,
    Merge = 0x117,
}

impl TryFrom<u16> for CurrentCommand {
    type Error = &'static str;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0xc1 => Ok(CurrentCommand::Select),
            0xc3 => Ok(CurrentCommand::Insert),
            0xc4 => Ok(CurrentCommand::Delete),
            0xc5 => Ok(CurrentCommand::Update),
            0xd2 => Ok(CurrentCommand::Abort),
            0xd4 => Ok(CurrentCommand::BeginXact),
            0xd5 => Ok(CurrentCommand::EndXact),
            0xf0 => Ok(CurrentCommand::BulkInsert),
            0x20 => Ok(CurrentCommand::OpenCursor),
            0x117 => Ok(CurrentCommand::Merge),
            // All unknown values are treated as None, and considered valid.
            _ => Ok(CurrentCommand::None),
        }
    }
}

/// Represents the different sub-types of environment change tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EnvChangeTokenSubType {
    Database = 1,
    Language = 2,
    CharacterSet = 3,
    PacketSize = 4,
    UnicodeDataSortingLocalId = 5,
    UnicodeDataSortingComparisonFlags = 6,
    SqlCollation = 7,
    BeginTransaction = 8,
    CommitTransaction = 9,
    RollbackTransaction = 10,
    EnlistDtcTransaction = 11,
    DefectTransaction = 12,
    DatabaseMirroringPartner = 13,
    PromoteTransaction = 15,
    TransactionManagerAddress = 16,
    TransactionEnded = 17,
    ResetConnection = 18,
    UserInstanceName = 19,
    Routing = 20,
}

impl From<u8> for EnvChangeTokenSubType {
    fn from(value: u8) -> Self {
        match value {
            1 => EnvChangeTokenSubType::Database,
            2 => EnvChangeTokenSubType::Language,
            3 => EnvChangeTokenSubType::CharacterSet,
            4 => EnvChangeTokenSubType::PacketSize,
            5 => EnvChangeTokenSubType::UnicodeDataSortingLocalId,
            6 => EnvChangeTokenSubType::UnicodeDataSortingComparisonFlags,
            7 => EnvChangeTokenSubType::SqlCollation,
            8 => EnvChangeTokenSubType::BeginTransaction,
            9 => EnvChangeTokenSubType::CommitTransaction,
            10 => EnvChangeTokenSubType::RollbackTransaction,
            11 => EnvChangeTokenSubType::EnlistDtcTransaction,
            12 => EnvChangeTokenSubType::DefectTransaction,
            13 => EnvChangeTokenSubType::DatabaseMirroringPartner,
            15 => EnvChangeTokenSubType::PromoteTransaction,
            16 => EnvChangeTokenSubType::TransactionManagerAddress,
            17 => EnvChangeTokenSubType::TransactionEnded,
            18 => EnvChangeTokenSubType::ResetConnection,
            19 => EnvChangeTokenSubType::UserInstanceName,
            20 => EnvChangeTokenSubType::Routing,
            // Panic on unknown values, since From must be infallible.
            _ => panic!("Invalid value for EnvChangeTokenSubType: {}", value),
        }
    }
}

/// A generic struct that stores the old/new values of an environment change.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct EnvChangeTokenValuePairs<T> {
    old_value: T,
    new_value: T,
}

impl<T> EnvChangeTokenValuePairs<T> {
    /// Creates a new instance of EnvChangeTokenValue.
    pub fn new(old_value: T, new_value: T) -> Self {
        Self {
            old_value,
            new_value,
        }
    }

    /// Gets a reference to the old value.
    pub fn old_value(&self) -> &T {
        &self.old_value
    }

    /// Gets a reference to the new value.
    pub fn new_value(&self) -> &T {
        &self.new_value
    }
}
