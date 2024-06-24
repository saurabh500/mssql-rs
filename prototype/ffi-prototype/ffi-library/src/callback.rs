// Caller is at ..\..\ffi-app\Callback.cpp
use std::mem::ManuallyDrop;
use bytes::{Buf, BytesMut};
use std::io::{Cursor,Read};
use byteorder::{LittleEndian, ReadBytesExt};
use crate::TdsError;
use libc::c_int;
use super::{parse_all,Parse};

type Callback = unsafe extern "C" fn(&EnvChange) -> c_int;

pub struct CallbackParser {
    pub call_back: Option<Callback>
}

impl CallbackParser {
    fn new() -> Self{
        Self {
            call_back: None,
        }
    }
}

impl Parse for CallbackParser {
    fn parse(&self, buf: BytesMut) -> crate::Result<()> {
        let env_change = decode(buf)?;
        if let Some(cb) = self.call_back {
            unsafe {
                cb(&env_change);
            }        
        }
    
        Ok(())    
    }
}

#[no_mangle]
pub extern "C" fn register_callback(ptr: *mut CallbackParser, cb: Option<Callback>) {
    let parser = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    parser.call_back = cb;
}

#[no_mangle]
pub extern "C" fn callback_parser_new() -> *mut CallbackParser {
    Box::into_raw(Box::new(CallbackParser::new()))
}

#[no_mangle]
pub extern "C" fn callback_parse_token(ptr: *const CallbackParser) -> i32 {
    let parser = unsafe {
        assert!(!ptr.is_null());
        &*ptr
    };

    let result = parse_all(parser);
    match result {
        Ok(()) => 0,
        Err(_) => 1,
    }
}

#[no_mangle]
pub extern "C" fn callback_parser_free(ptr: *mut CallbackParser) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        _ = Box::from_raw(ptr);
    }
}

fn decode(src: BytesMut) -> crate::Result<EnvChange>
{
    let buf_size = src.len();
    let mut buf = Cursor::new(src);
    let token_size = buf.get_u16_le();
    println!("Token {} buf {}", token_size, buf_size);
    let ty_byte = buf.read_u8()?;

    let ty = EnvChangeTy::try_from(ty_byte)
        .map_err(|_| TdsError::Message(format!("invalid envchange type {:x}", ty_byte).into()))?;

    let token = match ty {
        EnvChangeTy::Database => {
            let len = buf.read_u8()? as u16;
            let mut dn = DatabaseName::new(len);

            for item in dn.dbname_new.iter_mut().take(len as usize) {
                *item = buf.read_u16::<LittleEndian>()?;
            }

            // Old databse name is not collected.
            let len = buf.read_u8()? as usize;
            buf.advance(len);

            EnvChange {
                env_type: ty_byte,
                env_change_data: EnvChangeData {
                    database_name: ManuallyDrop::new(dn)
                },
            }
        }
        EnvChangeTy::PacketSize => {
            let len = buf.read_u8()? as usize;
            let mut bytes = vec![0; len];

            for item in bytes.iter_mut().take(len) {
                *item = buf.read_u16::<LittleEndian>()?;
            }

            let new_value = String::from_utf16(&bytes[..])?;

            // Old packet size is not collected.
            let len = buf.read_u8()? as usize;
            buf.advance(len);

            EnvChange {
                env_type: ty_byte,
                env_change_data: EnvChangeData {
                    packet_size:ManuallyDrop::new(PacketSize {
                        packet_size_new: new_value.parse()?,
                    })
                },
            }
        }
        EnvChangeTy::SqlCollation => {
            let len = buf.read_u8()? as usize;
            let mut new_value = vec![0; len];
            buf.read_exact(&mut new_value[0..len])?;
            let mut collation = Collation::default();

            if len == 5 {
                collation.sortid = new_value[4];
                collation.wcid = u32::from_le_bytes([
                    new_value[0],
                    new_value[1],
                    new_value[2],
                    new_value[3],
                ]);
            }

            // Old collation is not collected.
            let len = buf.read_u8()? as usize;
            buf.advance(len);

            EnvChange {
                env_type: ty_byte,
                env_change_data: EnvChangeData {
                    collation:ManuallyDrop::new(collation)
                }
            }
        }
        _ => return Err(TdsError::Message(format!("Unsuported ENV type {}", ty_byte))),
    };

    Ok(token)
}

back_to_enum! {
    #[repr(u32)]
    enum EnvChangeTy {
        Database = 1,
        Language = 2,
        CharacterSet = 3,
        PacketSize = 4,
        UnicodeDataSortingLID = 5,
        UnicodeDataSortingCFL = 6,
        SqlCollation = 7,
        /// below here: >= TDSv7.2
        BeginTransaction = 8,
        CommitTransaction = 9,
        RollbackTransaction = 10,
        EnlistDTCTransaction = 11,
        DefectTransaction = 12,
        Rtls = 13,
        PromoteTransaction = 15,
        TransactionManagerAddress = 16,
        TransactionEnded = 17,
        ResetConnection = 18,
        UserName = 19,
        /// below here: TDS v7.4
        Routing = 20,
    }
}

const SYSNAMELEN: usize = 128;
const MAX_CHARSET_NAME: usize = 30;
const MAX_PROTOCOL_PREFIX_LEN: usize = 7;
const FILENAME_MAX: usize = 260;
const MAX_PROT_SRVINSTANCE_LEN: usize = MAX_PROTOCOL_PREFIX_LEN + FILENAME_MAX + 1;
const MAX_ALTERNATE_SERVER_LENGTH_IN_CHAR: usize = 1024;


#[repr(C)]
struct DatabaseName
{
    dbname_new_size: u16,
    dbname_new: [u16; SYSNAMELEN + 1],
}

impl DatabaseName {
    fn new(len: u16) -> Self {
        println!("Database name {}", len);
        assert!((len as usize) <= SYSNAMELEN);
        Self {
            dbname_new_size: len,
            dbname_new: [0u16; SYSNAMELEN + 1],
        }
    }
}

#[repr(C)]
struct Language
{
    lang_new_size: u16,
    lang_new: [u16; SYSNAMELEN + 1],
}

#[allow(dead_code)]
impl Language {
    fn new(len: u16) -> Self {
        assert!((len as usize) <= SYSNAMELEN);
        Self {
            lang_new_size: len,
            lang_new: [0u16; SYSNAMELEN + 1],
        }
    }
}

#[derive(Default)]
#[repr(C)]
struct PacketSize
{
    packet_size_new: u32,
}

#[repr(C)]
struct CharSet
{
    char_set_new_size: u16,
    char_set_new: [u16; MAX_CHARSET_NAME + 1],
    case_sensitive: c_int,
}

#[allow(dead_code)]
impl CharSet {
    fn new(len: u16) -> Self {
        assert!((len as usize) <= MAX_CHARSET_NAME);
        Self {
            char_set_new_size: len,
            char_set_new: [0u16; MAX_CHARSET_NAME + 1],
            case_sensitive: 0,
        }
    }
}

#[derive(Default)]
#[repr(C)]
struct Collation
{
    wcid: u32,
    sortid: u8,
}

#[derive(Default)]
#[repr(C)]
struct Transaction
{
    xact_id: u64,
}

#[repr(C)]
struct LogShipping
{
    partner_node_size: u16,
    partner_node: [u16; MAX_PROT_SRVINSTANCE_LEN],
}

#[allow(dead_code)]
impl LogShipping {
    fn new(len: u16) -> Self {
        assert!((len as usize) < MAX_PROT_SRVINSTANCE_LEN);
        Self {
            partner_node_size: len,
            partner_node: [0u16; MAX_PROT_SRVINSTANCE_LEN],
        }
    }
}

#[repr(C)]
struct Routing
{
    routing_data_length: u16,
    protocol: u8,
    protocol_property: u16,
    alternate_server_size: u16,
    alternate_server: [u16; MAX_ALTERNATE_SERVER_LENGTH_IN_CHAR + 1],
}

#[allow(dead_code)]
impl Routing {
    fn new(len: u16) -> Self {
        assert!((len as usize) <= MAX_ALTERNATE_SERVER_LENGTH_IN_CHAR);
        Self {
            routing_data_length: 0,
            protocol: 0,
            protocol_property: 0,
            alternate_server_size: len,
            alternate_server: [0u16; MAX_ALTERNATE_SERVER_LENGTH_IN_CHAR + 1],
        }
    }
}

#[repr(C)]
union EnvChangeData {
    database_name: ManuallyDrop<DatabaseName>,
    language: ManuallyDrop<Language>,
    packet_size: ManuallyDrop<PacketSize>,
    unicode_compare_style: u32,
    unicode_lcid: u32,
    char_set: ManuallyDrop<CharSet>,
    collation: ManuallyDrop<Collation>,
    transaction: ManuallyDrop<Transaction>,
    log_shipping: ManuallyDrop<LogShipping>,
    routing: ManuallyDrop<Routing>,
}

#[repr(C)]
pub struct EnvChange {
    env_type: u8,
    env_change_data: EnvChangeData,
}
