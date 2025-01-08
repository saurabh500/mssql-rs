pub enum TdsVersion {
    V7_4 = 0x74000004,
    V8_0 = 0x08000000,
}

pub struct LoginOptions {
    pub tds_version: TdsVersion,
    pub packet_size: u32,
    pub client_prog_ver: u32,
    pub client_pid: u32,
    pub connection_id: u32,
    pub client_time_zone: i32,
    pub client_lcid: u32,
}

pub enum OptionSqlType {
    Default,
    TSQL,
}

pub enum ApplicationIntent {
    ReadWrite,
    ReadOnly,
}

pub enum OptionOleDb {
    Off,
    On,
}

pub struct TypeFlags {
    sql_type: OptionSqlType,
    ole_db: OptionOleDb,
    access_intent: ApplicationIntent,
    value: u8,
}

impl TypeFlags {
    pub fn value(&self) -> u8 {
        todo!()
    }
}

pub enum OptionEndian {
    LittleEndian,
    BigEndian,
}

pub enum OptionCharset {
    Ascii,
    Ebcdic,
}

pub enum OptionFloat {
    IEEE,
    VAX,
    ND5000,
}

pub enum OptionBcpDumpload {
    On,
    Off,
}

pub enum OptionUseDb {
    Off,
    On,
}

pub enum OptionInitDb {
    Warn,
    Fatal,
}

pub enum OptionLangWarn {
    Off,
    On,
}

pub struct OptionFlags1 {
    endian: OptionEndian,
    charset: OptionCharset,
    float: OptionFloat,
    bcp_dumpload: OptionBcpDumpload,
    use_db: OptionUseDb,
    init_db: OptionInitDb,
    lang_warn: OptionLangWarn,
    value: u8,
}

impl OptionFlags1 {
    //TODO:
}

pub enum OptionInitLang {
    Warn,
    Fatal,
}

pub enum OptionOdbc {
    Off,
    On,
}

pub enum OptionUser {
    Normal,
    Reserved,
    RemUser,
    ReplicationLogic,
}

pub enum OptionIntegratedSecurity {
    Off,
    On,
}

pub struct OptionFlags2 {
    init_lang: OptionInitLang,
    odbc: OptionOdbc,
    user: OptionUser,
    integrated_security: OptionIntegratedSecurity,
    value: u8,
}

pub enum OptionChangePassword {
    No,
    Yes,
}

pub struct OptionFlags3 {
    change_password: OptionChangePassword,
    binary_xml: bool,
    spawn_user_instance: bool,
    extension_used: bool,
    value: u8,
}
