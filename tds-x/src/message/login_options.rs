#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
pub enum ApplicationIntent {
    ReadWrite,
    ReadOnly,
}

pub enum OptionOleDb {
    Off,
    On,
}

pub struct TypeFlags {
    pub(crate) sql_type: OptionSqlType,
    pub(crate) ole_db: OptionOleDb,
    pub(crate) access_intent: ApplicationIntent,
    pub(crate) value: u8,
}

impl TypeFlags {
    pub fn value(&self) -> u8 {
        todo!()
    }
}

#[derive(PartialEq)]
pub enum OptionEndian {
    LittleEndian,
    BigEndian,
}

#[derive(PartialEq)]
pub enum OptionCharset {
    Ascii,
    Ebcdic,
}

#[derive(PartialEq)]
pub enum OptionFloat {
    IEEE,
    VAX,
    ND5000,
}

#[derive(PartialEq)]
pub enum OptionBcpDumpload {
    On,
    Off,
}

#[derive(PartialEq)]
pub enum OptionUseDb {
    Off,
    On,
}

#[derive(PartialEq)]
pub enum OptionInitDb {
    Warn,
    Fatal,
}

#[derive(PartialEq)]
pub enum OptionLangWarn {
    Off,
    On,
}

pub(crate) trait OptionsValue {
    fn value(&self) -> u8;
}

pub struct OptionFlags1 {
    endian: OptionEndian,
    charset: OptionCharset,
    float: OptionFloat,
    bcp_dumpload: OptionBcpDumpload,
    use_db: OptionUseDb,
    init_db: OptionInitDb,
    lang_warn: OptionLangWarn,
}

impl OptionFlags1 {
    const OPTION_ENDIAN_BIT_INDEX: u8 = 0x01;
    const OPTION_CHARSET_BIT_INDEX: u8 = 0x02;
    const OPTION_FLOAT_BIT_INDEX_VAX: u8 = 0x04;
    const OPTION_FLOAT_BIT_INDEX_ND5000: u8 = 0x08;
    const OPTION_BCP_DUMPLOAD_BIT_INDEX: u8 = 0x10;
    const OPTION_USE_DB_BIT_INDEX: u8 = 0x20;
    const OPTION_INIT_DB_INDEX_BIT: u8 = 0x40;
    const OPTION_LANG_WARN_BIT_INDEX: u8 = 0x80;

    pub(crate) fn default() -> OptionFlags1 {
        OptionFlags1 {
            endian: OptionEndian::LittleEndian,
            charset: OptionCharset::Ascii,
            float: OptionFloat::IEEE,
            bcp_dumpload: OptionBcpDumpload::On,
            use_db: OptionUseDb::On,
            init_db: OptionInitDb::Fatal,
            lang_warn: OptionLangWarn::On,
        }
    }

    fn set_bit(value: &mut u8, condition: bool, bit_index: u8) {
        if condition {
            *value |= bit_index;
        } else {
            *value &= u8::MAX - bit_index;
        }
    }

    fn set_endian_bit(&self, value: &mut u8) {
        Self::set_bit(
            value,
            self.endian != OptionEndian::LittleEndian,
            Self::OPTION_ENDIAN_BIT_INDEX,
        );
    }

    fn set_charset_bit(&self, value: &mut u8) {
        Self::set_bit(
            value,
            self.charset != OptionCharset::Ascii,
            Self::OPTION_CHARSET_BIT_INDEX,
        );
    }

    fn set_float_bits(&self, value: &mut u8) {
        match self.float {
            OptionFloat::IEEE => {
                *value &= u8::MAX - Self::OPTION_FLOAT_BIT_INDEX_VAX;
                *value &= u8::MAX - Self::OPTION_FLOAT_BIT_INDEX_ND5000;
            }
            OptionFloat::VAX => {
                *value |= Self::OPTION_FLOAT_BIT_INDEX_VAX;
                *value &= u8::MAX - Self::OPTION_FLOAT_BIT_INDEX_ND5000;
            }
            OptionFloat::ND5000 => {
                *value &= u8::MAX - Self::OPTION_FLOAT_BIT_INDEX_VAX;
                *value |= Self::OPTION_FLOAT_BIT_INDEX_ND5000;
            }
        }
    }

    fn set_bcp_dumpload_bit(&self, value: &mut u8) {
        Self::set_bit(
            value,
            self.bcp_dumpload != OptionBcpDumpload::On,
            Self::OPTION_BCP_DUMPLOAD_BIT_INDEX,
        );
    }

    fn set_use_db_bit(&self, value: &mut u8) {
        Self::set_bit(
            value,
            self.use_db == OptionUseDb::On,
            Self::OPTION_USE_DB_BIT_INDEX,
        );
    }

    fn set_init_db_bit(&self, value: &mut u8) {
        Self::set_bit(
            value,
            self.init_db == OptionInitDb::Fatal,
            Self::OPTION_INIT_DB_INDEX_BIT,
        );
    }

    fn set_lang_warn_bit(&self, value: &mut u8) {
        Self::set_bit(
            value,
            self.lang_warn == OptionLangWarn::On,
            Self::OPTION_LANG_WARN_BIT_INDEX,
        );
    }
}

impl OptionsValue for OptionFlags1 {
    fn value(&self) -> u8 {
        let mut computed_value: u8 = 0;

        self.set_endian_bit(&mut computed_value);
        self.set_charset_bit(&mut computed_value);
        self.set_float_bits(&mut computed_value);
        self.set_bcp_dumpload_bit(&mut computed_value);
        self.set_use_db_bit(&mut computed_value);
        self.set_init_db_bit(&mut computed_value);
        self.set_lang_warn_bit(&mut computed_value);
        computed_value
    }
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
    ReplicationLogin,
}

pub enum OptionIntegratedSecurity {
    Off,
    On,
}

pub struct OptionFlags2 {
    pub(crate) init_lang: OptionInitLang,
    pub(crate) odbc: OptionOdbc,
    pub(crate) user: OptionUser,
    pub(crate) integrated_security: OptionIntegratedSecurity,
}

impl OptionsValue for OptionFlags2 {
    fn value(&self) -> u8 {
        todo!()
    }
}

pub enum OptionChangePassword {
    No,
    Yes,
}

pub struct OptionFlags3 {
    pub(crate) change_password: OptionChangePassword,
    pub(crate) binary_xml: bool,
    pub(crate) spawn_user_instance: bool,
    pub(crate) extension_used: bool,
    pub(crate) unknown_collation_handling: bool,
}

impl OptionsValue for OptionFlags3 {
    fn value(&self) -> u8 {
        todo!()
    }
}
