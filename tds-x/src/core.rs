#[derive(PartialEq, Debug)]
pub enum SQLServerVersion {
    SqlServerNotsupported = 0,
    SqlServer2000 = 8,
    SqlServer2005 = 9,
    SqlServer2008 = 10, // This also represents 2008R2.
    SqlServer2012 = 11,
    SqlServer2014 = 12,
    SqlServer2016 = 13,
    SqlServer2017 = 14,
    SqlServer2019 = 15,
    SqlServer2022 = 16,
    SqlServer2022lus = 17,
}

impl From<u8> for SQLServerVersion {
    fn from(v: u8) -> Self {
        match v {
            0 => SQLServerVersion::SqlServerNotsupported,
            8 => SQLServerVersion::SqlServer2000,
            9 => SQLServerVersion::SqlServer2005,
            10 => SQLServerVersion::SqlServer2008,
            11 => SQLServerVersion::SqlServer2012,
            12 => SQLServerVersion::SqlServer2014,
            13 => SQLServerVersion::SqlServer2016,
            14 => SQLServerVersion::SqlServer2017,
            15 => SQLServerVersion::SqlServer2019,
            16 => SQLServerVersion::SqlServer2022,
            17 => SQLServerVersion::SqlServer2022lus,
            _ => SQLServerVersion::SqlServerNotsupported,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub build: u16,
    pub revision: u16,
}

impl Version {
    pub fn new(major: u8, minor: u8, build: u16, revision: u16) -> Self {
        Version {
            major,
            minor,
            build,
            revision,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum EncryptionSetting {
    NotSupported,
    Optional,
    Required,
    Strict,
    LoginOnly,
}
