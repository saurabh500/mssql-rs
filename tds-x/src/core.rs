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
