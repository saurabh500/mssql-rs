#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnValueStatus {
    OutputParam = 0x01,
    Udf = 0x02,
}

impl From<u8> for ReturnValueStatus {
    fn from(value: u8) -> Self {
        match value {
            0x01 => ReturnValueStatus::OutputParam,
            0x02 => ReturnValueStatus::Udf,
            _ => panic!("Invalid value for SqlInterfaceType"),
        }
    }
}
