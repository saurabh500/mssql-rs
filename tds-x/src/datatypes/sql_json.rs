use core::fmt;
use std::fmt::Debug;

#[derive(PartialEq, Clone)]
pub struct SqlJson {
    pub bytes: Vec<u8>,
}

impl SqlJson {
    pub fn as_string(&self) -> String {
        String::from_utf8(self.bytes.clone()).unwrap()
    }

    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl Debug for SqlJson {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Json: {}", self.as_string())
    }
}
