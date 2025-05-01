use crate::{query::metadata::ColumnMetadata, token::tokens::SqlCollation};
use core::fmt;
use std::{fmt::Debug, fmt::Display};

use super::sqldatatypes::{is_unicode_type, TypeInfoVariant};

#[derive(PartialEq)]
pub enum EncodingType {
    Utf8,
    Utf16,
    LcidBased(SqlCollation),
}

#[derive(PartialEq)]
pub struct SqlString {
    pub bytes: Vec<u8>,
    encoding_type: EncodingType,
}

impl SqlString {
    pub fn new(bytes: Vec<u8>, encoding_type: EncodingType) -> Self {
        SqlString {
            bytes,
            encoding_type,
        }
    }

    pub fn from_utf8_string(string: String) -> Self {
        let utf16_bytes = string
            .encode_utf16()
            .flat_map(|f| f.to_le_bytes())
            .collect::<Vec<u8>>();
        SqlString::new(utf16_bytes, EncodingType::Utf16)
    }

    pub fn to_utf8_string(&self) -> String {
        match self.encoding_type {
            // TODO: Investigation needed. When creating a Utf8 strings from the vector, the string is weirdly encoded.
            // UTF16 decode works better.
            EncodingType::Utf8 => String::from_utf8(self.bytes.clone()).unwrap(),
            EncodingType::Utf16 => {
                let mut u16_buffer = Vec::with_capacity(self.bytes.len() / 2);
                self.bytes
                    .chunks(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .for_each(|item| u16_buffer.push(item));

                String::from_utf16(&u16_buffer).unwrap()
            }
            EncodingType::LcidBased(_) => {
                unimplemented!("LCID based encoding not implemented");
            }
        }
    }
}

impl Debug for SqlString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let EncodingType::LcidBased(_) = self.encoding_type {
            write!(f, "{:?}", self.bytes)
        } else {
            write!(f, "{:?}", self.to_utf8_string())
        }
    }
}

impl Display for SqlString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let EncodingType::LcidBased(_) = self.encoding_type {
            write!(f, "{:?}", self.bytes)
        } else {
            write!(f, "{}", self.to_utf8_string())
        }
    }
}

pub fn get_encoding_type(metadata: &ColumnMetadata) -> EncodingType {
    let collation = match metadata.type_info.type_info_variant {
        TypeInfoVariant::PartialLen(_, _, collation, _, _) => collation,
        TypeInfoVariant::VarLenString(_, _, collation) => collation,
        _ => None,
    };

    if is_unicode_type(metadata.data_type) {
        EncodingType::Utf16
    } else if collation.is_some() && collation.unwrap().utf8() {
        EncodingType::Utf8
    } else {
        EncodingType::LcidBased(collation.unwrap())
    }
}
