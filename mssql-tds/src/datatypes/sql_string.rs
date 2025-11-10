// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::{query::metadata::ColumnMetadata, token::tokens::SqlCollation};
use core::fmt;
use std::{fmt::Debug, fmt::Display};

use super::sqldatatypes::{TypeInfoVariant, is_unicode_type};

#[derive(PartialEq, Clone)]
pub enum EncodingType {
    Utf8,
    Utf16,
    LcidBased(SqlCollation),
    // This is to be used when we want to have an empty encoding, which
    // is later written over the protocol by getting the collation from the connection.
    DelayedSet,
}

#[derive(PartialEq, Clone)]
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
                unimplemented!("LCID based encoding conversion to UTF8 not implemented");
            }
            EncodingType::DelayedSet => {
                // DelayedSet encoding is not defined, so we return the bytes as a UTF-8 string.
                unimplemented!("DelayedSet encoding conversion to UTF8 not implemented");
            }
        }
    }
}

impl Debug for SqlString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.encoding_type {
            EncodingType::LcidBased(_) => write!(f, "{:?}", self.bytes),
            EncodingType::DelayedSet => write!(f, "DelayedSet encoded: {:?}", self.bytes.len()),
            _ => write!(f, "{:?}", self.to_utf8_string()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_string_new() {
        let bytes = vec![72, 0, 101, 0, 108, 0, 108, 0, 111, 0];
        let sql_str = SqlString::new(bytes.clone(), EncodingType::Utf16);
        assert_eq!(sql_str.bytes, bytes);
    }

    #[test]
    fn test_from_utf8_string() {
        let input = "Hello World".to_string();
        let sql_str = SqlString::from_utf8_string(input.clone());
        assert_eq!(sql_str.to_utf8_string(), input);
    }

    #[test]
    fn test_to_utf8_string_utf16() {
        let bytes = vec![72, 0, 105, 0];
        let sql_str = SqlString::new(bytes, EncodingType::Utf16);
        assert_eq!(sql_str.to_utf8_string(), "Hi");
    }

    #[test]
    fn test_to_utf8_string_utf8() {
        let bytes = "Test".as_bytes().to_vec();
        let sql_str = SqlString::new(bytes, EncodingType::Utf8);
        assert_eq!(sql_str.to_utf8_string(), "Test");
    }

    #[test]
    fn test_sql_string_clone() {
        let sql_str = SqlString::from_utf8_string("Clone test".to_string());
        let cloned = sql_str.clone();
        assert_eq!(sql_str.bytes, cloned.bytes);
    }

    #[test]
    fn test_sql_string_debug_utf16() {
        let sql_str = SqlString::from_utf8_string("Debug".to_string());
        let debug_str = format!("{:?}", sql_str);
        assert!(debug_str.contains("Debug"));
    }

    #[test]
    fn test_sql_string_debug_delayed_set() {
        let sql_str = SqlString::new(vec![1, 2, 3, 4, 5], EncodingType::DelayedSet);
        let debug_str = format!("{:?}", sql_str);
        assert!(debug_str.contains("DelayedSet"));
        assert!(debug_str.contains("5"));
    }

    #[test]
    fn test_sql_string_display_utf16() {
        let sql_str = SqlString::from_utf8_string("Display".to_string());
        let display_str = format!("{}", sql_str);
        assert_eq!(display_str, "Display");
    }

    #[test]
    fn test_sql_string_equality() {
        let sql_str1 = SqlString::from_utf8_string("Equal".to_string());
        let sql_str2 = SqlString::from_utf8_string("Equal".to_string());
        let sql_str3 = SqlString::from_utf8_string("Different".to_string());
        assert_eq!(sql_str1, sql_str2);
        assert_ne!(sql_str1, sql_str3);
    }

    #[test]
    fn test_from_utf8_string_empty() {
        let sql_str = SqlString::from_utf8_string(String::new());
        assert_eq!(sql_str.to_utf8_string(), "");
        assert!(sql_str.bytes.is_empty());
    }

    #[test]
    fn test_from_utf8_string_special_chars() {
        let input = "Hello! @#$%^&*()".to_string();
        let sql_str = SqlString::from_utf8_string(input.clone());
        assert_eq!(sql_str.to_utf8_string(), input);
    }

    #[test]
    fn test_from_utf8_string_unicode() {
        let input = "Hello World".to_string();
        let sql_str = SqlString::from_utf8_string(input.clone());
        assert_eq!(sql_str.to_utf8_string(), input);
    }

    #[test]
    fn test_sql_string_new_utf8() {
        let bytes = "UTF8 String".as_bytes().to_vec();
        let sql_str = SqlString::new(bytes.clone(), EncodingType::Utf8);
        assert_eq!(sql_str.bytes, bytes);
        assert_eq!(sql_str.to_utf8_string(), "UTF8 String");
    }

    #[test]
    fn test_sql_string_new_delayed_set() {
        let bytes = vec![1, 2, 3, 4];
        let sql_str = SqlString::new(bytes.clone(), EncodingType::DelayedSet);
        assert_eq!(sql_str.bytes, bytes);
    }
}
