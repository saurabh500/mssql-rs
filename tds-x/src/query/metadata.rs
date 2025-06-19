use crate::datatypes::sqldatatypes::{TdsDataType, TypeInfo, TypeInfoVariant};

use std::fmt;

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub user_type: u32,
    pub flags: u16,
    pub type_info: TypeInfo,
    pub data_type: TdsDataType,
    pub column_name: String,
    pub multi_part_name: Option<MultiPartName>,
}

impl ColumnMetadata {
    pub fn is_nullable(&self) -> bool {
        (self.flags & 0x01) != 0x00
    }
    pub fn is_case_sensitive(&self) -> bool {
        (self.flags & 0x02) != 0x00
    }
    pub fn is_identity(&self) -> bool {
        (self.flags & 0x10) != 0x00
    }
    pub fn is_computed(&self) -> bool {
        (self.flags & 0x20) != 0x00
    }
    pub fn is_sparse_column_set(&self) -> bool {
        (self.flags & 0x1000) != 0x00
    }
    pub fn is_encrypted(&self) -> bool {
        (self.flags & 0x2000) != 0x00
    }
    pub fn is_plp(&self) -> bool {
        matches!(
            self.type_info.type_info_variant,
            TypeInfoVariant::PartialLen(_, _, _, _, _)
        )
    }
    pub fn get_scale(&self) -> u8 {
        match self.type_info.type_info_variant {
            TypeInfoVariant::VarLenScale(_, scale) => scale,
            TypeInfoVariant::VarLenPrecisionScale(_, _, _, scale) => scale,
            _ => unreachable!("get_scale called on a type that does not have scale"),
        }
    }
}

impl fmt::Display for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,  "Column Name: {}\nData Type: {:?} (UserType: {})\nFlags: [Nullable: {}, CaseSensitive: {}, Identity: {}, Computed: {}, \
        SparseColumnSet: {}, Encrypted: {}, MultiPartName: {:?}]\n",
        self.column_name,
        self.data_type,
        self.user_type,
        self.is_nullable(),
        self.is_case_sensitive(),
        self.is_identity(),
        self.is_computed(),
        self.is_sparse_column_set(),
        self.is_encrypted(),
        self.multi_part_name)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MultiPartName {
    pub(crate) server_name: Option<String>,
    pub(crate) catalog_name: Option<String>,
    pub(crate) schema_name: Option<String>,
    pub(crate) table_name: String,
}

#[derive(Debug)]
pub(crate) struct ColumnEncryptionMetadata {
    pub key_count: u8,
    pub key_details: Vec<ColumnEncryptionKeyDetails>,
    pub db_id: u32,
    pub key_id: u32,
}

#[derive(Debug)]
pub(crate) struct ColumnEncryptionKeyDetails {
    pub encrypted_cek: Vec<u8>,
    pub algo: String,
    pub key_path: String,
    pub key_store_name: String,
}
