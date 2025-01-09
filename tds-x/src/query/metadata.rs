use crate::token::tokens::SqlCollation;

use super::sqldatatypes::SqlDataType;
use std::fmt;

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub user_type: u32,
    pub flags: u16,
    pub data_type: SqlDataType,
    pub length: i32,
    pub precision: u8,
    pub scale: u8,
    pub column_name: String,
    pub is_nullable: bool,
    pub is_case_sensitive: bool,
    pub is_identity: bool,
    pub is_computed: bool,
    pub is_sparse_column_set: bool,
    pub is_encrypted: bool,
    pub collation: Option<SqlCollation>, // Option handles cases where this might not be set
}

impl fmt::Display for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,  "Column Name: {}\nData Type: {:?} (UserType: {})\nLength: {}, Precision: {}, Scale: {}\n\
        Collation: {:?}\nFlags: [Nullable: {}, CaseSensitive: {}, Identity: {}, Computed: {}, \
        SparseColumnSet: {}, Encrypted: {}]\n",
        self.column_name,
        self.data_type,
        self.user_type,
        self.length,
        self.precision,
        self.scale,
        self.collation,
        self.is_nullable,
        self.is_case_sensitive,
        self.is_identity,
        self.is_computed,
        self.is_sparse_column_set,
        self.is_encrypted)
    }
}
