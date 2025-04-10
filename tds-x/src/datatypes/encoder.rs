use std::collections::HashSet;

use async_trait::async_trait;

use crate::{
    core::TdsResult, read_write::packet_writer::PacketWriter, token::tokens::SqlCollation,
};

use super::{decoder::ColumnValues, sqldatatypes::TdsDataType};

#[async_trait]
pub(crate) trait Encoder {
    async fn encode(
        &self,
        packet_writer: &mut PacketWriter<'_>,
        tds_type: TdsDataType,
        value: &ColumnValues,
        collation: &SqlCollation,
    ) -> TdsResult<()>;

    fn get_supported_datatypes(&self) -> &HashSet<TdsDataType>;

    fn get_string_name(&self, tds_type: &TdsDataType) -> &str;
}
