use super::headers::{write_headers, TdsHeaders, TransactionDescriptorHeader};
use super::messages::{PacketType, Request};
use crate::connection::tds_connection::ExecutionContext;
use crate::core::TdsResult;
use crate::read_write::packet_writer::PacketWriter;
use async_trait::async_trait;

pub(crate) struct SqlBatch {
    pub sql_command: String,
    pub headers: Vec<TdsHeaders>,
}

impl Default for SqlBatch {
    fn default() -> Self {
        let transaction_descriptor_header =
            TransactionDescriptorHeader::create_non_transaction_header();
        Self {
            sql_command: String::new(),
            headers: Vec::from([transaction_descriptor_header.into()]),
        }
    }
}

impl SqlBatch {
    pub fn new(sql_command: String, execution_context: &ExecutionContext) -> Self {
        let transaction_descriptor_header = match execution_context.transaction_descriptor {
            0 => TransactionDescriptorHeader::create_non_transaction_header(),
            transaction_descriptor => TransactionDescriptorHeader::new(
                transaction_descriptor,
                execution_context.outstanding_requests,
            ),
        };
        Self {
            sql_command,
            headers: Vec::from([transaction_descriptor_header.into()]),
        }
    }
}

#[async_trait]
impl Request for SqlBatch {
    fn packet_type(&self) -> PacketType {
        PacketType::SqlBatch
    }

    async fn serialize<'a, 'b>(&'a self, packet_writer: &'a mut PacketWriter<'b>) -> TdsResult<()>
    where
        'b: 'a,
    {
        write_headers(&self.headers, packet_writer).await?;
        packet_writer
            .write_string_unicode_async(&self.sql_command)
            .await?;
        packet_writer.finalize().await?;
        Ok(())
    }
}
