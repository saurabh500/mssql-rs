use super::headers::{write_headers, TdsHeaders, TransactionDescriptorHeader};
use super::messages::{PacketType, Request};
use crate::core::TdsResult;
use crate::read_write::{packet_writer::PacketWriter, reader_writer::NetworkWriter};
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
    pub fn new(sql_command: String) -> Self {
        let transaction_descriptor_header =
            TransactionDescriptorHeader::create_non_transaction_header();
        Self {
            sql_command,
            headers: Vec::from([transaction_descriptor_header.into()]),
        }
    }
}

#[async_trait]
impl<'a> Request<'a> for SqlBatch {
    fn packet_type(&self) -> PacketType {
        PacketType::SqlBatch
    }

    fn create_packet_writer(&self, writer: &'a mut dyn NetworkWriter) -> PacketWriter<'a> {
        self.packet_type().create_packet_writer(writer)
    }

    async fn serialize(&self, writer: &mut dyn NetworkWriter) -> TdsResult<()> {
        let mut packet_writer = self.create_packet_writer(writer);
        write_headers(&self.headers, &mut packet_writer).await?;
        packet_writer
            .write_string_unicode_async(&self.sql_command)
            .await?;
        packet_writer.finalize().await?;
        Ok(())
    }
}
