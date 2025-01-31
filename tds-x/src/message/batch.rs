use crate::read_write::{packet_writer::PacketWriter, reader_writer::NetworkWriter};
use async_trait::async_trait;
use std::{
    io::Error,
    sync::atomic::{AtomicU32, Ordering},
};

use super::messages::{PacketType, Request};

pub(crate) enum TdsHeaders {
    TransactionDescriptor(TransactionDescriptorHeader),
    TraceActivity(TraceActivityHeader),
    QueryNotifications(QueryNotificationsHeader),
}

impl From<TransactionDescriptorHeader> for TdsHeaders {
    fn from(header: TransactionDescriptorHeader) -> Self {
        TdsHeaders::TransactionDescriptor(header)
    }
}

// Trait representing the abstract TdsHeader
#[async_trait(?Send)]
pub(crate) trait TdsHeader {
    fn header_type(&self) -> u16;
    fn calculate_length(&self) -> i32;
    async fn write_async(&self, writer: &mut PacketWriter) -> Result<(), std::io::Error>;
}

// Static counter for non-transaction request count
static NON_TRANSACTION_REQUEST_COUNT: AtomicU32 = AtomicU32::new(0);

// Struct for TransactionDescriptorHeader
pub(crate) struct TransactionDescriptorHeader {
    transaction_descriptor: u64,
    outstanding_request_count: u32,
}

impl TransactionDescriptorHeader {
    pub fn new(transaction_descriptor: u64, outstanding_request_count: u32) -> Self {
        Self {
            transaction_descriptor,
            outstanding_request_count,
        }
    }

    pub fn create_non_transaction_header() -> Self {
        let count = NON_TRANSACTION_REQUEST_COUNT.fetch_add(1, Ordering::SeqCst);
        Self::new(0, count + 1)
    }
}

#[async_trait(?Send)]
impl TdsHeader for TransactionDescriptorHeader {
    fn header_type(&self) -> u16 {
        0x0002
    }

    fn calculate_length(&self) -> i32 {
        18 // 4 (HeaderLength) + 2 (HeaderType) + 8 (TransactionDescriptor) + 4 (OutstandingRequestCount)
    }

    async fn write_async(&self, writer: &mut PacketWriter) -> Result<(), std::io::Error> {
        let header_length = self.calculate_length();
        writer.write_i32_async(header_length).await?; // HeaderLength
        writer.write_u16_async(self.header_type()).await?; // HeaderType
        writer.write_u64_async(self.transaction_descriptor).await?; // TransactionDescriptor
        writer
            .write_u32_async(self.outstanding_request_count)
            .await?; // OutstandingRequestCount
        Ok(())
    }
}

// QueryNotificationsHeader struct
pub(crate) struct QueryNotificationsHeader {
    notification_data: Vec<u8>,
}

impl QueryNotificationsHeader {
    pub fn new(notification_data: Vec<u8>) -> Self {
        Self { notification_data }
    }
}

#[async_trait(?Send)]
impl TdsHeader for QueryNotificationsHeader {
    fn header_type(&self) -> u16 {
        0x0001 // HeaderType for QueryNotificationsHeader
    }

    fn calculate_length(&self) -> i32 {
        // Total length = HeaderLength (4 bytes) + HeaderType (2 bytes) + NotificationData length
        (6 + self.notification_data.len()) as i32
    }

    async fn write_async(&self, _writer: &mut PacketWriter) -> Result<(), std::io::Error> {
        let _length = self.calculate_length();
        unimplemented!("QueryNotificationsHeader::write_async");
        //     writer.write_int32_async(header_length).await; // Write HeaderLength
        //     writer.write_uint16_async(self.header_type()).await; // Write HeaderType
        //     writer.write_bytes_async(&self.notification_data).await; // Write NotificationData
    }
}

pub(crate) struct TraceActivityHeader {
    pub id: uuid::Uuid,
    pub sequence_number: i32,
}

impl TraceActivityHeader {
    pub fn new(id: uuid::Uuid) -> Self {
        // Interlocked.Increment(ref sequenceNumber);
        static SEQUENCE_NUMBER: AtomicU32 = AtomicU32::new(0);
        let sequence_number = SEQUENCE_NUMBER.fetch_add(1, Ordering::SeqCst) as i32;
        Self {
            id,
            sequence_number,
        }
    }
}

#[async_trait(?Send)]
impl TdsHeader for TraceActivityHeader {
    fn header_type(&self) -> u16 {
        0x0003
    }

    fn calculate_length(&self) -> i32 {
        // Total length of header = HeaderLength (4 bytes) + HeaderType (2 bytes) + ActivityId (16 bytes) + Sequence Number (4)
        6 + 16 + 4
    }

    async fn write_async(&self, writer: &mut PacketWriter) -> Result<(), std::io::Error> {
        let header_len = self.calculate_length();
        writer.write_i32_async(header_len).await?;
        writer.write_u16_async(self.header_type()).await?;
        writer.write_async(self.id.as_bytes()).await?;
        writer.write_i32_async(self.sequence_number).await?;
        Ok(())
    }
}

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

impl SqlBatch {
    async fn write_headers(&self, packet_writer: &mut PacketWriter<'_>) -> Result<(), Error> {
        let _ = packet_writer;

        // Start with the length field size.
        let mut header_len = 4;
        for header in &self.headers {
            match header {
                TdsHeaders::TransactionDescriptor(header) => {
                    header_len += header.calculate_length();
                }
                TdsHeaders::TraceActivity(header) => {
                    header_len += header.calculate_length();
                }
                TdsHeaders::QueryNotifications(header) => {
                    header_len += header.calculate_length();
                }
            }
        }

        packet_writer.write_i32_async(header_len).await?;
        for header in &self.headers {
            match header {
                TdsHeaders::TransactionDescriptor(header) => {
                    header.write_async(packet_writer).await?;
                }
                TdsHeaders::TraceActivity(header) => {
                    header.write_async(packet_writer).await?;
                }
                TdsHeaders::QueryNotifications(header) => {
                    header.write_async(packet_writer).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl<'a> Request<'a> for SqlBatch {
    fn packet_type(&self) -> PacketType {
        PacketType::SqlBatch
    }

    fn create_packet_writer(&self, writer: &'a mut dyn NetworkWriter) -> PacketWriter<'a> {
        PacketWriter::new(self.packet_type(), writer)
    }

    async fn serialize(&self, writer: &mut dyn NetworkWriter) -> Result<(), Error> {
        let mut packet_writer = self.create_packet_writer(writer);
        self.write_headers(&mut packet_writer).await?;
        packet_writer
            .write_string_unicode_async(&self.sql_command)
            .await?;
        packet_writer.finalize().await?;
        Ok(())
    }
}
