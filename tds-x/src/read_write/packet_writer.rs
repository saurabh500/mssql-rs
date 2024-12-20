use crate::message::messages::PacketType;

use super::writer::NetworkWriter;

pub struct PacketWriter<'a> {
    packet_type: PacketType,
    network_writer: &'a dyn NetworkWriter,
}

impl<'a> PacketWriter<'a> {
    async fn write_byte_async(&self, _value: u8) {}

    async fn write_i16_async(&self, _value: i16) {}

    async fn write_u16_async(&self, _value: u16) {}

    async fn write_i32_async(&self, _value: i32) {}

    async fn write_u32_async(&self, _value: u32) {}

    async fn write_i64_async(&self, _value: i64) {}

    async fn write_u64_async(&self, _value: u64) {}

    async fn write_i16_be_async(&self, _value: i16) {}

    async fn write_i32_be_async(&self, _value: i32) {}

    async fn write_i64_be_async(&self, _value: i64) {}

    async fn write_string_ascii_async(&self, _value: String) {}

    async fn write_string_unicode_async(&self, _value: String) {}

    async fn write_async(&self, _content: &[u8]) {}

    async fn finalize(&self) {}
}
