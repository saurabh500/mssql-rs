use crate::message::messages::{PacketStatusFlags, PacketType};
use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
use std::io::{Cursor, Write};

use super::writer::NetworkWriter;

/// A packet writer that writes data to a buffer and if needed flushes it to the network as needed.
///
/// TODO: There is a bug right now, where the buffer may overflow. This needs to be fixed.
pub struct PacketWriter<'a> {
    packet_type: PacketType,
    network_writer: &'a dyn NetworkWriter,
    max_payload_size: usize,
    packet_id: u8,
    payload_cursor: Cursor<Vec<u8>>,
}

impl<'a> PacketWriter<'a> {
    const PACKET_HEADER_SIZE: u16 = 8;

    pub fn new(packet_type: PacketType, network_writer: &'a dyn NetworkWriter) -> PacketWriter<'a> {
        let packet_size: usize = network_writer.packet_size() as usize;
        let buffer: Vec<u8> = Vec::with_capacity(packet_size); // Adjust the capacity as needed
        let mut buffer_cursor = Cursor::new(buffer);

        // Position the cursor at the end of the header. The header will be populated later.
        buffer_cursor.set_position(Self::PACKET_HEADER_SIZE as u64);

        PacketWriter {
            packet_type,
            network_writer,
            max_payload_size: packet_size - (Self::PACKET_HEADER_SIZE as usize),
            packet_id: 1,
            payload_cursor: buffer_cursor,
        }
    }

    /// Writes a byte to the buffer.
    ///
    /// # Arguments
    ///
    /// * `value` - The byte value to write to the buffer.
    ///
    async fn write_byte_async(&mut self, value: u8) {
        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, value);
    }

    async fn write_i16_async(&mut self, value: i16) {
        let _ = WriteBytesExt::write_i16::<LittleEndian>(&mut self.payload_cursor, value);
    }

    async fn write_u16_async(&mut self, value: u16) {
        let _ = WriteBytesExt::write_u16::<LittleEndian>(&mut self.payload_cursor, value);
    }

    async fn write_i32_async(&mut self, _value: i32) {
        let _ =
            byteorder::WriteBytesExt::write_i32::<LittleEndian>(&mut self.payload_cursor, _value);
    }

    async fn write_u32_async(&mut self, value: u32) {
        let _ =
            byteorder::WriteBytesExt::write_u32::<LittleEndian>(&mut self.payload_cursor, value);
    }

    async fn write_i64_async(&mut self, value: i64) {
        let _ =
            byteorder::WriteBytesExt::write_i64::<LittleEndian>(&mut self.payload_cursor, value);
    }

    async fn write_u64_async(&mut self, value: u64) {
        let _ =
            byteorder::WriteBytesExt::write_u64::<LittleEndian>(&mut self.payload_cursor, value);
    }

    async fn write_i16_be_async(&mut self, value: i16) {
        let _ = byteorder::WriteBytesExt::write_i16::<BigEndian>(&mut self.payload_cursor, value);
    }

    async fn write_i32_be_async(&mut self, value: i32) {
        let _ = byteorder::WriteBytesExt::write_i32::<BigEndian>(&mut self.payload_cursor, value);
    }

    async fn write_i64_be_async(&mut self, value: i64) {
        let _ = byteorder::WriteBytesExt::write_i64::<BigEndian>(&mut self.payload_cursor, value);
    }

    async fn write_string_ascii_async(&mut self, _value: &str) {
        todo!()
    }

    async fn write_string_unicode_async(&mut self, _value: &str) {
        todo!()
    }

    async fn write_async(&mut self, content: &[u8]) {
        let _ = self.payload_cursor.write_all(content);
    }

    async fn finalize(&mut self) {
        if (self.payload_cursor.position()) > Self::PACKET_HEADER_SIZE as u64 {
            self.populate_header_and_send(true).await;
        }
    }

    async fn populate_header_and_send(&mut self, is_last_packet: bool) {
        let packet_length: usize = self.payload_cursor.position() as usize;

        // Position at the header start and start writing the header.
        self.payload_cursor.set_position(0);

        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, self.packet_type as u8);
        let mut status: PacketStatusFlags = PacketStatusFlags::Normal;
        if is_last_packet {
            status = PacketStatusFlags::Eom;
        }

        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, status as u8);

        let _ =
            WriteBytesExt::write_u16::<BigEndian>(&mut self.payload_cursor, packet_length as u16);

        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, 0);

        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, self.packet_id);
        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, 0);

        todo!("Need to flush to the network.");
        // self.network_writer.send_data_to_network_stream(RawPacket {
        //     buffer: self.payload_cursor.into_inner(),
        //     length: self.payload_cursor.position() as i32,
        //     offset: 0,
        // });

        // self.packet_id = self.packet_id.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_write::raw_packet::RawPacket;
    use futures::executor::block_on;

    struct MockNetworkWriter {
        size: u32,
    }

    impl NetworkWriter for MockNetworkWriter {
        fn packet_size(&self) -> u32 {
            self.size
        }

        #[must_use]
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        fn send_data_to_network_stream<'life0, 'async_trait>(
            &'life0 self,
            _packet: RawPacket,
        ) -> ::core::pin::Pin<Box<dyn ::core::future::Future<Output = ()> + 'async_trait>>
        where
            'life0: 'async_trait,
            Self: 'async_trait,
        {
            todo!()
        }
    }

    #[test]
    fn test_write_byte_async() {
        let mock = MockNetworkWriter { size: 8 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mock);
        block_on(writer.write_byte_async(0xAB));
        assert_eq!(writer.payload_cursor.into_inner()[8..], vec![0xAB]);
    }

    #[test]
    fn test_write_i16_async() {
        let mock = MockNetworkWriter { size: 8 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mock);
        block_on(writer.write_i16_async(0x1234));
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0x1234i16.to_le_bytes()
        );
    }

    #[test]
    fn test_write_u32_async() {
        let mock = MockNetworkWriter { size: 8 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mock);
        block_on(writer.write_u32_async(0xDEADBEEF));
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0xDEADBEEFu32.to_le_bytes()
        );
    }

    #[test]
    fn test_write_i64_async() {
        let mock = MockNetworkWriter { size: 16 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mock);
        block_on(writer.write_i64_async(0x1122334455667788));
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0x1122334455667788i64.to_le_bytes()
        );
    }
}
