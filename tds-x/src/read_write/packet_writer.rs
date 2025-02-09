use crate::message::messages::{PacketStatusFlags, PacketType};
use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
use std::io::{Cursor, Error, Write};
use tracing::event;

use super::reader_writer::NetworkWriter;

/// A packet writer that writes data to a buffer and if needed flushes it to the network as needed.
///
/// TODO: There is a bug right now, where the buffer may overflow. This needs to be fixed.
pub struct PacketWriter<'a> {
    packet_type: PacketType,
    network_writer: &'a mut dyn NetworkWriter,
    max_payload_size: usize,
    packet_id: u8,
    payload_cursor: Cursor<Vec<u8>>,
}

impl<'a> PacketWriter<'a> {
    pub const PACKET_HEADER_SIZE: usize = 8;

    pub fn new(
        packet_type: PacketType,
        network_writer: &'a mut dyn NetworkWriter,
    ) -> PacketWriter<'a> {
        let packet_size: usize = network_writer.packet_size() as usize;
        let buffer: Vec<u8> = Vec::with_capacity(packet_size); // Adjust the capacity as needed
        let mut buffer_cursor = Cursor::new(buffer);

        // Position the cursor at the end of the header. The header will be populated later.
        buffer_cursor.set_position(Self::PACKET_HEADER_SIZE as u64);

        PacketWriter {
            packet_type,
            network_writer,
            max_payload_size: packet_size - (Self::PACKET_HEADER_SIZE),
            packet_id: 1,
            payload_cursor: buffer_cursor,
        }
    }

    pub(crate) fn position(&self) -> i32 {
        (self.payload_cursor.position() - Self::PACKET_HEADER_SIZE as u64) as i32
    }

    /// Writes a byte to the buffer.
    ///
    /// # Arguments
    ///
    /// * `value` - The byte value to write to the buffer.
    ///
    pub(crate) async fn write_byte_async(&mut self, value: u8) -> Result<(), Error> {
        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_i16_async(&mut self, value: i16) -> Result<(), Error> {
        let _ = WriteBytesExt::write_i16::<LittleEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_u16_async(&mut self, value: u16) -> Result<(), Error> {
        let _ = WriteBytesExt::write_u16::<LittleEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_i32_async(&mut self, _value: i32) -> Result<(), Error> {
        let _ =
            byteorder::WriteBytesExt::write_i32::<LittleEndian>(&mut self.payload_cursor, _value);
        Ok(())
    }

    pub(crate) async fn write_u32_async(&mut self, value: u32) -> Result<(), Error> {
        let _ =
            byteorder::WriteBytesExt::write_u32::<LittleEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_i64_async(&mut self, value: i64) -> Result<(), Error> {
        let _ =
            byteorder::WriteBytesExt::write_i64::<LittleEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_u64_async(&mut self, value: u64) -> Result<(), Error> {
        let _ =
            byteorder::WriteBytesExt::write_u64::<LittleEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_i16_be_async(&mut self, value: i16) -> Result<(), Error> {
        let _ = byteorder::WriteBytesExt::write_i16::<BigEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_i32_be_async(&mut self, value: i32) -> Result<(), Error> {
        let _ = byteorder::WriteBytesExt::write_i32::<BigEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_i64_be_async(&mut self, value: i64) -> Result<(), Error> {
        let _ = byteorder::WriteBytesExt::write_i64::<BigEndian>(&mut self.payload_cursor, value);
        Ok(())
    }

    pub(crate) async fn write_string_ascii_async(&mut self, _value: &str) -> Result<(), Error> {
        todo!()
    }

    pub(crate) async fn write_string_unicode_async(&mut self, value: &str) -> Result<(), Error> {
        // TODO: The performance of this might be terrible. There are allocations happening for every string.
        // 1. Consider using the iterator on encode_utf16 directly and writing to the output buffer,
        // fill up the buffer, send out the packet, rinse and repeat.
        let unicode_bytes = value
            .encode_utf16()
            .flat_map(|u| u.to_le_bytes())
            .collect::<Vec<u8>>();
        let _ = self.write_async(&unicode_bytes[0..]).await;
        Ok(())
    }

    pub(crate) fn write_i32_at_index(&mut self, index: usize, value: i32) {
        let position = self.payload_cursor.position();
        self.payload_cursor
            .set_position((Self::PACKET_HEADER_SIZE + index) as u64);
        let _ =
            byteorder::WriteBytesExt::write_i32::<LittleEndian>(&mut self.payload_cursor, value);
        self.payload_cursor.set_position(position);
    }

    pub(crate) async fn write_async(&mut self, content: &[u8]) -> Result<(), Error> {
        let _ = self.payload_cursor.write_all(content);
        Ok(())
    }

    pub(crate) async fn finalize(&mut self) -> Result<(), Error> {
        if (self.payload_cursor.position()) > Self::PACKET_HEADER_SIZE as u64 {
            self.populate_header_and_send(true).await?;
        }
        Ok(())
    }

    async fn populate_header_and_send(&mut self, is_last_packet: bool) -> Result<(), Error> {
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

        let _ = WriteBytesExt::write_u16::<BigEndian>(&mut self.payload_cursor, 0);

        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, self.packet_id);
        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, 0);

        let data_slice = &self.payload_cursor.get_ref().as_slice()[..packet_length];
        self.network_writer.send(data_slice).await?;

        event!(
            tracing::Level::DEBUG,
            "Sending packet of size: {:?}",
            packet_length
        );
        event!(tracing::Level::DEBUG, "Packet content: {:?}", data_slice);
        // Add the counter for the packet and increment by 1 for the next packet.
        self.packet_id = self.packet_id.wrapping_add(1);
        self.payload_cursor
            .set_position(Self::PACKET_HEADER_SIZE as u64);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::executor::block_on;

    // Expose copy of internal buffer in PacketWriter for tests in other modules.
    impl PacketWriter<'_> {
        pub(crate) fn get_payload(&self) -> Cursor<Vec<u8>> {
            self.payload_cursor.clone()
        }
    }

    pub(crate) struct MockNetworkWriter {
        pub(crate) size: u32,
    }

    #[async_trait]
    impl NetworkWriter for MockNetworkWriter {
        fn packet_size(&self) -> u32 {
            self.size
        }

        #[must_use]
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        async fn send(&mut self, _data: &[u8]) -> Result<(), std::io::Error> {
            // No op
            Ok(())
        }

        fn get_packet_writer(&mut self, _: PacketType) -> PacketWriter<'_> {
            unimplemented!();
        }
    }

    #[test]
    fn test_write_byte_async() {
        let mut mock = MockNetworkWriter { size: 8 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);
        block_on(writer.write_byte_async(0xAB)).unwrap();
        assert_eq!(writer.payload_cursor.into_inner()[8..], vec![0xAB]);
    }

    #[test]
    fn test_write_i16_async() {
        let mut mock = MockNetworkWriter { size: 8 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);
        block_on(writer.write_i16_async(0x1234)).unwrap();
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0x1234i16.to_le_bytes()
        );
    }

    #[test]
    fn test_write_u32_async() {
        let mut mock = MockNetworkWriter { size: 8 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);
        block_on(writer.write_u32_async(0xDEADBEEF)).unwrap();
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0xDEADBEEFu32.to_le_bytes()
        );
    }

    #[test]
    fn test_write_i64_async() {
        let mut mock = MockNetworkWriter { size: 16 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);
        block_on(writer.write_i64_async(0x1122334455667788)).unwrap();
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0x1122334455667788i64.to_le_bytes()
        );
    }

    #[test]
    fn test_finalize_with_data() {
        let mut mock = MockNetworkWriter { size: 16 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);
        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.finalize()).unwrap();
        assert_eq!(
            writer.payload_cursor.position(),
            PacketWriter::PACKET_HEADER_SIZE as u64
        );
        assert_eq!(writer.packet_id, 2);
    }

    #[test]
    fn test_finalize_without_data() {
        let mut mock = MockNetworkWriter { size: 16 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);
        block_on(writer.finalize()).unwrap();
        assert_eq!(
            writer.payload_cursor.position(),
            PacketWriter::PACKET_HEADER_SIZE as u64
        );
        assert_eq!(writer.packet_id, 1);
    }

    #[test]
    fn test_write_at_index() {
        let mut mock = MockNetworkWriter { size: 16 };
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock);

        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.write_byte_async(0xAB)).unwrap();
        block_on(writer.write_byte_async(0xAB)).unwrap();
        let value: i32 = 1234;
        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..12],
            [0xAB, 0xAB, 0xAB, 0xAB]
        );
        writer.write_i32_at_index(0, value);
        assert_eq!(
            writer.payload_cursor.into_inner()[8..12],
            value.to_le_bytes()
        );
    }
}
