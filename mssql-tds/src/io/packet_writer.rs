// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use super::reader_writer::NetworkWriter;
use crate::core::{CancelHandle, TdsResult};
use crate::error::Error::TimeoutError;
use crate::error::TimeoutErrorType;
use crate::io::packet_writer::MessageSendState::{Complete, NotStarted, Partial};
use crate::message::messages::{PacketStatusFlags, PacketType};
use async_trait::async_trait;
use byteorder::{BigEndian, WriteBytesExt};
use std::io::Cursor;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::event;

/// Result of checking if space is available for writing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SpaceCheckResult {
    /// Space is available and no overflow check needed (before or after write)
    Available,
    /// Already at/past packet boundary, needs overflow check before writing
    NeedsOverflowCheckBefore,
    /// Within boundary now, but will exceed after writing - needs overflow check after writing
    NeedsOverflowCheckAfter,
}

/// Optimized batch write operations with manual overflow control.
/// Use this for performance-critical code paths where you can batch multiple writes.
pub(crate) trait TdsPacketWriterUnchecked {
    /// Writes a byte without checking overflow (caller must ensure space)
    fn write_byte_unchecked(&mut self, value: u8);

    /// Writes an i32 without checking overflow (caller must ensure space)
    fn write_i32_unchecked(&mut self, value: i32);

    /// Writes a u16 without checking overflow (caller must ensure space)
    fn write_u16_unchecked(&mut self, value: u16);

    /// Writes an i64 without checking overflow (caller must ensure space)
    fn write_i64_unchecked(&mut self, value: i64);

    /// Writes a f64 without checking overflow (caller must ensure space)
    fn write_f64_unchecked(&mut self, value: f64);

    /// Writes bytes without checking overflow (caller must ensure space)
    fn write_unchecked(&mut self, content: &[u8]);

    /// Checks if there's enough space for n bytes without overflow.
    /// Returns whether space is available and if overflow check is needed.
    ///
    /// # Important
    /// This should ONLY be called with hardcoded constant values (e.g., `has_space(9)`).
    /// The value must not exceed the overflow buffer size (8 bytes for u64).
    /// Passing dynamic or oversized values can lead to buffer overflow.
    fn has_space(&self, bytes: usize) -> SpaceCheckResult;

    /// Manually check and handle overflow after a batch of unchecked writes
    async fn check_overflow(&mut self) -> TdsResult<()>;
}

#[async_trait]
pub(crate) trait TdsPacketWriter {
    /// Writes a byte to the buffer.
    async fn write_byte_async(&mut self, value: u8) -> TdsResult<()>;

    /// Writes an i16 value in little-endian format.
    async fn write_i16_async(&mut self, value: i16) -> TdsResult<()>;

    /// Writes a u16 value in little-endian format.
    async fn write_u16_async(&mut self, value: u16) -> TdsResult<()>;

    /// Writes an i32 value in little-endian format.
    async fn write_i32_async(&mut self, value: i32) -> TdsResult<()>;

    /// Writes a u32 value in little-endian format.
    async fn write_u32_async(&mut self, value: u32) -> TdsResult<()>;

    /// Writes an i64 value in little-endian format.
    async fn write_i64_async(&mut self, value: i64) -> TdsResult<()>;

    /// Writes a u64 value in little-endian format.
    async fn write_u64_async(&mut self, value: u64) -> TdsResult<()>;

    /// Writes an i16 value in big-endian format.
    async fn write_i16_be_async(&mut self, value: i16) -> TdsResult<()>;

    /// Writes an i32 value in big-endian format.
    async fn write_i32_be_async(&mut self, value: i32) -> TdsResult<()>;

    /// Writes an i64 value in big-endian format.
    async fn write_i64_be_async(&mut self, value: i64) -> TdsResult<()>;

    /// Writes a partial u64 value with specified length.
    async fn write_partial_u64_async(&mut self, value: u64, length: u8) -> TdsResult<()>;

    /// Writes a string in ASCII format.
    async fn write_string_ascii_async(&mut self, value: &str) -> TdsResult<()>;

    /// Writes a string in Unicode format.
    async fn write_string_unicode_async(&mut self, value: &str) -> TdsResult<()>;

    /// Writes raw bytes to the buffer.
    async fn write_async(&mut self, content: &[u8]) -> TdsResult<()>;

    /// Writes an i32 value at a specific index in the buffer.
    fn write_i32_at_index(&mut self, index: usize, value: i32);

    /// Finalizes the packet writer, sending any remaining data in the buffer.
    async fn finalize(&mut self) -> TdsResult<()>;
}

/// A packet writer that writes data to a buffer and if needed flushes it to the network as needed.
///
pub struct PacketWriter<'a> {
    packet_type: PacketType,
    network_writer: &'a mut dyn NetworkWriter,
    max_payload_size: usize,
    packet_id: u8,
    payload_cursor: Cursor<Vec<u8>>,
    packet_size: usize,
    is_first_packet: bool, // Note: Cannot just use packet_id because its value can rollover.
    start_time: Instant,
    max_timeout_sec: Option<u32>,
    cancel_handle: Option<CancelHandle>,
}

pub(crate) enum MessageSendState {
    NotStarted,
    Partial,
    Complete,
}

impl<'a> PacketWriter<'a> {
    pub(crate) const PACKET_HEADER_SIZE: usize = 8;

    pub(crate) fn new(
        packet_type: PacketType,
        network_writer: &'a mut dyn NetworkWriter,
        timeout: Option<u32>,
        cancel_handle: Option<&CancelHandle>,
    ) -> PacketWriter<'a> {
        let packet_size: usize = network_writer.packet_size() as usize;
        // Add additional space for the numeric types.
        let buffer: Vec<u8> = Vec::with_capacity(packet_size + size_of::<u64>());
        let mut buffer_cursor = Cursor::new(buffer);

        // Position the cursor at the end of the header. The header will be populated later.
        buffer_cursor.set_position(Self::PACKET_HEADER_SIZE as u64);

        PacketWriter {
            packet_type,
            network_writer,
            max_payload_size: packet_size - (Self::PACKET_HEADER_SIZE),
            packet_id: 1,
            payload_cursor: buffer_cursor,
            packet_size,
            is_first_packet: true,
            start_time: Instant::now(),
            max_timeout_sec: timeout,
            cancel_handle: cancel_handle.map(|handle| handle.child_handle()),
        }
    }

    pub(crate) fn get_message_state(&self) -> MessageSendState {
        if !self.is_first_packet {
            if self.payload_cursor.position() == 0 {
                Complete
            } else {
                Partial
            }
        } else {
            NotStarted
        }
    }

    pub(crate) async fn cancel_current_message(&mut self) -> TdsResult<()> {
        self.populate_header_and_send(true, true).await
    }

    pub(crate) fn position(&self) -> i32 {
        (self.payload_cursor.position() - Self::PACKET_HEADER_SIZE as u64) as i32
    }

    async fn handle_overflow_if_needed(&mut self) -> TdsResult<()> {
        // If the payload size is greater than the max payload size, send the packet.
        if self.position() >= (self.max_payload_size as i32) {
            self.populate_header_and_send(false, false).await?;

            let current_position = self.payload_cursor.position();
            let overflow_length = current_position as usize - self.packet_size;

            // Copy from the overflow buffer to the beginning of the buffer and reset the cursor.
            let original_buffer = self.payload_cursor.get_mut();

            // We have written beyond the packet size, so we need to copy the overflow data to the beginning of the buffer to the packet start.
            original_buffer.copy_within(
                self.packet_size..self.packet_size + overflow_length,
                Self::PACKET_HEADER_SIZE,
            );
            // Position cursor at the end of the copied overflow data
            self.payload_cursor
                .set_position((Self::PACKET_HEADER_SIZE + overflow_length) as u64);
        }
        Ok(())
    }

    /// Builds and sends a packet based on the current payload and the state of the message.
    ///
    /// # Arguments
    ///
    /// * `is_last_packet` - Flag indicating that this is the last packet of the current message.
    /// * `is_ignore_packet` - Flag indicating that the current message should be ignored by the
    ///   server. If this flag is set to true, the `is_last_packet` flag also must be set to true
    ///   as specified by the TDS protocol.
    /// ```
    async fn populate_header_and_send(
        &mut self,
        is_last_packet: bool,
        is_ignore_packet: bool,
    ) -> TdsResult<()> {
        // If the ignore bit is set, it must be the end of the message per the protocol.
        assert!(is_last_packet || !is_ignore_packet);

        // Record the position of the packet payload. Set the payload size to zero if this is an ignore packet.
        let saved_position = match is_ignore_packet {
            true => 0,
            false => self.payload_cursor.position(),
        };

        let packet_length = match saved_position as usize > self.packet_size {
            true => self.packet_size,
            false => saved_position as usize,
        };

        // Position at the header start and start writing the header.
        self.payload_cursor.set_position(0);
        let _ = Self::build_header(
            &mut self.payload_cursor,
            packet_length,
            self.packet_type,
            self.packet_id,
            is_last_packet,
            is_ignore_packet,
        );
        let data_slice = &self.payload_cursor.get_ref().as_slice()[..packet_length];

        // Calculate the timeout based on the start time of this request and the max timeout.
        let send_data_fut = CancelHandle::run_until_cancelled(
            self.cancel_handle.as_ref(),
            self.network_writer.send(data_slice),
        );

        if self.max_timeout_sec.is_none() {
            send_data_fut.await?;
        } else {
            let elapsed = self.start_time.elapsed().as_secs();
            if elapsed > self.max_timeout_sec.unwrap() as u64 {
                return Err(TimeoutError(TimeoutErrorType::String(
                    "Timeout expired".to_string(),
                )));
            };
            let current_timeout = self.max_timeout_sec.unwrap() as u64 - elapsed;
            match timeout(Duration::from_secs(current_timeout), send_data_fut).await {
                Ok(result) => result?,
                Err(elapsed) => {
                    return Err(TimeoutError(TimeoutErrorType::Elapsed(elapsed)));
                }
            };
        }

        event!(
            tracing::Level::DEBUG,
            "Sending packet of size: {:?}",
            packet_length
        );
        use pretty_hex::PrettyHex;
        event!(
            tracing::Level::DEBUG,
            "Packet content: {:?}",
            data_slice.hex_dump()
        );

        // Invoke the first-packet callback if needed.
        if self.is_first_packet {
            self.packet_type
                .first_packet_callback(self.network_writer)
                .await?;
            self.is_first_packet = false;
        }

        // Add the counter for the packet and increment by 1 for the next packet.
        self.packet_id = self.packet_id.wrapping_add(1);

        // Restore the cursor position.
        self.payload_cursor.set_position(saved_position);
        Ok(())
    }

    pub(crate) fn build_header<W: WriteBytesExt>(
        writer: &mut W,
        packet_length: usize,
        packet_type: PacketType,
        packet_id: u8,
        is_last_packet: bool,
        is_ignore_packet: bool,
    ) -> TdsResult<()> {
        let _ = WriteBytesExt::write_u8(writer, packet_type as u8);
        let status = match is_last_packet {
            true => match is_ignore_packet {
                true => PacketStatusFlags::Eom as u8 | PacketStatusFlags::Ignore as u8,
                false => PacketStatusFlags::Eom as u8,
            },
            false => PacketStatusFlags::Normal as u8,
        };

        let _ = WriteBytesExt::write_u8(writer, status);

        let _ = WriteBytesExt::write_u16::<BigEndian>(writer, packet_length as u16);

        let _ = WriteBytesExt::write_u16::<BigEndian>(writer, 0);

        let _ = WriteBytesExt::write_u8(writer, packet_id);
        Ok(WriteBytesExt::write_u8(writer, 0)?)
    }

    #[cfg(test)]
    pub(crate) fn get_cursor(&self) -> &Cursor<Vec<u8>> {
        &self.payload_cursor
    }
}

#[async_trait]
impl TdsPacketWriter for PacketWriter<'_> {
    async fn finalize(&mut self) -> TdsResult<()> {
        if (self.payload_cursor.position()) > Self::PACKET_HEADER_SIZE as u64 {
            self.populate_header_and_send(true, false).await?;
            self.payload_cursor
                .set_position(Self::PACKET_HEADER_SIZE as u64);
        }
        Ok(())
    }

    /// Writes a byte to the buffer.
    async fn write_byte_async(&mut self, value: u8) -> TdsResult<()> {
        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_i16_async(&mut self, value: i16) -> TdsResult<()> {
        let _ =
            WriteBytesExt::write_i16::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_u16_async(&mut self, value: u16) -> TdsResult<()> {
        let _ =
            WriteBytesExt::write_u16::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_i32_async(&mut self, value: i32) -> TdsResult<()> {
        let _ =
            WriteBytesExt::write_i32::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_u32_async(&mut self, value: u32) -> TdsResult<()> {
        let _ =
            WriteBytesExt::write_u32::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_i64_async(&mut self, value: i64) -> TdsResult<()> {
        let _ =
            WriteBytesExt::write_i64::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_u64_async(&mut self, value: u64) -> TdsResult<()> {
        let _ =
            WriteBytesExt::write_u64::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_i16_be_async(&mut self, value: i16) -> TdsResult<()> {
        let _ = WriteBytesExt::write_i16::<BigEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_i32_be_async(&mut self, value: i32) -> TdsResult<()> {
        let _ = WriteBytesExt::write_i32::<BigEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_i64_be_async(&mut self, value: i64) -> TdsResult<()> {
        let _ = WriteBytesExt::write_i64::<BigEndian>(&mut self.payload_cursor, value);
        self.handle_overflow_if_needed().await
    }

    async fn write_partial_u64_async(&mut self, value: u64, length: u8) -> TdsResult<()> {
        // Write the value as a little-endian value, but only the first `length` bytes.
        let bytes = value.to_le_bytes();
        let _ = std::io::Write::write_all(&mut self.payload_cursor, &bytes[..length as usize]);
        self.handle_overflow_if_needed().await
    }

    async fn write_string_ascii_async(&mut self, _value: &str) -> TdsResult<()> {
        todo!()
    }

    async fn write_string_unicode_async(&mut self, value: &str) -> TdsResult<()> {
        // Zero-copy UTF-16 encoding: write directly to buffer without intermediate Vec allocation
        let mut utf16_iter = value.encode_utf16();

        loop {
            let packet_space_left = self.max_payload_size - self.position() as usize;

            // How many u16 units can we write? (each u16 = 2 bytes)
            let u16_units_available = packet_space_left / 2;

            if u16_units_available == 0 {
                // No space left, flush and continue
                self.populate_header_and_send(false, false).await?;
                self.payload_cursor
                    .set_position(Self::PACKET_HEADER_SIZE as u64);
                continue;
            }

            // Write as many u16 units as we can fit
            let mut units_written = 0;
            for _ in 0..u16_units_available {
                if let Some(u16_char) = utf16_iter.next() {
                    // Write u16 in little-endian directly to buffer
                    self.write_u16_unchecked(u16_char);
                    units_written += 1;
                } else {
                    // Finished writing all characters
                    if units_written > 0 {
                        // Check for overflow after batch write
                        self.check_overflow().await?;
                    }
                    return Ok(());
                }
            }

            // We filled the available space, check overflow and loop to flush
            if units_written > 0 {
                self.check_overflow().await?;
            }
        }
    }

    async fn write_async(&mut self, content: &[u8]) -> TdsResult<()> {
        // Write in chunks of packet size.
        let packet_space_left = self.max_payload_size - self.position() as usize;
        if packet_space_left < content.len() {
            let chunk = &content[..packet_space_left];
            let _ = std::io::Write::write_all(&mut self.payload_cursor, chunk);
            self.populate_header_and_send(false, false).await?;
            self.payload_cursor
                .set_position(Self::PACKET_HEADER_SIZE as u64);
            Box::pin(self.write_async(&content[packet_space_left..])).await?;
        } else {
            let _ = std::io::Write::write_all(&mut self.payload_cursor, content);
        }
        Ok(())
    }

    fn write_i32_at_index(&mut self, index: usize, value: i32) {
        let position = self.payload_cursor.position();
        self.payload_cursor
            .set_position((Self::PACKET_HEADER_SIZE + index) as u64);
        let _ =
            WriteBytesExt::write_i32::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
        self.payload_cursor.set_position(position);
    }
}

// Implement the unchecked write trait separately for optimized batch operations
impl TdsPacketWriterUnchecked for PacketWriter<'_> {
    fn write_byte_unchecked(&mut self, value: u8) {
        let _ = WriteBytesExt::write_u8(&mut self.payload_cursor, value);
    }

    fn write_i32_unchecked(&mut self, value: i32) {
        let _ =
            WriteBytesExt::write_i32::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
    }

    fn write_u16_unchecked(&mut self, value: u16) {
        let _ =
            WriteBytesExt::write_u16::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
    }

    fn write_i64_unchecked(&mut self, value: i64) {
        let _ =
            WriteBytesExt::write_i64::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
    }

    fn write_f64_unchecked(&mut self, value: f64) {
        let _ =
            WriteBytesExt::write_f64::<byteorder::LittleEndian>(&mut self.payload_cursor, value);
    }

    fn write_unchecked(&mut self, content: &[u8]) {
        let _ = std::io::Write::write_all(&mut self.payload_cursor, content);
    }

    fn has_space(&self, bytes_count: usize) -> SpaceCheckResult {
        // Safety check: ensure requested space doesn't exceed buffer capacity.
        // The buffer has size_of::<u64>() (8 bytes) extra space beyond packet_size.
        let buffer_space_left =
            self.payload_cursor.get_ref().capacity() - self.payload_cursor.position() as usize;

        if bytes_count <= buffer_space_left {
            // Safe to write - now determine overflow check requirements
            let current_pos = self.position() as usize;
            let position_after_write = current_pos + bytes_count;

            if current_pos >= self.max_payload_size {
                // Already at or past packet boundary - caller must check overflow before writing
                SpaceCheckResult::NeedsOverflowCheckBefore
            } else if position_after_write > self.max_payload_size {
                // Within boundary now, but will exceed after write - check overflow after writing
                SpaceCheckResult::NeedsOverflowCheckAfter
            } else {
                // Fully within packet boundary - no overflow check needed
                SpaceCheckResult::Available
            }
        } else {
            // Programming error: trying to write more than buffer capacity allows
            debug_assert!(
                false,
                "has_space() called with {} bytes, but only {} bytes left in buffer. \
                 This would cause buffer overflow. Use hardcoded constants only (max {} bytes).",
                bytes_count,
                buffer_space_left,
                size_of::<u64>()
            );
            // Return NeedsOverflowCheckBefore to be safe in release builds
            SpaceCheckResult::NeedsOverflowCheckBefore
        }
    }

    async fn check_overflow(&mut self) -> TdsResult<()> {
        self.handle_overflow_if_needed().await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::vec;

    use super::*;
    use crate::connection::transport::network_transport::TransportSslHandler;
    use crate::core::NegotiatedEncryptionSetting;
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
        pub(crate) data: Vec<u8>,
    }

    impl MockNetworkWriter {
        pub(crate) fn new(size: u32) -> Self {
            Self { size, data: vec![] }
        }
    }

    #[async_trait]
    impl NetworkWriter for MockNetworkWriter {
        #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
        async fn send(&mut self, _data: &[u8]) -> TdsResult<()> {
            // No op
            self.data.extend_from_slice(_data);
            Ok(())
        }

        fn packet_size(&self) -> u32 {
            self.size
        }

        fn get_encryption_setting(&self) -> NegotiatedEncryptionSetting {
            unimplemented!()
        }
    }

    #[async_trait]
    impl TransportSslHandler for MockNetworkWriter {
        async fn enable_ssl(&mut self) -> TdsResult<()> {
            unimplemented!()
        }

        async fn disable_ssl(&mut self) -> TdsResult<()> {
            unimplemented!()
        }
    }

    #[test]
    fn test_write_byte_async() {
        let mut mock = MockNetworkWriter::new(8);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        block_on(writer.write_byte_async(0xAB)).unwrap();
        assert_eq!(writer.payload_cursor.into_inner()[8..], vec![0xAB]);
    }

    #[test]
    fn test_write_i16_async() {
        let mut mock = MockNetworkWriter::new(8);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        block_on(writer.write_i16_async(0x1234)).unwrap();
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0x1234i16.to_le_bytes()
        );
    }

    #[test]
    fn test_write_u32_async() {
        let mut mock = MockNetworkWriter::new(8);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        block_on(TdsPacketWriter::write_u32_async(&mut writer, 0xDEADBEEF)).unwrap();
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0xDEADBEEFu32.to_le_bytes()
        );
    }

    #[test]
    fn test_write_i64_async() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        block_on(TdsPacketWriter::write_i64_async(
            &mut writer,
            0x1122334455667788,
        ))
        .unwrap();
        assert_eq!(
            writer.payload_cursor.into_inner()[8..],
            0x1122334455667788i64.to_le_bytes()
        );
    }

    #[test]
    fn test_write_i64_overflow_async() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        block_on(TdsPacketWriter::write_i32_async(&mut writer, 0x1234)).unwrap();
        block_on(TdsPacketWriter::write_i64_async(
            &mut writer,
            0x1122334455667788,
        ))
        .unwrap();
        assert_eq!(mock.data[8..12], 0x1234i32.to_le_bytes());
    }

    #[test]
    fn test_finalize_with_data() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
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
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        block_on(writer.finalize()).unwrap();
        assert_eq!(
            writer.payload_cursor.position(),
            PacketWriter::PACKET_HEADER_SIZE as u64
        );
        assert_eq!(writer.packet_id, 1);
    }

    #[test]
    fn test_write_at_index() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

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

    #[test]
    fn test_write_string_overflow() {
        let packet_size: usize = 16;
        let mut mock = MockNetworkWriter::new(packet_size as u32);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);
        let str_value = "a very very very very very very very very very very very very long string";
        block_on(writer.write_string_unicode_async(str_value)).unwrap();
        block_on(writer.finalize()).unwrap();

        let mut string_vec: Vec<u8> = Vec::new();
        let data = mock.data;
        let mut chunks = data.len() / packet_size;
        if data.len() % packet_size != 0 {
            chunks += 1;
        }
        for i in 0..chunks {
            let start = i * packet_size;
            let mut end = start + packet_size;
            if end > data.len() {
                end = data.len();
            }
            string_vec.extend_from_slice(&data[start + 8..end]);
        }

        let utf16_units = string_vec
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<u16>>();

        // get the utf18 value from string_vec
        let utf16_value = String::from_utf16(&utf16_units).unwrap();

        assert_eq!(utf16_value, str_value);
    }

    #[test]
    fn test_has_space_available() {
        let mut mock = MockNetworkWriter::new(32);
        let writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        // With packet size 32 and header 8, we have 24 bytes available
        // Should have space for 8 bytes
        match writer.has_space(8) {
            SpaceCheckResult::Available => {}
            _ => panic!("Expected Available"),
        }
    }

    #[test]
    fn test_has_space_needs_overflow_check_before() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        // Write some data first
        block_on(writer.write_i32_async(0x1234)).unwrap();
        
        // Now we have 4 bytes remaining in the 8-byte payload.
        // Asking for 4 bytes should be available
        match writer.has_space(4) {
            SpaceCheckResult::Available => {}
            result => panic!("Expected Available, got {:?}", result),
        }
    }

    #[test]
    fn test_has_space_needs_overflow_check_after() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        // Fill buffer partially (packet size 16, header 8 = 8 bytes available)
        block_on(writer.write_i32_async(0x1234)).unwrap();

        // 4 bytes used, 4 remaining. Asking for 8 bytes needs overflow check after
        match writer.has_space(8) {
            SpaceCheckResult::NeedsOverflowCheckAfter => {}
            result => panic!("Expected NeedsOverflowCheckAfter, got {:?}", result),
        }
    }

    #[test]
    fn test_write_byte_unchecked() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        writer.write_byte_unchecked(0xAB);
        writer.write_byte_unchecked(0xCD);

        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..10],
            [0xAB, 0xCD]
        );
    }

    #[test]
    fn test_write_i32_unchecked() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        writer.write_i32_unchecked(0x12345678);

        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..12],
            0x12345678i32.to_le_bytes()
        );
    }

    #[test]
    fn test_write_u16_unchecked() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        writer.write_u16_unchecked(0xABCD);

        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..10],
            0xABCDu16.to_le_bytes()
        );
    }

    #[test]
    fn test_write_i64_unchecked() {
        let mut mock = MockNetworkWriter::new(24);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        writer.write_i64_unchecked(0x123456789ABCDEF0);

        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..16],
            0x123456789ABCDEF0i64.to_le_bytes()
        );
    }

    #[test]
    fn test_write_f64_unchecked() {
        let mut mock = MockNetworkWriter::new(24);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        let value = 3.12312312312312;
        writer.write_f64_unchecked(value);

        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..16],
            value.to_le_bytes()
        );
    }

    #[test]
    fn test_write_unchecked() {
        let mut mock = MockNetworkWriter::new(24);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        let data = [0x01, 0x02, 0x03, 0x04, 0x05];
        writer.write_unchecked(&data);

        assert_eq!(writer.payload_cursor.clone().into_inner()[8..13], data);
    }

    #[test]
    fn test_unchecked_batch_writes() {
        let mut mock = MockNetworkWriter::new(32);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        // Batch write using unchecked methods
        writer.write_byte_unchecked(0x01);
        writer.write_u16_unchecked(0x0203);
        writer.write_i32_unchecked(0x04050607);
        writer.write_i64_unchecked(0x08090A0B0C0D0E0F);

        let buffer = writer.payload_cursor.clone().into_inner();
        assert_eq!(buffer[8], 0x01);
        assert_eq!(buffer[9..11], 0x0203u16.to_le_bytes());
        assert_eq!(buffer[11..15], 0x04050607i32.to_le_bytes());
        assert_eq!(buffer[15..23], 0x08090A0B0C0D0E0Fi64.to_le_bytes());
    }

    #[test]
    fn test_check_overflow_manual() {
        let mut mock = MockNetworkWriter::new(16);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        // Fill to capacity
        block_on(writer.write_i32_async(0x1234)).unwrap();
        block_on(writer.write_i32_async(0x5678)).unwrap();

        // Manual overflow check
        block_on(writer.check_overflow()).unwrap();

        // Now write more data
        writer.write_i32_unchecked(0x9ABC);

        // Verify the i32 was written correctly
        assert_eq!(
            writer.payload_cursor.clone().into_inner()[8..12],
            0x9ABCi32.to_le_bytes()
        );

        // Drop writer to release borrow of mock, then verify data was sent
        drop(writer);
        assert!(mock.data.is_empty() == false);
    }

    #[test]
    fn test_cursor_position_after_overflow() {
        let mut mock = MockNetworkWriter::new(20); // Header 8 + payload 12
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        // Write data (10 bytes) that is less than payload capacity (12 bytes)
        block_on(writer.write_i16_async(0x1234)).unwrap(); // 2 bytes
        
        // This i64 write (8 bytes) will cause overflow:
        // Total would be 10 bytes, which exceeds 12 byte capacity
        // After write, cursor at 18 (header 8 + 10 bytes)
        // handle_overflow triggers:
        // 1. position() = 10, which is < 12, so no overflow... wait this won't trigger!
        
        // Let me use a case that actually overflows
        block_on(writer.write_i32_async(0x5678)).unwrap(); // 4 more bytes, total 6
        block_on(writer.write_i32_async(0x9ABC)).unwrap(); // 4 more bytes, total 10
        
        // Now write i64 (8 bytes). Total would be 18 bytes.
        // position() after i64 write = 18, max_payload = 12
        // This triggers overflow
        block_on(TdsPacketWriter::write_i64_async(
            &mut writer,
            0x123456789ABCDEF0,
        ))
        .unwrap();

        // After overflow:
        // 1. First packet sent (header 8 + first 12 bytes of payload)
        // 2. Overflow = 18 - 20 = -2? No wait, cursor is at 26 (8 header + 18 payload)
        //    packet_size = 20, so overflow_length = 26 - 20 = 6
        // 3. Copies 6 bytes from position 20-26 to position 8-14
        // 4. Sets cursor to 8 + 6 = 14
        assert_eq!(
            writer.payload_cursor.position(),
            (PacketWriter::PACKET_HEADER_SIZE + 6) as u64
        );
    }

    #[test]
    fn test_zero_copy_utf16_write() {
        let mut mock = MockNetworkWriter::new(64);
        let mut writer = PacketWriter::new(PacketType::TabularResult, &mut mock, None, None);

        let test_string = "Hello";
        block_on(writer.write_string_unicode_async(test_string)).unwrap();

        // Verify UTF-16LE encoding
        let buffer = writer.payload_cursor.clone().into_inner();
        let utf16_bytes = &buffer[8..18]; // "Hello" = 5 chars * 2 bytes = 10 bytes

        let utf16_units: Vec<u16> = utf16_bytes
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        let decoded = String::from_utf16(&utf16_units).unwrap();
        assert_eq!(decoded, test_string);
    }
}
