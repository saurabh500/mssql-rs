// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

use crate::connection::transport::network_transport::PRE_NEGOTIATED_PACKET_SIZE;
use crate::core::CancelHandle;
use crate::io::packet_writer::PacketWriter;
use crate::message::messages::PacketType;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::time::Instant;

pub(crate) struct TdsReadBuffer {
    pub(crate) buffer_position: usize,
    pub(crate) buffer_length: usize,
    pub(crate) max_packet_size: usize,
    pub(crate) working_buffer: Vec<u8>,
    /// Bytes that have been read from the network but are beyond the current packet.
    /// This happens when a single read returns data for multiple TDS packets.
    pub(crate) pending_bytes: usize,
    /// The offset where pending bytes are located in working_buffer.
    pub(crate) pending_bytes_offset: usize,
}

impl TdsReadBuffer {
    pub(crate) fn new(packet_size: usize) -> Self {
        let packet_storage = packet_size * 2;
        Self {
            buffer_position: 0,
            buffer_length: 0,
            max_packet_size: packet_size,
            working_buffer: vec![0; packet_storage],
            pending_bytes: 0,
            pending_bytes_offset: 0,
        }
    }

    pub(crate) fn change_packet_size(&mut self, packet_size: u32) {
        if packet_size != self.max_packet_size as u32 {
            self.max_packet_size = packet_size as usize;
            self.working_buffer.resize(packet_size as usize * 2, 0);
            self.buffer_position = 0;
            self.buffer_length = 0;
            self.pending_bytes = 0;
            self.pending_bytes_offset = 0;
        }
    }

    pub(crate) fn do_we_have_enough_data(&self, byte_count: usize) -> bool {
        let remaining_bytes = self.buffer_length - self.buffer_position;
        remaining_bytes >= byte_count
    }

    pub(crate) fn get_remaining_byte_count(&self) -> usize {
        self.buffer_length - self.buffer_position
    }

    pub(crate) fn consume_bytes(&mut self, byte_count: usize) {
        if byte_count > (self.buffer_length - self.buffer_position) {
            panic!("Not enough data to consume");
        }

        self.buffer_position += byte_count;
        if self.buffer_length == self.buffer_position {
            self.buffer_length = 0;
            self.buffer_position = 0;
        }
    }

    /// Sets the position to 0 and the length to the specified length.
    pub(crate) fn reset_to_length(&mut self, length: usize) {
        self.buffer_position = 0;
        self.buffer_length = length;
    }

    pub(crate) fn shift_data_to_front(&mut self) {
        let remaining = self.get_remaining_byte_count();

        // Move pending bytes right after where remaining data will be
        if self.pending_bytes > 0 {
            let pending_src_start = self.pending_bytes_offset;
            let pending_src_end = self.pending_bytes_offset + self.pending_bytes;
            let pending_dest = remaining;
            self.working_buffer
                .copy_within(pending_src_start..pending_src_end, pending_dest);
            self.pending_bytes_offset = remaining;
        }

        // Now move the remaining data to front
        self.working_buffer
            .copy_within(self.buffer_position..self.buffer_length, 0);
        self.buffer_position = 0;
        self.buffer_length = remaining;
    }

    pub(crate) fn remove_header_from_packet(&mut self, new_packet_size: usize) {
        self.working_buffer.copy_within(
            self.buffer_length + 8..self.buffer_length + new_packet_size,
            self.buffer_length,
        );
        self.buffer_length += new_packet_size - 8;
    }

    pub(crate) fn get_slice(&self) -> &[u8] {
        &self.working_buffer[self.buffer_position..]
    }
}

impl Debug for TdsReadBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TdsReadBuffer")
            .field("buffer_position", &self.buffer_position)
            .field("buffer_length", &self.buffer_length)
            .field("max_packet_size", &self.max_packet_size)
            .finish()
    }
}

impl Index<usize> for TdsReadBuffer {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.working_buffer[index]
    }
}

impl IndexMut<usize> for TdsReadBuffer {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.working_buffer[index]
    }
}

impl Deref for TdsReadBuffer {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        &self.working_buffer
    }
}

impl DerefMut for TdsReadBuffer {
    fn deref_mut(&mut self) -> &mut Vec<u8> {
        &mut self.working_buffer
    }
}

struct TdsWriteBuffer {
    packet_type: PacketType,
    max_payload_size: usize,
    packet_id: u8,
    payload_cursor: Cursor<Vec<u8>>,
    packet_size: usize,
    is_first_packet: bool, // Note: Cannot just use packet_id because its value can rollover.
    start_time: Instant,
    max_timeout_sec: Option<u32>,
    cancel_handle: Option<CancelHandle>,
}

impl TdsWriteBuffer {
    fn new(packet_size: usize) -> Self {
        // Add additional space for the numeric types.
        let buffer: Vec<u8> = Vec::with_capacity(packet_size + size_of::<u64>());
        let mut buffer_cursor = Cursor::new(buffer);

        // Position the cursor at the end of the header. The header will be populated later.
        buffer_cursor.set_position(PacketWriter::PACKET_HEADER_SIZE as u64);

        Self {
            packet_type: PacketType::PreLogin,
            max_payload_size: 0,
            packet_id: 1,
            payload_cursor: buffer_cursor,
            packet_size: PRE_NEGOTIATED_PACKET_SIZE as usize,
            is_first_packet: true,
            start_time: Instant::now(),
            max_timeout_sec: None,
            cancel_handle: None,
        }
    }

    pub(crate) fn change_packet_size(&mut self, packet_size: u32) {
        self.packet_size = packet_size as usize;
        self.payload_cursor
            .get_mut()
            .resize(packet_size as usize, 0);
        self.payload_cursor
            .set_position(PacketWriter::PACKET_HEADER_SIZE as u64);
        self.is_first_packet = true;
        self.start_time = Instant::now();
    }

    pub(crate) fn set_packet_type(&mut self, packet_type: PacketType) {
        self.packet_type = packet_type;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that demonstrates the buffer overflow bug when reset_reader() doesn't call
    /// change_packet_size() after packet size negotiation.
    ///
    /// Scenario:
    /// 1. Pre-login: packet_size = 4096, buffer = 8192 bytes (4096 × 2)
    /// 2. Login completes: server negotiates packet_size = 8000
    /// 3. BUG: If reset_reader() only calls reset_to_length(0) without change_packet_size(),
    ///    the buffer remains at 8192 bytes
    /// 4. Server sends 8000-byte packet → code tries to read beyond buffer → panic
    ///
    /// FIX: reset_reader() must call change_packet_size() to resize buffer to 16000 bytes
    #[test]
    fn test_buffer_resize_after_packet_size_change() {
        // Initial state: pre-login packet size of 4096
        let initial_packet_size: usize = 4096;
        let mut buffer = TdsReadBuffer::new(initial_packet_size);

        // Verify initial buffer size: 4096 * 2 = 8192
        assert_eq!(buffer.working_buffer.len(), 8192);
        assert_eq!(buffer.max_packet_size, 4096);

        // Simulate packet size negotiation to 8000 (like after login)
        let negotiated_packet_size: u32 = 8000;

        // BUG SIMULATION: Only reset_to_length without change_packet_size
        // This is what the buggy TdsTransport::reset_reader() was doing
        buffer.reset_to_length(0);

        // Buffer is still 8192 - NOT enough for 8000 * 2 = 16000
        assert_eq!(buffer.working_buffer.len(), 8192);
        assert_eq!(buffer.max_packet_size, 4096); // Still old value!

        // This would cause a panic when trying to read a packet larger than 8192/2 = 4096
        // because read_tds_packet reads into base_offset + max_packet_size slice

        // FIX: Call change_packet_size BEFORE reset_to_length
        buffer.change_packet_size(negotiated_packet_size);

        // Now buffer is properly sized: 8000 * 2 = 16000
        assert_eq!(buffer.working_buffer.len(), 16000);
        assert_eq!(buffer.max_packet_size, 8000);

        // Safe to read 8000-byte packets now
        assert!(buffer.working_buffer.len() >= negotiated_packet_size as usize * 2);
    }

    /// Test that change_packet_size is idempotent when called with the same size
    #[test]
    fn test_change_packet_size_same_size_is_noop() {
        let packet_size: usize = 4096;
        let mut buffer = TdsReadBuffer::new(packet_size);

        // Set some state
        buffer.buffer_position = 100;
        buffer.buffer_length = 500;

        // Call with same size - should be no-op (preserves state)
        buffer.change_packet_size(packet_size as u32);

        // State should be preserved since size didn't change
        assert_eq!(buffer.buffer_position, 100);
        assert_eq!(buffer.buffer_length, 500);
        assert_eq!(buffer.working_buffer.len(), 8192);
    }

    /// Test that change_packet_size resets buffer state when size changes
    #[test]
    fn test_change_packet_size_resets_state_on_size_change() {
        let initial_size: usize = 4096;
        let mut buffer = TdsReadBuffer::new(initial_size);

        // Set some state
        buffer.buffer_position = 100;
        buffer.buffer_length = 500;

        // Change to different size - should reset state
        buffer.change_packet_size(8000);

        // State should be reset
        assert_eq!(buffer.buffer_position, 0);
        assert_eq!(buffer.buffer_length, 0);
        assert_eq!(buffer.working_buffer.len(), 16000);
        assert_eq!(buffer.max_packet_size, 8000);
    }
}
