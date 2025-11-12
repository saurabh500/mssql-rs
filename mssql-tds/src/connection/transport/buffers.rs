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
}

impl TdsReadBuffer {
    pub(crate) fn new(packet_size: usize) -> Self {
        let packet_storage = packet_size * 2;
        Self {
            buffer_position: 0,
            buffer_length: 0,
            max_packet_size: packet_size,
            working_buffer: vec![0; packet_storage],
        }
    }

    pub(crate) fn change_packet_size(&mut self, packet_size: u32) {
        if packet_size != self.max_packet_size as u32 {
            self.max_packet_size = packet_size as usize;
            self.working_buffer.resize(packet_size as usize * 2, 0);
            self.buffer_position = 0;
            self.buffer_length = 0;
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
