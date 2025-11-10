// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TDS Protocol implementation for the mock server

use bytes::{Buf, BufMut, BytesMut};
use std::io;
use thiserror::Error;
use tracing::{debug, trace};

/// TDS Packet Header size (8 bytes)
pub const PACKET_HEADER_SIZE: usize = 8;

/// Maximum packet size
pub const MAX_PACKET_SIZE: usize = 4096;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Invalid packet type: {0}")]
    InvalidPacketType(u8),

    #[error("Invalid packet size: {0}")]
    InvalidPacketSize(usize),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// TDS Packet Types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    SqlBatch = 0x01,
    PreLogin = 0x12,
    TabularResult = 0x04,
    Login7 = 0x10,
    RpcRequest = 0x03,
}

impl TryFrom<u8> for PacketType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(PacketType::SqlBatch),
            0x12 => Ok(PacketType::PreLogin),
            0x04 => Ok(PacketType::TabularResult),
            0x10 => Ok(PacketType::Login7),
            0x03 => Ok(PacketType::RpcRequest),
            _ => Err(ProtocolError::InvalidPacketType(value)),
        }
    }
}

/// TDS Packet Status
#[derive(Debug, Clone, Copy)]
pub struct PacketStatus(u8);

impl PacketStatus {
    pub fn normal() -> Self {
        Self(0x00)
    }

    pub fn end_of_message() -> Self {
        Self(0x01)
    }

    pub fn is_end_of_message(&self) -> bool {
        self.0 & 0x01 != 0
    }
}

/// TDS Packet Header
#[derive(Debug)]
pub struct PacketHeader {
    pub packet_type: PacketType,
    pub status: PacketStatus,
    pub length: u16,
    pub spid: u16,
    pub packet_id: u8,
    pub window: u8,
}

impl PacketHeader {
    pub fn new(packet_type: PacketType, length: u16, packet_id: u8) -> Self {
        Self {
            packet_type,
            status: PacketStatus::end_of_message(),
            length,
            spid: 0,
            packet_id,
            window: 0,
        }
    }

    pub fn parse(buf: &mut impl Buf) -> Result<Self, ProtocolError> {
        if buf.remaining() < PACKET_HEADER_SIZE {
            return Err(ProtocolError::Protocol(
                "Not enough data for header".to_string(),
            ));
        }

        let packet_type = PacketType::try_from(buf.get_u8())?;
        let status = PacketStatus(buf.get_u8());
        let length = buf.get_u16();
        let spid = buf.get_u16();
        let packet_id = buf.get_u8();
        let window = buf.get_u8();

        debug!(
            ?packet_type,
            ?status,
            length,
            spid,
            packet_id,
            window,
            "Parsed packet header"
        );

        Ok(Self {
            packet_type,
            status,
            length,
            spid,
            packet_id,
            window,
        })
    }

    pub fn write(&self, buf: &mut impl BufMut) {
        buf.put_u8(self.packet_type as u8);
        buf.put_u8(self.status.0);
        buf.put_u16(self.length);
        buf.put_u16(self.spid);
        buf.put_u8(self.packet_id);
        buf.put_u8(self.window);
    }
}

/// TDS Token Types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenType {
    ColMetadata = 0x81,
    Row = 0xD1,
    Done = 0xFD,
    DoneProc = 0xFE,
    DoneInProc = 0xFF,
    EnvChange = 0xE3,
    LoginAck = 0xAD,
    Error = 0xAA,
    Info = 0xAB,
}

/// Build a PreLogin response packet
pub fn build_prelogin_response() -> BytesMut {
    let mut response = BytesMut::new();

    // PreLogin response with ENCRYPTION set to NOT_SUPPORTED
    // Option tokens (PL_OPTION_TOKEN)

    // VERSION token (0x00)
    response.put_u8(0x00); // VERSION
    response.put_u16(0x000F); // Offset to VERSION data (15 bytes after option tokens)
    response.put_u16(0x0006); // Length of VERSION data (6 bytes)

    // ENCRYPTION token (0x01)
    response.put_u8(0x01); // ENCRYPTION
    response.put_u16(0x0015); // Offset to ENCRYPTION data (21 bytes after option tokens)
    response.put_u16(0x0001); // Length of ENCRYPTION data (1 byte)

    // TERMINATOR (0xFF)
    response.put_u8(0xFF);

    // Version data: 16.0.0.0 (6 bytes)
    response.put_u8(0x10); // Major version (16)
    response.put_u8(0x00); // Minor version (0)
    response.put_u16(0x0000); // Build number
    response.put_u16(0x0000); // Sub-build number

    // ENCRYPTION data: NOT_SUPPORTED (1 byte)
    // 0x00 = ENCRYPT_OFF
    // 0x02 = ENCRYPT_NOT_SUP (not supported)
    response.put_u8(0x02); // ENCRYPT_NOT_SUP

    wrap_in_packet(PacketType::TabularResult, response)
}

/// Build a LoginAck token response with EnvChange for collation
pub fn build_login_ack() -> BytesMut {
    let mut token_data = BytesMut::new();

    // LoginAck token (0xAD)
    token_data.put_u8(TokenType::LoginAck as u8);

    // Calculate token length (we'll update this)
    let length_pos = token_data.len();
    token_data.put_u16(0); // Placeholder for length

    // Interface (SQL Server)
    token_data.put_u8(0x01);

    // TDS version (7.4)
    token_data.put_u32(0x74000004);

    // Program name: "MockTdsServer"
    let prog_name = "MockTdsServer";
    token_data.put_u8(prog_name.len() as u8);
    for ch in prog_name.encode_utf16() {
        token_data.put_u16_le(ch);
    }

    // Server version (16.0.0.0)
    token_data.put_u8(16); // Major
    token_data.put_u8(0); // Minor
    token_data.put_u16(0); // Build

    // Update the length field
    let token_length = (token_data.len() - length_pos - 2) as u16;
    let mut length_bytes = &mut token_data[length_pos..length_pos + 2];
    length_bytes.put_u16(token_length);

    // Add EnvChange token for collation (0xE3)
    token_data.put_u8(TokenType::EnvChange as u8);

    // EnvChange token length
    let env_length_pos = token_data.len();
    token_data.put_u16(0); // Placeholder

    // Type: SQL_COLLATION (7)
    token_data.put_u8(7);

    // New value length: 5 bytes for collation
    token_data.put_u8(5);

    // Collation data: SQL_Latin1_General_CP1_CI_AS (LCID: 0x0409, flags: 0x00D001, sortId: 0x00)
    token_data.put_u32_le(0x09040000); // LCID (little endian)
    token_data.put_u8(0xD0); // Flags

    // Old value length: 0 (no old value)
    token_data.put_u8(0);

    // Update EnvChange length
    let env_token_length = (token_data.len() - env_length_pos - 2) as u16;
    let mut env_length_bytes = &mut token_data[env_length_pos..env_length_pos + 2];
    env_length_bytes.put_u16(env_token_length);

    // Add EnvChange token for database name (0xE3)
    token_data.put_u8(TokenType::EnvChange as u8);

    // EnvChange token length
    let db_env_length_pos = token_data.len();
    token_data.put_u16(0); // Placeholder

    // Type: DATABASE (1)
    token_data.put_u8(1);

    // New database name: "master"
    let db_name = "master";
    token_data.put_u8(db_name.len() as u8);
    for ch in db_name.encode_utf16() {
        token_data.put_u16_le(ch);
    }

    // Old value length: 0 (no old value)
    token_data.put_u8(0);

    // Update EnvChange length
    let db_env_token_length = (token_data.len() - db_env_length_pos - 2) as u16;
    let mut db_env_length_bytes = &mut token_data[db_env_length_pos..db_env_length_pos + 2];
    db_env_length_bytes.put_u16(db_env_token_length);

    // Add EnvChange token for packet size (0xE3)
    token_data.put_u8(TokenType::EnvChange as u8);

    // EnvChange token length
    let ps_env_length_pos = token_data.len();
    token_data.put_u16(0); // Placeholder

    // Type: PACKETSIZE (4)
    token_data.put_u8(4);

    // New packet size: "4096" (as string in UTF-16LE)
    let new_packet_size_str = "4096";
    token_data.put_u8(new_packet_size_str.len() as u8);
    for ch in new_packet_size_str.encode_utf16() {
        token_data.put_u16_le(ch);
    }

    // Old packet size: "4096" (as string in UTF-16LE)
    let old_packet_size_str = "4096";
    token_data.put_u8(old_packet_size_str.len() as u8);
    for ch in old_packet_size_str.encode_utf16() {
        token_data.put_u16_le(ch);
    }

    // Update EnvChange length
    let ps_env_token_length = (token_data.len() - ps_env_length_pos - 2) as u16;
    let mut ps_env_length_bytes = &mut token_data[ps_env_length_pos..ps_env_length_pos + 2];
    ps_env_length_bytes.put_u16(ps_env_token_length);

    token_data
}

/// Build a DONE token
pub fn build_done_token(row_count: u64) -> BytesMut {
    let mut token_data = BytesMut::new();

    // DONE token (0xFD)
    token_data.put_u8(TokenType::Done as u8);

    // Status: DONE_FINAL (0x00)
    token_data.put_u16(0x0000);

    // CurCmd: SELECT (0xC1)
    token_data.put_u16(0x00C1);

    // RowCount (8 bytes)
    token_data.put_u64_le(row_count);

    token_data
}

/// Build a query result from a QueryResponse
pub fn build_query_result(response: &crate::query_response::QueryResponse) -> BytesMut {
    let mut result = BytesMut::new();

    // ColMetadata token (0x81)
    result.put_u8(TokenType::ColMetadata as u8);
    result.put_u16_le(response.columns.len() as u16); // Column count

    // Serialize each column
    for col in &response.columns {
        result.put_u32_le(0); // UserType
        result.put_u16_le(0x0000); // Flags: not nullable, no special flags
        result.put_u8(col.data_type.tds_type_code()); // Type code
        result.put_u8(col.data_type.max_length()); // Max length

        // Column name (UTF-16LE)
        let name_len = col.name.chars().count() as u8;
        result.put_u8(name_len);
        for ch in col.name.encode_utf16() {
            result.put_u16_le(ch);
        }
    }

    // Serialize each row
    for row in &response.rows {
        result.put_u8(TokenType::Row as u8);
        for value in &row.values {
            value.write_to_buffer(&mut result);
        }
    }

    // DONE token
    result.extend_from_slice(&build_done_token(response.rows.len() as u64));

    wrap_in_packet(PacketType::TabularResult, result)
}

/// Build an error response
pub fn build_error_response(message: &str) -> BytesMut {
    let mut response = BytesMut::new();

    // Error token (0xAA)
    response.put_u8(TokenType::Error as u8);

    // Calculate token length (we'll update this)
    let length_pos = response.len();
    response.put_u16(0); // Placeholder for length

    // Error number
    response.put_u32_le(50000);

    // State
    response.put_u8(1);

    // Class (severity)
    response.put_u8(16);

    // Message
    response.put_u16_le(message.len() as u16);
    for ch in message.encode_utf16() {
        response.put_u16_le(ch);
    }

    // Server name (empty)
    response.put_u8(0);

    // Procedure name (empty)
    response.put_u8(0);

    // Line number
    response.put_u32_le(1);

    // Update the length field
    let token_length = (response.len() - length_pos - 2) as u16;
    let mut length_bytes = &mut response[length_pos..length_pos + 2];
    length_bytes.put_u16(token_length);

    // Add DONE token
    response.extend_from_slice(&build_done_token(0));

    wrap_in_packet(PacketType::TabularResult, response)
}

/// Wrap token data in a TDS packet
fn wrap_in_packet(packet_type: PacketType, data: BytesMut) -> BytesMut {
    let total_length = (PACKET_HEADER_SIZE + data.len()) as u16;

    let mut packet = BytesMut::with_capacity(total_length as usize);
    let header = PacketHeader::new(packet_type, total_length, 1);
    header.write(&mut packet);
    packet.extend_from_slice(&data);

    trace!(
        "Built packet: type={:?}, length={}",
        packet_type, total_length
    );
    packet
}

/// Parse a SQL batch from packet data
pub fn parse_sql_batch(data: &[u8]) -> Result<String, ProtocolError> {
    if data.is_empty() {
        return Err(ProtocolError::Protocol("Empty SQL batch".to_string()));
    }

    // Skip the ALL_HEADERS section
    let mut offset = 0;

    // Read total length of ALL_HEADERS (DWORD)
    if data.len() < 4 {
        return Err(ProtocolError::Protocol(
            "Invalid SQL batch format".to_string(),
        ));
    }

    let all_headers_len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    offset += all_headers_len;

    if offset > data.len() {
        return Err(ProtocolError::Protocol(
            "Invalid ALL_HEADERS length".to_string(),
        ));
    }

    // The rest is the SQL text in UTF-16LE
    let sql_bytes = &data[offset..];

    // Convert UTF-16LE to String
    let u16_vec: Vec<u16> = sql_bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();

    let sql = String::from_utf16(&u16_vec)
        .map_err(|_| ProtocolError::Protocol("Invalid UTF-16 in SQL batch".to_string()))?;

    debug!("Parsed SQL: {}", sql.trim());
    Ok(sql.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_header_parse() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x01); // SqlBatch
        buf.put_u8(0x01); // EOM
        buf.put_u16(100); // Length
        buf.put_u16(0); // SPID
        buf.put_u8(1); // Packet ID
        buf.put_u8(0); // Window

        let header = PacketHeader::parse(&mut buf).unwrap();
        assert_eq!(header.packet_type, PacketType::SqlBatch);
        assert!(header.status.is_end_of_message());
        assert_eq!(header.length, 100);
    }

    #[test]
    fn test_prelogin_response() {
        let response = build_prelogin_response();
        assert!(response.len() >= PACKET_HEADER_SIZE);
    }

    #[test]
    fn test_select_one_result() {
        use crate::query_response::QueryResponse;
        let response = build_query_result(&QueryResponse::select_one());
        assert!(response.len() >= PACKET_HEADER_SIZE);
    }
}
