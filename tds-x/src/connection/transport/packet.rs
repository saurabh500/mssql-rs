use super::network_transport::NetworkTransport;

pub struct PacketWriter {}

impl PacketWriter {
    fn write(_transport: &dyn NetworkTransport, _data: &[u8]) {}

    // write primitives, write different endianness, etc.
}

pub struct PacketReader {
    // Same as above, but for reading
    // Packet reader will read from the Network Reader.
    // Data can be spread across multiple packets.
}
