use super::raw_packet::RawPacket;
use async_trait::async_trait;

#[async_trait(?Send)]
pub trait NetworkWriter {
    async fn send_data_to_network_stream(&self, packet: RawPacket);
    async fn send(&self, data: &[u8], start: i32, end: i32);
    fn packet_size(&self) -> u32;
}

#[async_trait(?Send)]
pub trait NetworkReader {
    async fn get_data_from_network_stream(&self, packet: RawPacket) -> i32;
    fn packet_size(&self) -> u32;
}
