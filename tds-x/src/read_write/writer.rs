use super::raw_packet::RawPacket;
use async_trait::async_trait;

#[async_trait(?Send)]
pub trait NetworkWriter {
    async fn send_data_to_network_stream(&self, packet: RawPacket);
    fn packet_size(&self) -> i32;
}

#[async_trait(?Send)]
pub trait NetworkReader {
    async fn get_data_from_network_stream(&self, packet: RawPacket) -> i32;
    fn packet_size(&self) -> i32;
}
