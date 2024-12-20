use crate::read_write::raw_packet::RawPacket;
use crate::read_write::writer::NetworkReader;

pub struct PacketReader<'a> {
    network_reader: &'a dyn NetworkReader,
}
impl<'a> PacketReader<'a> {
    pub fn is_data_available(&self) -> bool {
        todo!()
    }

    fn do_we_have_enough_data(&self, _byte_count: i32) -> bool {
        todo!()
    }

    async fn read_tds_packet(&self) {
        todo!()
    }

    async fn get_new_tds_packet(&self) -> RawPacket {
        todo!()
    }

    fn consume_bytes(&self, _byte_count: i32) {
        todo!()
    }

    pub async fn skip_forward(&self, _length: i32) {
        todo!()
    }

    pub async fn read_byte(&self) -> u8 {
        todo!()
    }

    pub async fn read_int16(&self) -> i16 {
        todo!()
    }

    pub async fn read_uint16(&self) -> u16 {
        todo!()
    }

    pub async fn read_int32(&self) -> i32 {
        todo!()
    }

    pub async fn read_uint32(&self) -> u32 {
        todo!()
    }

    pub async fn read_int64(&self) -> i64 {
        todo!()
    }

    pub async fn read_uint64(&self) -> u64 {
        todo!()
    }

    pub async fn read_int16_big_endian(&self) -> i16 {
        todo!()
    }

    pub async fn read_int32_big_endian(&self) -> i32 {
        todo!()
    }

    pub async fn read_int64_big_endian(&self) -> i64 {
        todo!()
    }

    pub async fn read_float32(&self) -> f32 {
        todo!()
    }

    pub async fn read_float64(&self) -> f64 {
        todo!()
    }

    pub async fn read_bytes(&self, _buffer: Vec<u8>, _offset: i32, _length: i32) -> i32 {
        todo!()
    }

    pub async fn read_u8_varbyte(&self) -> Vec<u8> {
        todo!()
    }

    pub async fn read_u16_varbyte(&self) -> Vec<u8> {
        todo!()
    }

    pub async fn read_varchar_with_byte_length(&self) -> String {
        todo!()
    }

    pub async fn read_u8_varchar(&self) -> String {
        todo!()
    }

    pub async fn read_unicode(&self, _length: u32) -> String {
        todo!()
    }

    pub async fn read_unicode_with_byte_length(&self, _length: u32) -> String {
        todo!()
    }
}
