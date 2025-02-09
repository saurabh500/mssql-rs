use crate::core::EncryptionSetting;
use crate::handler::handler_factory::SessionSettings;
use crate::message::messages::PacketType;
use async_trait::async_trait;
use std::io::Error;

use super::packet_reader::PacketReader;
use super::packet_writer::PacketWriter;

#[async_trait]
pub trait NetworkWriter: Send {
    async fn send(&mut self, data: &[u8]) -> Result<(), Error>;
    fn packet_size(&self) -> u32;
    fn get_packet_writer(&mut self, packet_type: PacketType) -> PacketWriter<'_>;
}

#[async_trait]
pub trait NetworkReader: Send {
    async fn receive(&mut self, buffer: &mut [u8]) -> Result<usize, Error>;
    fn packet_size(&self) -> u32;
    fn get_packet_reader(&mut self) -> PacketReader<'_>;
}

#[async_trait]
pub(crate) trait NetworkReaderWriter: NetworkReader + NetworkWriter {
    fn notify_encryption_setting_change(&mut self, setting: EncryptionSetting);
    fn notify_session_setting_change(&mut self, settings: &SessionSettings);
}

#[cfg(test)]
mod tests {
    use crate::connection::client_context::ClientContext;
    use crate::connection::transport::network_transport::tests::MAX_BUFFER_SIZE;
    use crate::connection::transport::network_transport::tests::{
        create_client_server_network_transport, create_readable_network_transport,
    };
    use futures::StreamExt;
    use rand::Rng;
    use tokio_util::codec::{BytesCodec, FramedRead};

    #[tokio::test]
    async fn test_send_data() {
        let context = ClientContext::default();
        let (transport, server_side) = create_readable_network_transport(&context);

        let mut network_writer = transport;

        // Fill data_to_send with random values
        let mut rng = rand::thread_rng();
        let data_vector: Vec<u8> = (0..MAX_BUFFER_SIZE).map(|_| rng.gen()).collect();

        // Setup the reader to read the data.
        let mut framed_reader = FramedRead::new(server_side, BytesCodec::new());

        // Send the data and read it from the other end of the pipe.
        let result = network_writer.send(&data_vector[..]).await;
        match result {
            Ok(_) => {}
            Err(e) => panic!("Error sending data: {}", e),
        }

        let received = framed_reader
            .next()
            .await
            .expect("No data")
            .expect("Decode error");

        assert_eq!(received.as_ref(), &data_vector[..]);
    }

    #[tokio::test]
    async fn test_send_recv() {
        let context = ClientContext::default();
        let (mut client_transport, mut server_transport) =
            create_client_server_network_transport(&context);

        // Fill data_to_send with random values
        let mut rng = rand::thread_rng();
        let data_vector: Vec<u8> = (0..MAX_BUFFER_SIZE).map(|_| rng.gen()).collect();

        // Send the data.
        let result = server_transport.send(&data_vector[..]).await;
        match result {
            Ok(_) => {}
            Err(e) => panic!("Error sending data: {}", e),
        }

        let mut buffer = vec![0u8; MAX_BUFFER_SIZE];
        let bytes_read = client_transport.receive(&mut buffer).await.unwrap();

        // Verify we read exactly `data_size` bytes and that they match what was written
        assert_eq!(bytes_read, MAX_BUFFER_SIZE);
        assert_eq!(buffer, data_vector);
    }
}
