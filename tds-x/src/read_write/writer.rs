use crate::connection::transport::network_transport::NetworkTransport;
use async_trait::async_trait;
use std::io::Error;

#[async_trait(?Send)]
pub trait NetworkWriter {
    async fn send(&mut self, data: &[u8]) -> Result<(), Error>;
    fn packet_size(&self) -> u32;
}

#[async_trait(?Send)]
pub trait NetworkReader {
    fn packet_size(&self) -> u32;
}

#[async_trait(?Send)]
pub trait NetworkReaderWriter: NetworkReader + NetworkWriter {}

pub struct NetworkReaderWriterImpl<'a, 'n> {
    pub(crate) transport: &'a mut NetworkTransport<'n>, // Enforce that this struct has a shorter lifetime than the transport.
    pub(crate) packet_size: u32,
}

#[async_trait(?Send)]
impl NetworkReader for NetworkReaderWriterImpl<'_, '_> {
    fn packet_size(&self) -> u32 {
        todo!()
    }
}

#[async_trait(?Send)]
impl NetworkWriter for NetworkReaderWriterImpl<'_, '_> {
    async fn send(&mut self, data: &[u8]) -> Result<(), Error> {
        self.transport.send(data).await
    }

    fn packet_size(&self) -> u32 {
        self.packet_size
    }
}

#[async_trait(?Send)]
impl NetworkReaderWriter for NetworkReaderWriterImpl<'_, '_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::client_context::ClientContext;
    use crate::connection::transport::network_transport::tests::create_readable_network_transport;
    use crate::connection::transport::network_transport::tests::MAX_BUFFER_SIZE;
    use futures::StreamExt;
    use rand::Rng;
    use tokio_util::codec::{BytesCodec, FramedRead};

    #[tokio::test]
    async fn test_send_data() {
        let context = ClientContext::default();
        let (mut transport, server_side) = create_readable_network_transport(&context);

        let mut network_writer = NetworkReaderWriterImpl {
            transport: &mut transport,
            packet_size: 0,
        };

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
}
