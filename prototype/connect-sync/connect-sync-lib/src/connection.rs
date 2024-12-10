//! `Connection` provides a capability to connect to a SQL Server.
//! For now, SQL authentication is supported.
use crate::login::LoginMessage;
use crate::packet::{Packet, PacketHeader, PacketType, HEADER_BYTES};
use crate::prelogin::PreloginMessage;
use crate::token::decode_token;
use crate::transport::{create_tls_stream, TdsTransport, TransportStream};
use crate::Result;
use bytes::{Buf, BufMut, BytesMut};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use tracing::{event, Level};

pub(crate) trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> Result<()>;
}

pub(crate) trait Decode<B: Buf> {
    fn decode(src: &mut B) -> Result<Self>
    where
        Self: Sized;
}

pub struct Connection {
    transport: TransportStream,
}

impl Connection {
    fn new(transport: TransportStream) -> Self {
        Connection { transport }
    }

    pub fn connect(host: &str, user: &str, password: &str) -> crate::Result<Connection> {
        let stream: TcpStream = TcpStream::connect(host)?;
        let transport = TransportStream::new_tcp_stream(stream);

        let mut connection = Connection::new(transport);
        event!(Level::INFO, "Sending Prelogin message.");
        let prelogin = connection.prelogin()?;
        event!(Level::INFO, "Prelogin: {:?}", prelogin);
        let bytes = prelogin.version.to_be_bytes();
        event!(
            Level::INFO,
            "Server version: {}.{}.{}",
            bytes[0],
            bytes[1],
            ((bytes[2] as u16) << 8) + (bytes[3] as u16)
        );
        let connection = connection.tls_handshake(host)?;
        event!(Level::INFO, "TLS handshake complete!");
        event!(Level::INFO, "Sending login message.");
        let mut connection = connection.login(user, password)?;

        let packet = connection.collect_packet()?;
        let (_, mut payload) = packet.into_parts();
        decode_token(&mut payload)?;
        Ok(connection)
    }

    fn prelogin(&mut self) -> Result<PreloginMessage> {
        let msg = PreloginMessage::new();
        let pre_login = PacketHeader::new(PacketType::PreLogin, 0);
        self.send(pre_login, msg)?;

        let packet = self.collect_packet()?;
        let (_, mut payload) = packet.into_parts();
        let response = PreloginMessage::decode(&mut payload)?;
        Ok(response)
    }

    fn tls_handshake(self, host: &str) -> Result<Self> {
        let Self { transport, .. } = self;

        let stream = transport.into_inner();
        let mut tls_stream = create_tls_stream(host, TdsTransport::new(stream))?;
        tls_stream.get_mut().handshake_complete();

        let transport = TransportStream::new_tls_stream(tls_stream);
        Ok(Self { transport })
    }

    fn send<E>(&mut self, header: PacketHeader, item: E) -> Result<()>
    where
        E: Encode<BytesMut>,
    {
        let mut data = BytesMut::new();
        item.encode(&mut data)?;
        let packet = Packet::new(header, data);
        let mut payload = BytesMut::new();
        packet.encode(&mut payload)?;
        event!(
            Level::DEBUG,
            "Sending a packet ({} bytes)",
            payload.len() + HEADER_BYTES,
        );
        self.transport.write_all(&payload)?;
        self.transport.flush()?;
        Ok(())
    }

    fn login(mut self, user: &str, password: &str) -> crate::Result<Self> {
        let mut login_message = LoginMessage::new();

        login_message.user_name(user);
        login_message.password(password);

        let login = PacketHeader::new(PacketType::TDSv7Login, 1);
        self.send(login, login_message)?;

        // We do not support encryption yet. Use the TCP stream directly.
        let Self { transport, .. } = self;
        let tcp = transport.into_inner();
        self.transport = TransportStream::new_tcp_stream(tcp);

        Ok(self)
    }

    fn collect_packet(&mut self) -> crate::Result<Packet> {
        let mut buffer = [0; 4096];
        let mut size = self.transport.read(&mut buffer[..])?;
        while size == 0 {
            event!(Level::TRACE, "Sleeping");
            thread::sleep(Duration::from_secs(1));
            size = self.transport.read(&mut buffer[..])?;
        }

        let mut buf = BytesMut::new();
        buf.put(&buffer[..size]);

        event!(Level::DEBUG, "Collected packet {} bytes", buf.len());
        let packet: Packet = Packet::decode(&mut buf)?;
        Ok(packet)
    }
}
