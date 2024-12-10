//! `Connection` provides a capability to connect to a SQL Server.
//! For now, SQL authentication is supported.
use crate::decode::Decode;
use crate::encode::Encode;
use crate::header::{PacketHeader, PacketStatus};
use crate::login::LoginMessage;
use crate::packet::Packet;
use crate::pre_login::{EncryptionLevel, PreloginMessage};
use crate::protocol::{create_tls_stream, Protocol, TlsPreloginWrapper};
use crate::token::decode_token;
use crate::PacketCodec;
use crate::HEADER_BYTES;
use async_std::net::TcpStream;
use asynchronous_codec::Framed;
use bytes::BytesMut;
use futures::sink::SinkExt;
use futures_util::stream::TryStreamExt;
use tracing::{event, Level};

/// Provides a capability to connect to a SQL Server.
pub struct Connection {
    protocol: Framed<Protocol, PacketCodec>,
}

impl Connection {
    //! Establishes a connection to the SQL Server.
    pub async fn connect(host: &str, user: &str, password: &str) -> crate::Result<Connection> {
        let stream = TcpStream::connect(&host).await?;
        let protocol = Framed::new(Protocol::TcpStream(stream), PacketCodec);

        let mut connection = Self { protocol };

        event!(Level::INFO, "Sending Prelogin message.");
        let prelogin = connection.prelogin().await?;
        event!(Level::INFO, "Prelogin done: {:?}", prelogin);
        let bytes = prelogin.version.to_be_bytes();
        event!(
            Level::INFO,
            "Server version: {}.{}.{}",
            bytes[0],
            bytes[1],
            ((bytes[2] as u16) << 8) + (bytes[3] as u16)
        );

        let connection = connection.tls_handshake(host).await?;
        event!(Level::INFO, "TLS handshake completed.");
        event!(Level::INFO, "Sending login message.");
        let mut connection = connection.login(prelogin, user, password).await?;

        let packet = connection.collect_packet().await?;
        let (_, mut payload) = packet.into_parts();
        decode_token(&mut payload)?;

        Ok(connection)
    }

    /// Sends a prelogin message to the server.
    /// Gets back a prelogin message from the server.
    async fn prelogin(&mut self) -> crate::Result<PreloginMessage> {
        let mut msg = PreloginMessage::new();
        msg.encryption = EncryptionLevel::Off;
        msg.fed_auth_required = false;

        self.send(PacketHeader::pre_login(1), msg).await?;

        let packet = self.collect_packet().await?;
        let (_, mut payload) = packet.into_parts();
        let response = PreloginMessage::decode(&mut payload)?;
        Ok(response)
    }

    /// Sends a login message to the server.
    async fn login<'a>(
        mut self,
        _prelogin: PreloginMessage,
        user: &str,
        password: &str,
    ) -> crate::Result<Self> {
        let mut login_message = LoginMessage::new();

        login_message.readonly(false);

        login_message.user_name(user);
        login_message.password(password);

        self.send(PacketHeader::login(2), login_message).await?;

        // We do not support encryption yet. Use the TCP stream directly.
        let Self { protocol, .. } = self;
        let tcp = protocol.into_inner().into_inner();
        self.protocol = Framed::new(Protocol::TcpStream(tcp), PacketCodec);

        Ok(self)
    }

    /// Creates a new TLS stream and wraps the connection with it.
    /// Initilizes the TLS handshake.
    async fn tls_handshake(self, host: &str) -> crate::Result<Self> {
        let Self { protocol, .. } = self;
        let mut stream = match protocol.into_inner() {
            Protocol::TcpStream(tcp) => {
                create_tls_stream(TlsPreloginWrapper::new(tcp), host).await?
            }
            _ => unreachable!(),
        };

        stream.get_mut().handshake_complete();

        let protocol = Framed::new(Protocol::Tls(stream), PacketCodec);

        Ok(Self { protocol })
    }

    /// Sends an item as a packet to the server.
    async fn send<E>(&mut self, mut header: PacketHeader, item: E) -> crate::Result<()>
    where
        E: Sized + Encode<BytesMut>,
    {
        let packet_size = 4096_usize - HEADER_BYTES;

        let mut payload = BytesMut::new();
        item.encode(&mut payload)?;
        if payload.len() > packet_size {
            return Err(crate::Error::Protocol("Packet too big".into()));
        }

        header.set_status(PacketStatus::EndOfMessage);
        event!(
            Level::DEBUG,
            "Sending a packet ({} bytes)",
            payload.len() + HEADER_BYTES,
        );

        let packet = Packet::new(header, payload);
        self.protocol.send(packet).await?;
        self.protocol.flush().await?;

        Ok(())
    }

    /// Collects a packet from the server.
    pub(crate) async fn collect_packet(&mut self) -> crate::Result<Packet> {
        match self.protocol.try_next().await? {
            Some(packet) => Ok(packet),
            None => Err(crate::Error::Protocol("No packet".into())),
        }
    }
}
