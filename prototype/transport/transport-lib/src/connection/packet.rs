use super::super::parser::{Decode, Encode};
use crate::TdsError;
use bytes::{Buf, BufMut, BytesMut};

pub const HEADER_BYTES: usize = 8;

uint_enum! {
    #[repr(u8)]
    pub enum PacketType {
        SQLBatch = 1,
        PreTDSv7Login = 2,
        Rpc = 3,
        TabularResult = 4,
        AttentionSignal = 6,
        BulkLoad = 7,
        /// Federated Authentication Token
        Fat = 8,
        TransactionManagerReq = 14,
        TDSv7Login = 16,
        Sspi = 17,
        PreLogin = 18,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PacketHeader {
    ty: PacketType,
    status: u8, //PacketStatus,
    /// [BE] the length of the packet (including the 8 header bytes)
    /// must match the negotiated size sending from client to server [since TDSv7.3] after login
    /// (only if not EndOfMessage)
    length: u16,
    /// [BE] the process ID on the server, for debugging purposes only
    spid: u16,
    /// packet id
    id: u8,
    /// currently unused
    window: u8,
}

impl PacketHeader {
    pub fn new(ty: PacketType, id: u8) -> PacketHeader {
        PacketHeader {
            ty,
            status: 1, // EndOfMessage = 1
            length: 0,
            spid: 0,
            id,
            window: 0,
        }
    }

    pub fn length(&self) -> u16 {
        self.length
    }

    pub fn get_type(&self) -> PacketType {
        self.ty
    }

    pub fn is_last(&self) -> bool {
        self.status == 1
    }
}

impl<B> Encode<B> for PacketHeader
where
    B: BufMut,
{
    fn encode(self, dst: &mut B) -> crate::Result<()> {
        dst.put_u8(self.ty as u8);
        dst.put_u8(self.status as u8);
        dst.put_u16(self.length);
        dst.put_u16(self.spid);
        dst.put_u8(self.id);
        dst.put_u8(self.window);

        Ok(())
    }
}

impl Decode<BytesMut> for PacketHeader {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let raw_ty = src.get_u8();

        let ty = PacketType::try_from(raw_ty).map_err(|_| {
            TdsError::Message(format!("header: invalid packet type: {}", raw_ty).into())
        })?;

        let status = src.get_u8(); //PacketStatus::try_from(src.get_u8()).map_err(|_| Error::Protocol("header: invalid packet status".into()))?;

        let header = PacketHeader {
            ty,
            status,
            length: src.get_u16(),
            spid: src.get_u16(),
            id: src.get_u8(),
            window: src.get_u8(),
        };

        Ok(header)
    }
}

#[derive(Debug)]
pub struct Packet {
    pub(crate) header: PacketHeader,
    pub(crate) payload: BytesMut,
}

impl Packet {
    pub(crate) fn new(header: PacketHeader, payload: BytesMut) -> Self {
        Self { header, payload }
    }

    pub(crate) fn into_parts(self) -> (PacketHeader, BytesMut) {
        (self.header, self.payload)
    }
}

impl Encode<BytesMut> for Packet {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let size = (self.payload.len() as u16 + HEADER_BYTES as u16).to_be_bytes();

        self.header.encode(dst)?;
        dst.extend(self.payload);

        dst[2] = size[0];
        dst[3] = size[1];

        Ok(())
    }
}

impl Decode<BytesMut> for Packet {
    fn decode(src: &mut BytesMut) -> crate::Result<Self> {
        Ok(Self {
            header: PacketHeader::decode(src)?,
            payload: src.split(),
        })
    }
}
