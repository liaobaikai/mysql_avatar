use bytes::{BufMut, BytesMut};

/// Codec for MySql protocol packets.
///
/// Codec supports both plain and compressed protocols.
#[derive(Debug)]
pub struct PlainPacketCodec {
    /// Chunk sequence id.
    pub seq_id: u8,
}

impl PlainPacketCodec {
    pub fn new() -> Self {
        Self { seq_id: 0 }
    }

    pub fn encode(&mut self, src: &BytesMut) -> BytesMut {
        let len = src.len();
        let mut dst = BytesMut::with_capacity(len + 4);
        // payload_length
        dst.put_u8((len & 0xff) as u8);
        dst.put_u8(((len >> 8) & 0xff) as u8);
        dst.put_u8(((len >> 16) & 0xff) as u8);

        // sequence_id
        dst.put_u8(self.seq_id);

        // payload
        dst.extend_from_slice(&src);

        self.seq_id += 1;

        dst
    }
}
