use bytes::BytesMut;
pub mod handshake;

pub const SCRAMBLE_BUFFER_SIZE: usize = 20;
// 握手挑战值
#[derive(Debug)]
pub struct Scramble {
    data: BytesMut,
    scramble_1: [u8; 8],
    scramble_2: [u8; SCRAMBLE_BUFFER_SIZE - 8],
}

#[allow(unused)]
impl Scramble {
    #[allow(deprecated)]
    pub fn new() -> Self {
        let mut scramble_1 = [0u8; 8];
        rand::Rng::fill(&mut rand::thread_rng(), &mut scramble_1);

        let mut scramble_2 = [0u8; SCRAMBLE_BUFFER_SIZE - 8];
        rand::Rng::fill(&mut rand::thread_rng(), &mut scramble_2);

        let mut data = BytesMut::with_capacity(SCRAMBLE_BUFFER_SIZE);
        data.extend_from_slice(&scramble_1);
        data.extend_from_slice(&scramble_2);

        Self {
            data,
            scramble_1,
            scramble_2
        }
    }

    pub fn scramble_1(&self) -> [u8; 8] {
        self.scramble_1
    }

    pub fn scramble_2(&self) -> [u8; SCRAMBLE_BUFFER_SIZE - 8] {
        self.scramble_2
    }

    pub fn data(&self) -> &BytesMut {
        &self.data
    }
    
    pub fn data_mut(&mut self) -> &mut BytesMut {
        &mut self.data
    }
    
}

