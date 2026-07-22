use crate::udpoll::PeerId;

pub enum Command {
    Send {
        peer: PeerId,
        data: Vec<u8>,
        reliable: bool,
    },
    Close {
        peer: PeerId,
    },
    Shutdown,
}
