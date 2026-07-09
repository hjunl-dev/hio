use std::sync::mpsc::Sender;

pub(crate) mod tcp;
pub(crate) mod udp;

//
// Transport
//

pub type ConnId = u64;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum TransportType {
    Tcp = 0,
    Udp = 1,
}

#[derive(Clone)]
pub enum TransportCommand {
    Send(ConnId, Vec<u8>),
    Close(ConnId),
    Shutdown,
}

pub struct PollerHandle {
    pub tx: Sender<TransportCommand>,
}

impl PollerHandle {
    pub fn send(&self, id: ConnId, data: Vec<u8>) {
        let _ = self.tx.send(TransportCommand::Send(id, data));
    }

    pub fn close(&self, id: ConnId) {
        let _ = self.tx.send(TransportCommand::Close(id));
    }
}

pub trait TransportHandler: Send + Sync + 'static {
    fn on_connect(&self, h: &PollerHandle);
    fn on_data(&self, data: &[u8], h: &PollerHandle);
    fn on_close(&self, h: &PollerHandle);
}
