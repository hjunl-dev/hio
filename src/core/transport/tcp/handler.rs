use std::sync::mpsc::Sender;

use crate::core::transport::tcp::{command::TransportCommand, connection::ConnId};

//
// PollerHandle is a handle for sending commands to the poller thread.
//

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
