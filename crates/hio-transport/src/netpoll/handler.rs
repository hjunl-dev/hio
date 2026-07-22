use std::sync::mpsc::Sender;

use crate::{ConnId, TransportCommand};

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
