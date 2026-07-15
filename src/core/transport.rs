pub(crate) mod netpoll;
pub(crate) mod udpoll;

use std::{net::SocketAddr, sync::mpsc::Sender};

use crate::core::transport::netpoll::{ConnId, command::TransportCommand, handler::PollerHandle};

//
// Transport
//

pub trait TransportHandler: Send + Sync + 'static {
    fn on_connect(&self, id: ConnId, h: &PollerHandle);
    fn on_data(&self, id: ConnId, data: &[u8], h: &PollerHandle);
    fn on_close(&self, id: ConnId);
}

pub trait TransportBackendHandle: Send {
    fn shutdown(self: Box<Self>);
}

pub struct Transport {
    local_addr: SocketAddr,
    cmd_tx: Sender<TransportCommand>,
    backend_handle: Option<Box<dyn TransportBackendHandle>>,
}

impl Transport {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn handle(&self) -> PollerHandle {
        PollerHandle {
            tx: self.cmd_tx.clone(),
        }
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        if let Some(bh) = self.backend_handle.take() {
            bh.shutdown();
        }
    }
}
