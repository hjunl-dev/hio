pub(crate) mod backend;
pub(crate) mod command;
pub(crate) mod connection;
pub(crate) mod handler;
pub(crate) mod inbox;
pub(crate) mod reactor;

use std::{
    io,
    net::{SocketAddr, TcpListener, ToSocketAddrs, UdpSocket},
    sync::{
        Arc,
        mpsc::{self, Sender},
    },
};

use crate::core::{
    concurrent::Executor,
    transport::{
        backend::{TcpBackendKind, UdpBackendKind},
        command::TransportCommand,
        handler::PollerHandle,
    },
};

//
// Transport
//

pub type ConnId = u64;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum TransportType {
    NetPoll = 0,
    Udpoll = 1,
}

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
    command_tx: Sender<TransportCommand>,
    backend_handle: Option<Box<dyn TransportBackendHandle>>,
}

impl Transport {
    pub fn netpoll_run<A, H>(
        addr: A,
        backend_kind: TcpBackendKind,
        executor: Arc<dyn Executor>,
        transport_handler: Arc<H>,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs,
        H: TransportHandler,
    {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel();

        let bh: Box<dyn TransportBackendHandle> = match backend_kind {
            TcpBackendKind::Reactor => {
                reactor::spawn(listener, tx.clone(), rx, executor, transport_handler)
            }
            TcpBackendKind::ThreadPerConnection => todo!(),
        };

        Ok(Self {
            local_addr,
            command_tx: tx.clone(),
            backend_handle: Some(bh),
        })
    }

    pub fn udpoll_run<A, H>(
        addr: A,
        backend_kind: UdpBackendKind,
        executor: Arc<dyn Executor>,
        transport_handler: Arc<H>,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs,
        H: TransportHandler,
    {
        let socket = UdpSocket::bind(addr)?;
        let local_addr = socket.local_addr()?;
        let (tx, rx) = mpsc::channel();

        let bh: Box<dyn TransportBackendHandle> = match backend_kind {
            UdpBackendKind::UnreliableReactor => todo!(),
            UdpBackendKind::UnreliableThreadPerSocket => todo!(),
            UdpBackendKind::ReliableReactor => todo!(),
        };

        Ok(Self {
            local_addr,
            command_tx: tx.clone(),
            backend_handle: Some(bh),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn handle(&self) -> PollerHandle {
        PollerHandle {
            tx: self.command_tx.clone(),
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
