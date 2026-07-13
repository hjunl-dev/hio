use std::{
    io,
    net::{SocketAddr, TcpListener, ToSocketAddrs},
    sync::{
        Arc,
        mpsc::{self, Sender},
    },
};

use crate::core::{
    concurrent::Executor,
    transport::tcp::{
        backend::{BackendHandle, BackendKind},
        command::TransportCommand,
        handler::{PollerHandle, TransportHandler},
        reactor,
    },
};

pub struct Server {
    tx: Sender<TransportCommand>,
    local_addr: SocketAddr,
    backend_handle: Option<Box<dyn BackendHandle>>,
}

impl Server {
    pub fn bind<A, H>(
        addr: A,
        backend: BackendKind,
        executor: Arc<dyn Executor>,
        handler: Arc<H>,
    ) -> io::Result<Self>
    where
        A: ToSocketAddrs,
        H: TransportHandler,
    {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?;
        let (tx, rx) = mpsc::channel();

        let handle: Box<dyn BackendHandle> = match backend {
            BackendKind::Reactor => reactor::spawn(listener, tx.clone(), rx, executor, handler),
            BackendKind::ThreadPerConnection => {
                todo!()
            }
        };

        Ok(Server {
            tx,
            local_addr,
            backend_handle: Some(handle),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn handle(&self) -> PollerHandle {
        PollerHandle {
            tx: self.tx.clone(),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.tx.send(TransportCommand::Shutdown);
        if let Some(bh) = self.backend_handle.take() {
            bh.shutdown();
        }
    }
}
