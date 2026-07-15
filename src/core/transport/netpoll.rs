pub(crate) mod command;
pub(crate) mod connection;
pub(crate) mod handler;
pub(crate) mod inbox;
pub(crate) mod reactor;
pub(crate) mod thread_per_conn;

use std::{
    io,
    net::{TcpListener, ToSocketAddrs},
    sync::{Arc, mpsc},
};

use crate::core::{
    concurrent::Executor,
    transport::{Transport, TransportHandler},
};

//
// netpoll
//

pub type ConnId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Reactor,
    ThreadPerConnection,
}

pub fn run<A, H>(
    addr: A,
    bk: BackendKind,
    exec: Arc<dyn Executor>,
    th: Arc<H>,
) -> io::Result<Transport>
where
    A: ToSocketAddrs,
    H: TransportHandler,
{
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;
    let (tx, rx) = mpsc::channel();

    let bh = match bk {
        BackendKind::Reactor => reactor::spawn(listener, tx.clone(), rx, exec, th),
        BackendKind::ThreadPerConnection => {
            thread_per_conn::spawn(listener, local_addr, tx.clone(), rx, exec, th)
        }
    };

    Ok(Transport {
        local_addr,
        cmd_tx: tx.clone(),
        backend_handle: Some(bh),
    })
}
