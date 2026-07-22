pub(crate) mod command;

use std::{
    io,
    net::{ToSocketAddrs, UdpSocket},
    sync::{Arc, mpsc},
};

use crate::{Transport, TransportHandler};
use hio_concurrent::Executor;

//
// udpoll
//

pub type PeerId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    UnreliableReactor,
    UnreliableThreadPerSocket,
    ReliableReactor,
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
    let socket = UdpSocket::bind(addr)?;
    let local_addr = socket.local_addr()?;
    let (tx, rx) = mpsc::channel();
    let bh = match bk {
        BackendKind::UnreliableReactor => todo!(),
        BackendKind::UnreliableThreadPerSocket => todo!(),
        BackendKind::ReliableReactor => todo!(),
    };

    Ok(Transport {
        local_addr,
        cmd_tx: tx,
        backend_handle: Some(bh),
    })
}
