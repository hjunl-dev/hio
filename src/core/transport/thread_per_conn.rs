use std::{
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
};

use crate::core::{
    concurrent::Executor,
    transport::{TransportBackendHandle, TransportHandler, command::TransportCommand},
};

pub fn spawn<H: TransportHandler>(
    listener: TcpListener,
    local_addr: SocketAddr,
    tx: Sender<TransportCommand>,
    rx: Receiver<TransportCommand>,
    executor: Arc<dyn Executor>,
    handler: Arc<H>,
) -> Box<dyn TransportBackendHandle> {
    todo!()
}
