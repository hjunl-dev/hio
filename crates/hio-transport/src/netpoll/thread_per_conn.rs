use std::{
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
};

use crate::{TransportBackendHandle, TransportCommand, TransportHandler};
use hio_concurrent::Executor;

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
