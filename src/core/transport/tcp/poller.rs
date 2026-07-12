use std::{net::SocketAddr, sync::mpsc::Sender};

use crate::core::transport::tcp::{backend::BackendHandle, command::TransportCommand};

pub struct Poller {
    tx: Sender<TransportCommand>,
    local_addr: SocketAddr,
    handle: Option<Box<dyn BackendHandle>>,
}

impl Poller {}
