use std::{
    collections::HashMap,
    net::TcpListener,
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
};

use crate::core::{
    concurrent::Executor,
    transport::tcp::{
        command::TransportCommand,
        connection::{ConnId, Connection},
        handler::TransportHandler,
        inbox::Inbox,
    },
};

//
//
//

struct Entry<H: TransportHandler> {
    conn: Connection,
    inbox: Arc<Inbox<H>>,
}

struct Core<H: TransportHandler> {
    listener: TcpListener,
    conn_map: HashMap<ConnId, Entry<H>>,
    next_id: ConnId,
    tx: Sender<TransportCommand>,
    rx: Receiver<TransportCommand>,
    executor: Arc<dyn Executor>,
    handler: Arc<H>,
    scratch: Vec<u8>,
}
