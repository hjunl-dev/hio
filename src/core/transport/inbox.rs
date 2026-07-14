use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use crate::core::{
    concurrent::Executor,
    transport::{ConnId, TransportHandler, handler::PollerHandle},
};

//
// Inbox is a queue for messages received from the poller thread.
// It is owned by the connection and handles messages such as data received, connection closed, etc.
//

pub enum InboxMsg {
    Connected,
    Data(Vec<u8>),
    Closed,
}

pub struct Inbox<H: TransportHandler> {
    id: ConnId,
    queue: Mutex<VecDeque<InboxMsg>>,
    running: AtomicBool,
    handler: Arc<H>,
}

impl<H: TransportHandler> Inbox<H> {
    pub fn new(id: ConnId, handler: Arc<H>) -> Arc<Self> {
        Arc::new(Self {
            id,
            queue: Mutex::new(VecDeque::new()),
            running: AtomicBool::new(false),
            handler,
        })
    }

    // Call only from the poller thread.
    // Enqueue a message and submit a drain job if none is running.
    pub fn enqueue(inbox: &Arc<Self>, msg: InboxMsg, executor: &dyn Executor, h: &PollerHandle) {
        todo!()
    }

    fn drain(self: Arc<Self>, h: PollerHandle) {
        todo!()
    }
}
