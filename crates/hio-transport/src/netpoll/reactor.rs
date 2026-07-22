use std::thread;
use std::{
    collections::HashMap,
    net::TcpListener,
    sync::{
        Arc,
        mpsc::{Receiver, Sender},
    },
    thread::JoinHandle,
};

use crate::{
    ConnId, TransportBackendHandle, TransportCommand, TransportHandler,
    netpoll::{connection::Connection, inbox::Inbox},
};
use hio_concurrent::Executor;

//
//
//

struct Entry<H: TransportHandler> {
    conn: Connection,
    inbox: Arc<Inbox<H>>,
}

struct ReactorHandle {
    join_handle: Option<JoinHandle<()>>,
}

impl TransportBackendHandle for ReactorHandle {
    fn shutdown(mut self: Box<Self>) {
        if let Some(jh) = self.join_handle.take() {
            let _ = jh.join();
        }
    }
}

struct ReactorCore<H: TransportHandler> {
    listener: TcpListener,
    conn_map: HashMap<ConnId, Entry<H>>,
    next_id: ConnId,
    tx: Sender<TransportCommand>,
    rx: Receiver<TransportCommand>,
    executor: Arc<dyn Executor>,
    handler: Arc<H>,
    scratch: Vec<u8>,
}

impl<H: TransportHandler> ReactorCore<H> {
    pub fn run_loop(mut self) {
        // Reactor event loop implementation goes here
    }
}

pub fn spawn<H: TransportHandler>(
    listener: TcpListener,
    tx: Sender<TransportCommand>,
    rx: Receiver<TransportCommand>,
    executor: Arc<dyn Executor>,
    handler: Arc<H>,
) -> Box<dyn TransportBackendHandle> {
    listener.set_nonblocking(true).expect("");

    let core = ReactorCore {
        listener,
        conn_map: HashMap::new(),
        next_id: 1,
        tx,
        rx,
        executor,
        handler,
        scratch: vec![0u8; 64 * 1024],
    };

    let join_handle = thread::spawn(move || {
        core.run_loop();
    });
    Box::new(ReactorHandle {
        join_handle: Some(join_handle),
    })
}
