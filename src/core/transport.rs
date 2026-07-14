pub(crate) mod netpoll;
pub(crate) mod udpoll;

//
// Transport
//

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum TransportType {
    NetPoll = 0,
    Udpoll = 1,
}

pub trait TransportBackendHandle: Send {
    fn shutdown(self: Box<Self>);
}