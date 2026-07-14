use crate::core::transport::netpoll::connection::ConnId;

//
// Command for sending commands to the poller thread.
//

#[derive(Clone)]
pub enum TransportCommand {
    Send(ConnId, Vec<u8>),
    Close(ConnId),
    Shutdown,
}
