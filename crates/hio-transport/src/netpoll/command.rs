//
// Command for sending commands to the poller thread.
//

use crate::ConnId;

#[derive(Clone)]
pub enum TransportCommand {
    Send(ConnId, Vec<u8>),
    Close(ConnId),
    Shutdown,
}
