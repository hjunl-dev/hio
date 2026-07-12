pub(crate) mod tcp;
pub(crate) mod udp;

//
// Transport
//

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum TransportType {
    Tcp = 0,
    Udp = 1,
}