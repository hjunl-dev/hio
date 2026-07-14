//
// Backend kindand handle of TCP implementation.
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpBackendKind {
    Reactor,
    ThreadPerConnection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UdpBackendKind {
    UnreliableReactor,
    UnreliableThreadPerSocket,
    ReliableReactor,
}