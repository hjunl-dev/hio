



#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    UnreliableReactor,
    UnreliableThreadPerSocket,
    ReliableReactor,
}