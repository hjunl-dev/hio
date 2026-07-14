//
// Backend kindand handle of TCP implementation.
//

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    Reactor,
    ThreadPerConnection,
}