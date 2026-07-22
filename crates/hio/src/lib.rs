//
// hio: umbrella crate. Owns the C ABI surface and re-exports the
// public API from the inner crates so downstream users see one facade.
//

mod ffi;
mod ffi_support;

// Public facade — keeps the historical `hio::…` paths stable.
pub use hio_concurrent::{BQType, ExecutorType, Job, ThreadPool, create_bq, create_executor};
pub use hio_core::ScopedTimer;
