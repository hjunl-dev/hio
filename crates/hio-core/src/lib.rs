//
// hio-core: foundational types shared across the workspace.
// Leaf crate — depends on nothing but std.
//

use std::cell::Cell;
use std::time::Instant;

//
// LastError
//

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum HioLastError {
    // Common
    Success = 0,
    Failed = 1,
    // 1000~
    InvalidParam = 1000,
    InvalidState = 1001,
    InvalidOperation = 1002,
    ResourceUnavailable = 1003,
    Timeout = 1004,
    MutexPoisoned = 1005,
}

thread_local! {
    static HIO_LAST_ERROR: Cell<HioLastError> = Cell::new(HioLastError::Success);
}

pub fn get_last_error() -> HioLastError {
    HIO_LAST_ERROR.with(|e| e.get())
}

pub fn set_last_error(error: HioLastError) {
    HIO_LAST_ERROR.with(|e| e.set(error));
}

//
// ScopedTimer for measuring the time taken by a block of code.
//

pub struct ScopedTimer<'a> {
    label: &'a str,
    start: Instant,
}

impl<'a> ScopedTimer<'a> {
    pub fn new(label: &'a str) -> Self {
        Self {
            label,
            start: Instant::now(),
        }
    }
}

impl<'a> Drop for ScopedTimer<'a> {
    fn drop(&mut self) {
        println!("[{}] Elapsed time: {:?}", self.label, self.start.elapsed());
    }
}
