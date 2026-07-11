//
// LastError
//

use std::cell::Cell;

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
