pub(crate) mod concurrent;
pub(crate) mod transport;

use std::{cell::Cell, ffi::c_void, time::Instant};

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

//
// CachePadded for preventing false sharing between threads.
//

// todo: need to fix align for different architectures, currently only works for x86_64
#[repr(align(128))]
pub struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    pub const fn new(t: T) -> Self {
        Self(t)
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

//
// UserDataWrapper for safely passing user data to C callbacks.
//

pub struct UserDataWrapper(*const c_void);

impl UserDataWrapper {
    pub fn new(ptr: *const c_void) -> Self {
        Self(ptr)
    }
}

impl std::ops::Deref for UserDataWrapper {
    type Target = *const c_void;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl Send for UserDataWrapper {}
unsafe impl Sync for UserDataWrapper {}
