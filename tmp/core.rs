pub(crate) mod concurrent;
pub(crate) mod runtime;
pub(crate) mod transport;

use std::{
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
    time::Instant,
};

use crate::error::{HioLastError, set_last_error};

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

//
// FFI utils
//

pub struct FfiHandle<T>(T);

impl<T> FfiHandle<T> {
    pub fn new(t: T) -> Self {
        Self(t)
    }
}

fn panic_message(e: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = e.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

pub fn ffi_wrap_safe<R, F>(fallback: R, logic: F) -> R
where
    F: FnOnce() -> Result<R, HioLastError>,
{
    set_last_error(HioLastError::Success);

    match catch_unwind(AssertUnwindSafe(logic)) {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            set_last_error(e);
            fallback
        }
        Err(e) => {
            eprintln!("Caught panic in FFI call: {:?}", panic_message(&e));
            set_last_error(HioLastError::Failed);
            fallback
        }
    }
}
