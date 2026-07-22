//
// FFI support utilities. These live in the umbrella crate rather than a
// foundation crate so that the inner libraries (concurrent, transport) do
// not inherit any C-ABI / catch_unwind concerns.
//

use std::{
    ffi::c_void,
    panic::{catch_unwind, AssertUnwindSafe},
};

use hio_core::{set_last_error, HioLastError};

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
