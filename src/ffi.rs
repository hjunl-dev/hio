//
// HIO FFI
//

use std::ffi::c_void;

use crate::core::{self, HioLastError};

#[unsafe(no_mangle)]
pub extern "C" fn hio_create_runtime() -> *const c_void {
    std::ptr::null()
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_destroy_runtime(_runtime: *const c_void) -> bool {
    false
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_get_last_error() -> HioLastError {
    core::get_last_error()
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_get_version() -> *const std::os::raw::c_char {
    std::ptr::null()
}
