//
// HIO FFI
//

use crate::ffi_support::ffi_wrap_safe;
use hio_core::{get_last_error, HioLastError};
use hio_runtime::{HioRuntime, HioRuntimeConfig};

#[unsafe(no_mangle)]
pub extern "C" fn hio_create_config() -> *mut HioRuntimeConfig {
    ffi_wrap_safe(std::ptr::null_mut(), || {
        Ok(Box::into_raw(Box::new(HioRuntimeConfig::new())))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_destroy_config(config: *mut HioRuntimeConfig) -> bool {
    ffi_wrap_safe(false, || {
        if config.is_null() {
            return Ok(false);
        }
        let _auto_free = unsafe { Box::from_raw(config) };
        Ok(true)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_create_runtime(config: *mut HioRuntimeConfig) -> *mut HioRuntime {
    ffi_wrap_safe(std::ptr::null_mut(), || {
        if config.is_null() {
            return Err(HioLastError::InvalidParam);
        }
        let cfg = unsafe { config.as_mut().ok_or(HioLastError::InvalidParam)? };
        let runtime = HioRuntime::new(cfg);
        Ok(Box::into_raw(Box::new(runtime)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_destroy_runtime(runtime: *mut HioRuntime) -> bool {
    ffi_wrap_safe(false, || {
        if runtime.is_null() {
            return Ok(false);
        }
        let _auto_free = unsafe { Box::from_raw(runtime) };
        Ok(true)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_get_last_error() -> HioLastError {
    get_last_error()
}

#[unsafe(no_mangle)]
pub extern "C" fn hio_get_version() -> *const std::os::raw::c_char {
    std::ptr::null()
}
