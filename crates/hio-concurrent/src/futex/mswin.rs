//
// Platform futex for windows (mswin 8+)
//
// WaitOnAddress
//      BOOL WaitOnAddress(
//      [in]           volatile VOID *Address,
//      [in]           PVOID         CompareAddress,
//      [in]           SIZE_T        AddressSize,
//      [in, optional] DWORD         dwMilliseconds);
//
// WakeByAddressSingle
//      VOID WakeByAddressSingle(
//      [in]         PVOID Address);
//
// WakeByAddressAll
//      VOID WakeByAddressAll(
//      [in]         PVOID Address);
//

use std::{os::raw::c_void, sync::atomic::AtomicU32, time::Duration};

mod win_api {
    use std::ffi::c_void;

    pub(crate) const INFINITE: u32 = u32::MAX; // 0xFFFFFFFF

    #[link(name = "synchronization")]
    unsafe extern "system" {
        // https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitonaddress
        pub(crate) unsafe fn WaitOnAddress(
            addr: *const c_void,
            compare: *const c_void,
            addr_size: usize,
            ms: u32,
        ) -> i32;

        // https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-wakebyaddresssingle
        pub(crate) unsafe fn WakeByAddressSingle(addr: *const c_void);

        // https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-wakebyaddressall
        pub(crate) unsafe fn WakeByAddressAll(addr: *const c_void);
    }
}

#[inline]
fn wait_ms(addr: &AtomicU32, expected: u32, ms: u32) {
    unsafe {
        win_api::WaitOnAddress(
            addr.as_ptr().cast(),
            &expected as *const u32 as *const c_void,
            size_of::<u32>(),
            ms,
        );
    }
}

#[inline]
pub fn wait(addr: &AtomicU32, expected: u32) {
    wait_ms(addr, expected, win_api::INFINITE);
}

#[inline]
pub fn wait_timeout(addr: &AtomicU32, expected: u32, timeout: Duration) {
    let ms = timeout
        .as_nanos()
        .div_ceil(1_000_000)
        .clamp(1, (win_api::INFINITE - 1) as u128) as u32;
    wait_ms(addr, expected, ms);
}

#[inline]
pub fn wake_one(addr: &AtomicU32) {
    unsafe {
        win_api::WakeByAddressSingle(addr.as_ptr().cast());
    }
}

#[inline]
pub fn wake_all(addr: &AtomicU32) {
    unsafe {
        win_api::WakeByAddressAll(addr.as_ptr().cast());
    }
}
