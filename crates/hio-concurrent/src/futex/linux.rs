//
// Platform futex for linux (2.6.0+, FUTEX_PRIVATE_FLAG: 2.6.22+)
//
// glibc provides no wrapper; invoke the raw syscall(2) directly.
//
//      long syscall(SYS_futex,
//      [in]           uint32_t        *uaddr,     // must be 4-byte aligned
//      [in]           int              futex_op,  // op | FUTEX_PRIVATE_FLAG
//      [in]           uint32_t         val,
//      [in, optional] const timespec  *timeout,   // aliased as uint32_t val2
//      [in, optional] uint32_t        *uaddr2,
//      [in, optional] uint32_t         val3);
//
// FUTEX_WAIT (op = 0)
//      syscall(SYS_futex, uaddr, FUTEX_WAIT|FUTEX_PRIVATE_FLAG,
//              expected, timeout /* relative, CLOCK_MONOTONIC; NULL = infinite */,
//              NULL, 0);
//      Atomically checks *uaddr == expected and blocks only if it holds.
//      ret  0        : woken by FUTEX_WAKE, or spurious wakeup
//      ret -EAGAIN   : *uaddr != expected (did not block)
//      ret -EINTR    : interrupted by a signal (never auto-restarted with timeout)
//      ret -ETIMEDOUT: timeout expired
//
// FUTEX_WAKE (op = 1)
//      syscall(SYS_futex, uaddr, FUTEX_WAKE|FUTEX_PRIVATE_FLAG,
//              nr_wake, NULL, NULL, 0);
//      nr_wake is reinterpreted as a signed int by the kernel.
//          wake_one : 1
//          wake_all : INT_MAX (i32::MAX). u32::MAX becomes -1 and wakes only one.
//      ret >= 0      : number of waiters actually woken
//
// FUTEX_WAIT_BITSET (op = 9) / FUTEX_WAKE_BITSET (op = 10)
//      OR in FUTEX_CLOCK_REALTIME to treat timeout as an absolute deadline.
//      val3 (bitset) must be non-zero, otherwise EINVAL.
//      Use FUTEX_BITSET_MATCH_ANY (!0) to match all.
//
// FUTEX_PRIVATE_FLAG (128)
//      Process-private futex. Skips the shared-mapping lookup, so it is faster.
//      Must be omitted when the address lives in memory shared across processes.
//
// Notes
//      - Not a mirror of the Windows API: WaitOnAddress takes a runtime size
//        (1/2/4/8 bytes), whereas futex is always 32-bit. Standardizing on
//        AtomicU32 keeps the shim portable.
//      - Spurious wakeups are possible; callers must always re-check the
//        predicate in a loop.
//      - libc::syscall is variadic, so argument types are passed through
//        verbatim. When no timeout is used, pass an explicitly typed
//        std::ptr::null::<libc::timespec>().
//

use std::{sync::atomic::AtomicU32, time::Duration};

mod libc_api {
    use std::sync::atomic::AtomicU32;

    pub(crate) const OP_WAIT: libc::c_int = libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG;
    pub(crate) const OP_WAKE: libc::c_int = libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG;

    #[inline]
    pub(crate) fn sys_futex(
        addr: &AtomicU32,
        op: libc::c_int,
        val: u32,
        ts: *const libc::timespec,
    ) {
        unsafe {
            libc::syscall(libc::SYS_futex, addr.as_ptr(), op, val, ts);
        }
    }
}

#[inline]
pub fn wait(addr: &AtomicU32, expected: u32) {
    libc_api::sys_futex(addr, libc_api::OP_WAIT, expected, std::ptr::null());
}

#[inline]
pub fn wait_timeout(addr: &AtomicU32, expected: u32, timeout: Duration) {
    let ts = libc::timespec {
        tv_sec: timeout.as_secs().min(libc::time_t::MAX as u64) as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as _,
    };
    libc_api::sys_futex(addr, libc_api::OP_WAIT, expected, &ts);
}

#[inline]
pub fn wake_one(addr: &AtomicU32) {
    libc_api::sys_futex(addr, libc_api::OP_WAKE, 1, std::ptr::null());
}

#[inline]
pub fn wake_all(addr: &AtomicU32) {
    // val must be i32::max (not u32::max)
    libc_api::sys_futex(addr, libc_api::OP_WAKE, i32::max as u32, std::ptr::null());
}
