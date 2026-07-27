#[cfg(target_os = "windows")]
#[path = "futex/mswin.rs"]
mod platform_futex;

#[cfg(any(target_os = "linux", target_os = "android"))]
#[path = "futex/linux.rs"]
mod platform_futex;

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

//
// platform_futex
// {
//      wait(addr: &AtomicU32, expected: u32);
//      wait_timeout(addr: &AtomicU32, expected: u32);
//      wake_one(addr: &AtomicU32);
//      wake_all(addr: &AtomicU32);
// }
//

#[derive(Debug)]
#[repr(transparent)]
pub struct FutexWord {
    word: AtomicU32,
}

impl FutexWord {
    pub const fn new(n: u32) -> Self {
        Self {
            word: AtomicU32::new(n),
        }
    }

    #[inline]
    pub fn load(&self, o: Ordering) -> u32 {
        self.word.load(o)
    }

    #[inline]
    pub fn fetch_add(&self, val: u32, o: Ordering) -> u32 {
        self.word.fetch_add(val, o)
    }

    #[inline]
    pub fn fetch_sub(&self, val: u32, o: Ordering) -> u32 {
        self.word.fetch_sub(val, o)
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        curr: u32,
        new: u32,
        so: Ordering,
        fo: Ordering,
    ) -> Result<u32, u32> {
        self.word.compare_exchange(curr, new, so, fo)
    }

    #[inline]
    pub fn compare_exchange_weak(
        &self,
        curr: u32,
        new: u32,
        so: Ordering,
        fo: Ordering,
    ) -> Result<u32, u32> {
        self.word.compare_exchange_weak(curr, new, so, fo)
    }

    // wait & wake based on platform futex
    #[inline]
    pub fn wait(&self, expected: u32) {
        platform_futex::wait(&self.word, expected);
    }

    #[inline]
    pub fn wait_timeout(&self, expected: u32, timeout: Duration) {
        platform_futex::wait_timeout(&self.word, expected, timeout);
    }

    #[inline]
    pub fn wake_one(&self) {
        platform_futex::wake_one(&self.word);
    }

    #[inline]
    pub fn wake_all(&self) {
        platform_futex::wake_all(&self.word);
    }
}
