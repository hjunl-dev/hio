//! Type1: futex(atomic-wait) 기반. MSVC C++20 counting_semaphore 미러링.

use std::sync::atomic::{AtomicU32, Ordering::*};

use crate::{
    futex::FutexWord,
    semaphore::{MAX_PERMITS, Semaphore},
};
use hio_core::HioLastError::{self, Failed};

#[derive(Debug)]
pub struct FutexSemaphore {
    permits: FutexWord,
    waiters: AtomicU32,
}

impl FutexSemaphore {
    pub const fn new(permits: u32) -> Self {
        assert!(permits <= MAX_PERMITS);
        Self {
            permits: FutexWord::new(permits),
            waiters: AtomicU32::new(0),
        }
    }

    #[inline]
    fn try_decrement(&self) -> bool {
        let mut cur = self.permits.load(Relaxed);
        while cur > 0 {
            match self
                .permits
                .compare_exchange_weak(cur, cur - 1, Acquire, Relaxed)
            {
                Ok(_) => return true,
                Err(n) => cur = n,
            }
        }
        false
    }

    #[cold]
    fn acquire_slow(&self) {
        self.waiters.fetch_add(1, SeqCst);
        loop {
            if self.try_decrement() {
                self.waiters.fetch_sub(1, Relaxed);
                return;
            }
            if self.permits.load(SeqCst) == 0 {
                self.permits.wait(0);
            }
        }
    }

    #[cold]
    fn wake_waiters(&self, n: u32, waiting: u32) {
        if waiting <= n {
            self.permits.wake_all();
        } else {
            for _ in 0..n {
                self.permits.wake_one();
            }
        }
    }
}

impl Semaphore for FutexSemaphore {
    fn make(permits: u32) -> Self {
        Self::new(permits)
    }

    fn acquire(&self) {
        if self.try_decrement() {
            return;
        }
        self.acquire_slow();
    }

    fn try_acquire(&self) -> Result<(), HioLastError> {
        let cur = self.permits.load(Relaxed);
        if cur > 0
            && self
                .permits
                .compare_exchange(cur, cur - 1, Acquire, Relaxed)
                .is_ok()
        {
            Ok(())
        } else {
            Err(Failed)
        }
    }

    fn release(&self, n: u32) {
        if n == 0 {
            return;
        }
        let prev = self.permits.fetch_add(n, SeqCst);
        assert!(prev <= MAX_PERMITS - n, "semaphore permit overflow");

        let waiting = self.waiters.load(SeqCst);
        if waiting != 0 {
            self.wake_waiters(n, waiting);
        }
    }

    fn available_permits(&self) -> u32 {
        self.permits.load(Relaxed)
    }
}
