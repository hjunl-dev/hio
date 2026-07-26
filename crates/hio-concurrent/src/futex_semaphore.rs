//! Type1: futex(atomic-wait) 기반. MSVC C++20 counting_semaphore 미러링.

use std::sync::atomic::{AtomicU32, Ordering::*};

use atomic_wait::{wait, wake_all, wake_one};
use hio_core::HioLastError::{self, Failed};

use crate::Semaphore;

pub const MAX_PERMITS: u32 = u32::MAX >> 1;

#[derive(Debug)]
pub struct FutexSemaphore {
    count: AtomicU32,
    waiters: AtomicU32,
}

impl FutexSemaphore {
    pub const fn new(permits: u32) -> Self {
        assert!(permits <= MAX_PERMITS);
        Self {
            count: AtomicU32::new(permits),
            waiters: AtomicU32::new(0),
        }
    }

    #[inline]
    fn try_decrement(&self) -> bool {
        let mut cur = self.count.load(Relaxed);
        while cur > 0 {
            match self
                .count
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
            if self.count.load(SeqCst) == 0 {
                wait(&self.count, 0);
            }
        }
    }

    #[cold]
    fn wake_waiters(&self, n: u32, waiting: u32) {
        if waiting <= n {
            wake_all(&self.count);
        } else {
            for _ in 0..n {
                wake_one(&self.count);
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
        let cur = self.count.load(Relaxed);
        if cur > 0
            && self
                .count
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
        let prev = self.count.fetch_add(n, SeqCst);
        assert!(prev <= MAX_PERMITS - n, "semaphore permit overflow");

        let waiting = self.waiters.load(SeqCst);
        if waiting != 0 {
            self.wake_waiters(n, waiting);
        }
    }

    fn available_permits(&self) -> u32 {
        self.count.load(Relaxed)
    }
}
