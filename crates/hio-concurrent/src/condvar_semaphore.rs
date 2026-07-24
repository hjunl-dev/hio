use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicU32, Ordering},
};

use crate::Semaphore;

pub struct CondvarSemaphore {
    permits: AtomicU32,
    waiters: AtomicU32,
    lock: Mutex<()>,
    cv: Condvar,
}

impl CondvarSemaphore {
    pub fn new(permits: u32) -> Self {
        Self {
            permits: AtomicU32::new(permits),
            waiters: AtomicU32::new(0),
            lock: Mutex::new(()),
            cv: Condvar::new(),
        }
    }

    #[inline]
    fn try_get_permit(&self) -> bool {
        let mut current = self.permits.load(Ordering::Relaxed);
        while current > 0 {
            match self.permits.compare_exchange(
                current,
                current - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(n) => current = n,
            }
        }
        false
    }

    #[inline]
    fn acquire_slow(&self) {}
}

impl Semaphore for CondvarSemaphore {
    fn make(permits: u32) -> Self
    where
        Self: Sized,
    {
        todo!()
    }

    fn acquire(&self) {
        todo!()
    }

    fn try_acquire(&self) {
        todo!()
    }

    fn acquire_timeout(&self) {
        todo!()
    }

    fn release(&self, n: u32) {
        todo!()
    }

    fn available_permits(&self) -> u32 {
        self.permits.load(Ordering::Acquire)
    }
}
