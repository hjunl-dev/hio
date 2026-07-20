use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicU32, Ordering},
};

use crate::error::HioLastError::{self};

//
// Semaphore impl
//

pub struct Semaphore {
    count: AtomicU32,   // permit counter, for fast path
    sp_lock: Mutex<()>, // lock for slow path
    sp_cv: Condvar,     // cv for slow path (wait/notify, c++20 -> atomic wait)
}

impl Semaphore {
    pub fn new(permits: u32) -> Self {
        Self {
            count: AtomicU32::new(permits),
            sp_lock: Mutex::new(()),
            sp_cv: Condvar::new(),
        }
    }

    pub fn acquire(&self) -> Result<(), HioLastError> {
        // fast path
        if self.try_get_permit() {
            return Ok(());
        }
        // slow path
        if let Ok(g) = self.sp_lock.lock() {
            if let Ok(g) = self.sp_cv.wait_while(g, |_g| !self.try_get_permit()) {
                return Ok(());
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    pub fn try_acquire(&self) -> Result<(), HioLastError> {
        let current = self.count.load(Ordering::SeqCst);
        if current == 0 {
            return Err(HioLastError::ResourceUnavailable);
        }
        if self
            .count
            .compare_exchange_weak(current, current - 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Ok(());
        } else {
            return Err(HioLastError::Failed);
        }
    }

    pub fn release(&self, n: u32) -> Result<(), HioLastError> {
        if n == 0 {
            return Err(HioLastError::InvalidParam);
        }
        self.count.fetch_add(n, Ordering::Release);
        let _g = self.sp_lock.lock().unwrap_or_else(|e| e.into_inner());
        if n == 1 {
            self.sp_cv.notify_one();
        } else {
            self.sp_cv.notify_all();
        }
        Ok(())
    }

    fn try_get_permit(&self) -> bool {
        let mut current = self.count.load(Ordering::Relaxed);
        while current > 0 {
            match self.count.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(a) => current = a,
            }
        }
        false
    }
}
