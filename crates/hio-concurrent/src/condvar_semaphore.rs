use std::{
    hint,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use hio_core::HioLastError;

use crate::Semaphore;

const SPIN_LIMIT: u32 = 40;

struct WaiterGuard<'a>(&'a AtomicU32);

impl<'a> WaiterGuard<'a> {
    #[inline]
    fn enter(waiters: &'a AtomicU32) -> Self {
        // [앵커 1/4] Dekker store side (acquire 방향)
        waiters.fetch_add(1, Ordering::SeqCst);
        Self(waiters)
    }
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

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
        loop {
            if current == 0 {
                return false;
            }
            match self.permits.compare_exchange_weak(
                current,
                current - 1,
                Ordering::Acquire, // release의 SeqCst RMW와 synchronizes-with
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(n) => current = n,
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn acquire_slow(&self, timeout: Option<Duration>) -> Result<(), HioLastError> {
        let deadline: Option<Option<Instant>> = timeout.map(|t| Instant::now().checked_add(t));
        let _w = WaiterGuard::enter(&self.waiters);
        let mut g = self.lock.lock().unwrap_or_else(|e| e.into_inner());

        let mut spins: u32 = 0;

        loop {
            if self.try_get_permit() {
                return Ok(());
            }

            let remaining = match deadline {
                None | Some(None) => None,
                Some(Some(d)) => {
                    let now = Instant::now();
                    if now >= d {
                        return Err(HioLastError::Timeout);
                    }
                    Some(d - now)
                }
            };

            if self.permits.load(Ordering::SeqCst) != 0 {
                drop(g);
                if spins < SPIN_LIMIT {
                    spins += 1;
                    hint::spin_loop();
                } else {
                    thread::yield_now();
                }
                g = self.lock.lock().unwrap_or_else(|e| e.into_inner());
                continue;
            }
            spins = 0;
            g = match remaining {
                None => self.cv.wait(g).unwrap_or_else(|e| e.into_inner()),
                Some(dur) => {
                    self.cv
                        .wait_timeout(g, dur)
                        .unwrap_or_else(|e| e.into_inner())
                        .0
                }
            };
        }
    }
}

impl Semaphore for CondvarSemaphore {
    fn make(permits: u32) -> Self
    where
        Self: Sized,
    {
        Self::new(permits)
    }

    fn acquire(&self) {
        if self.try_get_permit() {
            return;
        }
        let _ = self.acquire_slow(None);
    }

    fn try_acquire(&self) -> Result<(), HioLastError> {
        if self.try_get_permit() {
            Ok(())
        } else {
            Err(HioLastError::Failed)
        }
    }

    // fn acquire_timeout(&self, timeout: Duration) -> Result<(), HioLastError> {
    //     if self.try_get_permit() {
    //         return Ok(());
    //     }
    //     self.acquire_slow(Some(timeout))
    // }

    fn release(&self, n: u32) {
        if n == 0 {
            return;
        }
        let mut current = self.permits.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(n);
            match self.permits.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(a) => current = a,
            }
        }

        if self.waiters.load(Ordering::SeqCst) == 0 {
            return;
        }

        drop(self.lock.lock().unwrap_or_else(|e| e.into_inner()));

        if n == 1 {
            self.cv.notify_one();
        } else {
            self.cv.notify_all();
        }
    }

    fn available_permits(&self) -> u32 {
        self.permits.load(Ordering::Relaxed)
    }
}
