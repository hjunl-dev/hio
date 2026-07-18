use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
};

use crate::{
    Job,
    core::concurrent::Executor,
    error::HioLastError::{self, MutexPoisoned},
};

struct Inner {
    thread_count: Mutex<usize>,
    max_limit_cv: Condvar,
    max_threads: usize,
    disposed: AtomicBool,
}

impl Inner {
    fn new(max_threads: usize) -> Self {
        Self {
            thread_count: Mutex::new(0),
            max_limit_cv: Condvar::new(),
            max_threads,
            disposed: AtomicBool::new(false),
        }
    }

    fn acquire(&self) -> bool {
        let mut cnt_g = match self.thread_count.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };

        if self.max_threads > 0 {
            cnt_g = match self.max_limit_cv.wait_while(cnt_g, |cnt_g| {
                !self.is_disposed() && *cnt_g >= self.max_threads
            }) {
                Ok(g) => g,
                Err(_) => return false,
            }
        }

        if self.is_disposed() {
            return false;
        }
        *cnt_g += 1;
        true
    }

    fn release(&self) {
        if let Ok(mut cnt_g) = self.thread_count.lock() {
            *cnt_g -= 1;
            self.max_limit_cv.notify_one();
        }
    }

    #[inline]
    fn count(&self) -> usize {
        let mut count = 0;
        if let Ok(cnt_g) = self.thread_count.lock() {
            count = *cnt_g;
        }
        count
    }

    #[inline]
    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

struct InnerGuard(Arc<Inner>);

impl Drop for InnerGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub struct ThreadPerTaskPool {
    inner: Arc<Inner>,
    stack_size: Option<usize>,
    handles: Mutex<Vec<JoinHandle<()>>>,
    seq: AtomicUsize,
}

impl ThreadPerTaskPool {
    pub fn new(max_threads: usize) -> Self {
        Self {
            inner: Arc::new(Inner::new(max_threads)),
            stack_size: None,
            handles: Mutex::new(Vec::new()),
            seq: AtomicUsize::new(0),
        }
    }
    pub fn with_stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = Some(stack_size);
        self
    }

    fn reap(&self) {
        if let Ok(mut hs) = self.handles.lock() {
            let mut i = 0;
            while i < hs.len() {
                if hs[i].is_finished() {
                    let h = hs.swap_remove(i);
                    let _ = h.join();
                } else {
                    i += 1;
                }
            }
        }
    }
}

impl Executor for ThreadPerTaskPool {
    fn submit(&self, job: Job) -> Result<(), HioLastError> {
        if self.is_disposed() {
            return Err(HioLastError::ResourceUnavailable);
        }
        // Select only threads that have already finished and join them
        // prevents the infinite growth of handle vec size.
        self.reap();

        if !self.inner.acquire() {
            return Err(HioLastError::ResourceUnavailable);
        }

        let inner_clone = Arc::clone(&self.inner);
        let name = format!(
            "ThreadPerTaskPoolWorker-{}",
            self.seq.fetch_add(1, Ordering::Relaxed)
        );
        let mut builder = thread::Builder::new().name(name);
        if let Some(stack_size) = self.stack_size {
            builder = builder.stack_size(stack_size);
        }

        match builder.spawn(move || {
            let _guard = InnerGuard(inner_clone);
            job();
        }) {
            Ok(h) => {
                self.handles
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(h);
                Ok(())
            }
            Err(_) => {
                self.inner.release();
                Err(HioLastError::Failed)
            }
        }
    }

    fn dispose(&mut self) {
        if self
            .inner
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            drop(
                self.inner
                    .thread_count
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()),
            );
            self.inner.max_limit_cv.notify_all();

            let handles: Vec<JoinHandle<()>> = self
                .handles
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .drain(..)
                .collect();
            for h in handles {
                let _ = h.join();
            }
        }
    }

    fn is_disposed(&self) -> bool {
        self.inner.is_disposed()
    }

    fn worker_count(&self) -> usize {
        self.inner.count()
    }
}

impl Drop for ThreadPerTaskPool {
    fn drop(&mut self) {
        self.dispose();
    }
}

//
// Tests for the ThreadPerTaskPool implementation
//

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;
    use std::time::Duration;

    #[test]
    fn test_dispose_joins_all_tasks() {
        let counter = Arc::new(AtomicI32::new(0));
        let repeat = 64;

        {
            let pool = ThreadPerTaskPool::new(8).with_stack_size(256 * 1024);
            for _ in 0..repeat {
                let c = Arc::clone(&counter);
                pool.submit(Box::new(move || {
                    thread::sleep(Duration::from_millis(5));
                    c.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), repeat);
    }

    #[test]
    fn test_backpressure_caps_concurrency() {
        const MAX: usize = 4;
        let concurrent = Arc::new(AtomicI32::new(0));
        let peak = Arc::new(AtomicI32::new(0));

        {
            let pool = ThreadPerTaskPool::new(MAX).with_stack_size(256 * 1024);
            for _ in 0..32 {
                let cur = Arc::clone(&concurrent);
                let pk = Arc::clone(&peak);
                pool.submit(Box::new(move || {
                    let now = cur.fetch_add(1, Ordering::SeqCst) + 1;
                    pk.fetch_max(now, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(10));
                    cur.fetch_sub(1, Ordering::SeqCst);
                }))
                .unwrap();
            }
        }

        assert!(peak.load(Ordering::SeqCst) <= MAX as i32);
    }

    #[test]
    fn test_panic_isolation() {
        let counter = Arc::new(AtomicI32::new(0));

        {
            let pool = ThreadPerTaskPool::new(2).with_stack_size(256 * 1024);

            for _ in 0..8 {
                let _ = pool.submit(Box::new(|| panic!("task panic")));
            }
            for _ in 0..8 {
                let c = Arc::clone(&counter);
                pool.submit(Box::new(move || {
                    c.fetch_add(1, Ordering::SeqCst);
                }))
                .unwrap();
            }
        }

        assert_eq!(counter.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn test_reject_after_dispose() {
        let mut pool = ThreadPerTaskPool::new(4).with_stack_size(256 * 1024);
        pool.submit(Box::new(|| thread::sleep(Duration::from_millis(5))))
            .unwrap();

        pool.dispose();
        assert!(pool.is_disposed());
        assert_eq!(pool.worker_count(), 0);
        assert!(pool.submit(Box::new(|| {})).is_err());

        pool.dispose();
    }

    #[test]
    fn test_reap_bounds_handle_vec() {
        let pool = ThreadPerTaskPool::new(4).with_stack_size(256 * 1024);
        for _ in 0..200 {
            pool.submit(Box::new(|| {})).unwrap();
        }
        let remaining = pool.handles.lock().unwrap().len();
        assert!(remaining <= 8, "handles not reaped: {remaining}");
    }
}
