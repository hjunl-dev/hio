use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
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
        if let Ok(cnt_g) = self.thread_count.lock() {
            if self.max_threads > 0 {
                if let Ok(mut cnt_g) = self.max_limit_cv.wait_while(cnt_g, |cnt_g| {
                    !self.disposed.load(Ordering::Acquire) && *cnt_g >= self.max_threads
                }) {
                    if self.disposed.load(Ordering::Acquire) {
                        return false;
                    }

                    *cnt_g += 1;
                    return true;
                }
            }
        }
        false
    }

    fn release(&self) {
        if let Ok(mut cnt_g) = self.thread_count.lock() {
            *cnt_g -= 1;
            self.max_limit_cv.notify_one();
        }
    }

    fn count(&self) -> usize {
        let mut count = 0;
        if let Ok(cnt_g) = self.thread_count.lock() {
            count = *cnt_g;
        }
        count
    }

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
}
