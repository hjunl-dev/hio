use std::{
    collections::VecDeque,
    sync::{
        Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::core::{HioLastError, concurrent::BQ};

//
// Primitive for building Array Blocking Queue
//

struct Inner<T> {
    buf: VecDeque<T>,
    pop_waiters: usize,
    push_waiters: usize,
}

//
// ArrayBQ impl
//

pub struct ArrayBQ<T: Send> {
    capacity: usize,
    disposed: AtomicBool,
    not_empty: Condvar,
    not_full: Condvar,
    inner: Mutex<Inner<T>>,
}

impl<T: Send> ArrayBQ<T> {
    pub fn new(capacity: usize) -> Self {
        let buf = if capacity == usize::MAX {
            VecDeque::new()
        } else {
            VecDeque::with_capacity(capacity)
        };

        Self {
            capacity,
            disposed: AtomicBool::new(false),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            inner: Mutex::new(Inner {
                buf,
                pop_waiters: 0,
                push_waiters: 0,
            }),
        }
    }

    fn en_q(&self, item: T, mut g: MutexGuard<'_, Inner<T>>) {
        let prev_count = g.buf.len();
        g.buf.push_back(item);

        // cascade notify push waiters if queue is not full
        if prev_count + 1 < self.capacity && g.push_waiters > 0 {
            self.not_full.notify_one();
        }
        // was empty, notify pop waiters
        if prev_count == 0 && g.pop_waiters > 0 {
            self.not_empty.notify_one();
        }
    }

    fn de_q(&self, mut g: MutexGuard<'_, Inner<T>>) -> Result<T, HioLastError> {
        let prev_count = g.buf.len();
        let item = g.buf.pop_front();

        // This should not happen, as we only call de_q when the queue is not empty.
        if item.is_none() {
            return Err(HioLastError::InvalidState);
        }
        // cascade notify pop waiters if queue is not empty
        if prev_count - 1 > 0 && g.pop_waiters > 0 {
            self.not_empty.notify_one();
        }
        // was full, notify push waiters
        if prev_count == self.capacity && g.push_waiters > 0 {
            self.not_full.notify_one();
        }
        Ok(item.unwrap())
    }
}

impl<T: Send> BQ<T> for ArrayBQ<T> {
    fn push(&self, item: T) -> Result<(), HioLastError> {
        if let Ok(mut g) = self.inner.lock() {
            g.push_waiters += 1;

            match self
                .not_full
                .wait_while(g, |g| !self.is_disposed() && g.buf.len() >= self.capacity)
            {
                Ok(mut g) => {
                    g.push_waiters -= 1;
                    if self.is_disposed() {
                        return Err(HioLastError::ResourceUnavailable);
                    }
                    self.en_q(item, g);
                    return Ok(());
                }
                Err(e) => {
                    e.into_inner().push_waiters -= 1;
                    return Err(HioLastError::MutexPoisoned);
                }
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    fn pop(&self) -> Result<T, HioLastError> {
        if let Ok(mut g) = self.inner.lock() {
            g.pop_waiters += 1;

            match self
                .not_empty
                .wait_while(g, |g| !self.is_disposed() && g.buf.is_empty())
            {
                Ok(mut g) => {
                    g.pop_waiters -= 1;
                    if self.is_disposed() {
                        return Err(HioLastError::ResourceUnavailable);
                    }
                    return self.de_q(g);
                }
                Err(e) => {
                    e.into_inner().pop_waiters -= 1;
                    return Err(HioLastError::MutexPoisoned);
                }
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    fn dispose(&self) {
        if self
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            let _g = self.inner.lock();
            self.not_empty.notify_all();
            self.not_full.notify_all();
        }
    }

    fn size(&self) -> usize {
        self.inner.lock().map(|g| g.buf.len()).unwrap_or(0)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }
    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

impl<T: Send> Drop for ArrayBQ<T> {
    fn drop(&mut self) {
        self.dispose();
    }
}
