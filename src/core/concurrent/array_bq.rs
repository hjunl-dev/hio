use std::{
    collections::VecDeque,
    sync::{Condvar, Mutex, atomic::AtomicBool},
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
// Array Blocking Queue
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

    fn en_q(&self, item: T) -> Result<(), HioLastError> {
        todo!()
    }

    fn de_q(&self) -> Result<T, HioLastError> {
        todo!()
    }
}

impl<T: Send> BQ<T> for ArrayBQ<T> {
    fn push(&self, item: T) -> Result<(), HioLastError> {
        todo!()
    }

    fn pop(&self) -> Result<T, HioLastError> {
        todo!()
    }

    fn dispose(&self) {
        todo!()
    }

    fn size(&self) -> usize {
        todo!()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn is_disposed(&self) -> bool {
        self.disposed.load(std::sync::atomic::Ordering::SeqCst)
    }
}
