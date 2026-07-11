use std::{
    cell::UnsafeCell,
    sync::{
        Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::{
    core::{CachePadded, concurrent::BQ},
    error::HioLastError,
};

//
// Primitive for building Linked Blocking Queue
//

struct Node<T> {
    item: Option<T>,
    next: *mut Node<T>,
}

impl<T> Node<T> {
    fn new(item: Option<T>) -> Box<Self> {
        Box::new(Self {
            item,
            next: std::ptr::null_mut(),
        })
    }
    fn dummy() -> Box<Self> {
        Self::new(None)
    }
}

struct PopSide<T> {
    head_lock: Mutex<()>,
    not_empty: Condvar,
    head: UnsafeCell<*mut Node<T>>,
}

struct PushSide<T> {
    tail_lock: Mutex<()>,
    not_full: Condvar,
    tail: UnsafeCell<*mut Node<T>>,
}

//
// LinkedBQ impl
//

pub struct LinkedBQ<T: Send> {
    capacity: usize,
    count: CachePadded<AtomicUsize>,
    disposed: CachePadded<AtomicBool>,
    pop_side: CachePadded<PopSide<T>>,   // head
    push_side: CachePadded<PushSide<T>>, // tail
}

impl<T: Send> LinkedBQ<T> {
    pub fn new(capacity: usize) -> Self {
        let dummy_ptr = Box::into_raw(Node::dummy());
        Self {
            capacity,
            count: CachePadded(AtomicUsize::new(0)),
            disposed: CachePadded(AtomicBool::new(false)),
            pop_side: CachePadded(PopSide {
                head_lock: Mutex::new(()),
                not_empty: Condvar::new(),
                head: UnsafeCell::new(dummy_ptr),
            }),
            push_side: CachePadded(PushSide {
                tail_lock: Mutex::new(()),
                not_full: Condvar::new(),
                tail: UnsafeCell::new(dummy_ptr),
            }),
        }
    }

    unsafe fn en_q(&self, item: Box<Node<T>>, tail_lock_g: MutexGuard<'_, ()>) {
        let pp_old_tail = self.push_side.tail.get();
        let p_new_tail = Box::into_raw(item);

        unsafe {
            // Link the new node to the end of the queue
            (**pp_old_tail).next = p_new_tail;
            // Update the tail pointer to point to the new node
            (*pp_old_tail) = p_new_tail;
        }

        let prev_count = self.count.fetch_add(1, Ordering::Release);
        // cascade notify push waiters if queue is not full
        if prev_count + 1 < self.capacity {
            self.push_side.not_full.notify_one();
        }
        // was empty (empty -> 1), notify pop waiters);
        drop(tail_lock_g);
        if prev_count == 0 {
            let _g = self
                .pop_side
                .head_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.pop_side.not_empty.notify_one();
        }
    }

    unsafe fn de_q(&self, head_lock_g: MutexGuard<'_, ()>) -> Result<T, HioLastError> {
        let pp_old_head = self.pop_side.head.get();
        let item: T;
        unsafe {
            let p_new_head = (**pp_old_head).next;
            if p_new_head.is_null() {
                // dequeue from an empty queue, which should not happen if used correctly
                return Err(HioLastError::InvalidOperation);
            }

            let tmp = (*p_new_head).item.take();
            if tmp.is_none() {
                // This should not happen, as the new head should always have an item
                return Err(HioLastError::InvalidState);
            }
            // Move the item out of the node
            drop(Box::from_raw(*pp_old_head));
            // Update the head pointer to point to the new head
            (*pp_old_head) = p_new_head;
            item = tmp.unwrap();
        }

        let prev_count = self.count.fetch_sub(1, Ordering::Release);
        // cascade notify push waiters if queue is not full
        if prev_count > 1 {
            self.pop_side.not_empty.notify_one();
        }
        // was full (full -> not full), notify push waiters
        drop(head_lock_g);
        if prev_count == self.capacity {
            let _g = self
                .push_side
                .tail_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.push_side.not_full.notify_one();
        }
        Ok(item)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.count.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.count.load(Ordering::Acquire) == self.capacity
    }
}

impl<T: Send> BQ<T> for LinkedBQ<T> {
    fn push(&self, item: T) -> Result<(), HioLastError> {
        if let Ok(g) = self.push_side.tail_lock.lock() {
            if let Ok(g) = self
                .push_side
                .not_full
                .wait_while(g, |_g| !self.is_disposed() && self.is_full())
            {
                if self.is_disposed() {
                    return Err(HioLastError::ResourceUnavailable);
                }
                unsafe {
                    self.en_q(Node::new(Some(item)), g);
                }
                return Ok(());
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    fn pop(&self) -> Result<T, HioLastError> {
        if let Ok(g) = self.pop_side.head_lock.lock() {
            if let Ok(g) = self
                .pop_side
                .not_empty
                .wait_while(g, |_g| !self.is_disposed() && self.is_empty())
            {
                if self.is_disposed() && self.is_empty() {
                    return Err(HioLastError::ResourceUnavailable);
                }
                unsafe {
                    return self.de_q(g);
                }
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    fn dispose(&self) {
        if self
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            {
                let _g = self
                    .pop_side
                    .head_lock
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                self.pop_side.not_empty.notify_all();
            }
            {
                let _g = self
                    .push_side
                    .tail_lock
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                self.push_side.not_full.notify_all();
            }
        }
    }

    fn size(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

impl<T: Send> Drop for LinkedBQ<T> {
    fn drop(&mut self) {
        let mut current = unsafe { *self.pop_side.head.get() };
        while !current.is_null() {
            unsafe {
                let tmp = Box::from_raw(current);
                current = tmp.next;
            }
        }
    }
}

unsafe impl<T: Send> Send for LinkedBQ<T> {}
unsafe impl<T: Send> Sync for LinkedBQ<T> {}
