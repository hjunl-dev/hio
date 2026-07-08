use std::{
    cell::UnsafeCell,
    sync::{
        Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use crate::core::{CachePadded, HioLastError, concurrent::BQ};

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

struct HeadSide<T> {
    head_lock: Mutex<()>,
    not_empty: Condvar,
    head: UnsafeCell<*mut Node<T>>,
}

struct TailSide<T> {
    tail_lock: Mutex<()>,
    not_full: Condvar,
    tail: UnsafeCell<*mut Node<T>>,
}

//
// Linked Blocking Queue
//

pub struct LinkedBQ<T: Send> {
    capacity: usize,
    count: CachePadded<AtomicUsize>,
    disposed: CachePadded<AtomicBool>,
    head_side: CachePadded<HeadSide<T>>,
    tail_side: CachePadded<TailSide<T>>,
}

impl<T: Send> LinkedBQ<T> {
    pub fn new(capacity: usize) -> Self {
        let dummy_ptr = Box::into_raw(Node::dummy());
        Self {
            capacity,
            count: CachePadded(AtomicUsize::new(0)),
            disposed: CachePadded(AtomicBool::new(false)),
            head_side: CachePadded(HeadSide {
                head_lock: Mutex::new(()),
                not_empty: Condvar::new(),
                head: UnsafeCell::new(dummy_ptr),
            }),
            tail_side: CachePadded(TailSide {
                tail_lock: Mutex::new(()),
                not_full: Condvar::new(),
                tail: UnsafeCell::new(dummy_ptr),
            }),
        }
    }

    unsafe fn en_q(&self, item: Box<Node<T>>) {
        let pp_old_tail = self.tail_side.tail.get();
        let p_new_tail = Box::into_raw(item);

        unsafe {
            // Link the new node to the end of the queue
            (**pp_old_tail).next = p_new_tail;
            // Update the tail pointer to point to the new node
            (*pp_old_tail) = p_new_tail;
        }
    }

    unsafe fn de_q(&self) -> Result<T, HioLastError> {
        let pp_old_head = self.head_side.head.get();

        unsafe {
            let p_new_head = (**pp_old_head).next;
            if p_new_head.is_null() {
                // dequeue from an empty queue, which should not happen if used correctly
                return Err(HioLastError::InvalidOperation);
            }

            let item = (*p_new_head).item.take();
            if item.is_none() {
                // This should not happen, as the new head should always have an item
                return Err(HioLastError::InvalidState);
            }
            // Move the item out of the node
            drop(Box::from_raw(*pp_old_head));
            // Update the head pointer to point to the new head
            (*pp_old_head) = p_new_head;
            Ok(item.unwrap())
        }
    }
}

impl<T: Send> BQ<T> for LinkedBQ<T> {
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
        todo!()
    }

    fn is_disposed(&self) -> bool {
        todo!()
    }
}

unsafe impl<T: Send> Send for LinkedBQ<T> {}
unsafe impl<T: Send> Sync for LinkedBQ<T> {}
