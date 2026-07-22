use std::{
    cell::UnsafeCell,
    sync::{
        Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::{BQ, CachePadded, CondWaiters};
use hio_core::HioLastError;

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
    pop_lock: Mutex<CondWaiters>,
    not_empty: Condvar,
    head: UnsafeCell<*mut Node<T>>,
}

struct PushSide<T> {
    push_lock: Mutex<CondWaiters>,
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
                pop_lock: Mutex::new(CondWaiters::default()),
                not_empty: Condvar::new(),
                head: UnsafeCell::new(dummy_ptr),
            }),
            push_side: CachePadded(PushSide {
                push_lock: Mutex::new(CondWaiters::default()),
                not_full: Condvar::new(),
                tail: UnsafeCell::new(dummy_ptr),
            }),
        }
    }

    unsafe fn en_q(&self, item: Box<Node<T>>, push_lock_g: MutexGuard<'_, CondWaiters>) {
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
        if prev_count + 1 < self.capacity && push_lock_g.any() {
            self.push_side.not_full.notify_one();
        }
        // was empty (empty -> 1), notify pop waiters);
        drop(push_lock_g);
        if prev_count == 0 {
            let pop_lock_g: MutexGuard<'_, CondWaiters> = self
                .pop_side
                .pop_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if pop_lock_g.any() {
                self.pop_side.not_empty.notify_one();
            }
        }
    }

    unsafe fn de_q(&self, pop_lock_g: MutexGuard<'_, CondWaiters>) -> Result<T, HioLastError> {
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
        if prev_count > 1 && pop_lock_g.any() {
            self.pop_side.not_empty.notify_one();
        }
        // was full (full -> not full), notify push waiters
        drop(pop_lock_g);
        if prev_count == self.capacity {
            let push_lock_g = self
                .push_side
                .push_lock
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if push_lock_g.any() {
                self.push_side.not_full.notify_one();
            }
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
        if let Ok(mut g) = self.push_side.push_lock.lock() {
            g.enter();

            match self
                .push_side
                .not_full
                .wait_while(g, |_g| !self.is_disposed() && self.is_full())
            {
                Ok(mut g) => {
                    g.leave();
                    if self.is_disposed() {
                        return Err(HioLastError::ResourceUnavailable);
                    }
                    unsafe {
                        self.en_q(Node::new(Some(item)), g);
                    }
                    return Ok(());
                }
                Err(e) => {
                    e.into_inner().leave();
                    return Err(HioLastError::MutexPoisoned);
                }
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    fn pop(&self) -> Result<T, HioLastError> {
        if let Ok(mut g) = self.pop_side.pop_lock.lock() {
            g.enter();

            match self
                .pop_side
                .not_empty
                .wait_while(g, |_g| !self.is_disposed() && self.is_empty())
            {
                Ok(mut g) => {
                    g.leave();
                    if self.is_disposed() && self.is_empty() {
                        return Err(HioLastError::ResourceUnavailable);
                    }
                    unsafe {
                        return self.de_q(g);
                    }
                }
                Err(e) => {
                    e.into_inner().leave();
                    return Err(HioLastError::MutexPoisoned);
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
                    .pop_lock
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                self.pop_side.not_empty.notify_all();
            }
            {
                let _g = self
                    .push_side
                    .push_lock
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BQ;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::Duration;

    struct DropCounter {
        counter: Arc<AtomicUsize>,
    }
    impl DropCounter {
        fn new(counter: &Arc<AtomicUsize>) -> Self {
            Self {
                counter: counter.clone(),
            }
        }
    }
    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn fifo_order_single_thread() {
        let q = LinkedBQ::<i32>::new(4);
        for i in 0..4 {
            q.push(i).unwrap();
        }
        assert_eq!(q.size(), 4);
        for i in 0..4 {
            assert_eq!(q.pop().unwrap(), i);
        }
        assert_eq!(q.size(), 0);
    }

    #[test]
    fn capacity_and_size() {
        let q = LinkedBQ::<u8>::new(2);
        assert_eq!(q.capacity(), 2);
        assert_eq!(q.size(), 0);
        q.push(1).unwrap();
        q.push(2).unwrap();
        assert_eq!(q.size(), 2);
        q.pop().unwrap();
        assert_eq!(q.size(), 1);
    }

    #[test]
    fn push_blocks_when_full() {
        let q = Arc::new(LinkedBQ::<i32>::new(1));
        q.push(10).unwrap();

        let progressed = Arc::new(AtomicBool::new(false));
        let (q2, p2) = (q.clone(), progressed.clone());
        let h = thread::spawn(move || {
            q2.push(20).unwrap();
            p2.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!progressed.load(Ordering::SeqCst), "push가 블로킹되지 않음");

        assert_eq!(q.pop().unwrap(), 10);
        h.join().unwrap();
        assert!(progressed.load(Ordering::SeqCst));
        assert_eq!(q.pop().unwrap(), 20);
    }

    #[test]
    fn pop_blocks_when_empty() {
        let q = Arc::new(LinkedBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop().unwrap());

        thread::sleep(Duration::from_millis(50));
        q.push(42).unwrap();
        assert_eq!(h.join().unwrap(), 42);
    }

    #[test]
    fn dispose_drains_remaining_items() {
        let q = LinkedBQ::<i32>::new(8);
        q.push(1).unwrap();
        q.push(2).unwrap();
        q.push(3).unwrap();

        q.dispose();
        assert!(q.is_disposed());

        assert_eq!(q.pop().unwrap(), 1);
        assert_eq!(q.pop().unwrap(), 2);
        assert_eq!(q.pop().unwrap(), 3);
        assert!(matches!(q.pop(), Err(HioLastError::ResourceUnavailable)));
    }

    #[test]
    fn push_fails_after_dispose() {
        let q = LinkedBQ::<i32>::new(4);
        q.dispose();
        assert!(matches!(q.push(1), Err(HioLastError::ResourceUnavailable)));
    }

    #[test]
    fn dispose_wakes_blocked_pop() {
        let q = Arc::new(LinkedBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop());

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        assert!(matches!(
            h.join().unwrap(),
            Err(HioLastError::ResourceUnavailable)
        ));
    }

    #[test]
    fn dispose_wakes_blocked_push() {
        let q = Arc::new(LinkedBQ::<i32>::new(1));
        q.push(1).unwrap();
        let q2 = q.clone();
        let h = thread::spawn(move || q2.push(2));

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        assert!(matches!(
            h.join().unwrap(),
            Err(HioLastError::ResourceUnavailable)
        ));
    }

    #[test]
    fn mpmc_no_loss_no_duplication() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const PER_PRODUCER: usize = 10_000;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;
        const EXPECTED_SUM: usize = TOTAL * (TOTAL + 1) / 2;

        let q = Arc::new(LinkedBQ::<usize>::new(64));
        let sum = Arc::new(AtomicUsize::new(0));
        let cnt = Arc::new(AtomicUsize::new(0));

        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|_| {
                let (q, sum, cnt) = (q.clone(), sum.clone(), cnt.clone());
                thread::spawn(move || {
                    while let Ok(v) = q.pop() {
                        sum.fetch_add(v, Ordering::Relaxed);
                        cnt.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let q = q.clone();
                thread::spawn(move || {
                    for i in 0..PER_PRODUCER {
                        let v = p * PER_PRODUCER + i + 1;
                        q.push(v).unwrap();
                    }
                })
            })
            .collect();

        for h in producers {
            h.join().unwrap();
        }
        q.dispose();

        for h in consumers {
            h.join().unwrap();
        }

        assert_eq!(
            cnt.load(Ordering::Relaxed),
            TOTAL,
            "소비 개수 불일치(유실/중복)"
        );
        assert_eq!(
            sum.load(Ordering::Relaxed),
            EXPECTED_SUM,
            "합 불일치(유실/중복)"
        );
    }

    #[test]
    fn unbounded_never_blocks_push() {
        let q = LinkedBQ::<usize>::new(usize::MAX);
        for i in 0..1000 {
            q.push(i).unwrap();
        }
        assert_eq!(q.size(), 1000);
        assert_eq!(q.pop().unwrap(), 0);
    }

    #[test]
    fn no_leak_on_full_drain_then_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = LinkedBQ::<DropCounter>::new(16);
            for _ in 0..8 {
                q.push(DropCounter::new(&counter)).unwrap();
            }
            for _ in 0..8 {
                drop(q.pop().unwrap());
            }
        }
        assert_eq!(counter.load(Ordering::SeqCst), 8, "누수 또는 이중해제");
    }

    #[test]
    fn no_leak_on_drop_with_pending_items() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = LinkedBQ::<DropCounter>::new(16);
            for _ in 0..8 {
                q.push(DropCounter::new(&counter)).unwrap();
            }
        }
        assert_eq!(counter.load(Ordering::SeqCst), 8, "미소비 아이템 누수");
    }

    #[test]
    fn no_leak_on_dispose_partial_drain_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = LinkedBQ::<DropCounter>::new(16);
            for _ in 0..10 {
                q.push(DropCounter::new(&counter)).unwrap();
            }
            q.dispose();
            for _ in 0..4 {
                drop(q.pop().unwrap());
            }
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            10,
            "부분 drain 후 총 해제 불일치"
        );
    }
}
