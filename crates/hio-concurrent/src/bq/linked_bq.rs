use std::{
    cell::UnsafeCell,
    sync::{
        Condvar, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use hio_core::HioLastError;

use crate::bq::{BQ, CachePadded, CondWaiters, ensure_capacity};

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
    lock: Mutex<()>,
    not_empty: Condvar,
    waiters: CondWaiters,
    head: UnsafeCell<*mut Node<T>>,
}

struct PushSide<T> {
    lock: Mutex<()>,
    not_full: Condvar,
    waiters: CondWaiters,
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
        let capacity = ensure_capacity(capacity);
        let dummy_ptr = Box::into_raw(Node::dummy());
        Self {
            capacity,
            count: CachePadded(AtomicUsize::new(0)),
            disposed: CachePadded(AtomicBool::new(false)),
            pop_side: CachePadded(PopSide {
                lock: Mutex::new(()),
                not_empty: Condvar::new(),
                waiters: CondWaiters::new(),
                head: UnsafeCell::new(dummy_ptr),
            }),
            push_side: CachePadded(PushSide {
                lock: Mutex::new(()),
                not_full: Condvar::new(),
                waiters: CondWaiters::new(),
                tail: UnsafeCell::new(dummy_ptr),
            }),
        }
    }

    #[inline]
    fn lock_push(&self) -> MutexGuard<'_, ()> {
        self.push_side
            .lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn lock_pop(&self) -> MutexGuard<'_, ()> {
        self.pop_side
            .lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.count.load(Ordering::Acquire) == 0
    }

    #[inline]
    fn is_full(&self) -> bool {
        self.count.load(Ordering::Acquire) >= self.capacity
    }

    fn signal_not_empty(&self) {
        let _g = self.lock_pop();
        if self.pop_side.waiters.any() {
            self.pop_side.not_empty.notify_one();
        }
    }

    fn signal_not_full(&self) {
        let _g = self.lock_push();
        if self.push_side.waiters.any() {
            self.push_side.not_full.notify_one();
        }
    }

    unsafe fn en_q(&self, node: Box<Node<T>>, g: MutexGuard<'_, ()>) {
        debug_assert!(self.count.load(Ordering::Relaxed) < self.capacity);

        let pp_tail = self.push_side.tail.get();
        let p_new_tail = Box::into_raw(node);

        unsafe {
            (**pp_tail).next = p_new_tail;
            *pp_tail = p_new_tail;
        }
        let prev = self.count.fetch_add(1, Ordering::Release);
        // still not-full after push, cadade to next producer
        if prev + 1 < self.capacity && self.push_side.waiters.any() {
            self.push_side.not_full.notify_one();
        }
        drop(g);
        // was empty, notify pop waiters
        if prev == 0 {
            self.signal_not_empty();
        }
    }

    unsafe fn unlink_head(&self) -> T {
        let pp_head = self.pop_side.head.get();
        unsafe {
            let p_new_head = (**pp_head).next;
            debug_assert!(!p_new_head.is_null(), "count > 0 인데 next 가 null 이다");

            let item = (*p_new_head)
                .item
                .take()
                .expect("실노드는 항상 item 을 가진다");

            drop(Box::from_raw(*pp_head));
            *pp_head = p_new_head;
            item
        }
    }

    unsafe fn de_q(&self, g: MutexGuard<'_, ()>) -> T {
        let item = unsafe { self.unlink_head() };
        let prev = self.count.fetch_sub(1, Ordering::Release);
        // still non-empty after pop, cascade to next consumer
        if prev > 1 && self.pop_side.waiters.any() {
            self.pop_side.not_empty.notify_one();
        }
        drop(g);
        // was full, notify push waiters
        if prev == self.capacity {
            self.signal_not_full();
        }
        item
    }
}

impl<T: Send> BQ<T> for LinkedBQ<T> {
    fn push(&self, item: T) -> Result<(), (HioLastError, T)> {
        let mut g = self.lock_push();

        if !self.is_disposed() && self.is_full() {
            let _wg = self.push_side.waiters.enter();
            g = self
                .push_side
                .not_full
                .wait_while(g, |_| !self.is_disposed() && self.is_full())
                .unwrap_or_else(PoisonError::into_inner);
        }
        if self.is_disposed() {
            return Err((HioLastError::ResourceUnavailable, item));
        }

        unsafe { self.en_q(Node::new(Some(item)), g) };
        Ok(())
    }

    fn try_push(&self, item: T) -> Result<(), (HioLastError, T)> {
        let g = self.lock_push();

        if self.is_disposed() {
            return Err((HioLastError::ResourceUnavailable, item));
        }
        if self.is_full() {
            return Err((HioLastError::WouldBlock, item));
        }

        unsafe { self.en_q(Node::new(Some(item)), g) };
        Ok(())
    }

    fn push_timeout(&self, item: T, dur: Duration) -> Result<(), (HioLastError, T)> {
        let mut g = self.lock_push();
        let mut timed_out = false;

        if !self.is_disposed() && self.is_full() {
            let _wg = self.push_side.waiters.enter();
            let (guard, timeout_result) = self
                .push_side
                .not_full
                .wait_timeout_while(g, dur, |_| !self.is_disposed() && self.is_full())
                .unwrap_or_else(PoisonError::into_inner);
            g = guard;
            timed_out = timeout_result.timed_out();
        }
        if timed_out {
            return Err((HioLastError::Timeout, item));
        }
        if self.is_disposed() {
            return Err((HioLastError::ResourceUnavailable, item));
        }

        unsafe { self.en_q(Node::new(Some(item)), g) };
        Ok(())
    }

    fn pop(&self) -> Result<T, HioLastError> {
        let mut g = self.lock_pop();

        if !self.is_disposed() && self.is_empty() {
            let _wg = self.pop_side.waiters.enter();
            g = self
                .pop_side
                .not_empty
                .wait_while(g, |_| !self.is_disposed() && self.is_empty())
                .unwrap_or_else(PoisonError::into_inner);
        }
        if self.is_empty() {
            debug_assert!(self.is_disposed());
            return Err(HioLastError::ResourceUnavailable);
        }

        Ok(unsafe { self.de_q(g) })
    }

    fn try_pop(&self) -> Result<T, HioLastError> {
        let g = self.lock_pop();

        if self.is_empty() {
            return Err(if self.is_disposed() {
                HioLastError::ResourceUnavailable
            } else {
                HioLastError::WouldBlock
            });
        }

        Ok(unsafe { self.de_q(g) })
    }

    fn pop_timeout(&self, dur: Duration) -> Result<T, HioLastError> {
        let mut g = self.lock_pop();
        let mut timed_out = false;

        if !self.is_disposed() && self.is_empty() {
            let _wg = self.pop_side.waiters.enter();
            let (guard, timeout_result) = self
                .pop_side
                .not_empty
                .wait_timeout_while(g, dur, |_| !self.is_disposed() && self.is_empty())
                .unwrap_or_else(PoisonError::into_inner);
            g = guard;
            timed_out = timeout_result.timed_out();
        }
        if timed_out {
            return Err(HioLastError::Timeout);
        }
        if self.is_empty() {
            debug_assert!(self.is_disposed());
            return Err(HioLastError::ResourceUnavailable);
        }

        Ok(unsafe { self.de_q(g) })
    }

    fn drain(&self) -> Vec<T> {
        let g = self.lock_pop();

        let n = self.count.load(Ordering::Acquire);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(unsafe { self.unlink_head() });
        }
        if n > 0 {
            self.count.fetch_sub(n, Ordering::Release);
        }
        drop(g);

        if n > 0 {
            let _pg = self.lock_push();
            if self.push_side.waiters.any() {
                self.push_side.not_full.notify_all();
            }
        }
        out
    }

    fn dispose(&self) {
        if self
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            {
                let _g = self.lock_pop();
                self.pop_side.not_empty.notify_all();
            }
            {
                let _g = self.lock_push();
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

//
// Tests for LinkedBQ
//

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::thread;
    use std::time::Duration;

    #[derive(Debug)]
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
        assert!(
            !progressed.load(Ordering::SeqCst),
            "push did not block on a full queue"
        );

        assert_eq!(q.pop().unwrap(), 10);
        h.join().unwrap();
        assert!(
            progressed.load(Ordering::SeqCst),
            "blocked push was not resumed after a pop freed capacity"
        );
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
        assert!(
            matches!(q.pop(), Err(HioLastError::ResourceUnavailable)),
            "pop on a drained, disposed queue must fail"
        );
    }

    #[test]
    fn push_fails_after_dispose() {
        let q = LinkedBQ::<i32>::new(4);
        q.dispose();

        let (err, item) = q.push(1).expect_err("push must fail after dispose");
        assert!(
            matches!(err, HioLastError::ResourceUnavailable),
            "unexpected error kind: {err:?}"
        );
        assert_eq!(item, 1, "failed push must return the original item");
    }

    #[test]
    fn dispose_wakes_blocked_pop() {
        let q = Arc::new(LinkedBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop());

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        assert!(
            matches!(h.join().unwrap(), Err(HioLastError::ResourceUnavailable)),
            "dispose must wake a blocked pop with ResourceUnavailable"
        );
    }

    #[test]
    fn dispose_wakes_blocked_push() {
        let q = Arc::new(LinkedBQ::<i32>::new(1));
        q.push(1).unwrap();
        let q2 = q.clone();
        let h = thread::spawn(move || q2.push(2));

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        let res = h.join().unwrap();
        assert!(
            matches!(res, Err((HioLastError::ResourceUnavailable, 2))),
            "a push woken by dispose must return the error together with its item"
        );
    }

    #[test]
    fn failed_push_returns_item_without_dropping() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = LinkedBQ::<DropCounter>::new(4);
            q.dispose();

            let (err, returned) = q
                .push(DropCounter::new(&counter))
                .expect_err("push must fail after dispose");

            assert!(
                matches!(err, HioLastError::ResourceUnavailable),
                "unexpected error kind: {err:?}"
            );
            assert_eq!(
                counter.load(Ordering::SeqCst),
                0,
                "failed push dropped the item instead of returning it"
            );

            drop(returned);
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "returned item was not dropped exactly once"
            );
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "double free or leak after queue drop"
        );
    }

    #[test]
    fn failed_push_on_blocked_path_returns_item() {
        let counter = Arc::new(AtomicUsize::new(0));
        let q = Arc::new(LinkedBQ::<DropCounter>::new(1));
        q.push(DropCounter::new(&counter)).unwrap();

        let (q2, c2) = (q.clone(), counter.clone());
        let h = thread::spawn(move || q2.push(DropCounter::new(&c2)));

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        let (err, returned) = h
            .join()
            .unwrap()
            .expect_err("a push woken by dispose must fail");
        assert!(
            matches!(err, HioLastError::ResourceUnavailable),
            "unexpected error kind: {err:?}"
        );
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "item was lost on the blocking push path"
        );

        drop(returned);
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "returned item was not dropped exactly once"
        );
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
            "consumed count mismatch (item loss or duplication)"
        );
        assert_eq!(
            sum.load(Ordering::Relaxed),
            EXPECTED_SUM,
            "checksum mismatch (item loss or duplication)"
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
        assert_eq!(
            counter.load(Ordering::SeqCst),
            8,
            "leak or double free after a full drain"
        );
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
        assert_eq!(
            counter.load(Ordering::SeqCst),
            8,
            "unconsumed items leaked when the queue was dropped"
        );
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
            "drop count mismatch after dispose with a partial drain"
        );
    }
}
