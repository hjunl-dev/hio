use std::{
    collections::VecDeque,
    sync::{
        Condvar, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::bq::{BQ, CondWaiters, Signals, ensure_capacity};
use hio_core::HioLastError;

//
// ArrayBQ impl
//

pub struct ArrayBQ<T: Send> {
    capacity: usize,
    disposed: AtomicBool,
    not_empty: Condvar,
    not_full: Condvar,
    push_waiters: CondWaiters,
    pop_waiters: CondWaiters,
    buf: Mutex<VecDeque<T>>,
}

impl<T: Send> ArrayBQ<T> {
    pub fn new(capacity: usize) -> Self {
        let capacity = ensure_capacity(capacity);
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
            push_waiters: CondWaiters::new(),
            pop_waiters: CondWaiters::new(),
            buf: Mutex::new(buf),
        }
    }

    #[inline]
    fn lock(&self) -> MutexGuard<'_, VecDeque<T>> {
        self.buf.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[inline]
    fn signal(&self, s: Signals) {
        if s.signal_not_empty {
            self.not_empty.notify_one();
        }
        if s.signal_not_full {
            self.not_full.notify_one();
        }
    }

    #[inline]
    fn is_full(&self, buf: &VecDeque<T>) -> bool {
        buf.len() >= self.capacity
    }

    fn en_q(&self, item: T, buf: &mut VecDeque<T>) -> Signals {
        let prev = buf.len();
        buf.push_back(item);
        Signals {
            // was empty, notify pop waiters
            signal_not_empty: prev == 0 && self.pop_waiters.any(),
            // still not-full after push, cadade to next producer
            signal_not_full: prev + 1 < self.capacity && self.push_waiters.any(),
        }
    }

    fn de_q(&self, buf: &mut VecDeque<T>) -> (Option<T>, Signals) {
        let prev = buf.len();
        let item = buf.pop_front();
        let s = Signals {
            // still non-empty after pop, cascade to next consumer
            signal_not_empty: prev > 1 && self.pop_waiters.any(),
            // was full, notify push waiters
            signal_not_full: prev == self.capacity && self.push_waiters.any(),
        };
        (item, s)
    }
}

impl<T: Send> BQ<T> for ArrayBQ<T> {
    fn push(&self, item: T) -> Result<(), (HioLastError, T)> {
        let mut g = self.lock();

        if !self.is_disposed() && self.is_full(&g) {
            let _wg = self.push_waiters.enter();
            g = self
                .not_full
                .wait_while(g, |g| !self.is_disposed() && self.is_full(&g))
                .unwrap_or_else(PoisonError::into_inner);
        }
        if self.is_disposed() {
            return Err((HioLastError::ResourceUnavailable, item));
        }

        let s = self.en_q(item, &mut g);
        drop(g);
        self.signal(s);
        Ok(())
    }

    fn try_push(&self, item: T) -> Result<(), (HioLastError, T)> {
        todo!()
    }

    fn push_timeout(&self, item: T, timeout: std::time::Duration) -> Result<(), (HioLastError, T)> {
        todo!()
    }

    fn pop(&self) -> Result<T, HioLastError> {
        let mut g = self.lock();

        if !self.is_disposed() && g.is_empty() {
            let _w = self.pop_waiters.enter();
            g = self
                .not_empty
                .wait_while(g, |b| !self.is_disposed() && b.is_empty())
                .unwrap_or_else(PoisonError::into_inner);
        }
        if g.is_empty() {
            debug_assert!(self.is_disposed());
            return Err(HioLastError::ResourceUnavailable);
        }

        let (item, sig) = self.de_q(&mut g);
        drop(g);
        self.signal(sig);
        Ok(item.unwrap())
    }

    fn try_pop(&self) -> Result<T, HioLastError> {
        todo!()
    }

    fn pop_timeout(&self, timeout: std::time::Duration) -> Result<T, HioLastError> {
        todo!()
    }

    fn drain(&self) -> Vec<T> {
        todo!()
    }

    fn dispose(&self) {
        if self
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            {
                let _g = self.lock();
            }
            self.not_empty.notify_all();
            self.not_full.notify_all();
        }
    }

    fn size(&self) -> usize {
        self.lock().len()
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

//
// Tests for ArrayBQ
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

    // 1. 단일 스레드 FIFO 순서
    #[test]
    fn fifo_order_single_thread() {
        let q = ArrayBQ::<i32>::new(4);
        for i in 0..4 {
            q.push(i).unwrap();
        }
        assert_eq!(q.size(), 4);
        for i in 0..4 {
            assert_eq!(q.pop().unwrap(), i);
        }
        assert_eq!(q.size(), 0);
    }

    // 2. capacity / size
    #[test]
    fn capacity_and_size() {
        let q = ArrayBQ::<u8>::new(2);
        assert_eq!(q.capacity(), 2);
        assert_eq!(q.size(), 0);
        q.push(1).unwrap();
        q.push(2).unwrap();
        assert_eq!(q.size(), 2);
        q.pop().unwrap();
        assert_eq!(q.size(), 1);
    }

    // 3. full일 때 push 블로킹 → pop 후 재개
    #[test]
    fn push_blocks_when_full() {
        let q = Arc::new(ArrayBQ::<i32>::new(1));
        q.push(10).unwrap(); // 가득 참

        let progressed = Arc::new(AtomicBool::new(false));
        let (q2, p2) = (q.clone(), progressed.clone());
        let h = thread::spawn(move || {
            q2.push(20).unwrap(); // full → 블로킹
            p2.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            !progressed.load(Ordering::SeqCst),
            "push did not block on a full queue"
        );

        assert_eq!(q.pop().unwrap(), 10); // 슬롯 확보 → pusher 깨어남
        h.join().unwrap();
        assert!(
            progressed.load(Ordering::SeqCst),
            "blocked push was not resumed after a pop freed capacity"
        );
        assert_eq!(q.pop().unwrap(), 20);
    }

    // 4. empty일 때 pop 블로킹 → push 후 재개
    #[test]
    fn pop_blocks_when_empty() {
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop().unwrap());

        thread::sleep(Duration::from_millis(50));
        q.push(42).unwrap();
        assert_eq!(h.join().unwrap(), 42);
    }

    // 5. dispose drain 시맨틱: 남은 원소는 소진, 이후 ResourceUnavailable
    #[test]
    fn dispose_drains_remaining_items() {
        let q = ArrayBQ::<i32>::new(8);
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

    // 6. dispose 후 push 실패 (에러와 함께 아이템 반환)
    #[test]
    fn push_fails_after_dispose() {
        let q = ArrayBQ::<i32>::new(4);
        q.dispose();

        let (err, item) = q.push(1).expect_err("push must fail after dispose");
        assert!(
            matches!(err, HioLastError::ResourceUnavailable),
            "unexpected error kind: {err:?}"
        );
        assert_eq!(item, 1, "failed push must return the original item");
    }

    // 7. dispose가 블로킹된 pop을 깨움
    #[test]
    fn dispose_wakes_blocked_pop() {
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop());

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        assert!(
            matches!(h.join().unwrap(), Err(HioLastError::ResourceUnavailable)),
            "dispose must wake a blocked pop with ResourceUnavailable"
        );
    }

    // 8. dispose가 블로킹된 push를 깨움
    #[test]
    fn dispose_wakes_blocked_push() {
        let q = Arc::new(ArrayBQ::<i32>::new(1));
        q.push(1).unwrap(); // full
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

    // 9. 실패한 push는 아이템을 조기 drop하지 않고 그대로 반환
    #[test]
    fn failed_push_returns_item_without_dropping() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = ArrayBQ::<DropCounter>::new(4);
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

    // 10. 블로킹 경로에서 깨어난 push도 아이템을 반환
    #[test]
    fn failed_push_on_blocked_path_returns_item() {
        let counter = Arc::new(AtomicUsize::new(0));
        let q = Arc::new(ArrayBQ::<DropCounter>::new(1));
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

    // 11. MPMC 정확성: 유실/중복 없이 합 보존 (cascade notify 검증)
    #[test]
    fn mpmc_no_loss_no_duplication() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const PER_PRODUCER: usize = 10_000;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;
        const EXPECTED_SUM: usize = TOTAL * (TOTAL + 1) / 2; // 1..=TOTAL 가우스 합

        let q = Arc::new(ArrayBQ::<usize>::new(64)); // 작은 capacity로 블로킹 유발
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
                        let v = p * PER_PRODUCER + i + 1; // 1..=TOTAL 유일값
                        q.push(v).unwrap();
                    }
                })
            })
            .collect();

        for h in producers {
            h.join().unwrap();
        }
        q.dispose(); // 남은 원소 drain 후 consumer 종료 유도

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

    // 12. 무제한 큐(capacity == usize::MAX)는 push가 블로킹되지 않음
    #[test]
    fn unbounded_never_blocks_push() {
        let q = ArrayBQ::<usize>::new(usize::MAX);
        for i in 0..1000 {
            q.push(i).unwrap();
        }
        assert_eq!(q.size(), 1000);
        assert_eq!(q.pop().unwrap(), 0);
    }

    // 13. 미소비 아이템은 큐 drop 시 정리
    #[test]
    fn no_leak_on_drop_with_pending_items() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = ArrayBQ::<DropCounter>::new(16);
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

    // 14. dispose 후 부분 drain, 나머지는 drop에서 정리
    #[test]
    fn no_leak_on_dispose_partial_drain_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = ArrayBQ::<DropCounter>::new(16);
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
