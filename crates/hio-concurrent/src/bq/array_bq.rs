use std::{
    collections::VecDeque,
    sync::{
        Condvar, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
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

    fn en_q_commit(&self, item: T, mut g: MutexGuard<'_, VecDeque<T>>) {
        let prev = g.len();
        g.push_back(item);
        let s = Signals {
            // was empty, notify pop waiters
            signal_not_empty: prev == 0 && self.pop_waiters.any(),
            // still not-full after push, cadade to next producer
            signal_not_full: prev + 1 < self.capacity && self.push_waiters.any(),
        };
        drop(g);
        self.signal(s);
    }

    fn de_q_commit(&self, mut g: MutexGuard<'_, VecDeque<T>>) -> T {
        let prev = g.len();
        let item = g.pop_front().expect("caller guarantees non-empty");
        let s = Signals {
            // still non-empty after pop, cascade to next consumer
            signal_not_empty: prev > 1 && self.pop_waiters.any(),
            // was full, notify push waiters
            signal_not_full: prev == self.capacity && self.push_waiters.any(),
        };
        drop(g);
        self.signal(s);
        item
    }
}

impl<T: Send> BQ<T> for ArrayBQ<T> {
    fn push(&self, item: T) -> Result<(), (HioLastError, T)> {
        let mut g = self.lock();

        if !self.is_disposed() && self.is_full(&g) {
            let _wg = self.push_waiters.enter();
            g = self
                .not_full
                .wait_while(g, |buf| !self.is_disposed() && self.is_full(&buf))
                .unwrap_or_else(PoisonError::into_inner);
        }
        if self.is_disposed() {
            return Err((HioLastError::ResourceUnavailable, item));
        }
        // enqueue & drop guard
        self.en_q_commit(item, g);
        Ok(())
    }

    fn try_push(&self, item: T) -> Result<(), (HioLastError, T)> {
        let g = self.lock();

        if self.is_disposed() {
            return Err((HioLastError::ResourceUnavailable, item));
        }
        if self.is_full(&g) {
            return Err((HioLastError::WouldBlock, item));
        }
        // enqueue & drop guard
        self.en_q_commit(item, g);
        Ok(())
    }

    fn push_timeout(&self, item: T, dur: Duration) -> Result<(), (HioLastError, T)> {
        let mut g = self.lock();
        let mut timed_out = false;

        if !self.is_disposed() && self.is_full(&g) {
            let _wg = self.push_waiters.enter();
            let (guard, timeout_result) = self
                .not_full
                .wait_timeout_while(g, dur, |buf| !self.is_disposed() && self.is_full(&buf))
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
        // enqueue & drop guard
        self.en_q_commit(item, g);
        Ok(())
    }

    fn pop(&self) -> Result<T, HioLastError> {
        let mut g = self.lock();

        if !self.is_disposed() && g.is_empty() {
            let _wg = self.pop_waiters.enter();
            g = self
                .not_empty
                .wait_while(g, |buf| !self.is_disposed() && buf.is_empty())
                .unwrap_or_else(PoisonError::into_inner);
        }
        if g.is_empty() {
            debug_assert!(self.is_disposed());
            return Err(HioLastError::ResourceUnavailable);
        }

        let item = self.de_q_commit(g);
        Ok(item)
    }

    fn try_pop(&self) -> Result<T, HioLastError> {
        let g = self.lock();

        if g.is_empty() {
            let err = if self.is_disposed() {
                HioLastError::ResourceUnavailable
            } else {
                HioLastError::WouldBlock
            };
            return Err(err);
        }

        let item = self.de_q_commit(g);
        Ok(item)
    }

    fn pop_timeout(&self, dur: Duration) -> Result<T, HioLastError> {
        let mut g = self.lock();
        let mut timed_out = false;

        if !self.is_disposed() && g.is_empty() {
            let _wg = self.pop_waiters.enter();
            let (guard, timeout_result) = self
                .not_empty
                .wait_timeout_while(g, dur, |buf| !self.is_disposed() && buf.is_empty())
                .unwrap_or_else(PoisonError::into_inner);
            g = guard;
            timed_out = timeout_result.timed_out();
        }
        if timed_out {
            return Err(HioLastError::Timeout);
        }
        if self.is_disposed() && g.is_empty() {
            return Err(HioLastError::ResourceUnavailable);
        }

        let item = self.de_q_commit(g);
        Ok(item)
    }

    fn drain(&self) -> Vec<T> {
        let mut g = self.lock();
        let had = g.len();
        let items = g.drain(..).collect();
        let wake_producers = had > 0 && self.push_waiters.any();
        drop(g);

        if wake_producers {
            self.not_full.notify_all();
        }
        items
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
    use std::thread;
    use std::time::Duration;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Instant,
    };

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

    // ===========================================================================
    // 기존 `mod tests` 에 이어 붙일 테스트들.
    //
    // 상단 use 에 다음이 필요하다:
    //     use std::time::Instant;
    // ===========================================================================

    // --- try_push / try_pop ---------------------------------------------------

    // 15. try_push 는 가득 찬 큐에서 즉시 실패하고 아이템을 돌려준다
    #[test]
    fn try_push_fails_when_full() {
        let q = ArrayBQ::<i32>::new(1);
        q.push(1).unwrap();

        let (err, item) = q
            .try_push(2)
            .expect_err("try_push must fail on a full queue");
        assert!(
            matches!(err, HioLastError::WouldBlock),
            "unexpected error kind: {err:?}"
        );
        assert_eq!(item, 2, "failed try_push must return the original item");
        assert_eq!(q.size(), 1, "failed try_push must not modify the queue");
    }

    // 16. try_pop 은 빈 큐에서 즉시 실패한다
    #[test]
    fn try_pop_fails_when_empty() {
        let q = ArrayBQ::<i32>::new(4);
        assert!(matches!(q.try_pop(), Err(HioLastError::WouldBlock)));

        q.push(7).unwrap();
        assert_eq!(q.try_pop().unwrap(), 7);
        assert!(matches!(q.try_pop(), Err(HioLastError::WouldBlock)));
    }

    // 17. dispose 후 try_* 는 WouldBlock 이 아니라 ResourceUnavailable
    #[test]
    fn try_ops_report_resource_unavailable_after_dispose() {
        let q = ArrayBQ::<i32>::new(4);
        q.dispose();

        let (err, item) = q.try_push(1).expect_err("try_push must fail after dispose");
        assert!(
            matches!(err, HioLastError::ResourceUnavailable),
            "dispose must take precedence over WouldBlock: {err:?}"
        );
        assert_eq!(item, 1);
        assert!(matches!(
            q.try_pop(),
            Err(HioLastError::ResourceUnavailable)
        ));
    }

    // 18. dispose 후에도 try_pop 은 잔여 아이템을 배출한다 (drain semantics)
    #[test]
    fn try_pop_drains_remaining_items_after_dispose() {
        let q = ArrayBQ::<i32>::new(4);
        q.push(1).unwrap();
        q.push(2).unwrap();
        q.dispose();

        assert_eq!(q.try_pop().unwrap(), 1);
        assert_eq!(q.try_pop().unwrap(), 2);
        assert!(matches!(
            q.try_pop(),
            Err(HioLastError::ResourceUnavailable)
        ));
    }

    // 19. try_push 도 cascade 에 참여해 블로킹된 pop 을 깨운다
    #[test]
    fn try_push_wakes_blocked_pop() {
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop().unwrap());

        thread::sleep(Duration::from_millis(50));
        q.try_push(42).map_err(|(e, _)| e).unwrap();

        assert_eq!(
            h.join().unwrap(),
            42,
            "try_push must signal not_empty just like push"
        );
    }

    // 20. try_pop 도 cascade 에 참여해 블로킹된 push 를 깨운다
    #[test]
    fn try_pop_wakes_blocked_push() {
        let q = Arc::new(ArrayBQ::<i32>::new(1));
        q.push(1).unwrap(); // full

        let q2 = q.clone();
        let h = thread::spawn(move || q2.push(2));

        thread::sleep(Duration::from_millis(50));
        assert_eq!(q.try_pop().unwrap(), 1);

        h.join()
            .unwrap()
            .expect("try_pop must signal not_full just like pop");
        assert_eq!(q.pop().unwrap(), 2);
    }

    // --- 타임아웃 -------------------------------------------------------------

    // 21. pop_timeout 은 빈 큐에서 지정 시간만큼 대기한 뒤 만료된다
    //
    // 술어에서 `!` 가 빠지면 즉시 반환하면서 de_q_commit 이 빈 버퍼에서
    // 패닉하므로 여기서 잡힌다.
    #[test]
    fn pop_timeout_expires_on_empty_queue() {
        let q = ArrayBQ::<i32>::new(4);
        let dur = Duration::from_millis(120);

        let start = Instant::now();
        let res = q.pop_timeout(dur);
        let elapsed = start.elapsed();

        assert!(
            matches!(res, Err(HioLastError::Timeout)),
            "expected Timeout, got {res:?}"
        );
        // 플랫폼 타이머 해상도를 고려해 약간의 여유를 둔다.
        assert!(
            elapsed >= dur - Duration::from_millis(15),
            "pop_timeout returned too early: {elapsed:?}"
        );
    }

    // 22. push_timeout 은 가득 찬 큐에서 만료되며 **아이템을 돌려준다**
    //
    // 이 return 경로는 다른 어떤 테스트도 밟지 않는다.
    #[test]
    fn push_timeout_expires_and_returns_the_item() {
        let counter = Arc::new(AtomicUsize::new(0));
        let q = ArrayBQ::<DropCounter>::new(1);
        q.push(DropCounter::new(&counter)).unwrap(); // full

        let dur = Duration::from_millis(120);
        let start = Instant::now();
        let (err, returned) = q
            .push_timeout(DropCounter::new(&counter), dur)
            .expect_err("push_timeout must fail on a full queue");
        let elapsed = start.elapsed();

        assert!(
            matches!(err, HioLastError::Timeout),
            "unexpected error kind: {err:?}"
        );
        assert!(
            elapsed >= dur - Duration::from_millis(15),
            "push_timeout returned too early: {elapsed:?}"
        );
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "timed-out push dropped the item instead of returning it"
        );

        drop(returned);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    // 23. 데드라인 전에 아이템이 도착하면 즉시 반환한다 (풀 대기하지 않음)
    #[test]
    fn pop_timeout_returns_early_when_item_arrives() {
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        let q2 = q.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            q2.push(99).unwrap();
        });

        let start = Instant::now();
        let v = q
            .pop_timeout(Duration::from_secs(5))
            .expect("item arrived well before the deadline");
        let elapsed = start.elapsed();

        assert_eq!(v, 99);
        assert!(
            elapsed < Duration::from_secs(1),
            "pop_timeout waited for the full duration instead of waking on signal: {elapsed:?}"
        );
    }

    // 24. push_timeout 은 capacity 를 절대 넘기지 않는다
    //
    // 술어가 뒤집히면 en_q_commit 이 가득 찬 VecDeque 에 push 해서
    // 용량 계약이 조용히 깨진다. (en_q_commit 의 debug_assert 와 함께 쓸 것)
    #[test]
    fn push_timeout_never_exceeds_capacity() {
        const CAP: usize = 2;
        let q = ArrayBQ::<usize>::new(CAP);
        for i in 0..CAP {
            q.push(i).unwrap();
        }

        for _ in 0..3 {
            let res = q.push_timeout(999, Duration::from_millis(30));
            assert!(matches!(res, Err((HioLastError::Timeout, 999))));
            assert!(
                q.size() <= CAP,
                "queue grew past its capacity: {} > {CAP}",
                q.size()
            );
        }
    }

    // 25. 타임아웃 대기 중 dispose 되면 Timeout 이 아니라 ResourceUnavailable
    //
    // 검사 순서(timed_out → is_disposed)와 wait_timeout_while 의 보장에
    // 의존하는 부분이다. wait_timeout 으로 바꾸면 깨진다.
    #[test]
    fn dispose_wakes_timeout_waiters_with_resource_unavailable() {
        // pop 측
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop_timeout(Duration::from_secs(5)));

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        let res = h.join().unwrap();
        assert!(
            matches!(res, Err(HioLastError::ResourceUnavailable)),
            "dispose must win over the pending timeout: {res:?}"
        );

        // push 측 — 아이템 반환도 함께 확인
        let q = Arc::new(ArrayBQ::<i32>::new(1));
        q.push(1).unwrap();
        let q2 = q.clone();
        let h = thread::spawn(move || q2.push_timeout(2, Duration::from_secs(5)));

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        let res = h.join().unwrap();
        assert!(
            matches!(res, Err((HioLastError::ResourceUnavailable, 2))),
            "dispose must win over the pending timeout and return the item: {res:?}"
        );
    }

    // 26. 타임아웃이 반복돼도 이후 정상 블로킹이 동작한다 (대기자 카운터 회계)
    //
    // RAII 가드가 아니라 수동 enter/leave 로 회귀하면, 타임아웃 경로에서
    // leave 를 빠뜨려 카운터가 새고 이 테스트가 hang 으로 잡아낸다.
    #[test]
    fn waiter_accounting_survives_repeated_timeouts() {
        let q = Arc::new(ArrayBQ::<i32>::new(2));

        for _ in 0..20 {
            assert!(matches!(
                q.pop_timeout(Duration::from_millis(5)),
                Err(HioLastError::Timeout)
            ));
        }

        // 카운터가 망가졌다면 아래 블로킹 pop 이 시그널을 못 받고 멈춘다.
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop().unwrap());
        thread::sleep(Duration::from_millis(50));
        q.push(5).unwrap();
        assert_eq!(h.join().unwrap(), 5);
    }

    // --- drain ----------------------------------------------------------------

    // 27. drain 은 FIFO 순서로 전부 회수하고 큐를 비운다
    #[test]
    fn drain_collects_everything_in_fifo_order() {
        let q = ArrayBQ::<i32>::new(8);
        for i in 0..5 {
            q.push(i).unwrap();
        }

        assert_eq!(q.drain(), vec![0, 1, 2, 3, 4]);
        assert_eq!(q.size(), 0);

        assert!(
            q.drain().is_empty(),
            "draining an empty queue must be a no-op"
        );
    }

    // 28. drain 은 블로킹된 생산자를 전부 깨운다 (notify_all 경로)
    //
    // 이 코드베이스에서 dispose 를 제외한 유일한 notify_all 사용처다.
    #[test]
    fn drain_wakes_all_blocked_producers() {
        const CAP: usize = 3;
        const EXTRA: usize = 3; // 반드시 EXTRA <= CAP

        let q = Arc::new(ArrayBQ::<usize>::new(CAP));
        for i in 0..CAP {
            q.push(i).unwrap();
        }

        let blocked: Vec<_> = (0..EXTRA)
            .map(|i| {
                let q = q.clone();
                thread::spawn(move || q.push(100 + i))
            })
            .collect();

        thread::sleep(Duration::from_millis(50));
        let drained = q.drain();
        assert_eq!(drained.len(), CAP, "drain must take the resident items");

        // 깨어난 생산자들이 전부 성공해야 한다.
        for h in blocked {
            h.join()
                .unwrap()
                .expect("drain must free slots for waiters");
        }
        assert_eq!(q.size(), EXTRA);
    }

    // 29. dispose 후에도 drain 으로 잔여분을 회수할 수 있다
    #[test]
    fn drain_collects_remainder_after_dispose() {
        let q = ArrayBQ::<i32>::new(8);
        for i in 0..4 {
            q.push(i).unwrap();
        }
        q.dispose();

        assert_eq!(q.pop().unwrap(), 0);
        assert_eq!(q.drain(), vec![1, 2, 3]);
        assert!(matches!(q.pop(), Err(HioLastError::ResourceUnavailable)));
    }

    // 30. drain 이 회수한 아이템은 정확히 한 번 drop 된다
    #[test]
    fn drain_does_not_leak_or_double_free() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let q = ArrayBQ::<DropCounter>::new(16);
            for _ in 0..6 {
                q.push(DropCounter::new(&counter)).unwrap();
            }

            let drained = q.drain();
            assert_eq!(drained.len(), 6);
            assert_eq!(
                counter.load(Ordering::SeqCst),
                0,
                "drain must transfer ownership, not drop"
            );

            drop(drained);
            assert_eq!(counter.load(Ordering::SeqCst), 6);
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            6,
            "double free after queue drop"
        );
    }

    // 31. drain 은 소비자를 깨우지 않는다 (의도된 동작을 고정)
    //
    // drain 직후 큐는 비어 있으므로 not_empty 대기자를 깨워봐야 술어를
    // 재평가하고 다시 잠들 뿐이다. 나중에 누군가 "대칭을 맞추자"며
    // notify 를 추가하지 않도록 의도를 테스트로 박아 둔다.
    #[test]
    fn drain_leaves_blocked_consumers_asleep() {
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        q.push(1).unwrap();

        let woke = Arc::new(AtomicBool::new(false));
        let (q2, w2) = (q.clone(), woke.clone());
        let h = thread::spawn(move || {
            let r = q2.pop();
            w2.store(true, Ordering::SeqCst);
            r
        });

        thread::sleep(Duration::from_millis(50));
        // 소비자가 이미 1 을 가져갔을 수도 있으므로 두 경우를 모두 허용한다.
        let drained = q.drain();

        if drained.is_empty() {
            // 소비자가 먼저 가져간 경우
            assert_eq!(h.join().unwrap().unwrap(), 1);
        } else {
            // drain 이 가로챈 경우: 소비자는 여전히 대기 중이어야 한다
            assert_eq!(drained, vec![1]);
            thread::sleep(Duration::from_millis(50));
            assert!(
                !woke.load(Ordering::SeqCst),
                "drain must not wake consumers on an emptied queue"
            );
            q.push(2).unwrap();
            assert_eq!(h.join().unwrap().unwrap(), 2);
        }
    }

    // --- 통합 -----------------------------------------------------------------

    // 32. 전체 API 를 섞은 MPMC 스트레스
    //
    // 기존 11번은 push/pop 만 쓴다. 여기서는 try_*/timeout 이 cascade 체인에
    // 끼어들 때도 유실·중복이 없는지 본다. capacity 를 좁게 잡아 양쪽이
    // 실제로 블록되게 한다.
    #[test]
    fn mixed_api_mpmc_no_loss_no_duplication() {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const PER_PRODUCER: usize = 5_000;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;
        const EXPECTED_SUM: usize = TOTAL * (TOTAL + 1) / 2;

        let q = Arc::new(ArrayBQ::<usize>::new(4)); // 좁게
        let sum = Arc::new(AtomicUsize::new(0));
        let cnt = Arc::new(AtomicUsize::new(0));

        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|c| {
                let (q, sum, cnt) = (q.clone(), sum.clone(), cnt.clone());
                thread::spawn(move || {
                    loop {
                        // 소비자마다 다른 API 를 쓴다
                        let r = match c % 3 {
                            0 => q.pop(),
                            1 => match q.try_pop() {
                                Err(HioLastError::WouldBlock) => continue,
                                other => other,
                            },
                            _ => match q.pop_timeout(Duration::from_millis(5)) {
                                Err(HioLastError::Timeout) => continue,
                                other => other,
                            },
                        };
                        match r {
                            Ok(v) => {
                                sum.fetch_add(v, Ordering::Relaxed);
                                cnt.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => break, // ResourceUnavailable
                        }
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
                        let mut item = v;
                        loop {
                            let r = match i % 3 {
                                0 => q.push(item),
                                1 => q.try_push(item),
                                _ => q.push_timeout(item, Duration::from_millis(5)),
                            };
                            match r {
                                Ok(()) => break,
                                Err((HioLastError::WouldBlock, back))
                                | Err((HioLastError::Timeout, back)) => {
                                    item = back; // 재시도
                                }
                                Err((e, _)) => panic!("unexpected producer error: {e:?}"),
                            }
                        }
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
}
