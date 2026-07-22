use std::{
    collections::VecDeque,
    sync::{
        Condvar, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{BQ, CondWaiters};
use hio_core::HioLastError;

//
// Primitive for building Array Blocking Queue
//

struct Inner<T> {
    buf: VecDeque<T>,
    pop_waiters: CondWaiters,
    push_waiters: CondWaiters,
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
                pop_waiters: CondWaiters::default(),
                push_waiters: CondWaiters::default(),
            }),
        }
    }

    fn en_q(&self, item: T, mut g: MutexGuard<'_, Inner<T>>) {
        let prev_count = g.buf.len();
        g.buf.push_back(item);

        // cascade notify push waiters if queue is not full
        if prev_count + 1 < self.capacity && g.push_waiters.any() {
            self.not_full.notify_one();
        }
        // was empty, notify pop waiters
        if prev_count == 0 && g.pop_waiters.any() {
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
        if prev_count > 1 && g.pop_waiters.any() {
            self.not_empty.notify_one();
        }
        // was full, notify push waiters
        if prev_count == self.capacity && g.push_waiters.any() {
            self.not_full.notify_one();
        }
        Ok(item.unwrap())
    }
}

impl<T: Send> BQ<T> for ArrayBQ<T> {
    fn push(&self, item: T) -> Result<(), HioLastError> {
        if let Ok(mut g) = self.inner.lock() {
            g.push_waiters.enter();

            match self
                .not_full
                .wait_while(g, |g| !self.is_disposed() && g.buf.len() >= self.capacity)
            {
                Ok(mut g) => {
                    g.push_waiters.leave();
                    if self.is_disposed() {
                        return Err(HioLastError::ResourceUnavailable);
                    }
                    self.en_q(item, g);
                    return Ok(());
                }
                Err(e) => {
                    e.into_inner().push_waiters.leave();
                    return Err(HioLastError::MutexPoisoned);
                }
            }
        }
        Err(HioLastError::MutexPoisoned)
    }

    fn pop(&self) -> Result<T, HioLastError> {
        if let Ok(mut g) = self.inner.lock() {
            g.pop_waiters.enter();

            match self
                .not_empty
                .wait_while(g, |g| !self.is_disposed() && g.buf.is_empty())
            {
                Ok(mut g) => {
                    g.pop_waiters.leave();
                    if self.is_disposed() && g.buf.is_empty() {
                        return Err(HioLastError::ResourceUnavailable);
                    }
                    return self.de_q(g);
                }
                Err(e) => {
                    e.into_inner().pop_waiters.leave();
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
            let _g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
        assert!(!progressed.load(Ordering::SeqCst), "push가 블로킹되지 않음");

        assert_eq!(q.pop().unwrap(), 10); // 슬롯 확보 → pusher 깨어남
        h.join().unwrap();
        assert!(progressed.load(Ordering::SeqCst));
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
        assert!(matches!(q.pop(), Err(HioLastError::ResourceUnavailable)));
    }

    // 6. dispose 후 push 실패
    #[test]
    fn push_fails_after_dispose() {
        let q = ArrayBQ::<i32>::new(4);
        q.dispose();
        assert!(matches!(q.push(1), Err(HioLastError::ResourceUnavailable)));
    }

    // 7. dispose가 블로킹된 pop을 깨움
    #[test]
    fn dispose_wakes_blocked_pop() {
        let q = Arc::new(ArrayBQ::<i32>::new(4));
        let q2 = q.clone();
        let h = thread::spawn(move || q2.pop());

        thread::sleep(Duration::from_millis(50));
        q.dispose();

        assert!(matches!(
            h.join().unwrap(),
            Err(HioLastError::ResourceUnavailable)
        ));
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

        assert!(matches!(
            h.join().unwrap(),
            Err(HioLastError::ResourceUnavailable)
        ));
    }

    // 9. MPMC 정확성: 유실/중복 없이 합 보존 (cascade notify 검증)
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
            "소비 개수 불일치(유실/중복)"
        );
        assert_eq!(
            sum.load(Ordering::Relaxed),
            EXPECTED_SUM,
            "합 불일치(유실/중복)"
        );
    }

    // 10. 무제한 큐(capacity == usize::MAX)는 push가 블로킹되지 않음
    #[test]
    fn unbounded_never_blocks_push() {
        let q = ArrayBQ::<usize>::new(usize::MAX);
        for i in 0..1000 {
            q.push(i).unwrap();
        }
        assert_eq!(q.size(), 1000);
        assert_eq!(q.pop().unwrap(), 0);
    }
}
