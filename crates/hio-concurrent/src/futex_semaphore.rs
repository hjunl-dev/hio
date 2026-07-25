//! Type1: futex(atomic-wait) 기반. MSVC C++20 counting_semaphore 미러링.

use std::sync::atomic::{AtomicU32, Ordering::*};

use atomic_wait::{wait, wake_all, wake_one};
use hio_core::HioLastError::{self, Failed};

use crate::Semaphore;

pub const MAX_PERMITS: u32 = u32::MAX >> 1;

#[derive(Debug)]
pub struct FutexSemaphore {
    /// permit 카운터 = futex 워드. 이 주소에 직접 wait/wake 한다.
    count: AtomicU32,
    /// 잠들었거나 잠들려는 스레드 수의 상한. wake elision용.
    /// over-count는 성능 이슈(불필요한 wake), under-count는 정합성 이슈(놓친 wake).
    waiters: AtomicU32,
}

impl FutexSemaphore {
    pub const fn new(permits: u32) -> Self {
        assert!(permits <= MAX_PERMITS);
        Self {
            count: AtomicU32::new(permits),
            waiters: AtomicU32::new(0),
        }
    }

    /// CAS 루프로 permit 1개 감소. 성공 CAS는 Acquire:
    /// release(permit 반환)의 Release와 짝을 이뤄 자원 가시성 엣지를 만든다.
    #[inline]
    fn try_decrement(&self) -> bool {
        let mut cur = self.count.load(Relaxed); // 투기적 읽기
        while cur > 0 {
            match self
                .count
                .compare_exchange_weak(cur, cur - 1, Acquire, Relaxed)
            {
                Ok(_) => return true, // 루프 안이므로 weak이 최적
                Err(a) => cur = a,
            }
        }
        false
    }

    #[cold]
    fn acquire_slow(&self) {
        self.waiters.fetch_add(1, SeqCst); // W1
        loop {
            if self.try_decrement() {
                self.waiters.fetch_sub(1, Relaxed); // over-count는 안전
                return;
            }
            // W2: futex 진입 전 명시적 seq_cst load.
            // release의 [count += n (R1); waiters.load (R2)]와의 store-buffering을
            // 닫아 elision(wake 생략)을 안전하게 만든다. 크레이트 wait은 이 seq_cst
            // 지점을 노출하지 않으므로 여기서 직접 둔다. (MSVC _Wait()의 _Counter.load())
            if self.count.load(SeqCst) == 0 {
                // 커널이 futex 큐 락 안에서 count==0을 재검사한다.
                // 직전에 permit이 생겼다면 즉시 리턴 → 루프 상단에서 재시도.
                // spurious wakeup도 같은 루프가 흡수.
                wait(&self.count, 0);
            }
        }
    }
}

impl Semaphore for FutexSemaphore {
    fn make(permits: u32) -> Self {
        Self::new(permits)
    }

    fn acquire(&self) {
        if self.try_decrement() {
            return; // fast path: CAS 한 번, 커널 없음
        }
        self.acquire_slow();
    }

    fn try_acquire(&self) -> Result<(), HioLastError> {
        // 단발 strong CAS: spurious 실패 없음 → permit이 있고 무경합이면 반드시 성공.
        let cur = self.count.load(Relaxed);
        if cur > 0
            && self
                .count
                .compare_exchange(cur, cur - 1, Acquire, Relaxed)
                .is_ok()
        {
            Ok(())
        } else {
            Err(Failed)
        }
    }

    fn release(&self, n: u32) {
        if n == 0 {
            return;
        }
        // SeqCst (R1): waiter의 [waiters += 1 (W1); count.load (W2)]와의 Dekker.
        // 넷 다 SeqCst여야 "releaser가 증가값을 보거나, waiter가 증가값을 보거나,
        // 최소한 둘 중 하나"가 보장된다. 약한 순서는 lost wakeup을 허용한다.
        let prev = self.count.fetch_add(n, SeqCst);
        assert!(prev <= MAX_PERMITS - n, "semaphore permit overflow");

        let waiting = self.waiters.load(SeqCst); // R2
        if waiting != 0 {
            self.wake_waiters(n, waiting); // 잠든 스레드 있을 때만 (cold)
        }
        // 잠든 스레드 없음 → wake 시스템콜 생략 (elision)
    }

    fn available_permits(&self) -> u32 {
        self.count.load(Relaxed)
    }
}

impl FutexSemaphore {
    #[cold]
    fn wake_waiters(&self, n: u32, waiting: u32) {
        if waiting <= n {
            // update 이하 → 전원 깨워도 permit 못 받을 스레드가 없다
            wake_all(&self.count);
        } else {
            // 최대 n명만. 불필요한 herd 억제.
            for _ in 0..n {
                wake_one(&self.count);
            }
        }
    }
}
