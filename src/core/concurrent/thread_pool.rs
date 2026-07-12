use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

use crate::{
    core::concurrent::{BQ, BQType, Executor, Job, JobQueue, create_bq, linked_bq::LinkedBQ},
    error::HioLastError,
};

pub struct ThreadPool {
    num_workers: usize,
    disposed: AtomicBool,
    job_queue: JobQueue,
    workers: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn with_jq(job_queue: JobQueue, num_workers: usize) -> Self {
        // Create a thread pool
        let mut pool = Self {
            num_workers,
            disposed: AtomicBool::new(false),
            job_queue,
            workers: Vec::with_capacity(num_workers),
        };
        // Spawn worker threads
        for _ in 0..num_workers {
            let jq_clone = Arc::clone(&pool.job_queue);
            pool.workers.push(thread::spawn(move || {
                ThreadPool::worker_thread_proc(&(*jq_clone));
            }));
        }
        pool
    }

    pub fn new(num_workers: usize) -> Self {
        // Create a default job queue (LinkedBQ) with unlimited capacity (0)
        let jq = create_bq(BQType::Linked, 0);
        Self::with_jq(jq, num_workers)
    }

    fn worker_thread_proc(jq_ref: &dyn BQ<Job>) {
        while let Ok(job) = jq_ref.pop() {
            job();
        }
    }
}

impl Executor for ThreadPool {
    fn submit(&self, job: Job) -> Result<(), HioLastError> {
        self.job_queue.push(job)
    }

    fn worker_count(&self) -> usize {
        self.num_workers
    }

    fn dispose(&mut self) {
        if self
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.job_queue.dispose();
            for worker in self.workers.drain(..) {
                let _ = worker.join();
            }
        }
    }

    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.dispose();
    }
}

//
// Tests for the ThreadPool implementation
//

mod tests {
    use crate::core::ScopedTimer;
    use crate::core::concurrent::{BQType, create_bq};
    use crate::{ExecutorType, create_executor};

    use super::*;
    use std::sync::atomic::AtomicI32;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_thread_pool_with_array_bq() {
        let counter = Arc::new(AtomicI32::new(0));
        let repeat = 1_000_000;

        {
            let jq = create_bq::<Job>(BQType::Array, 0);
            let pool = create_executor(ExecutorType::ThreadPool, jq, 4);
            let _timer = ScopedTimer::new("test_thread_pool_with_array_bq");

            for _ in 0..repeat {
                let counter_clone = Arc::clone(&counter);
                let _ = pool.submit(Box::new(move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }));
            }
        }

        let result = counter.load(Ordering::SeqCst);
        println!("Counter value: {}", result);
        assert_eq!(result, repeat);
    }

    #[test]
    fn test_thread_pool_with_linked_bq() {
        let counter = Arc::new(AtomicI32::new(0));
        let repeat = 1_000_000;

        {
            let jq = create_bq::<Job>(BQType::Linked, 0);
            let pool = create_executor(ExecutorType::ThreadPool, jq, 0);
            let _timer = ScopedTimer::new("test_thread_pool_with_linked_bq");

            for _ in 0..repeat {
                let counter_clone = Arc::clone(&counter);
                let _ = pool.submit(Box::new(move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }));
            }
        }

        let result = counter.load(Ordering::SeqCst);
        println!("Counter value: {}", result);
        assert_eq!(result, repeat);
    }
}
