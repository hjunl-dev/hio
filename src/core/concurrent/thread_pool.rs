use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

use crate::core::concurrent::{BQ, Executor, Job, JobQueue, linked_bq::LinkedBQ};

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
                worker_thread_proc(&(*jq_clone));
            }));
        }
        pool
    }

    pub fn new(num_workers: usize) -> Self {
        // Create a default job queue (LinkedBQ) with unlimited capacity (0)
        let jq = Arc::new(LinkedBQ::new(0));
        Self::with_jq(jq, num_workers)
    }
}

impl Executor for ThreadPool {
    fn submit(&self, job: Job) {
        todo!()
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

fn worker_thread_proc(jq_ref: &dyn BQ<Job>) {
    while let Ok(job) = jq_ref.pop() {
        job();
    }
}
