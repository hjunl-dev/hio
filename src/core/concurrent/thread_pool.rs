use std::{
    sync::{Arc, atomic::AtomicBool},
    thread::{self, JoinHandle},
};

use crate::core::concurrent::{BQ, Executor, Job, JobQueue, linked_bq::LinkedBQ};

pub struct ThreadPool {
    num_workers: usize,
    running: Arc<AtomicBool>,
    job_queue: JobQueue,
    workers: Vec<JoinHandle<()>>,
}

impl ThreadPool {
    pub fn with_job_queue(job_queue: JobQueue, num_workers: usize) -> Self {
        let mut pool = Self {
            num_workers,
            running: Arc::new(AtomicBool::new(true)),
            job_queue,
            workers: Vec::with_capacity(num_workers),
        };
        for _ in 0..num_workers {
            let jq_clone = Arc::clone(&pool.job_queue);
            let running_clone = Arc::clone(&pool.running);
            pool.workers.push(thread::spawn(move || {
                while running_clone.load(std::sync::atomic::Ordering::SeqCst) {
                    if let Ok(job) = jq_clone.pop() {
                        job();
                    }
                }
            }));
        }
        pool
    }

    pub fn new(num_workers: usize) -> Self {
        // Create a default job queue (LinkedBQ) with unlimited capacity (0)
        let task_queue = Arc::new(LinkedBQ::new(0));
        Self::with_job_queue(task_queue, num_workers)
    }
}

impl Executor for ThreadPool {
    fn submit(&self, job: Job) {
        todo!()
    }

    fn worker_count(&self) -> usize {
        todo!()
    }

    fn shutdown(&self) {
        todo!()
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        todo!()
    }
}

fn thread_proc() {}
