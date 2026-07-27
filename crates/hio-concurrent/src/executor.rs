mod thread_per_task;
mod thread_pool;
mod work_stealing;

use crate::{bq::BQ, executor::thread_pool::ThreadPool};
use hio_core::HioLastError;
use std::{ffi::c_void, sync::Arc, thread};

//
// Executor
//

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum ExecutorType {
    ThreadPool = 0,
    ThreadPerTask = 1,
    WorkStealing = 2,
}

pub type Job = Box<dyn FnOnce() + Send + 'static>;
pub type CJobFnPtr = extern "C" fn(user_data: *const c_void);
pub type JobQueue = Arc<dyn BQ<Job>>;

pub trait Executor: Send + Sync {
    fn submit(&self, job: Job) -> Result<(), HioLastError>;
    fn dispose(&mut self);
    fn is_disposed(&self) -> bool;
    fn worker_count(&self) -> usize;
}

fn ensure_num_workers(num_workers: usize) -> usize {
    if num_workers == 0 {
        thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    } else {
        num_workers
    }
}

pub fn create_executor(
    executor_type: ExecutorType,
    job_queue: JobQueue,
    num_workers: usize,
) -> Arc<dyn Executor> {
    let num_workers = ensure_num_workers(num_workers);
    match executor_type {
        ExecutorType::ThreadPool => Arc::new(ThreadPool::with_jq(job_queue, num_workers)),
        ExecutorType::ThreadPerTask => todo!(),
        ExecutorType::WorkStealing => todo!(),
    }
}
