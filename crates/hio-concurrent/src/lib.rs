//
// hio-concurrent: blocking queues and executors.
//

mod array_bq;
mod linked_bq;
mod semaphore;
mod thread_per_task;
mod thread_pool;

pub use thread_pool::ThreadPool;

use std::{ffi::c_void, sync::Arc, thread};

use crate::linked_bq::LinkedBQ;
use hio_core::HioLastError;

//
// CachePadded for preventing false sharing between threads.
//
// Lives here because LinkedBQ is its only user; keeping it in this crate
// lets LinkedBQ construct it directly (the tuple field stays private).
//

// todo: need to fix align for different architectures, currently only works for x86_64
#[repr(align(128))]
pub struct CachePadded<T>(pub(crate) T);

impl<T> CachePadded<T> {
    pub const fn new(t: T) -> Self {
        Self(t)
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

//
// Blocking Queue
//

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub enum BQType {
    Array = 0,
    Linked = 1,
    LockFree = 2,
}

pub trait BQ<T: Send>: Send + Sync {
    fn push(&self, item: T) -> Result<(), HioLastError>;
    fn pop(&self) -> Result<T, HioLastError>;
    fn dispose(&self);
    fn size(&self) -> usize;
    fn capacity(&self) -> usize;
    fn is_disposed(&self) -> bool;
}

#[derive(Debug, Default, Clone, Copy)]
struct CondWaiters(usize);

impl CondWaiters {
    #[inline]
    fn enter(&mut self) {
        self.0 += 1;
    }
    #[inline]
    fn leave(&mut self) {
        if self.0 > 0 {
            self.0 -= 1;
        }
    }
    #[inline]
    fn any(&self) -> bool {
        self.0 > 0
    }
}

fn ensure_capacity(capacity: usize) -> usize {
    if capacity == 0 { usize::MAX } else { capacity }
}

pub fn create_bq<T: Send + 'static>(bq_type: BQType, capacity: usize) -> Arc<dyn BQ<T>> {
    let capacity = ensure_capacity(capacity);
    match bq_type {
        BQType::Array => Arc::new(array_bq::ArrayBQ::new(capacity)),
        BQType::Linked => Arc::new(LinkedBQ::new(capacity)),
        BQType::LockFree => todo!(),
    }
}

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
