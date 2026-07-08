pub(crate) mod array_bq;
pub(crate) mod linked_bq;
pub(crate) mod thread_pool;

use crate::core::HioLastError;
use std::{ffi::c_void, sync::Arc, thread};

//
// Blocking Queue
//

pub enum BQType {
    Array,
    Linked,
    LockFree,
}

pub trait BQ<T: Send>: Send + Sync {
    fn push(&self, item: T) -> Result<(), HioLastError>;
    fn pop(&self) -> Result<T, HioLastError>;
    fn dispose(&self);
    fn size(&self) -> usize;
    fn capacity(&self) -> usize;
    fn is_disposed(&self) -> bool;
}

fn ensure_capacity(capacity: usize) -> usize {
    if capacity == 0 { usize::MAX } else { capacity }
}

pub fn create_bq<T: Send>(bq_type: BQType, capacity: usize) -> Arc<dyn BQ<T>> {
    let capacity = ensure_capacity(capacity);
    match bq_type {
        BQType::Array => todo!(),
        BQType::Linked => todo!(),
        BQType::LockFree => todo!(),
    }
}

//
// Executor
//

pub type Task = Box<dyn FnOnce() + Send + 'static>;
pub type CTaskPtr = extern "C" fn(user_data: *const c_void);

pub trait Executor: Send + Sync {
    fn submit(&self, task: Task);
    fn worker_count(&self) -> usize;
    fn shutdown(&self);
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
