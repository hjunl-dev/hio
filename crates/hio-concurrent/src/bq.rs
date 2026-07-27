mod array_bq;
mod linked_bq;
mod lock_free_sync_q;

use hio_core::HioLastError;
use std::sync::Arc;

// todo: need to fix align for different architectures, currently only works for x86_64
#[repr(align(128))]
pub struct CachePadded<T>(T);

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

fn ensure_capacity(capacity: usize) -> usize {
    if capacity == 0 { usize::MAX } else { capacity }
}

pub fn create_bq<T: Send + 'static>(bq_type: BQType, capacity: usize) -> Arc<dyn BQ<T>> {
    let capacity = ensure_capacity(capacity);
    match bq_type {
        BQType::Array => Arc::new(array_bq::ArrayBQ::new(capacity)),
        BQType::Linked => Arc::new(linked_bq::LinkedBQ::new(capacity)),
        BQType::LockFree => todo!(),
    }
}
