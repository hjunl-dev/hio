mod array_bq;
mod linked_bq;
mod lock_free_sync_q;

use hio_core::HioLastError;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

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

#[derive(Debug)]
struct WaiterGuard<'a>(&'a AtomicUsize);

impl Drop for WaiterGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct CondWaiters(AtomicUsize);

impl CondWaiters {
    #[inline]
    pub const fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    #[inline]
    #[must_use]
    pub fn enter(&self) -> WaiterGuard<'_> {
        self.0.fetch_add(1, Ordering::Relaxed);
        WaiterGuard(&self.0)
    }
    #[inline]
    pub fn any(&self) -> bool {
        self.0.load(Ordering::Relaxed) > 0
    }
    #[inline]
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Signals {
    signal_not_empty: bool,
    signal_not_full: bool,
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
    match bq_type {
        BQType::Array => Arc::new(array_bq::ArrayBQ::new(capacity)),
        BQType::Linked => Arc::new(linked_bq::LinkedBQ::new(capacity)),
        BQType::LockFree => todo!(),
    }
}
