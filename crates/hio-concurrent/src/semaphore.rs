mod condvar_semaphore;
mod futex_semaphore;

use crate::semaphore::futex_semaphore::FutexSemaphore;
use hio_core::HioLastError;
use std::sync::Arc;

//
// Semaphore
//

pub const MAX_PERMITS: u32 = u32::MAX >> 1;

pub enum SemaphoreType {
    FutexSem = 0,
    CondvarSem = 1,
}

pub trait Semaphore: Send + Sync {
    fn make(permits: u32) -> Self
    where
        Self: Sized;

    fn acquire(&self);
    fn try_acquire(&self) -> Result<(), HioLastError>;
    fn release(&self, n: u32);
    fn available_permits(&self) -> u32;
}

fn ensure_permits(permits: u32) -> u32 {
    if permits == 0 { 1 } else { permits }
}

pub fn create_semaphore(sem_type: SemaphoreType, permits: u32) -> Arc<dyn Semaphore> {
    match sem_type {
        SemaphoreType::FutexSem => Arc::new(FutexSemaphore::new(permits)),
        SemaphoreType::CondvarSem => todo!(),
    }
}
