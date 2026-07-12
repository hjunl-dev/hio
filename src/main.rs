use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use hio::ScopedTimer;
use hio::{BQType, ExecutorType, Job, ThreadPool, create_bq, create_executor};

fn test_thread_pool_with_array_bq() {
    let counter = Arc::new(AtomicI32::new(0));
    let repeat = 1_000_000;

    {
        let jq = create_bq::<Job>(BQType::Array, 0);
        let pool = create_executor(ExecutorType::ThreadPool, jq, 0);
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

fn main() {
    test_thread_pool_with_array_bq();
    test_thread_pool_with_linked_bq();
}
