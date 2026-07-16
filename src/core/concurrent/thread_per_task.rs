use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use crate::{
    core::concurrent::{Executor, Job},
    error::HioLastError,
};

pub struct ThreadPerTaskPool {
    disposed: AtomicBool,
    // 현재 실행 중인 스레드 개수를 추적하기 위한 카운터
    active_workers: Arc<AtomicUsize>,
    // 종료(dispose) 시 모든 스레드가 끝날 때까지 기다리기 위해 핸들을 보관
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl ThreadPerTaskPool {
    pub fn new() -> Self {
        Self {
            disposed: AtomicBool::new(false),
            active_workers: Arc::new(AtomicUsize::new(0)),
            handles: Mutex::new(Vec::new()),
        }
    }
}

impl Executor for ThreadPerTaskPool {
    fn submit(&self, job: Job) -> Result<(), HioLastError> {
        // 이미 dispose 된 상태라면 에러 반환
        if self.is_disposed() {
            // 프로젝트의 HioLastError 구조에 맞게 에러를 반환해 주세요.
            // 예: return Err(HioLastError::Disposed);
            panic!("Cannot submit job: ThreadPerTaskPool is disposed"); 
        }

        let active_workers = Arc::clone(&self.active_workers);
        
        // 작업 시작 전 워커 수 증가
        active_workers.fetch_add(1, Ordering::SeqCst);

        // 큐를 거치지 않고 즉시 스레드를 생성하여 작업을 실행
        let handle = thread::spawn(move || {
            job();
            // 작업 완료 후 워커 수 감소
            active_workers.fetch_sub(1, Ordering::SeqCst);
        });

        // 생성된 핸들을 저장 (Graceful Shutdown을 위해)
        if let Ok(mut handles) = self.handles.lock() {
            // 메모리 누수를 방지하기 위해 이미 종료된 스레드 핸들은 주기적으로 정리합니다.
            // (Rust 1.61+ 이상부터 JoinHandle::is_finished() 사용 가능)
            handles.retain(|h| !h.is_finished());
            handles.push(handle);
        }

        Ok(())
    }

    fn worker_count(&self) -> usize {
        self.active_workers.load(Ordering::SeqCst)
    }

    fn dispose(&mut self) {
        if self
            .disposed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // 스레드가 새로 생성되는 것을 막은 후, 실행 중인 모든 스레드의 종료를 대기합니다.
            if let Ok(mut handles) = self.handles.lock() {
                for handle in handles.drain(..) {
                    let _ = handle.join();
                }
            }
        }
    }

    fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }
}

impl Drop for ThreadPerTaskPool {
    fn drop(&mut self) {
        self.dispose();
    }
}