use core::task::Waker;
use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Shutdown {
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) task: Arc<Mutex<Option<Waker>>>,
}

impl Shutdown {
    /// set shutdown to true and notify related task.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Release);
        match &*self.task.lock().unwrap() {
            Some(w) => w.clone().wake(),
            None => {}
        };
    }
    /// check if task is already shutdown
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Acquire)
    }
}
