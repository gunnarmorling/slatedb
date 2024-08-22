use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Counter {
    pub value: Arc<AtomicU64>,
}

impl Counter {
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn inc(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self {
            value: Arc::new(AtomicU64::default()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Metrics {
    pub immutable_memtable_flushes: Counter,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            immutable_memtable_flushes: Counter::default(),
        }
    }
}
