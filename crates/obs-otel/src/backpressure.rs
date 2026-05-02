//! Two-layer backpressure (spec 20 § 4.2).
//!
//! Layer 1 — the per-tier mpsc channel between the emit thread and the
//! sink — already lives in `obs-core::observer::workers`. Layer 2 —
//! the OTLP exporter's retry queue inside the sink — is implemented
//! here. Overflow drops on the worker side and increments the
//! counter.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Bounded retry queue. Drops on overflow.
#[derive(Debug)]
pub struct RetryQueue<T> {
    inner: Mutex<std::collections::VecDeque<T>>,
    capacity: usize,
    dropped: AtomicU64,
}

impl<T> RetryQueue<T> {
    /// New retry queue with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(std::collections::VecDeque::with_capacity(capacity)),
            capacity,
            dropped: AtomicU64::new(0),
        }
    }

    /// Push an item; returns `false` (and increments the dropped
    /// counter) when the queue is full.
    pub fn push(&self, item: T) -> bool {
        let mut inner = self.inner.lock();
        if inner.len() >= self.capacity {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        inner.push_back(item);
        true
    }

    /// Pop the oldest item, if any.
    pub fn pop(&self) -> Option<T> {
        self.inner.lock().pop_front()
    }

    /// Total dropped on overflow.
    #[allow(dead_code)]
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Current depth.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.inner.lock().len()
    }
}
