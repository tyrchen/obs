//! Per-sink batching helper. Spec 20 § 4.

use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Bounded batch with size + age trigger. Calls `flush_fn` when one of
/// the triggers fires.
#[derive(Debug)]
pub struct Batch<T> {
    inner: Mutex<BatchInner<T>>,
    max_records: usize,
    max_age: Duration,
}

#[derive(Debug)]
struct BatchInner<T> {
    items: Vec<T>,
    opened_at: Instant,
}

impl<T> Batch<T> {
    /// New batch.
    #[must_use]
    pub fn new(max_records: usize, max_age: Duration) -> Self {
        Self {
            inner: Mutex::new(BatchInner {
                items: Vec::with_capacity(max_records),
                opened_at: Instant::now(),
            }),
            max_records,
            max_age,
        }
    }

    /// Push one item. Returns `Some(items)` when the batch is ready to
    /// flush; otherwise `None`.
    pub fn push(&self, item: T) -> Option<Vec<T>> {
        let mut inner = self.inner.lock();
        inner.items.push(item);
        if inner.items.len() >= self.max_records || inner.opened_at.elapsed() >= self.max_age {
            let drained = std::mem::replace(&mut inner.items, Vec::with_capacity(self.max_records));
            inner.opened_at = Instant::now();
            return Some(drained);
        }
        None
    }

    /// Drain whatever is currently buffered.
    pub fn drain(&self) -> Vec<T> {
        let mut inner = self.inner.lock();
        let drained = std::mem::replace(&mut inner.items, Vec::with_capacity(self.max_records));
        inner.opened_at = Instant::now();
        drained
    }
}
