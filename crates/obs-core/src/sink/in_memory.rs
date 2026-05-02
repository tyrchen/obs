//! `InMemorySink` — bounded ring buffer for tests.
//!
//! Spec 61 § 2.4 + spec 72 § 2: the test harness observer collects
//! envelopes into this sink, and tests `drain()` to assert what was
//! emitted. Bounded so a runaway test cannot OOM the test binary.

use std::collections::VecDeque;
use std::sync::Arc;

use obs_proto::obs::v1::ObsEnvelope;
use parking_lot::Mutex;

use crate::registry::ScrubbedEnvelope;

use super::Sink;

const DEFAULT_CAPACITY: usize = 1024;

/// Test sink: collects envelopes into a bounded ring buffer.
#[derive(Debug, Clone)]
pub struct InMemorySink {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    capacity: usize,
    buffer: Mutex<VecDeque<ObsEnvelope>>,
}

impl InMemorySink {
    /// Create a sink with the default capacity (1024).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Create a sink with a specific ring buffer capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                capacity,
                buffer: Mutex::new(VecDeque::with_capacity(capacity)),
            }),
        }
    }

    /// Stable handle for `drain()` / `wait_for()` / `count()`. Cheap
    /// to clone — internally one `Arc` ref-count bump.
    #[must_use]
    pub fn handle(&self) -> InMemoryHandle {
        InMemoryHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Default for InMemorySink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for InMemorySink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        let cloned = env.envelope().clone();
        let mut buf = self.inner.buffer.lock();
        if buf.len() >= self.inner.capacity {
            // Drop oldest — bounded buffer.
            buf.pop_front();
        }
        buf.push_back(cloned);
    }
}

/// Stable handle to an [`InMemorySink`]. Clone-safe; share across
/// threads.
#[derive(Debug, Clone)]
pub struct InMemoryHandle {
    inner: Arc<Inner>,
}

impl InMemoryHandle {
    /// Drain all collected envelopes (clears the buffer). Order
    /// is FIFO — oldest first.
    #[must_use]
    pub fn drain(&self) -> Vec<ObsEnvelope> {
        let mut buf = self.inner.buffer.lock();
        buf.drain(..).collect()
    }

    /// Number of envelopes currently buffered.
    #[must_use]
    pub fn count(&self) -> usize {
        self.inner.buffer.lock().len()
    }

    /// Snapshot the buffer without draining.
    #[must_use]
    pub fn snapshot(&self) -> Vec<ObsEnvelope> {
        self.inner.buffer.lock().iter().cloned().collect()
    }

    /// Block the current thread until the buffer holds at least
    /// `min_count` envelopes, or `timeout` elapses. Returns the
    /// drained envelopes on success, `None` on timeout. Used by
    /// tests that emit on a background task.
    #[must_use]
    pub fn wait_for(
        &self,
        min_count: usize,
        timeout: std::time::Duration,
    ) -> Option<Vec<ObsEnvelope>> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if self.count() >= min_count {
                return Some(self.drain());
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
}
