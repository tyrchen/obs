//! `FanOutSink` — deliver one envelope to N child sinks.
//!
//! `StandardObserverBuilder::sink_for(tier, sink)` replaces the sink
//! slot for the tier. To fan a tier's output across multiple
//! destinations (e.g. `NdjsonFileSink` + `OtlpLogSink` during Phase 3,
//! or `S3Sink` + `OtlpLogSink` + live-tail at the boundary), wrap
//! them in a `FanOutSink`. Boundary-review § 3.4 (moved upstream from
//! `tok_obs::FanOutSink`).
//!
//! `ScrubbedEnvelope<'_>` is `Copy` — the per-deliver fan-out is
//! allocation-free. Each child does its own small copy-into-queue
//! internally.

use std::sync::Arc;

use super::{Sink, SinkFut};
use crate::registry::ScrubbedEnvelope;

/// Multiplex a single tier's output across N sinks.
///
/// Construction enforces `!sinks.is_empty()` — a zero-child fan-out
/// is almost certainly a config mistake (silent drop of every
/// envelope); the empty case is better served by an explicit
/// `NoopSink`.
pub struct FanOutSink {
    sinks: Vec<Arc<dyn Sink>>,
}

impl std::fmt::Debug for FanOutSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanOutSink")
            .field("children", &self.sinks.len())
            .finish()
    }
}

impl FanOutSink {
    /// Wrap `sinks` as a shared `Arc<dyn Sink>`.
    ///
    /// # Panics
    ///
    /// Panics when `sinks` is empty — caller bug.
    #[must_use]
    pub fn new(sinks: Vec<Arc<dyn Sink>>) -> Arc<Self> {
        assert!(
            !sinks.is_empty(),
            "FanOutSink requires at least one child sink",
        );
        Arc::new(Self { sinks })
    }

    /// Number of child sinks.
    #[must_use]
    pub fn children(&self) -> usize {
        self.sinks.len()
    }
}

impl Sink for FanOutSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        for sink in &self.sinks {
            sink.deliver(env);
        }
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async move {
            // Sequential await is fine: `flush` is a shutdown-adjacent
            // operation (drain batches). The downstream sinks aren't
            // coupled to each other, so running in order keeps the code
            // simple without noticeable latency.
            for sink in &self.sinks {
                sink.flush().await;
            }
        })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async move {
            for sink in &self.sinks {
                sink.shutdown().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingSink {
        deliveries: AtomicUsize,
        flushes: AtomicUsize,
        shutdowns: AtomicUsize,
    }

    impl std::fmt::Debug for CountingSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CountingSink").finish()
        }
    }

    impl Sink for CountingSink {
        fn deliver(&self, _env: ScrubbedEnvelope<'_>) {
            self.deliveries.fetch_add(1, Ordering::Relaxed);
        }
        fn flush(&self) -> SinkFut<'_> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        }
        fn shutdown(&self) -> SinkFut<'_> {
            self.shutdowns.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        }
    }

    #[test]
    #[should_panic(expected = "FanOutSink requires at least one child sink")]
    fn test_should_panic_on_empty_sinks() {
        let _ = FanOutSink::new(Vec::new());
    }

    #[tokio::test]
    async fn test_should_flush_and_shutdown_every_child() {
        let a: Arc<CountingSink> = Arc::new(CountingSink::default());
        let b: Arc<CountingSink> = Arc::new(CountingSink::default());
        let fan = FanOutSink::new(vec![a.clone() as Arc<dyn Sink>, b.clone() as Arc<dyn Sink>]);

        assert_eq!(fan.children(), 2);
        fan.flush().await;
        fan.shutdown().await;

        assert_eq!(a.flushes.load(Ordering::Relaxed), 1);
        assert_eq!(a.shutdowns.load(Ordering::Relaxed), 1);
        assert_eq!(b.flushes.load(Ordering::Relaxed), 1);
        assert_eq!(b.shutdowns.load(Ordering::Relaxed), 1);
    }
}
