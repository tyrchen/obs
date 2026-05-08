//! Per-tier worker pool — bounded `tokio::sync::mpsc` channels with
//! one drain task per tier. Spec 11 § 4.
//!
//! AUDIT is special: bounded blocking + spool fallback (spec 11 § 6.4)
//! — see [`crate::audit_spool`].

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use bytes::BytesMut;
use obs_proto::obs::v1::{ObsEnvelope, Tier};
use tokio::{
    runtime::Handle,
    sync::{Mutex as AsyncMutex, mpsc},
    task::JoinHandle,
};

use crate::{
    config::QueuesConfig,
    registry::{SchemaRegistry, ScrubbedEnvelope},
    sink::Sink,
};

/// Per-tier counters surfaced as `ObsSinkDropped` self-events.
#[derive(Debug, Default)]
pub struct WorkerCounters {
    /// Bytes dropped at emit-time mpsc send.
    pub channel_full_log: AtomicU64,
    /// Bytes dropped at emit-time mpsc send (METRIC).
    pub channel_full_metric: AtomicU64,
    /// Bytes dropped at emit-time mpsc send (TRACE).
    pub channel_full_trace: AtomicU64,
    /// Bytes dropped at emit-time mpsc send (AUDIT).
    pub channel_full_audit: AtomicU64,
    /// Total events delivered.
    pub delivered: AtomicU64,
}

/// Single-tier worker handle.
pub struct TierWorker {
    sender: parking_lot::Mutex<Option<mpsc::Sender<ObsEnvelope>>>,
    join: AsyncMutex<Option<JoinHandle<()>>>,
    shutdown: Arc<AtomicBool>,
    sink: Arc<dyn Sink>,
}

impl std::fmt::Debug for TierWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TierWorker")
            .field("alive", &self.sender.lock().is_some())
            .finish()
    }
}

impl TierWorker {
    /// Spawn a worker that drains a bounded mpsc channel and delivers
    /// each envelope to `sink` after running it through the per-tier
    /// scrubber.
    pub fn spawn(
        capacity: usize,
        sink: Arc<dyn Sink>,
        registry: Arc<SchemaRegistry>,
        counters: Arc<WorkerCounters>,
        tier: Tier,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<ObsEnvelope>(capacity.max(1));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_in = Arc::clone(&shutdown);
        let sink_in = Arc::clone(&sink);
        let registry_in = registry;
        let counters_in = counters;
        let join = tokio::spawn(async move {
            let mut scratch = BytesMut::with_capacity(4096);
            // Drain the channel until the sender side is dropped. The
            // observer's `shutdown()` drops the sender, which makes
            // `rx.recv()` return None and the loop exit cleanly.
            while let Some(env) = rx.recv().await {
                deliver_one(&env, &registry_in, &mut scratch, &sink_in);
                counters_in.delivered.fetch_add(1, Ordering::Relaxed);
                if shutdown_in.load(Ordering::Relaxed) && rx.is_empty() {
                    break;
                }
            }
            // Final non-blocking drain (in case shutdown raced with
            // an in-flight send).
            while let Ok(env) = rx.try_recv() {
                deliver_one(&env, &registry_in, &mut scratch, &sink_in);
                counters_in.delivered.fetch_add(1, Ordering::Relaxed);
            }
            sink_in.flush().await;
            let _ = tier;
        });
        Self {
            sender: parking_lot::Mutex::new(Some(tx)),
            join: AsyncMutex::new(Some(join)),
            shutdown,
            sink,
        }
    }

    /// Try to enqueue `env` on this tier's channel. The error variant
    /// returns the original envelope so the caller can spool it.
    /// Allow `result_large_err` because an envelope is a large struct
    /// and boxing it would defeat the spec's no-allocation contract on
    /// the hot path.
    #[allow(clippy::result_large_err)]
    pub fn try_send(&self, env: ObsEnvelope) -> Result<(), ObsEnvelope> {
        let guard = self.sender.lock();
        let Some(sender) = guard.as_ref() else {
            return Err(env);
        };
        match sender.try_send(env) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(env) | mpsc::error::TrySendError::Closed(env)) => {
                Err(env)
            }
        }
    }

    /// Bounded blocking send used by the AUDIT tier. The future
    /// resolves when the envelope is enqueued or the timeout elapses.
    /// See [`Self::try_send`] for the rationale on `result_large_err`.
    ///
    /// Currently the AUDIT path uses a sync `try_send` busy-wait loop
    /// with `std::thread::sleep` instead of this async helper, so the
    /// `#[allow(dead_code)]` keeps the helper around for callers that
    /// want a future-shaped variant.
    #[allow(clippy::result_large_err, dead_code)]
    pub async fn send_with_timeout(
        &self,
        env: ObsEnvelope,
        timeout: std::time::Duration,
    ) -> Result<(), ObsEnvelope> {
        let sender = match self.sender.lock().as_ref() {
            Some(s) => s.clone(),
            None => return Err(env),
        };
        let cloned = env.clone();
        match tokio::time::timeout(timeout, sender.send(env)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(mpsc::error::SendError(env))) => Err(env),
            Err(_) => Err(cloned),
        }
    }

    /// Drain in-flight envelopes and return when the worker is idle.
    pub async fn flush(&self) {
        // The mpsc backpressure plus the worker's `try_recv` drain
        // make a `flush` call best-effort: yield once to let the
        // worker poll, then call sink.flush() directly.
        tokio::task::yield_now().await;
        self.sink.flush().await;
    }

    /// Shut down the worker: drop the sender (so the receiver's
    /// `recv().await` returns `None` and the loop exits), wait for the
    /// task to finish, then call `Sink::shutdown`.
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Drop the sender so rx.recv() returns None.
        self.sender.lock().take();
        let mut guard = self.join.lock().await;
        if let Some(join) = guard.take() {
            let _ = join.await;
        }
        self.sink.shutdown().await;
    }

    /// Borrow the underlying sink.
    #[allow(dead_code)]
    pub fn sink(&self) -> &Arc<dyn Sink> {
        &self.sink
    }
}

fn deliver_one(
    env: &ObsEnvelope,
    registry: &Arc<SchemaRegistry>,
    scratch: &mut BytesMut,
    sink: &Arc<dyn Sink>,
) {
    scratch.clear();
    let scrubbed = match ScrubbedEnvelope::scrub(env, registry, scratch) {
        Ok(s) => s,
        Err(_) => {
            // Spec 14 § 8 last row — the unscrubbed envelope is never
            // passed to a sink. Drop and increment a counter (the
            // counter itself is surfaced via ObsSinkFailed in a later
            // milestone; for now we silently drop).
            return;
        }
    };
    sink.deliver(scrubbed);
}

/// Adapter: schedule a per-tier worker with a bounded queue, returning
/// the worker handle. Wired by `StandardObserverBuilder::build`.
pub fn spawn_tier_worker(
    tier: Tier,
    cfg: &QueuesConfig,
    sink: Arc<dyn Sink>,
    registry: Arc<SchemaRegistry>,
    counters: Arc<WorkerCounters>,
) -> Option<TierWorker> {
    let cap = match tier {
        Tier::Log => cfg.log,
        Tier::Metric => cfg.metric,
        Tier::Trace => cfg.trace,
        Tier::Audit => cfg.log, /* AUDIT capacity comes from AuditConfig; this caller passes its */
        // own
        _ => return None,
    } as usize;
    if Handle::try_current().is_err() {
        // No tokio runtime → fall back to in-emit-thread synchronous
        // delivery; do not spawn a worker.
        return None;
    }
    Some(TierWorker::spawn(cap, sink, registry, counters, tier))
}

/// Increment the per-tier `channel_full_*` counter when an emit-thread
/// `try_send` fails with full / closed.
pub fn note_channel_full(counters: &WorkerCounters, tier: Tier) {
    let target = match tier {
        Tier::Log => &counters.channel_full_log,
        Tier::Metric => &counters.channel_full_metric,
        Tier::Trace => &counters.channel_full_trace,
        Tier::Audit => &counters.channel_full_audit,
        _ => return,
    };
    target.fetch_add(1, Ordering::Relaxed);
}
