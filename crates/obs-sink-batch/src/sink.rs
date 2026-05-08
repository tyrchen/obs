//! [`BatchingSink`] — the generic wrapper every backend gets for free.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use buffa::Message;
use obs_core::{ScrubbedEnvelope, Sink, sink::SinkFut};
use obs_proto::obs::v1::ObsEnvelope;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    backend::{BatchBackend, UploadError},
    config::{BatchConfig, JitterMode},
    self_events,
    spool::{Spool, SpoolRecord},
};

/// Atomic counters exposed for observability / test-harness
/// introspection.
#[derive(Debug, Default)]
pub struct WorkerCounters {
    /// Envelopes dropped because the ingress mpsc was full.
    pub ingress_dropped: AtomicU64,
    /// Envelopes evicted by the per-partition ring-buffer overflow
    /// guard.
    pub partition_evicted: AtomicU64,
    /// Successful uploads.
    pub uploads: AtomicU64,
    /// Transient retries (not final failures).
    pub retries: AtomicU64,
    /// Batches handed off to the spool.
    pub spooled: AtomicU64,
    /// Spool records successfully recovered.
    pub recovered: AtomicU64,
    /// Spool records escalated to `failed/`.
    pub escalated: AtomicU64,
    /// Envelopes whose encoded size exceeded the 4 GiB frame cap.
    pub envelope_too_large: AtomicU64,
}

/// Batching sink parameterised over a [`BatchBackend`].
///
/// One `BatchingSink` owns one worker task and one spool subtree. The
/// sink is cheap to clone via `Arc` — pass the result of
/// [`Self::new`] as the `Arc<dyn Sink>` argument to
/// [`obs_sdk::InitBuilder::with_sink_for`].
pub struct BatchingSink<B: BatchBackend> {
    ingress: mpsc::Sender<ObsEnvelope>,
    counters: Arc<WorkerCounters>,
    worker: std::sync::Mutex<Option<JoinHandle<()>>>,
    shutdown_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    backend_id: &'static str,
    _phantom: std::marker::PhantomData<B>,
}

impl<B: BatchBackend> fmt::Debug for BatchingSink<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchingSink")
            .field("backend_id", &self.backend_id)
            .field(
                "ingress_dropped",
                &self.counters.ingress_dropped.load(Ordering::Relaxed),
            )
            .field("uploads", &self.counters.uploads.load(Ordering::Relaxed))
            .field("spooled", &self.counters.spooled.load(Ordering::Relaxed))
            .finish()
    }
}

impl<B: BatchBackend> BatchingSink<B> {
    /// Construct a new sink, spawn its worker, and drain any existing
    /// spool records under `{config.spool.root}/{backend.backend_id()}/`.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] when the spool directory cannot be
    /// opened.
    pub async fn new(backend: B, config: BatchConfig) -> std::io::Result<Arc<Self>> {
        let backend_id = backend.backend_id();
        let backend = Arc::new(backend);
        let counters = Arc::new(WorkerCounters::default());

        let spool = Spool::open(backend_id, &config.spool)
            .await
            .map_err(|e| match e {
                crate::spool::SpoolError::Io(io) => io,
                other => std::io::Error::other(other.to_string()),
            })?;

        let (ingress_tx, ingress_rx) = mpsc::channel::<ObsEnvelope>(config.ingress_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let worker = Worker {
            backend: Arc::clone(&backend),
            backend_id,
            config: config.clone(),
            spool: Arc::new(spool),
            counters: Arc::clone(&counters),
            rx: ingress_rx,
            shutdown: shutdown_rx,
            partitions: HashMap::new(),
            overflow_reported: HashSet::new(),
            rng_state: now_ns_wrapping(),
        };
        let handle = tokio::spawn(worker.run());

        Ok(Arc::new(Self {
            ingress: ingress_tx,
            counters,
            worker: std::sync::Mutex::new(Some(handle)),
            shutdown_tx: std::sync::Mutex::new(Some(shutdown_tx)),
            backend_id,
            _phantom: std::marker::PhantomData,
        }))
    }

    /// Access the live worker counters.
    #[must_use]
    pub fn counters(&self) -> &Arc<WorkerCounters> {
        &self.counters
    }

    /// Backend identifier — same value as [`BatchBackend::backend_id`].
    #[must_use]
    pub fn backend_id(&self) -> &'static str {
        self.backend_id
    }
}

impl<B: BatchBackend> Sink for BatchingSink<B> {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        let envelope = env.envelope().clone();
        match self.ingress.try_send(envelope) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let n = self
                    .counters
                    .ingress_dropped
                    .fetch_add(1, Ordering::Relaxed)
                    + 1;
                // Emit self-event once per power-of-two drop count to
                // avoid flooding the observer when the worker is wedged.
                if n.is_power_of_two() {
                    self_events::emit_ingress_dropped(self.backend_id, n);
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.counters
                    .ingress_dropped
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async { tokio::time::sleep(Duration::from_millis(50)).await })
    }

    fn shutdown(&self) -> SinkFut<'_> {
        let signal = self.shutdown_tx.lock().ok().and_then(|mut g| g.take());
        let handle = self.worker.lock().ok().and_then(|mut g| g.take());
        Box::pin(async move {
            if let Some(signal) = signal {
                let _ = signal.send(());
            }
            if let Some(handle) = handle {
                let _ = handle.await;
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

struct Worker<B: BatchBackend> {
    backend: Arc<B>,
    backend_id: &'static str,
    config: BatchConfig,
    spool: Arc<Spool>,
    counters: Arc<WorkerCounters>,
    rx: mpsc::Receiver<ObsEnvelope>,
    shutdown: oneshot::Receiver<()>,
    partitions: HashMap<Option<B::PartitionKey>, Partition<B::PartitionKey>>,
    overflow_reported: HashSet<u64>,
    rng_state: u64,
}

struct Partition<K> {
    key: Option<K>,
    key_hex: String,
    envelopes: VecDeque<ObsEnvelope>,
    bytes: u64,
    opened: Instant,
}

impl<B: BatchBackend> Worker<B> {
    async fn run(mut self) {
        // Recovery runs on the first retry-tick instead of blocking
        // the worker's startup. If recovery stalls on a slow S3
        // endpoint the ingress queue keeps draining, so `deliver`
        // doesn't drop new envelopes while the backlog is being
        // replayed.
        let mut retry_tick =
            tokio::time::interval_at(Instant::now(), self.config.spool.retry_interval);
        retry_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut first_tick = true;

        loop {
            let age_deadline = self.next_age_deadline();
            tokio::select! {
                biased;
                _ = &mut self.shutdown => {
                    self.drain_on_shutdown().await;
                    break;
                }
                maybe = self.rx.recv() => {
                    match maybe {
                        Some(env) => self.admit(env).await,
                        None => {
                            self.drain_on_shutdown().await;
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep_until(age_deadline) => {
                    self.flush_aged().await;
                }
                _ = retry_tick.tick() => {
                    if first_tick {
                        first_tick = false;
                        self.recover_spool().await;
                    } else {
                        self.drive_spool_retry().await;
                    }
                }
            }
        }
    }

    fn next_age_deadline(&self) -> Instant {
        self.partitions
            .values()
            .map(|p| p.opened + self.config.triggers.max_age)
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(60))
    }

    async fn admit(&mut self, env: ObsEnvelope) {
        let partition_key = self.backend.partition_key(&env);
        let key_hex = partition_hex(partition_key.as_ref());
        let encoded_size = u64::from(env.encoded_len());

        // Guard against oversized envelopes before they touch the
        // ring. An envelope whose encoded length exceeds the frame
        // cap cannot round-trip through the spool.
        if encoded_size > u64::from(u32::MAX) {
            self.counters
                .envelope_too_large
                .fetch_add(1, Ordering::Relaxed);
            self_events::emit_envelope_too_large(self.backend_id, &env.full_name, encoded_size);
            return;
        }

        let ring_cap = (self.config.triggers.max_events as usize)
            .saturating_mul(2)
            .max(4);
        // Clone the key once into the Partition; the HashMap's own
        // key is a second clone we can't avoid without `raw_entry`.
        let partition = self
            .partitions
            .entry(partition_key.clone())
            .or_insert_with_key(|k| Partition {
                key: k.clone(),
                key_hex: key_hex.clone(),
                envelopes: VecDeque::with_capacity(ring_cap),
                bytes: 0,
                opened: Instant::now(),
            });

        if partition.envelopes.len() >= ring_cap {
            let evicted = partition.envelopes.pop_front();
            if let Some(old) = evicted {
                let old_size = u64::from(old.encoded_len());
                partition.bytes = partition.bytes.saturating_sub(old_size);
                self.counters
                    .partition_evicted
                    .fetch_add(1, Ordering::Relaxed);
                // Deduplicate overflow self-events by partition hash
                // inside the current flush window.
                let bucket = hash_partition_hex(&partition.key_hex);
                if self.overflow_reported.insert(bucket) {
                    let evicted_total = self.counters.partition_evicted.load(Ordering::Relaxed);
                    self_events::emit_partition_overflow(
                        self.backend_id,
                        &partition.key_hex,
                        evicted_total,
                        self.config.triggers.max_events.saturating_mul(2),
                    );
                }
            }
        }
        partition.envelopes.push_back(env);
        partition.bytes = partition.bytes.saturating_add(encoded_size);

        let should_flush = partition.envelopes.len() as u32 >= self.config.triggers.max_events
            || partition.bytes >= self.config.triggers.max_bytes;
        if should_flush {
            let flushed = self.partitions.remove(&partition_key);
            if let Some(p) = flushed {
                self.flush_partition(p).await;
            }
        }
    }

    async fn flush_aged(&mut self) {
        let now = Instant::now();
        let keys: Vec<_> = self
            .partitions
            .iter()
            .filter(|(_, p)| now.duration_since(p.opened) >= self.config.triggers.max_age)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(p) = self.partitions.remove(&k) {
                self.flush_partition(p).await;
            }
        }
    }

    /// On-shutdown drain — best-effort per design § 9 Q1. Every
    /// still-buffered partition is written straight to the spool
    /// without the retry loop, then recovered by the next process
    /// start. This keeps the drain bounded regardless of how many
    /// partitions are in flight or how slow the backend has become.
    async fn drain_on_shutdown(&mut self) {
        let keys: Vec<_> = self.partitions.keys().cloned().collect();
        for k in keys {
            let Some(partition) = self.partitions.remove(&k) else {
                continue;
            };
            if partition.envelopes.is_empty() {
                continue;
            }
            let envs: Vec<ObsEnvelope> = partition.envelopes.into_iter().collect();
            let (backend_key, describe_partition) = match partition.key.as_ref() {
                Some(k) => self.backend.describe_key(k),
                None => (String::new(), String::new()),
            };
            let partition_label = if describe_partition.is_empty() {
                partition.key_hex.clone()
            } else {
                describe_partition
            };
            self.spool_and_emit(
                &partition.key_hex,
                &backend_key,
                &partition_label,
                &envs,
                0,
                "shutdown drain",
            )
            .await;
        }
    }

    async fn flush_partition(&mut self, partition: Partition<B::PartitionKey>) {
        self.overflow_reported.clear();
        let envs: Vec<ObsEnvelope> = partition.envelopes.into_iter().collect();
        if envs.is_empty() {
            return;
        }
        let events = u32::try_from(envs.len()).unwrap_or(u32::MAX);
        let start = Instant::now();

        // Build the describe_key pair once per flush for label reuse
        // in self-events.
        let (backend_key, describe_partition) = match partition.key.as_ref() {
            Some(k) => self.backend.describe_key(k),
            None => (String::new(), String::new()),
        };
        let partition_label = if describe_partition.is_empty() {
            partition.key_hex.clone()
        } else {
            describe_partition
        };

        // Encode the batch. An encode failure is fatal — nothing
        // further we can do with the envelopes; hand them to the
        // spool so a later version of the backend can re-process.
        let body = match partition.key.as_ref() {
            Some(k) => self.backend.encode_batch(k, &envs),
            None => {
                // No key means we didn't have a partition; the
                // backend can't encode without one, so we spool the
                // envelopes under a synthetic "default" partition.
                self.spool_and_emit(
                    &partition.key_hex,
                    &backend_key,
                    &partition_label,
                    &envs,
                    0,
                    "no partition key",
                )
                .await;
                return;
            }
        };

        let body = match body {
            Ok(body) => body,
            Err(e) => {
                let msg = e.to_string();
                self_events::emit_failed(
                    self.backend_id,
                    &backend_key,
                    &partition_label,
                    0,
                    &format!("encode: {msg}"),
                );
                self.spool_and_emit(
                    &partition.key_hex,
                    &backend_key,
                    &partition_label,
                    &envs,
                    0,
                    &format!("encode: {msg}"),
                )
                .await;
                return;
            }
        };

        let Some(key) = partition.key.as_ref() else {
            unreachable!("body builds only with Some(key)");
        };

        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while attempt < self.config.retry.max_attempts {
            attempt += 1;
            match self.backend.upload(key, &body, attempt).await {
                Ok(()) => {
                    self.counters.uploads.fetch_add(1, Ordering::Relaxed);
                    let duration_ms = start.elapsed().as_millis() as u64;
                    let bytes =
                        u64::try_from(envs.iter().map(|e| e.encoded_len() as usize).sum::<usize>())
                            .unwrap_or(u64::MAX);
                    self_events::emit_uploaded(
                        self.backend_id,
                        &backend_key,
                        &partition_label,
                        events,
                        bytes,
                        duration_ms,
                        attempt,
                    );
                    return;
                }
                Err(UploadError::Fatal(e)) => {
                    let msg = e.to_string();
                    self_events::emit_failed(
                        self.backend_id,
                        &backend_key,
                        &partition_label,
                        attempt,
                        &msg,
                    );
                    self.spool_and_emit(
                        &partition.key_hex,
                        &backend_key,
                        &partition_label,
                        &envs,
                        attempt,
                        &msg,
                    )
                    .await;
                    return;
                }
                Err(UploadError::Retry(e)) => {
                    let msg = e.to_string();
                    last_err = Some(msg.clone());
                    self_events::emit_retry(
                        self.backend_id,
                        &backend_key,
                        &partition_label,
                        attempt,
                        &msg,
                    );
                    if attempt < self.config.retry.max_attempts {
                        self.counters.retries.fetch_add(1, Ordering::Relaxed);
                        let delay = self.compute_backoff(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        let reason = last_err.unwrap_or_else(|| "retries exhausted".to_string());
        self.spool_and_emit(
            &partition.key_hex,
            &backend_key,
            &partition_label,
            &envs,
            attempt,
            &reason,
        )
        .await;
    }

    async fn spool_and_emit(
        &self,
        partition_hex: &str,
        backend_key: &str,
        partition_label: &str,
        envs: &[ObsEnvelope],
        attempts: u32,
        reason: &str,
    ) {
        let events = u32::try_from(envs.len()).unwrap_or(u32::MAX);
        match self.spool.write(partition_hex, envs).await {
            Ok(_) => {
                self.counters.spooled.fetch_add(1, Ordering::Relaxed);
                self_events::emit_spooled(
                    self.backend_id,
                    backend_key,
                    partition_label,
                    events,
                    attempts,
                );
            }
            Err(e) => {
                // Spool itself failed — surface as a failed event so
                // operators see the double fault. Nothing else we can
                // do; the envelopes are lost.
                self_events::emit_failed(
                    self.backend_id,
                    backend_key,
                    partition_label,
                    attempts,
                    &format!("spool write failed: {e}; upstream: {reason}"),
                );
            }
        }
    }

    async fn recover_spool(&mut self) {
        let records = match self.spool.list().await {
            Ok(r) => r,
            Err(e) => {
                self_events::emit_failed(
                    self.backend_id,
                    "",
                    "",
                    0,
                    &format!("spool list failed: {e}"),
                );
                return;
            }
        };
        for rec in records {
            self.try_reship(rec).await;
        }
    }

    async fn drive_spool_retry(&mut self) {
        let records = match self.spool.list().await {
            Ok(r) => r,
            Err(_) => return,
        };
        for rec in records {
            self.try_reship(rec).await;
        }
    }

    /// Re-ship one spool record through the normal batching pipeline.
    ///
    /// Per spec § 3.3 point 3, recovery "re-admits each record as if
    /// it just arrived from the ingress channel" — so we assemble a
    /// synthetic `Partition` and route it through `flush_partition`
    /// so it inherits the full retry policy. This avoids the trap
    /// where a backend upgrade changes the partition key derivation
    /// (we still use the ingest-time envelope fields to derive the
    /// key, but the *encoded body* is built fresh from the spooled
    /// envelopes).
    async fn try_reship(&mut self, rec: SpoolRecord) {
        let SpoolRecord {
            path,
            envelopes,
            first_failed_at_ms,
            partition_hex,
        } = rec;
        if envelopes.is_empty() {
            let _ = self.spool.remove(&path).await;
            return;
        }
        let Some(first) = envelopes.first() else {
            return;
        };
        let partition_key = self.backend.partition_key(first);
        let (backend_key, describe_partition) = match partition_key.as_ref() {
            Some(k) => self.backend.describe_key(k),
            None => (String::new(), String::new()),
        };
        let partition_label = if describe_partition.is_empty() {
            partition_hex.clone()
        } else {
            describe_partition.clone()
        };

        let Some(key) = partition_key.as_ref() else {
            // Cannot re-ship without a partition key; leave the
            // record on disk and let the next upgrade fix the
            // partitioner.
            let _ = (backend_key, partition_label, first_failed_at_ms);
            return;
        };

        let body = match self.backend.encode_batch(key, &envelopes) {
            Ok(b) => b,
            Err(e) => {
                self.escalate_if_stuck(&path, &partition_label, first_failed_at_ms, &e.to_string())
                    .await;
                return;
            }
        };

        // Walk the full retry policy (spec D4 + § 3.2). A transient
        // 5xx during the retry-tick window must not catapult the
        // record to `failed/` just because its age happens to exceed
        // `escalate_after` — give it the same attempt budget as a
        // fresh batch.
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while attempt < self.config.retry.max_attempts {
            attempt += 1;
            match self.backend.upload(key, &body, attempt).await {
                Ok(()) => {
                    self.counters.recovered.fetch_add(1, Ordering::Relaxed);
                    self_events::emit_recovered(
                        self.backend_id,
                        &partition_label,
                        envelopes.len() as u64,
                    );
                    let _ = self.spool.remove(&path).await;
                    return;
                }
                Err(UploadError::Fatal(e)) => {
                    last_err = Some(e.to_string());
                    break;
                }
                Err(UploadError::Retry(e)) => {
                    let msg = e.to_string();
                    last_err = Some(msg.clone());
                    self_events::emit_retry(
                        self.backend_id,
                        &backend_key,
                        &partition_label,
                        attempt,
                        &msg,
                    );
                    if attempt < self.config.retry.max_attempts {
                        self.counters.retries.fetch_add(1, Ordering::Relaxed);
                        let delay = self.compute_backoff(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        let reason = last_err.unwrap_or_else(|| "retries exhausted".to_string());
        self.escalate_if_stuck(&path, &partition_label, first_failed_at_ms, &reason)
            .await;
    }

    async fn escalate_if_stuck(
        &self,
        path: &std::path::Path,
        partition_label: &str,
        first_failed_at_ms: i64,
        last_err: &str,
    ) {
        let now_ms = now_ms_wrapping();
        let age_ms = now_ms.saturating_sub(first_failed_at_ms);
        let escalate_after_ms =
            i64::try_from(self.config.spool.escalate_after.as_millis()).unwrap_or(i64::MAX);
        if age_ms >= escalate_after_ms
            && let Ok(Some(dest)) = self.spool.move_to_failed(path).await
        {
            let age_minutes = u32::try_from(age_ms.max(0) / 60_000).unwrap_or(u32::MAX);
            self.counters.escalated.fetch_add(1, Ordering::Relaxed);
            self_events::emit_escalated(
                self.backend_id,
                partition_label,
                &dest.display().to_string(),
                age_minutes,
                last_err,
            );
        }
    }

    fn compute_backoff(&mut self, attempt: u32) -> Duration {
        let base_ms = self.config.retry.initial_backoff.as_millis() as u64;
        let exp = self
            .config
            .retry
            .multiplier
            .powi(i32::try_from(attempt.saturating_sub(1)).unwrap_or(0));
        let computed_ms =
            (base_ms as f64 * exp).min(self.config.retry.max_backoff.as_millis() as f64);
        let computed_ms = computed_ms.max(0.0) as u64;

        let jitter_ms = match self.config.retry.jitter {
            JitterMode::None => computed_ms,
            JitterMode::FullJitter => {
                let r = splitmix64(&mut self.rng_state);
                if computed_ms == 0 {
                    0
                } else {
                    r % (computed_ms + 1)
                }
            }
            JitterMode::HalfJitter => {
                let r = splitmix64(&mut self.rng_state);
                let half = computed_ms / 2;
                if computed_ms == 0 {
                    0
                } else {
                    half + r % (computed_ms - half + 1)
                }
            }
        };
        Duration::from_millis(jitter_ms)
    }
}

/// Stable hex rendering of the partition key's identity. Used as the
/// filesystem subdir under `{spool_root}/{backend_id}/`.
///
/// Uses BLAKE3 over the key's `Debug` rendering rather than
/// `std::hash::Hasher` because the filesystem layout must be stable
/// across Rust versions and process restarts — `DefaultHasher`
/// (SipHash-1-3) is documented as *not* providing that guarantee.
/// `Debug` is stable within a compiler version for derive'd impls;
/// that's enough for our use case (spool recovery within a
/// same-build process).
fn partition_hex<K: fmt::Debug>(key: Option<&K>) -> String {
    match key {
        None => "default".to_string(),
        Some(k) => {
            let rendered = format!("{k:?}");
            let hash = blake3::hash(rendered.as_bytes());
            let bytes = hash.as_bytes();
            format!(
                "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]
            )
        }
    }
}

/// Stable 64-bit bucket id for dedup inside the worker's
/// `overflow_reported` set. Unlike `partition_hex` this is never
/// persisted, but using a stable hash keeps the two behaviours
/// consistent.
fn hash_partition_hex(hex: &str) -> u64 {
    let hash = blake3::hash(hex.as_bytes());
    let b = hash.as_bytes();
    u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

fn now_ns_wrapping() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn now_ms_wrapping() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// SplitMix64 — a small deterministic PRNG. Good enough for retry
/// jitter; avoids pulling in `rand`.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    use buffa::EnumValue;
    use obs_core::{InMemoryObserver, install_observer, observer};
    use obs_proto::obs::v1::{ObsEnvelope, Severity as PSeverity, Tier as PTier};
    use tempfile::tempdir;

    use super::*;
    use crate::{
        BatchBackend, BatchConfig, BatchTriggers, JitterMode, RetryPolicy, SpoolConfig, UploadError,
    };

    struct MockBackend {
        attempts: Arc<AtomicU32>,
        behavior: Behavior,
    }

    #[derive(Clone, Copy)]
    enum Behavior {
        AlwaysOk,
        FailNThenOk(u32),
        FatalImmediately,
        AlwaysRetry,
    }

    impl BatchBackend for MockBackend {
        type PartitionKey = u32;
        type Body = Vec<ObsEnvelope>;
        type Error = String;

        fn backend_id(&self) -> &'static str {
            "mock"
        }

        fn partition_key(&self, _env: &ObsEnvelope) -> Option<Self::PartitionKey> {
            Some(0)
        }

        fn encode_batch(
            &self,
            _key: &Self::PartitionKey,
            envs: &[ObsEnvelope],
        ) -> Result<Self::Body, Self::Error> {
            Ok(envs.to_vec())
        }

        async fn upload(
            &self,
            _key: &Self::PartitionKey,
            _body: &Self::Body,
            _attempt: u32,
        ) -> Result<(), UploadError<Self::Error>> {
            let n = self.attempts.fetch_add(1, Ordering::Relaxed) + 1;
            match self.behavior {
                Behavior::AlwaysOk => Ok(()),
                Behavior::FailNThenOk(k) if n <= k => Err(UploadError::Retry("boom".into())),
                Behavior::FailNThenOk(_) => Ok(()),
                Behavior::FatalImmediately => Err(UploadError::Fatal("nope".into())),
                Behavior::AlwaysRetry => Err(UploadError::Retry("transient".into())),
            }
        }
    }

    fn sample_env() -> ObsEnvelope {
        ObsEnvelope {
            full_name: "obs.test.Probe".to_string(),
            tier: EnumValue::Known(PTier::TIER_LOG),
            sev: EnumValue::Known(PSeverity::SEVERITY_INFO),
            ts_ns: 1,
            ..Default::default()
        }
    }

    fn fast_config(root: std::path::PathBuf) -> BatchConfig {
        BatchConfig {
            ingress_capacity: 64,
            triggers: BatchTriggers {
                max_events: 2,
                max_bytes: u64::MAX,
                max_age: Duration::from_millis(20),
            },
            retry: RetryPolicy {
                max_attempts: 3,
                initial_backoff: Duration::from_millis(1),
                multiplier: 2.0,
                max_backoff: Duration::from_millis(10),
                jitter: JitterMode::None,
            },
            spool: SpoolConfig {
                root,
                max_bytes: 1 << 20,
                retry_interval: Duration::from_secs(60),
                escalate_after: Duration::from_secs(60),
                fsync_mode: crate::FsyncMode::None,
            },
        }
    }

    #[tokio::test]
    async fn test_successful_upload_closes_batch_at_count_trigger() {
        install_observer(InMemoryObserver::new());
        let dir = tempdir().unwrap();
        let cfg = fast_config(dir.path().to_path_buf());
        let backend = MockBackend {
            attempts: Arc::new(AtomicU32::new(0)),
            behavior: Behavior::AlwaysOk,
        };
        let sink = BatchingSink::new(backend, cfg).await.unwrap();

        // Count trigger fires at 2 envelopes.
        let registry = obs_core::SchemaRegistry::empty();
        let env1 = sample_env();
        let env2 = sample_env();
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env1, &registry));
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env2, &registry));

        // Give the worker a moment to pick up and upload.
        for _ in 0..50 {
            if sink.counters().uploads.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(sink.counters().uploads.load(Ordering::Relaxed), 1);

        let _ = observer();
    }

    #[tokio::test]
    async fn test_transient_errors_retry_then_succeed() {
        install_observer(InMemoryObserver::new());
        let dir = tempdir().unwrap();
        let cfg = fast_config(dir.path().to_path_buf());
        let attempts = Arc::new(AtomicU32::new(0));
        let backend = MockBackend {
            attempts: Arc::clone(&attempts),
            behavior: Behavior::FailNThenOk(2),
        };
        let sink = BatchingSink::new(backend, cfg).await.unwrap();
        let registry = obs_core::SchemaRegistry::empty();
        let env1 = sample_env();
        let env2 = sample_env();
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env1, &registry));
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env2, &registry));
        for _ in 0..100 {
            if sink.counters().uploads.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(sink.counters().uploads.load(Ordering::Relaxed), 1);
        assert!(attempts.load(Ordering::Relaxed) >= 3);
    }

    #[tokio::test]
    async fn test_fatal_spools_immediately() {
        install_observer(InMemoryObserver::new());
        let dir = tempdir().unwrap();
        let cfg = fast_config(dir.path().to_path_buf());
        let backend = MockBackend {
            attempts: Arc::new(AtomicU32::new(0)),
            behavior: Behavior::FatalImmediately,
        };
        let sink = BatchingSink::new(backend, cfg).await.unwrap();
        let registry = obs_core::SchemaRegistry::empty();
        let env1 = sample_env();
        let env2 = sample_env();
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env1, &registry));
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env2, &registry));
        for _ in 0..100 {
            if sink.counters().spooled.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(sink.counters().spooled.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retries_exhausted_spools() {
        install_observer(InMemoryObserver::new());
        let dir = tempdir().unwrap();
        let cfg = fast_config(dir.path().to_path_buf());
        let backend = MockBackend {
            attempts: Arc::new(AtomicU32::new(0)),
            behavior: Behavior::AlwaysRetry,
        };
        let sink = BatchingSink::new(backend, cfg).await.unwrap();
        let registry = obs_core::SchemaRegistry::empty();
        let env1 = sample_env();
        let env2 = sample_env();
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env1, &registry));
        sink.deliver(obs_core::ScrubbedEnvelope::for_test(&env2, &registry));
        for _ in 0..100 {
            if sink.counters().spooled.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(sink.counters().spooled.load(Ordering::Relaxed), 1);
    }
}
