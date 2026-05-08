#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

//! In-process live-tail subscriber registry for the obs SDK.
//!
//! Mount a [`LiveTailSink`] on any tier via
//! `StandardObserverBuilder::sink_for` (or wrap inside a
//! [`obs_core::FanOutSink`]) to mirror every envelope into bounded
//! subscriber channels. Each subscriber carries a [`SubscriberFilter`]
//! (label matchers + min-severity) applied per envelope, with bounded
//! caps sourced from [`LiveTailConfig`] so a flood of subscribers
//! cannot OOM the process.
//!
//! Boundary-review § 3.1 (moved upstream from `tok-obs::live_tail`).
//!
//! # Non-goals
//!
//! Subscription protocol, HTTP transport, authentication. Those belong
//! to the service's SSE / WebSocket handler, which calls
//! [`LiveTailRegistry::subscribe`] and reads from the returned
//! [`SubscriberReceiver`].
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use obs_live_tail::{LiveTailConfig, LiveTailRegistry, SubscriberFilter};
//!
//! # async fn demo() {
//! let registry = LiveTailRegistry::new(LiveTailConfig::default());
//! let sink = registry.sink();
//! // mount `sink` on the observer's LOG tier via
//! // `StandardObserverBuilder::sink_for(Tier::Log, sink)` …
//!
//! let mut filter = SubscriberFilter::default();
//! filter.label_matchers.insert("tenant_id".into(), "acme".into());
//! let (_id, mut rx) = registry.subscribe(filter).expect("under caps");
//! while let Some(env) = rx.recv().await {
//!     println!("{} {}", env.full_name, env.ts_ns);
//! }
//! # }
//! ```

use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use dashmap::DashMap;
use obs_core::{ScrubbedEnvelope, sink::SinkFut};
use obs_proto::obs::v1::ObsEnvelope;
use obs_types::Severity;
use thiserror::Error;
use tokio::sync::mpsc;

/// Identifier for a subscriber. Opaque; allocated monotonically per
/// registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriberId(u64);

impl SubscriberId {
    /// Underlying numeric id. Useful for admin-endpoint rendering.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Re-attach a numeric id for routes that parse it off a URL. A
    /// mismatched id returns `false` from [`LiveTailRegistry::kick`] /
    /// [`LiveTailRegistry::unsubscribe`].
    #[must_use]
    pub const fn from_u64(id: u64) -> Self {
        Self(id)
    }
}

impl fmt::Display for SubscriberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Caps + idle-timeout governing a [`LiveTailRegistry`].
///
/// `max_per_key` maps a label key (e.g. `"tenant_id"`, `"function_id"`)
/// to the maximum number of concurrent subscribers that filter on that
/// key. A subscriber whose filter matches on `k=v` counts against
/// `max_per_key["k"]`. `max_total` caps the registry globally.
#[derive(Debug, Clone)]
pub struct LiveTailConfig {
    /// Per-label-key caps. Missing keys fall back to `u32::MAX`
    /// (unbounded).
    pub max_per_key: HashMap<String, u32>,
    /// Global subscriber cap.
    pub max_total: u32,
    /// Idle-eviction budget. A subscriber is swept after this long
    /// without a successful delivery. Idle sweeps run on explicit
    /// [`LiveTailRegistry::sweep_idle`] calls — the caller owns the
    /// cadence.
    pub idle_timeout: Duration,
    /// Per-subscriber channel capacity. `0` treats as
    /// [`DEFAULT_CHANNEL`].
    pub channel_capacity: usize,
}

/// Default per-subscriber channel capacity — roughly a second of burst
/// at a typical LOG-tier emit rate. Overflow increments the drop
/// counter; a slow subscriber cannot back-pressure emit.
pub const DEFAULT_CHANNEL: usize = 256;

impl Default for LiveTailConfig {
    fn default() -> Self {
        Self {
            max_per_key: HashMap::new(),
            max_total: 500,
            idle_timeout: Duration::from_secs(300),
            channel_capacity: DEFAULT_CHANNEL,
        }
    }
}

/// Filter applied to every envelope before fan-out.
///
/// An empty filter matches everything. Concrete fields narrow the
/// match with AND semantics. `label_matchers` each require the
/// envelope's `labels[k] == v`. Label keys unknown to the envelope
/// cause the filter to reject (absent ≠ match).
#[derive(Debug, Clone, Default)]
pub struct SubscriberFilter {
    /// Required envelope labels — AND semantics. Use e.g.
    /// `"tenant_id" → "acme"`, `"function_id" → "handler-42"`.
    pub label_matchers: HashMap<String, String>,
    /// Minimum severity. Envelopes with `sev < min_severity` drop.
    pub min_severity: Option<Severity>,
}

impl SubscriberFilter {
    fn matches(&self, env: &ObsEnvelope) -> bool {
        for (k, want) in &self.label_matchers {
            let Some(got) = env.labels.get(k) else {
                return false;
            };
            if got != want {
                return false;
            }
        }
        if let Some(min) = self.min_severity {
            let got = match env.sev {
                ::buffa::EnumValue::Known(s) => proto_severity_rank(s),
                ::buffa::EnumValue::Unknown(_) => 0,
            };
            if got < severity_rank(min) {
                return false;
            }
        }
        true
    }
}

/// Outcome of [`LiveTailRegistry::subscribe`].
#[derive(Debug, Clone, Error)]
pub enum SubscribeError {
    /// `max_per_key[key]` exhausted. `key` is the label key whose cap
    /// tripped (e.g. `"tenant_id"`); `value` is the matcher value that
    /// triggered the check.
    #[error("live-tail: per-key cap exceeded ({current}/{cap}) for {key}={value}")]
    PerKeyCap {
        /// Label key whose per-key cap tripped.
        key: String,
        /// Matcher value being registered.
        value: String,
        /// Cap in effect.
        cap: u32,
        /// Current subscriber count matching this `(key, value)`.
        current: u32,
    },
    /// `max_total` exhausted.
    #[error("live-tail: total subscriber cap exceeded ({current}/{cap})")]
    TotalCap {
        /// Cap in effect.
        cap: u32,
        /// Current subscriber count.
        current: u32,
    },
}

/// Receiver handed back by [`LiveTailRegistry::subscribe`]. The
/// consumer (HTTP handler, CLI tool, test) reads envelopes off this
/// and forwards them until `recv` returns `None`.
#[derive(Debug)]
pub struct SubscriberReceiver {
    rx: mpsc::Receiver<ObsEnvelope>,
}

impl SubscriberReceiver {
    /// Await the next envelope. `None` once the sender drops.
    pub async fn recv(&mut self) -> Option<ObsEnvelope> {
        self.rx.recv().await
    }

    /// Non-async polling form for hand-rolled `Stream` adapters.
    pub fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<ObsEnvelope>> {
        self.rx.poll_recv(cx)
    }

    /// Sync try-recv. Returns `Err(TryRecvError::Empty)` when no
    /// envelope is queued, `Err(TryRecvError::Disconnected)` once the
    /// sender drops. Handy for tests that assert without `.await`.
    pub fn try_recv(&mut self) -> Result<ObsEnvelope, tokio::sync::mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

/// Admin snapshot of a subscriber.
#[derive(Debug, Clone)]
pub struct SubscriberInfo {
    /// Stable id.
    pub id: SubscriberId,
    /// Filter in effect (cloned).
    pub filter: SubscriberFilter,
    /// Wall-clock time of subscribe, in unix millis.
    pub connected_at_ms: i64,
    /// Envelopes delivered.
    pub delivered: u64,
    /// Envelopes dropped (slow subscriber — channel full).
    pub dropped: u64,
}

/// Process-scoped registry of active subscribers.
#[derive(Debug, Clone)]
pub struct LiveTailRegistry {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    subscribers: DashMap<SubscriberId, Subscriber>,
    next_id: AtomicU64,
    config: LiveTailConfig,
}

#[derive(Debug)]
struct Subscriber {
    filter: SubscriberFilter,
    tx: mpsc::Sender<ObsEnvelope>,
    delivered: AtomicU64,
    dropped: AtomicU64,
    connected_at_ms: i64,
    /// Millis-since-process-anchor of the last successful delivery.
    /// `AtomicU64` on the hot path — no `Mutex<Instant>`.
    last_delivery_ms: AtomicU64,
}

impl LiveTailRegistry {
    /// Build an empty registry governed by `config`.
    #[must_use]
    pub fn new(config: LiveTailConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                subscribers: DashMap::new(),
                next_id: AtomicU64::new(1),
                config,
            }),
        }
    }

    /// Attempt to register a subscriber. Enforces all per-key caps
    /// matched by `filter.label_matchers`, plus the global `max_total`.
    ///
    /// Eagerly sweeps subscribers whose receiver has dropped before
    /// running cap enforcement so bursty connect/disconnect reclaims
    /// slots promptly (without waiting for [`Self::sweep_idle`]).
    ///
    /// # Errors
    ///
    /// Returns a [`SubscribeError`] when any cap is exceeded.
    pub fn subscribe(
        &self,
        filter: SubscriberFilter,
    ) -> Result<(SubscriberId, SubscriberReceiver), SubscribeError> {
        self.sweep_closed_channels();
        self.enforce_caps(&filter)?;

        let id = SubscriberId(self.inner.next_id.fetch_add(1, Ordering::Relaxed));
        let capacity = if self.inner.config.channel_capacity == 0 {
            DEFAULT_CHANNEL
        } else {
            self.inner.config.channel_capacity
        };
        let (tx, rx) = mpsc::channel(capacity);
        let now_ms = now_unix_millis();
        let sub = Subscriber {
            filter,
            tx,
            delivered: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            connected_at_ms: now_ms,
            last_delivery_ms: AtomicU64::new(now_anchor_ms()),
        };
        self.inner.subscribers.insert(id, sub);
        Ok((id, SubscriberReceiver { rx }))
    }

    fn sweep_closed_channels(&self) {
        let mut to_drop: Vec<SubscriberId> = Vec::new();
        for entry in &self.inner.subscribers {
            if entry.value().tx.is_closed() {
                to_drop.push(*entry.key());
            }
        }
        for id in to_drop {
            self.unsubscribe(id);
        }
    }

    /// Remove a subscriber. Returns `true` when `id` matched.
    pub fn unsubscribe(&self, id: SubscriberId) -> bool {
        self.inner.subscribers.remove(&id).is_some()
    }

    /// Forcibly close `id`. Alias for [`Self::unsubscribe`] — kept as a
    /// distinct entry point so admin-endpoint callers can log the
    /// intent. Returns `true` when `id` matched.
    pub fn kick(&self, id: SubscriberId) -> bool {
        self.unsubscribe(id)
    }

    /// Evict every subscriber idle for more than `config.idle_timeout`.
    /// Returns the number of evictions.
    pub fn sweep_idle(&self) -> usize {
        let budget_ms =
            u64::try_from(self.inner.config.idle_timeout.as_millis()).unwrap_or(u64::MAX);
        let now_ms = now_anchor_ms();
        let mut to_remove = Vec::new();
        for entry in &self.inner.subscribers {
            let last_ms = entry.value().last_delivery_ms.load(Ordering::Relaxed);
            if now_ms.saturating_sub(last_ms) > budget_ms {
                to_remove.push(*entry.key());
            }
        }
        let removed = to_remove.len();
        for id in to_remove {
            self.unsubscribe(id);
        }
        removed
    }

    /// Count subscribers matching a predicate over their filter.
    pub fn count_by<F>(&self, mut predicate: F) -> u32
    where
        F: FnMut(&SubscriberFilter) -> bool,
    {
        let mut n: u32 = 0;
        for entry in &self.inner.subscribers {
            if predicate(&entry.value().filter) {
                n = n.saturating_add(1);
            }
        }
        n
    }

    /// Total live subscribers.
    #[must_use]
    pub fn total(&self) -> u32 {
        u32::try_from(self.inner.subscribers.len()).unwrap_or(u32::MAX)
    }

    /// Snapshot every subscriber. Order: ascending `SubscriberId`.
    #[must_use]
    pub fn list(&self) -> Vec<SubscriberInfo> {
        let mut rows = Vec::with_capacity(self.inner.subscribers.len());
        for entry in &self.inner.subscribers {
            let sub = entry.value();
            rows.push(SubscriberInfo {
                id: *entry.key(),
                filter: sub.filter.clone(),
                connected_at_ms: sub.connected_at_ms,
                delivered: sub.delivered.load(Ordering::Relaxed),
                dropped: sub.dropped.load(Ordering::Relaxed),
            });
        }
        rows.sort_by_key(|r| r.id);
        rows
    }

    /// Configuration snapshot.
    #[must_use]
    pub fn config(&self) -> &LiveTailConfig {
        &self.inner.config
    }

    /// Build a [`LiveTailSink`] backed by this registry. Mount on the
    /// observer via `StandardObserverBuilder::sink_for(tier, sink)`.
    #[must_use]
    pub fn sink(&self) -> Arc<LiveTailSink> {
        Arc::new(LiveTailSink {
            registry: self.clone(),
        })
    }

    fn enforce_caps(&self, filter: &SubscriberFilter) -> Result<(), SubscribeError> {
        let config = &self.inner.config;
        let total = self.total();
        if total >= config.max_total {
            return Err(SubscribeError::TotalCap {
                cap: config.max_total,
                current: total,
            });
        }
        for (key, value) in &filter.label_matchers {
            let cap = config.max_per_key.get(key).copied().unwrap_or(u32::MAX);
            if cap == u32::MAX {
                continue;
            }
            let current = self.count_by(|f| f.label_matchers.get(key) == Some(value));
            if current >= cap {
                return Err(SubscribeError::PerKeyCap {
                    key: key.clone(),
                    value: value.clone(),
                    cap,
                    current,
                });
            }
        }
        Ok(())
    }

    /// Fan an envelope out to every matching subscriber.
    ///
    /// Exposed publicly so callers can inject envelopes directly
    /// (replay CLIs, tests) without routing through an observer.
    pub fn deliver_envelope(&self, env: &ObsEnvelope) {
        let mut to_drop: Vec<SubscriberId> = Vec::new();
        for entry in &self.inner.subscribers {
            let sub = entry.value();
            if !sub.filter.matches(env) {
                continue;
            }
            match sub.tx.try_send(env.clone()) {
                Ok(()) => {
                    sub.delivered.fetch_add(1, Ordering::Relaxed);
                    sub.last_delivery_ms
                        .store(now_anchor_ms(), Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.dropped.fetch_add(1, Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    to_drop.push(*entry.key());
                }
            }
        }
        for id in to_drop {
            self.unsubscribe(id);
        }
    }
}

/// `Sink` impl that mirrors every envelope onto matching subscribers.
#[derive(Debug)]
pub struct LiveTailSink {
    registry: LiveTailRegistry,
}

impl obs_core::Sink for LiveTailSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        self.registry.deliver_envelope(env.envelope());
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async {})
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async {})
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

fn anchor() -> Instant {
    static A: OnceLock<Instant> = OnceLock::new();
    *A.get_or_init(Instant::now)
}

fn now_anchor_ms() -> u64 {
    let elapsed = anchor().elapsed();
    u64::try_from(elapsed.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
}

fn severity_rank(s: Severity) -> i32 {
    match s {
        Severity::Trace => 1,
        Severity::Debug => 2,
        Severity::Info => 3,
        Severity::Warn => 4,
        Severity::Error => 5,
        Severity::Fatal => 6,
        _ => 0,
    }
}

fn proto_severity_rank(s: obs_proto::obs::v1::Severity) -> i32 {
    use obs_proto::obs::v1::Severity as P;
    match s {
        P::SEVERITY_TRACE => 1,
        P::SEVERITY_DEBUG => 2,
        P::SEVERITY_INFO => 3,
        P::SEVERITY_WARN => 4,
        P::SEVERITY_ERROR => 5,
        P::SEVERITY_FATAL => 6,
        P::SEVERITY_UNSPECIFIED => 0,
    }
}

#[cfg(test)]
mod tests {
    use buffa::{__private::HashMap as LabelMap, EnumValue};
    use obs_proto::obs::v1::Severity as PSev;

    use super::*;

    fn config(max_per_function: u32, max_per_tenant: u32, total: u32) -> LiveTailConfig {
        let mut max_per_key = HashMap::new();
        max_per_key.insert("function_id".to_string(), max_per_function);
        max_per_key.insert("tenant_id".to_string(), max_per_tenant);
        LiveTailConfig {
            max_per_key,
            max_total: total,
            idle_timeout: Duration::from_secs(300),
            channel_capacity: DEFAULT_CHANNEL,
        }
    }

    fn env_with(tenant: &str, function: &str, sev: PSev) -> ObsEnvelope {
        let mut labels = LabelMap::default();
        labels.insert("tenant_id".to_string(), tenant.to_string());
        labels.insert("function_id".to_string(), function.to_string());
        ObsEnvelope {
            full_name: "obs.test.LiveTail".to_string(),
            sev: EnumValue::Known(sev),
            labels,
            ..Default::default()
        }
    }

    #[test]
    fn test_subscribe_returns_unique_ids() {
        let reg = LiveTailRegistry::new(config(10, 10, 10));
        let (a, _rx_a) = reg.subscribe(SubscriberFilter::default()).expect("a");
        let (b, _rx_b) = reg.subscribe(SubscriberFilter::default()).expect("b");
        assert_ne!(a, b);
        assert_eq!(reg.total(), 2);
    }

    #[test]
    fn test_subscribe_sweeps_closed_channels_before_cap_enforcement() {
        let reg = LiveTailRegistry::new(config(10, 10, 1));
        let (_a, _) = reg.subscribe(SubscriberFilter::default()).expect("a");
        // First rx dropped → its channel is closed. Second subscribe
        // must succeed because the closed entry is swept first.
        let (_b, _rx) = reg.subscribe(SubscriberFilter::default()).expect("b");
        assert_eq!(reg.total(), 1);
    }

    #[test]
    fn test_enforces_total_cap() {
        let reg = LiveTailRegistry::new(config(10, 10, 1));
        let (_a, _rx) = reg.subscribe(SubscriberFilter::default()).expect("first");
        let err = reg
            .subscribe(SubscriberFilter::default())
            .expect_err("cap hit");
        assert!(matches!(err, SubscribeError::TotalCap { .. }));
    }

    #[test]
    fn test_enforces_per_key_cap_for_function_id() {
        let reg = LiveTailRegistry::new(config(1, 10, 10));
        let mut filter = SubscriberFilter::default();
        filter
            .label_matchers
            .insert("function_id".to_string(), "fn-1".to_string());
        let (_id, _rx) = reg.subscribe(filter.clone()).expect("first");
        let err = reg.subscribe(filter).expect_err("cap hit");
        assert!(matches!(
            err,
            SubscribeError::PerKeyCap { ref key, .. } if key == "function_id"
        ));
    }

    #[test]
    fn test_enforces_per_key_cap_for_tenant_id() {
        let reg = LiveTailRegistry::new(config(10, 1, 10));
        let mut filter = SubscriberFilter::default();
        filter
            .label_matchers
            .insert("tenant_id".to_string(), "acme".to_string());
        let (_id, _rx) = reg.subscribe(filter.clone()).expect("first");
        let err = reg.subscribe(filter).expect_err("cap hit");
        assert!(matches!(
            err,
            SubscribeError::PerKeyCap { ref key, .. } if key == "tenant_id"
        ));
    }

    #[tokio::test]
    async fn test_delivers_matching_envelopes_and_filters_mismatches() {
        let reg = LiveTailRegistry::new(config(10, 10, 10));
        let mut filter = SubscriberFilter::default();
        filter
            .label_matchers
            .insert("function_id".to_string(), "fn-a".to_string());
        let (_id, mut rx) = reg.subscribe(filter).expect("ok");

        reg.deliver_envelope(&env_with("t1", "fn-a", PSev::SEVERITY_INFO));
        reg.deliver_envelope(&env_with("t1", "fn-b", PSev::SEVERITY_INFO));

        let got = rx.recv().await.expect("one envelope");
        assert_eq!(
            got.labels.get("function_id").map(String::as_str),
            Some("fn-a"),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), rx.recv())
                .await
                .is_err(),
            "fn-b must not reach the subscriber",
        );
    }

    #[test]
    fn test_min_severity_filter_drops_below_floor() {
        let reg = LiveTailRegistry::new(config(10, 10, 10));
        let filter = SubscriberFilter {
            label_matchers: HashMap::new(),
            min_severity: Some(Severity::Error),
        };
        let (_id, mut rx) = reg.subscribe(filter).expect("ok");
        reg.deliver_envelope(&env_with("t1", "fn-a", PSev::SEVERITY_INFO));
        reg.deliver_envelope(&env_with("t1", "fn-a", PSev::SEVERITY_ERROR));
        let got = rx.try_recv().expect("one envelope");
        assert_eq!(got.sev.to_i32(), PSev::SEVERITY_ERROR as i32);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_unsubscribe_removes_slot() {
        let reg = LiveTailRegistry::new(config(10, 10, 10));
        let (id, _rx) = reg.subscribe(SubscriberFilter::default()).expect("ok");
        assert!(reg.unsubscribe(id));
        assert_eq!(reg.total(), 0);
    }

    #[tokio::test]
    async fn test_drop_counter_increments_on_full_channel() {
        let reg = LiveTailRegistry::new(config(10, 10, 10));
        let (_id, _rx) = reg.subscribe(SubscriberFilter::default()).expect("ok");
        for _ in 0..(DEFAULT_CHANNEL + 4) {
            reg.deliver_envelope(&env_with("t1", "fn-a", PSev::SEVERITY_INFO));
        }
        let list = reg.list();
        assert_eq!(list.len(), 1);
        assert!(list[0].dropped > 0, "expected drops: {list:?}");
    }

    #[test]
    fn test_label_absent_means_no_match() {
        // Filter requiring `function_id` must reject envelopes that
        // don't carry that label at all — absent ≠ match.
        let reg = LiveTailRegistry::new(config(10, 10, 10));
        let mut filter = SubscriberFilter::default();
        filter
            .label_matchers
            .insert("function_id".to_string(), "fn-a".to_string());
        let (_id, mut rx) = reg.subscribe(filter).expect("ok");

        let env = ObsEnvelope {
            full_name: "obs.test.NoLabels".to_string(),
            sev: EnumValue::Known(PSev::SEVERITY_INFO),
            ..Default::default()
        };
        reg.deliver_envelope(&env);
        assert!(rx.try_recv().is_err(), "absent label must not match");
    }
}
