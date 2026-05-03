//! Per-batch OTLP trace payload, including the Started/Completed pair
//! mapping. Spec 20 § 2.5.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use obs_core::SchemaRegistry;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    env_config::{OtlpEndpoint, OtlpResourceAttrs},
    logs::ResourceMessage,
    mapping::{SpanEventRecord, SpanRecord, project_span},
};

/// Default time after which a pending Started event is considered
/// orphaned and dropped from the tracker. Spec 93 P1-7.
pub const DEFAULT_PAIR_TIMEOUT: Duration = Duration::from_secs(60);

/// `TracesData`-shape payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpTracePayload {
    /// Resource attrs.
    pub resource: ResourceMessage,
    /// Endpoint URL.
    pub endpoint: String,
    /// Span records.
    pub spans: Vec<SpanRecord>,
    /// `full_name`s whose Started entry timed out without a matching
    /// Completed; the trace sink turns these into
    /// `ObsSpanPairOrphaned` self-events. Spec 93 P1-7.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orphaned: Vec<String>,
}

/// Tracks pending `*Started` events keyed on `(trace_id, span_id)` so
/// we can attach them to the corresponding `*Completed` span when it
/// arrives. Spec 20 § 2.5 B / spec 93 P1-7.
#[derive(Debug)]
pub struct SpanPairTracker {
    pending: Mutex<HashMap<(String, String), Pending>>,
    timeout: Duration,
}

#[derive(Debug)]
struct Pending {
    record: SpanEventRecord,
    /// `full_name` of the started event — used to emit
    /// `ObsSpanPairOrphaned{full_name}` when the timeout elapses.
    full_name: String,
    queued_at: Instant,
}

impl Default for SpanPairTracker {
    fn default() -> Self {
        Self::with_timeout(DEFAULT_PAIR_TIMEOUT)
    }
}

impl SpanPairTracker {
    /// Construct a tracker with a custom orphan timeout.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
            timeout,
        }
    }

    fn note_started(&self, env: &obs_proto::obs::v1::ObsEnvelope) {
        if env.trace_id.is_empty() && env.span_id.is_empty() {
            return;
        }
        let key = (env.trace_id.clone(), env.span_id.clone());
        let event = SpanEventRecord {
            name: "started".to_string(),
            time_unix_nano: env.ts_ns,
            attributes: env
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        self.pending.lock().insert(
            key,
            Pending {
                record: event,
                full_name: env.full_name.clone(),
                queued_at: Instant::now(),
            },
        );
    }

    fn pop_started(&self, env: &obs_proto::obs::v1::ObsEnvelope) -> Option<SpanEventRecord> {
        let key = (env.trace_id.clone(), env.span_id.clone());
        self.pending.lock().remove(&key).map(|p| p.record)
    }

    /// Drop entries older than `timeout`. Returns the `full_name`s of
    /// expired Started events so the caller can emit
    /// `ObsSpanPairOrphaned` for each. Spec 93 P1-7.
    pub fn collect_orphaned(&self) -> Vec<String> {
        let mut out = Vec::new();
        let now = Instant::now();
        let timeout = self.timeout;
        let mut pending = self.pending.lock();
        pending.retain(|_, p| {
            let stale = now.saturating_duration_since(p.queued_at) >= timeout;
            if stale {
                out.push(p.full_name.clone());
            }
            !stale
        });
        out
    }
}

impl OtlpTracePayload {
    /// Project envelopes onto spans. Implements the three-pattern
    /// mapping (Started/Completed pair, duration field, point-in-time).
    ///
    /// Started/Completed pairing is now driven by `EventSchemaErased::
    /// spans_paired_with()` rather than suffix sniffing on
    /// `*Started`/`*Completed`. Spec 93 P1-7.
    #[must_use]
    pub fn from_envelopes(
        envs: &[obs_proto::obs::v1::ObsEnvelope],
        resource: &OtlpResourceAttrs,
        endpoint: &OtlpEndpoint,
        tracker: &SpanPairTracker,
        registry: &SchemaRegistry,
    ) -> Self {
        let mut spans = Vec::with_capacity(envs.len());
        for env in envs {
            // Look up paired sibling: if `spans_paired_with()` is
            // `Some(other)` and there is no matching pending Started for
            // (trace_id, span_id), this envelope is the Started half.
            // Otherwise it's the Completed half (or unrelated).
            let paired = registry.lookup(env).and_then(|s| s.spans_paired_with());
            let is_started_half = match paired {
                Some(_) => {
                    // Heuristic: if a Started entry already exists at
                    // (trace_id, span_id), this envelope is the
                    // Completed half. Otherwise treat as Started.
                    let key = (env.trace_id.clone(), env.span_id.clone());
                    let already_pending = tracker.pending.lock().contains_key(&key);
                    !already_pending
                }
                None => false,
            };
            // Fallback: legacy suffix sniffing for schemas that haven't
            // declared `paired_with` yet.
            let legacy_started =
                env.full_name.ends_with("Started") || env.full_name.ends_with(".Start");
            if is_started_half || legacy_started {
                tracker.note_started(env);
                continue;
            }
            // Pattern A — duration field on env.labels.
            let duration_ns = env
                .labels
                .get("latency_ns")
                .and_then(|v| v.parse::<u64>().ok())
                .or_else(|| {
                    env.labels
                        .get("latency_ms")
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(|ms| ms.saturating_mul(1_000_000))
                });
            let mut span = project_span(env, duration_ns);
            // Pattern B — pair with sibling Started, if any.
            if let Some(started) = tracker.pop_started(env) {
                span.events.push(started);
            }
            spans.push(span);
        }
        // Sweep orphans whose Started timed out without a Completed.
        let orphaned = tracker.collect_orphaned();
        Self {
            resource: ResourceMessage::from_attrs(resource),
            endpoint: endpoint.url.clone(),
            spans,
            orphaned,
        }
    }
}
