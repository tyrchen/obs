//! Per-batch OTLP trace payload, including the Started/Completed pair
//! mapping. Spec 20 § 2.5.

use std::collections::HashMap;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    env_config::{OtlpEndpoint, OtlpResourceAttrs},
    logs::ResourceMessage,
    mapping::{SpanEventRecord, SpanRecord, project_span},
};

/// `TracesData`-shape payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpTracePayload {
    /// Resource attrs.
    pub resource: ResourceMessage,
    /// Endpoint URL.
    pub endpoint: String,
    /// Span records.
    pub spans: Vec<SpanRecord>,
}

/// Tracks pending `*Started` events keyed on `(trace_id, span_id)` so
/// we can attach them to the corresponding `*Completed` span when it
/// arrives. Spec 20 § 2.5 B.
#[derive(Debug, Default)]
pub struct SpanPairTracker {
    pending: Mutex<HashMap<(String, String), SpanEventRecord>>,
}

impl SpanPairTracker {
    fn note_started(&self, env: &obs_proto::obs::v1::ObsEnvelope) {
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
        self.pending.lock().insert(key, event);
    }

    fn pop_started(&self, env: &obs_proto::obs::v1::ObsEnvelope) -> Option<SpanEventRecord> {
        let key = (env.trace_id.clone(), env.span_id.clone());
        self.pending.lock().remove(&key)
    }
}

impl OtlpTracePayload {
    /// Project envelopes onto spans. Implements the three-pattern
    /// mapping (Started/Completed pair, duration field, point-in-time).
    #[must_use]
    pub fn from_envelopes(
        envs: &[obs_proto::obs::v1::ObsEnvelope],
        resource: &OtlpResourceAttrs,
        endpoint: &OtlpEndpoint,
        tracker: &SpanPairTracker,
    ) -> Self {
        let mut spans = Vec::with_capacity(envs.len());
        for env in envs {
            // Started → record + skip emitting a span; the matching
            // Completed picks up the SpanEvent below.
            if env.full_name.ends_with("Started") || env.full_name.ends_with(".Start") {
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
        Self {
            resource: ResourceMessage {
                service_name: resource.service_name.clone(),
                service_version: resource.service_version.clone(),
                extra: resource.extra.clone(),
                schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
            },
            endpoint: endpoint.url.clone(),
            spans,
        }
    }
}
