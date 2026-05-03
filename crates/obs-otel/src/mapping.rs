//! Pure-data OTLP message structures used by the sinks. Spec 20 §§ 2.1–2.5.
//!
//! These mirror the OTLP protobuf wire shape but live in plain Rust
//! structs so we can serialise them as JSON (HTTP/JSON OTLP) or hand
//! them to a future `prost`-backed gRPC exporter without a circular
//! dependency.

use std::collections::BTreeMap;

use obs_proto::obs::v1::ObsEnvelope;
use obs_types::Severity;
use serde::{Deserialize, Serialize};

/// Severity number per OTLP `logs.proto` (spec 20 § 2.2).
#[must_use]
pub fn severity_to_otlp(sev: Severity) -> i32 {
    match sev {
        Severity::Trace => 1,
        Severity::Debug => 5,
        Severity::Info => 9,
        Severity::Warn => 13,
        Severity::Error => 17,
        Severity::Fatal => 21,
        _ => 0,
    }
}

/// Severity text per OTLP.
#[must_use]
pub fn severity_text(sev: Severity) -> &'static str {
    match sev {
        Severity::Trace => "TRACE",
        Severity::Debug => "DEBUG",
        Severity::Info => "INFO",
        Severity::Warn => "WARN",
        Severity::Error => "ERROR",
        Severity::Fatal => "FATAL",
        _ => "UNSPECIFIED",
    }
}

/// One OTLP `Resource`-shape record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMessage {
    /// `service.name`, `service.version`, etc.
    pub attributes: BTreeMap<String, String>,
    /// OTel semconv URL.
    pub schema_url: String,
}

/// One OTLP `LogRecord`-shape record (spec 20 § 2.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// `time_unix_nano`.
    pub time_unix_nano: u64,
    /// `observed_time_unix_nano` (caller fills with `Instant::now`).
    pub observed_time_unix_nano: u64,
    /// `severity_number`.
    pub severity_number: i32,
    /// `severity_text`.
    pub severity_text: String,
    /// 16-byte hex `trace_id`.
    pub trace_id: String,
    /// 8-byte hex `span_id`.
    pub span_id: String,
    /// Per-record attributes (spec 20 § 2.3 maps `env.labels` 1:1).
    pub attributes: BTreeMap<String, String>,
    /// Length of the payload bytes — informational; the actual bytes
    /// live in [`Self::body_bytes`] for the gRPC exporter to ship.
    pub body_bytes_len: usize,
    /// Buffa-encoded payload bytes (post-scrub) projected straight
    /// through to OTLP `LogRecord.body` as `AnyValue::BytesValue`.
    /// Empty when the upstream envelope had no payload. Spec 93
    /// P0-6 review fix.
    pub body_bytes: Vec<u8>,
}

/// One OTLP metric data point (spec 20 § 2.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    /// `service.<full_name>.<field>` instrument name.
    pub instrument: String,
    /// UCUM unit string (`ms`, `By`, etc.).
    pub unit: String,
    /// Aggregation kind (`counter` / `gauge` / `histogram`).
    pub kind: String,
    /// Attribute set (envelope labels + `event.name`).
    pub attributes: BTreeMap<String, String>,
    /// Last sampled value when known (counter increments / gauge values).
    pub value_u64: Option<u64>,
    /// Histogram bounds when applicable.
    pub bounds: Vec<f64>,
}

/// One OTLP `Span`-shape record (spec 20 § 2.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanRecord {
    /// Span name (envelope `full_name`).
    pub name: String,
    /// `start_time_unix_nano`.
    pub start_time_unix_nano: u64,
    /// `end_time_unix_nano` (== start when no duration field exists).
    pub end_time_unix_nano: u64,
    /// 16-byte hex trace id.
    pub trace_id: String,
    /// 8-byte hex span id.
    pub span_id: String,
    /// 8-byte hex parent span id when set.
    pub parent_span_id: String,
    /// `kind` — `internal`, `server`, `client`.
    pub kind: String,
    /// Status code derived from severity.
    pub status_code: String,
    /// Per-span attributes.
    pub attributes: BTreeMap<String, String>,
    /// Span events attached to this span (e.g. `Started` peer-event in
    /// the Started/Completed-pair mapping; spec 20 § 2.5 B).
    pub events: Vec<SpanEventRecord>,
}

/// A span-event attached to a span (spec 20 § 2.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEventRecord {
    /// Event name (`"started"` for the Started→Completed pattern).
    pub name: String,
    /// Event timestamp.
    pub time_unix_nano: u64,
    /// Per-event attributes.
    pub attributes: BTreeMap<String, String>,
}

/// Build the LogRecord projection for one envelope.
#[must_use]
pub fn project_log(env: &ObsEnvelope) -> LogRecord {
    let sev = match env.sev {
        ::buffa::EnumValue::Known(s) => proto_sev_to_native(s),
        ::buffa::EnumValue::Unknown(_) => Severity::Unspecified,
    };
    let mut attributes: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in env.labels.iter() {
        attributes.insert(k.clone(), v.clone());
    }
    attributes.insert("event.name".to_string(), env.full_name.clone());
    attributes.insert("obs.schema_hash".to_string(), env.schema_hash.to_string());
    if !env.parent_span_id.is_empty() {
        attributes.insert("obs.parent_span_id".to_string(), env.parent_span_id.clone());
    }
    if env.callsite_id != 0 {
        attributes.insert("obs.callsite_id".to_string(), env.callsite_id.to_string());
    }
    let reason = match env.sampling_reason {
        ::buffa::EnumValue::Known(r) => proto_reason_str(r),
        ::buffa::EnumValue::Unknown(_) => "UNSPECIFIED",
    };
    attributes.insert("obs.sampling_reason".to_string(), reason.to_string());
    LogRecord {
        time_unix_nano: env.ts_ns,
        observed_time_unix_nano: env.ts_ns,
        severity_number: severity_to_otlp(sev),
        severity_text: severity_text(sev).to_string(),
        trace_id: env.trace_id.clone(),
        span_id: env.span_id.clone(),
        attributes,
        body_bytes_len: env.payload.len(),
        body_bytes: env.payload.clone(),
    }
}

/// Build the SpanRecord projection for one envelope when the schema
/// declares a duration field. The caller must pre-resolve the
/// duration in nanoseconds (we don't decode the typed payload here).
#[must_use]
pub fn project_span(env: &ObsEnvelope, duration_ns: Option<u64>) -> SpanRecord {
    let sev = match env.sev {
        ::buffa::EnumValue::Known(s) => proto_sev_to_native(s),
        ::buffa::EnumValue::Unknown(_) => Severity::Unspecified,
    };
    let status_code = if sev >= Severity::Error {
        "ERROR".to_string()
    } else {
        "UNSET".to_string()
    };
    let end = env.ts_ns;
    let start = duration_ns.map(|d| end.saturating_sub(d)).unwrap_or(end);
    let mut attributes: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in env.labels.iter() {
        attributes.insert(k.clone(), v.clone());
    }
    attributes.insert("event.name".to_string(), env.full_name.clone());
    SpanRecord {
        name: env.full_name.clone(),
        start_time_unix_nano: start,
        end_time_unix_nano: end,
        trace_id: env.trace_id.clone(),
        span_id: env.span_id.clone(),
        parent_span_id: env.parent_span_id.clone(),
        kind: "INTERNAL".to_string(),
        status_code,
        attributes,
        events: Vec::new(),
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_sev_to_native(s: obs_proto::obs::v1::Severity) -> Severity {
    use obs_proto::obs::v1::Severity as P;
    match s {
        P::SEVERITY_UNSPECIFIED => Severity::Unspecified,
        P::SEVERITY_TRACE => Severity::Trace,
        P::SEVERITY_DEBUG => Severity::Debug,
        P::SEVERITY_INFO => Severity::Info,
        P::SEVERITY_WARN => Severity::Warn,
        P::SEVERITY_ERROR => Severity::Error,
        P::SEVERITY_FATAL => Severity::Fatal,
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_reason_str(r: obs_proto::obs::v1::SamplingReason) -> &'static str {
    use obs_proto::obs::v1::SamplingReason as P;
    match r {
        P::SAMPLING_REASON_UNSPECIFIED => "UNSPECIFIED",
        P::SAMPLING_REASON_HEAD_RATE => "HEAD_RATE",
        P::SAMPLING_REASON_TAIL_ERROR => "TAIL_ERROR",
        P::SAMPLING_REASON_SLOW => "SLOW",
        P::SAMPLING_REASON_FORENSIC => "FORENSIC",
        P::SAMPLING_REASON_AUDIT => "AUDIT",
        P::SAMPLING_REASON_RUNTIME => "RUNTIME",
        P::SAMPLING_REASON_OVERRIDE => "OVERRIDE",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_mapping() {
        assert_eq!(severity_to_otlp(Severity::Info), 9);
        assert_eq!(severity_to_otlp(Severity::Fatal), 21);
        assert_eq!(severity_text(Severity::Warn), "WARN");
    }

    #[test]
    fn test_project_log_attaches_event_name() {
        let mut env = ObsEnvelope {
            full_name: "myapp.v1.ObsRequestCompleted".to_string(),
            schema_hash: 0xCAFE_BABE,
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            ts_ns: 1_700_000_000_000_000_000,
            ..Default::default()
        };
        env.labels
            .insert("route".to_string(), "list_users".to_string());
        let log = project_log(&env);
        assert_eq!(log.severity_number, 9);
        assert_eq!(
            log.attributes.get("event.name"),
            Some(&"myapp.v1.ObsRequestCompleted".to_string())
        );
        assert_eq!(
            log.attributes.get("obs.schema_hash"),
            Some(&0xCAFE_BABE_u64.to_string())
        );
    }

    #[test]
    fn test_project_span_subtracts_duration() {
        let env = ObsEnvelope {
            full_name: "myapp.v1.ObsRequestCompleted".to_string(),
            ts_ns: 1_700_000_000_000_000_000,
            ..Default::default()
        };
        let span = project_span(&env, Some(5_000_000));
        assert_eq!(
            span.end_time_unix_nano - span.start_time_unix_nano,
            5_000_000
        );
        assert_eq!(span.kind, "INTERNAL");
    }
}
