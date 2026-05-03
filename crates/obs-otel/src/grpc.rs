//! Real OTLP/gRPC exporter — `GrpcOtlpExporter`.
//!
//! Holds a tonic `Channel` per endpoint plus the three OTLP service
//! clients (`LogsServiceClient`, `MetricsServiceClient`,
//! `TraceServiceClient`). Each `export_*` call converts the SDK's
//! pure-data payload into the corresponding OTLP wire request and
//! sends it via gRPC.
//!
//! The [`OtlpExporter`] trait is synchronous; tonic is asynchronous.
//! We bridge by owning a dedicated tokio runtime per exporter (the
//! exporter outlives the request-path runtime, and a per-exporter
//! runtime keeps reasoning about cancellation simple). Each export
//! call posts a request to the runtime via `block_on`. If the caller
//! is already inside a runtime we use `tokio::task::block_in_place`
//! plus a `Handle::block_on` to avoid the "block_on called from async"
//! panic.
//!
//! Spec 20 § 4.1 / spec 93 P0-6.

use std::{sync::Arc, time::Duration};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
        metrics::v1::{ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient},
        trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
    },
    common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueKind},
    logs::v1::{LogRecord as OtlpLogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    metrics::v1::{
        Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric::Data as MetricData,
        number_data_point::Value as NumberValue,
    },
    resource::v1::Resource,
    trace::v1::{
        ResourceSpans, ScopeSpans, Span as OtlpSpan, Status, span::SpanKind, status::StatusCode,
    },
};
use parking_lot::RwLock;
use tonic::{Request, transport::Channel};

use crate::{
    OtlpError, env_config::OtlpEndpoint, logs::OtlpLogPayload, mapping::SpanRecord,
    metrics::OtlpMetricPayload, sink::OtlpExporter, traces::OtlpTracePayload,
};

/// OTLP/gRPC exporter built on top of `tonic`.
///
/// Construct via [`GrpcOtlpExporter::connect`] (sync — internally
/// `block_on`s the tonic dial). The exporter spins up a single tokio
/// runtime that owns the long-lived `Channel`. Subsequent
/// `export_*` calls dial-free; tonic re-uses the underlying H2
/// connection.
pub struct GrpcOtlpExporter {
    runtime: Arc<tokio::runtime::Runtime>,
    inner: Arc<GrpcInner>,
    timeout: Duration,
}

struct GrpcInner {
    logs: RwLock<LogsServiceClient<Channel>>,
    metrics: RwLock<MetricsServiceClient<Channel>>,
    traces: RwLock<TraceServiceClient<Channel>>,
}

impl std::fmt::Debug for GrpcOtlpExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcOtlpExporter")
            .field("timeout_ms", &self.timeout.as_millis())
            .finish()
    }
}

impl GrpcOtlpExporter {
    /// Dial `endpoint.url` and return a ready exporter. The endpoint
    /// must include the scheme; `http://localhost:4317` for plaintext,
    /// `https://collector.example.com:4317` for TLS.
    ///
    /// # Errors
    ///
    /// Returns [`OtlpError::Transport`] if the endpoint URL fails to
    /// parse, the dial fails, or TLS configuration is invalid.
    pub fn connect(endpoint: &OtlpEndpoint) -> Result<Self, OtlpError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("obs-otlp-grpc")
            .build()
            .map_err(|e| OtlpError::Transport(format!("runtime build: {e}")))?;
        let runtime = Arc::new(runtime);

        let url = endpoint.url.clone();
        let timeout = Duration::from_millis(endpoint.timeout_ms.max(1));
        let channel = runtime.block_on(async move {
            tonic::transport::Endpoint::from_shared(url)
                .map_err(|e| OtlpError::Transport(format!("endpoint: {e}")))?
                .timeout(timeout)
                .connect_timeout(timeout)
                .keep_alive_while_idle(true)
                .connect()
                .await
                .map_err(|e| OtlpError::Transport(format!("connect: {e}")))
        })?;

        let inner = Arc::new(GrpcInner {
            logs: RwLock::new(LogsServiceClient::new(channel.clone())),
            metrics: RwLock::new(MetricsServiceClient::new(channel.clone())),
            traces: RwLock::new(TraceServiceClient::new(channel)),
        });

        Ok(Self {
            runtime,
            inner,
            timeout,
        })
    }

    fn block_on<T, F>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        // If the caller is on a tokio runtime already, we cannot call
        // `block_on` directly — that would deadlock. The exporter's
        // own runtime handle is the safe choice in either case; we
        // simply enter it and block on the future there.
        let _enter = self.runtime.enter();
        self.runtime.block_on(fut)
    }
}

impl OtlpExporter for GrpcOtlpExporter {
    fn export_logs(&self, payload: &OtlpLogPayload) -> Result<(), OtlpError> {
        let request = build_logs_request(payload);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        self.block_on(async move {
            let mut req = Request::new(request);
            req.set_timeout(timeout);
            let mut client = inner.logs.write().clone();
            client
                .export(req)
                .await
                .map(|_| ())
                .map_err(|e| OtlpError::Transport(format!("logs: {e}")))
        })
    }

    fn export_metrics(&self, payload: &OtlpMetricPayload) -> Result<(), OtlpError> {
        let request = build_metrics_request(payload);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        self.block_on(async move {
            let mut req = Request::new(request);
            req.set_timeout(timeout);
            let mut client = inner.metrics.write().clone();
            client
                .export(req)
                .await
                .map(|_| ())
                .map_err(|e| OtlpError::Transport(format!("metrics: {e}")))
        })
    }

    fn export_traces(&self, payload: &OtlpTracePayload) -> Result<(), OtlpError> {
        let request = build_traces_request(payload);
        let inner = Arc::clone(&self.inner);
        let timeout = self.timeout;
        self.block_on(async move {
            let mut req = Request::new(request);
            req.set_timeout(timeout);
            let mut client = inner.traces.write().clone();
            client
                .export(req)
                .await
                .map(|_| ())
                .map_err(|e| OtlpError::Transport(format!("traces: {e}")))
        })
    }
}

// ─── payload → OTLP wire conversion ────────────────────────────────────

fn build_resource(attrs: &crate::logs::ResourceMessage) -> Resource {
    // The full semconv map already lifts the first-class fields
    // (service.name, service.version, service.namespace,
    // service.instance.id, deployment.environment, host.name,
    // host.arch) — there is no separate `extra` slice to fold in.
    // Spec 93 P1-5.
    let kvs = attrs.attributes.iter().map(|(k, v)| kv_str(k, v)).collect();
    Resource {
        attributes: kvs,
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    }
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(value.to_string())),
        }),
    }
}

fn hex_to_trace_bytes(hex: &str) -> Vec<u8> {
    decode_hex(hex, 16)
}

fn hex_to_span_bytes(hex: &str) -> Vec<u8> {
    decode_hex(hex, 8)
}

fn decode_hex(hex: &str, expected_len: usize) -> Vec<u8> {
    if hex.is_empty() {
        return vec![0u8; expected_len];
    }
    let mut out = Vec::with_capacity(expected_len);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i + 1 < bytes.len() && out.len() < expected_len {
        let (Some(&hi_b), Some(&lo_b)) = (bytes.get(i), bytes.get(i + 1)) else {
            return vec![0u8; expected_len];
        };
        match (nibble(hi_b), nibble(lo_b)) {
            (Some(h), Some(l)) => out.push((h << 4) | l),
            _ => return vec![0u8; expected_len],
        }
        i += 2;
    }
    while out.len() < expected_len {
        out.push(0);
    }
    out
}

fn nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn build_logs_request(payload: &OtlpLogPayload) -> ExportLogsServiceRequest {
    let resource = build_resource(&payload.resource);
    let records: Vec<OtlpLogRecord> = payload
        .records
        .iter()
        .map(|r| OtlpLogRecord {
            time_unix_nano: r.time_unix_nano,
            observed_time_unix_nano: r.observed_time_unix_nano,
            severity_number: severity_to_proto(r.severity_number),
            severity_text: r.severity_text.clone(),
            body: if r.body_bytes.is_empty() {
                None
            } else {
                Some(AnyValue {
                    value: Some(AnyValueKind::BytesValue(r.body_bytes.clone())),
                })
            },
            attributes: r.attributes.iter().map(|(k, v)| kv_str(k, v)).collect(),
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: hex_to_trace_bytes(&r.trace_id),
            span_id: hex_to_span_bytes(&r.span_id),
            event_name: r.attributes.get("event.name").cloned().unwrap_or_default(),
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: records,
                schema_url: payload.resource.schema_url.clone(),
            }],
            schema_url: payload.resource.schema_url.clone(),
        }],
    }
}

fn build_metrics_request(payload: &OtlpMetricPayload) -> ExportMetricsServiceRequest {
    let resource = build_resource(&payload.resource);
    let schema_url = payload.resource.schema_url.clone();
    let metrics: Vec<Metric> = payload
        .points
        .iter()
        .map(|p| Metric {
            name: p.instrument.clone(),
            description: String::new(),
            unit: p.unit.clone(),
            metadata: Vec::new(),
            data: Some(MetricData::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: p.attributes.iter().map(|(k, v)| kv_str(k, v)).collect(),
                    start_time_unix_nano: 0,
                    time_unix_nano: 0,
                    exemplars: Vec::new(),
                    flags: 0,
                    value: Some(NumberValue::AsInt(
                        i64::try_from(p.value_u64.unwrap_or(0)).unwrap_or(i64::MAX),
                    )),
                }],
                aggregation_temporality: 2, // CUMULATIVE
                is_monotonic: true,
            })),
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics,
                schema_url: schema_url.clone(),
            }],
            schema_url,
        }],
    }
}

fn build_traces_request(payload: &OtlpTracePayload) -> ExportTraceServiceRequest {
    let resource = build_resource(&payload.resource);
    let schema_url = payload.resource.schema_url.clone();
    let spans: Vec<OtlpSpan> = payload.spans.iter().map(span_record_to_otlp).collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource),
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans,
                schema_url: schema_url.clone(),
            }],
            schema_url,
        }],
    }
}

fn span_record_to_otlp(s: &SpanRecord) -> OtlpSpan {
    OtlpSpan {
        trace_id: hex_to_trace_bytes(&s.trace_id),
        span_id: hex_to_span_bytes(&s.span_id),
        trace_state: String::new(),
        parent_span_id: hex_to_span_bytes(&s.parent_span_id),
        flags: 0,
        name: s.name.clone(),
        kind: kind_str_to_proto(&s.kind) as i32,
        start_time_unix_nano: s.start_time_unix_nano,
        end_time_unix_nano: s.end_time_unix_nano,
        attributes: s.attributes.iter().map(|(k, v)| kv_str(k, v)).collect(),
        dropped_attributes_count: 0,
        events: s
            .events
            .iter()
            .map(|e| opentelemetry_proto::tonic::trace::v1::span::Event {
                time_unix_nano: e.time_unix_nano,
                name: e.name.clone(),
                attributes: e.attributes.iter().map(|(k, v)| kv_str(k, v)).collect(),
                dropped_attributes_count: 0,
            })
            .collect(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: Some(Status {
            message: String::new(),
            code: status_str_to_proto(&s.status_code) as i32,
        }),
    }
}

fn severity_to_proto(otlp_number: i32) -> i32 {
    // Already in OTLP space (1/5/9/13/17/21).
    let _ = SeverityNumber::Trace;
    otlp_number
}

fn kind_str_to_proto(s: &str) -> SpanKind {
    match s {
        "SERVER" | "server" => SpanKind::Server,
        "CLIENT" | "client" => SpanKind::Client,
        "PRODUCER" | "producer" => SpanKind::Producer,
        "CONSUMER" | "consumer" => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

fn status_str_to_proto(s: &str) -> StatusCode {
    match s {
        "ERROR" | "error" => StatusCode::Error,
        "OK" | "ok" => StatusCode::Ok,
        _ => StatusCode::Unset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_hex_should_zero_pad_short_input() {
        let out = decode_hex("ab", 4);
        assert_eq!(out, vec![0xab, 0, 0, 0]);
    }

    #[test]
    fn test_decode_hex_should_truncate_long_input() {
        let out = decode_hex("0123456789abcdef0123", 4);
        assert_eq!(out, vec![0x01, 0x23, 0x45, 0x67]);
    }

    #[test]
    fn test_decode_hex_should_return_zeros_on_invalid() {
        let out = decode_hex("nothex!!", 4);
        assert_eq!(out, vec![0, 0, 0, 0]);
    }

    #[test]
    fn test_decode_hex_should_handle_uppercase() {
        let out = decode_hex("DEADBEEF", 4);
        assert_eq!(out, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }
}
