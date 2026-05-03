//! End-to-end smoke for the OTLP/gRPC exporter against the bundled
//! `MockOtelCollector`. Spec 93 P0-6 + P1-12.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::collections::BTreeMap;

use obs_otel::{
    GrpcOtlpExporter, LogRecord, MetricPoint, OtlpEndpoint, OtlpResourceAttrs, SpanRecord,
    logs::ResourceMessage, test::MockOtelCollector,
};

fn endpoint(url: String) -> OtlpEndpoint {
    OtlpEndpoint {
        url,
        protocol: obs_otel::OtlpProtocol::Grpc,
        headers: BTreeMap::new(),
        compression: String::new(),
        timeout_ms: 2_000,
    }
}

fn resource() -> OtlpResourceAttrs {
    OtlpResourceAttrs {
        service_name: "obs-test".to_string(),
        service_version: "0.0.1".to_string(),
        extra: BTreeMap::new(),
    }
}

#[test]
fn test_grpc_exporter_should_round_trip_logs_through_mock_collector() {
    let collector = MockOtelCollector::start().expect("start mock collector");
    let url = collector.endpoint().to_string();
    let state = collector.state();

    let exporter = GrpcOtlpExporter::connect(&endpoint(url.clone())).expect("connect");
    let res = resource();

    let payload = obs_otel::logs::OtlpLogPayload {
        resource: ResourceMessage {
            service_name: res.service_name.clone(),
            service_version: res.service_version.clone(),
            extra: res.extra.clone(),
            schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
        },
        endpoint: url,
        records: vec![LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            observed_time_unix_nano: 1_700_000_000_000_000_000,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            span_id: "0123456789abcdef".to_string(),
            attributes: BTreeMap::from_iter([
                ("event.name".to_string(), "obs.test.ObsHello".to_string()),
                ("route".to_string(), "/probe".to_string()),
            ]),
            body_bytes_len: 5,
            body_bytes: b"hello".to_vec(),
        }],
    };

    use obs_otel::OtlpExporter;
    exporter.export_logs(&payload).expect("export logs");

    let captured = state.take_logs();
    assert_eq!(captured.len(), 1, "collector should have received 1 batch");
    let req = &captured[0];
    assert_eq!(req.resource_logs.len(), 1);
    let rl = &req.resource_logs[0];
    let scope_logs = &rl.scope_logs[0];
    assert_eq!(scope_logs.log_records.len(), 1);
    let rec = &scope_logs.log_records[0];
    assert_eq!(rec.severity_number, 9);
    assert_eq!(rec.trace_id.len(), 16);
    assert_eq!(rec.trace_id[0], 0x01);
    assert_eq!(rec.span_id.len(), 8);
    // Spec 93 review fix: body bytes must round-trip via OTLP
    // `LogRecord.body` (AnyValue::BytesValue), not be silently dropped.
    let Some(body) = rec.body.as_ref() else {
        panic!("expected LogRecord.body to be Some")
    };
    let Some(value) = body.value.as_ref() else {
        panic!("expected AnyValue.value to be Some")
    };
    let any_kind = value;
    let bytes = match any_kind {
        opentelemetry_proto::tonic::common::v1::any_value::Value::BytesValue(b) => b.clone(),
        _ => panic!("expected BytesValue, got {any_kind:?}"),
    };
    assert_eq!(bytes, b"hello");
}

#[test]
fn test_grpc_exporter_should_carry_metrics_to_collector() {
    let collector = MockOtelCollector::start().expect("start mock collector");
    let url = collector.endpoint().to_string();
    let state = collector.state();

    let exporter = GrpcOtlpExporter::connect(&endpoint(url.clone())).expect("connect");

    let payload = obs_otel::metrics::OtlpMetricPayload {
        resource: ResourceMessage {
            service_name: "obs-test".to_string(),
            service_version: "0.0.1".to_string(),
            extra: BTreeMap::new(),
            schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
        },
        endpoint: url,
        points: vec![MetricPoint {
            instrument: "obs.test.ObsCounter.count".to_string(),
            unit: "1".to_string(),
            kind: "counter".to_string(),
            attributes: BTreeMap::from_iter([(
                "event.name".to_string(),
                "obs.test.ObsCounter".to_string(),
            )]),
            value_u64: Some(7),
            bounds: Vec::new(),
        }],
    };

    use obs_otel::OtlpExporter;
    exporter.export_metrics(&payload).expect("export metrics");

    let captured = state.take_metrics();
    assert_eq!(captured.len(), 1);
    let req = &captured[0];
    let rm = &req.resource_metrics[0];
    let metrics = &rm.scope_metrics[0].metrics;
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "obs.test.ObsCounter.count");
}

#[test]
fn test_grpc_exporter_should_carry_traces_to_collector() {
    let collector = MockOtelCollector::start().expect("start mock collector");
    let url = collector.endpoint().to_string();
    let state = collector.state();

    let exporter = GrpcOtlpExporter::connect(&endpoint(url.clone())).expect("connect");

    let payload = obs_otel::traces::OtlpTracePayload {
        resource: ResourceMessage {
            service_name: "obs-test".to_string(),
            service_version: "0.0.1".to_string(),
            extra: BTreeMap::new(),
            schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
        },
        endpoint: url,
        spans: vec![SpanRecord {
            name: "obs.test.ObsRequest".to_string(),
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            span_id: "0123456789abcdef".to_string(),
            parent_span_id: String::new(),
            kind: "SERVER".to_string(),
            status_code: "OK".to_string(),
            attributes: BTreeMap::new(),
            events: Vec::new(),
        }],
    };

    use obs_otel::OtlpExporter;
    exporter.export_traces(&payload).expect("export traces");

    let captured = state.take_traces();
    assert_eq!(captured.len(), 1);
    let req = &captured[0];
    let spans = &req.resource_spans[0].scope_spans[0].spans;
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "obs.test.ObsRequest");
    // SpanKind::Server == 2.
    assert_eq!(spans[0].kind, 2);
}
