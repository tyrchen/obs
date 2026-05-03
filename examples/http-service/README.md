# obs-example-http-service

An axum HTTP service that demonstrates the **tracing-focused** obs SDK
surface end-to-end:

- Typed events declared with `#[derive(obs::Event)]`
  (`ObsCheckoutAttempted`, `ObsCheckoutCompleted`).
- `obs-tower::ObsHttpLayer::server()` parses inbound W3C `traceparent`
  on every request and emits `ObsHttpRequestCompleted` once the
  response is produced. When no `traceparent` is present the layer
  synthesises a fresh one.
- `Severity::Warn` escalation on 4xx so tail-on-error sampling fires.
- `StdoutSink` for human-readable visibility, plus an optional
  OTLP/gRPC exporter via `obs_otel::GrpcOtlpExporter` when
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set in the environment.

## Run

```bash
# stdout-only (no collector required):
cargo run -p obs-example-http-service

# with a real OTLP/gRPC collector (e.g. otelcol-contrib --config ./collector.yaml):
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
  cargo run -p obs-example-http-service
```

In another terminal:

```bash
curl -i http://127.0.0.1:8080/healthz

# happy path:
curl -i -X POST http://127.0.0.1:8080/checkout \
  -H 'content-type: application/json' \
  -d '{"sku":"OBS-001","qty":1}'

# out-of-stock (4xx, escalates to WARN):
curl -i -X POST http://127.0.0.1:8080/checkout \
  -H 'content-type: application/json' \
  -d '{"sku":"OBS-002","qty":1}'

# inbound traceparent — obs-tower lifts trace_id/span_id onto the envelope:
curl -i -X POST http://127.0.0.1:8080/checkout \
  -H 'traceparent: 00-0123456789abcdef0123456789abcdef-fedcba9876543210-01' \
  -H 'content-type: application/json' \
  -d '{"sku":"OBS-001","qty":1}'
```

## What to look for

In the stdout output for the happy path you should see two events per
request:

- `ObsCheckoutAttempted` — emitted at request entry with the typed
  `sku` label and `qty` attribute.
- `ObsCheckoutCompleted` — emitted at request exit with the typed
  outcome label and `latency_ms`. On 4xx responses this fires at
  `WARN` severity, which marks the active scope and (when sinks are
  configured for it) triggers tail-buffer flush.

`obs-tower` itself emits an `ObsHttpRequestCompleted` envelope per
request with `route`, `method`, `status_class`, and `latency_ms`
labels — these are SDK built-ins, not user-defined.

## What this demonstrates

Mapping back to the patterns in `60-developer-experience.md`:

- **§ 4.3.A — request handler**: every handler is wrapped by
  `ObsHttpLayer::server()`; `obs::scope!` plumbs `trace_id`/`span_id`
  onto handler emits automatically (spec 95 P0-A).
- **§ 4.3.E — conditional severity escalation**: 4xx responses emit
  `ObsCheckoutCompleted` at `WARN` so tail-on-error sampling fires.
- **§ 4.3.B — outbound HTTP propagation**: the optional OTLP exporter
  inherits the active scope's `trace_id` (spec 95 P1-AC + P1-AF).

## Out of scope

Patterns deliberately omitted; see siblings under `examples/`:

- `#[obs::instrument]` on async fns → `examples/tracing-migration/`
- Multi-tenant per-task observer → `examples/multi-tenant/`
- `obs::forensic!` + `SpanTrace` → `examples/forensic-and-spantrace/`
