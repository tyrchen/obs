# Design — OpenTelemetry Mapping & Sinks

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [10-data-model.md](./10-data-model.md), [11-runtime-core.md](./11-runtime-core.md)

This spec defines:

1. The OpenTelemetry data-model contract — how a wide event projects
   into OTLP `LogRecord`, metric data points, and `Span`.
2. The `Sink` catalogue — every built-in sink, the `MakeWriter`
   abstraction, time/size-based rolling, and OTLP transport details.

Analytical sinks (Parquet, ClickHouse) are tied to the storage model
and live in [22-analytics-storage.md](./22-analytics-storage.md).
The `tracing` bridge sinks live in [30-tracing-bridge.md](./30-tracing-bridge.md).

> v3 changes: split out from the v2 monolithic architecture spec;
> added `MakeWriter` abstraction so writers (file, stderr, network,
> in-memory) compose with formatters; added time-based rolling
> (daily/hourly) on top of size-based; added formatter-style
> selection (Full/Compact/Pretty/JSON) on `StdoutSink`; consolidated
> OTLP retry/transport details from the v2 production-concerns
> section; cleaned up Resource attribute propagation across non-OTLP
> sinks.

## 1. The mapping is performed by sinks, not the core

The mapping is performed by the OTLP sinks, not the core; this keeps
the core free of OTel as a hard dependency. All mappings target OTLP
1.x (`opentelemetry-proto >= 1.4`).

## 2. OpenTelemetry mapping contract

### 2.1 OTel Resource (set once)

`service`, `instance`, `version` are **Resource** attributes in OTel,
not per-record attributes. The OTLP sinks build a single `Resource`
at construction and reuse it across every export RPC. The envelope
also carries them so non-OTLP sinks (Parquet, ClickHouse, NDJSON) get
the same identity without a separate Resource concept; the OTLP sinks
specifically **do not** also stamp them as per-LogRecord attributes
to avoid double-attribution.

| Envelope field / `ResourceAttrs` source | OTel Resource attribute (semconv) |
| --- | --- |
| `env.service`  | `service.name` |
| `env.instance` | `service.instance.id` |
| `env.version`  | `service.version` |
| `ResourceAttrs.namespace`   | `service.namespace` |
| `ResourceAttrs.environment` | `deployment.environment` |
| `ResourceAttrs.host.*`      | `host.name`, `host.arch`, etc. (auto-detected if `detect_host = true`) |
| `ResourceAttrs.extra[..]`   | passthrough into Resource attributes |

Resource `schema_url` is set to the OTel semconv URL the sink was
built against, e.g. `https://opentelemetry.io/schemas/1.27.0`.

`ResourceAttrs` (defined in [11-runtime-core.md § 7](./11-runtime-core.md#7-service-identity))
is also visible to the analytics sinks so a Parquet/ClickHouse row
isn't missing identity that the OTLP path carries.

#### Single source of truth for service / Resource identity

Identity flows through **one** structure (`ResourceAttrs` on the
observer) and is read by every sink. To prevent drift, the OTLP
sink builders' `.resource_attr(k, v)` setter is **shorthand** for
`StandardObserverBuilder::resource_attr(k, v)` — it writes into the
observer's `ArcSwap<ResourceAttrs>`, *not* into a sink-local copy.
The sink later reads from the observer's slot at delivery time.

Consequence: setting `.resource_attr("env", "prod")` on
`OtlpLogSink::builder()` makes that attribute visible to every sink
in the observer (Parquet, ClickHouse, even other OTLP sinks for the
metric / trace tiers), not just the log sink. This is the documented
behaviour and resolves the foot-gun where an operator might set the
attribute on the log sink expecting analytics rows to inherit it,
only to find them missing the field.

Sink-local attribute overrides are deliberately not supported. If a
sink needs different identity than the rest, install two observers
(one per sink target) — but in practice the use case for divergent
identity per sink is too narrow to motivate the API complexity.

### 2.2 Severity → OTLP `SeverityNumber`

OTLP defines `SeverityNumber` on a 1–24 scale (`opentelemetry-proto`
`logs.proto`). Our six-level enum maps to the canonical primary
buckets:

| `obs::Severity` | OTLP `SeverityNumber` | OTLP `SeverityText` |
| --- | --- | --- |
| `Trace` | `1`  (`SEVERITY_NUMBER_TRACE`) | `"TRACE"` |
| `Debug` | `5`  (`SEVERITY_NUMBER_DEBUG`) | `"DEBUG"` |
| `Info`  | `9`  (`SEVERITY_NUMBER_INFO`)  | `"INFO"`  |
| `Warn`  | `13` (`SEVERITY_NUMBER_WARN`)  | `"WARN"`  |
| `Error` | `17` (`SEVERITY_NUMBER_ERROR`) | `"ERROR"` |
| `Fatal` | `21` (`SEVERITY_NUMBER_FATAL`) | `"FATAL"` |

We deliberately use the bucket-floor numbers so other sources that
emit `WARN2`/`WARN3` etc. can interleave cleanly.

### 2.3 To OTLP Logs

A wide event maps 1:1 to an OTel `LogRecord`:

| Wide event | OTLP `LogRecord` |
| --- | --- |
| `env.ts_ns` | `time_unix_nano` |
| `Instant::now()` at `emit_envelope` | `observed_time_unix_nano` |
| `env.sev` | `severity_number` + `severity_text` per § 2.2 |
| `env.full_name` | `attributes["event.name"]` (semconv `event.name`) |
| `env.schema_hash` | `attributes["obs.schema_hash"]` (as `int_value`, u64) |
| `env.trace_id` (16-byte hex → 16 raw bytes) | `trace_id` |
| `env.span_id`  (8-byte hex → 8 raw bytes)   | `span_id` |
| `env.parent_span_id` | `attributes["obs.parent_span_id"]` (OTLP LogRecord has no parent_span_id) |
| `env.labels[k]` | `attributes[k]` (as `string_value`) |
| `env.sampling_reason` | `attributes["obs.sampling_reason"]` |
| `env.callsite_id` (when non-zero) | `attributes["obs.callsite_id"]` (as `int_value`, u64) |
| typed payload (decoded) | `body` as `KeyValueList` (opt-in; default off) |

If `body` decode is off, the LogRecord is emitted with
`body = bytes(env.payload)`; consumers that know the schema (via
`schema_hash`) can decode the buffa bytes themselves.

### 2.4 To OTLP Metrics

For each `FIELD_KIND_MEASUREMENT` field on a schema, a metric data
point is generated whose attribute set is the union of `env.labels`
(plus `event.name`).

| Schema annotation | Instrument | Aggregation | Notes |
| --- | --- | --- | --- |
| `metric: counter`   | `Sum`       | monotonic, **delta** temporality | Counter increments by the field's value on each emit; emit `0` to not increment. Negative values are rejected by `debug_assert!`, dropped in release. |
| `metric: gauge`     | `Gauge`     | last-value (per attribute set) | Value replaces previous reading. |
| `metric: histogram` | `Histogram` | explicit `bounds` from annotation | Bounds are sorted at codegen time. |

Instrument name is derived from the field's containing event +
field name: `<full_name>.<field>` lowercased and dot-separated, e.g.
`myapp.v1.obs_request_completed.latency_ms`. Unit is the `unit`
string from the annotation (UCUM: `ms`, `By`, `1`, `s`).

Because all LABEL fields are by construction `Low | Medium`
cardinality, the generated metric's attribute set is bounded at
compile time. Aggregation and export periodicity follow the OTel
`MeterProvider` configuration (default 60 s push interval; configurable
via `OtlpMetricSink::builder().push_interval(Duration::from_secs(15))`).

### 2.5 To OTLP Traces

The OTLP `Span` mapping has to handle three different patterns
without producing duplicate spans for the same logical operation:

#### A. Schema with `FIELD_KIND_DURATION_NS` — full span

The schema declares a duration field (e.g. `latency_ns`). One OTLP
span per envelope:

- `name = env.full_name`,
- `start_time_unix_nano = env.ts_ns - duration`,
- `end_time_unix_nano = env.ts_ns`,
- `parent_span_id = env.parent_span_id`,
- `kind = SPAN_KIND_INTERNAL` by default; set on the schema via
  `option (obs.v1.event).span_kind = SPAN_KIND_SERVER` for inbound
  edge events,
- `status_code` derived from severity: `SEVERITY_ERROR | FATAL →
  STATUS_CODE_ERROR`, otherwise `STATUS_CODE_UNSET`,
- `attributes := env.labels` plus `event.name = env.full_name`.

#### B. `Started` / `Completed` event pair — one span per pair

The canonical "I want span semantics with duration" pattern in obs
is the `ObsXxxStarted` + `ObsXxxCompleted` event pair (see
[60-dev-ergonomics.md § 4.3 A](./60-dev-ergonomics.md#a-request-handler-http)).
Naive mapping would emit one OTLP span per envelope (two spans),
plus a third *implied* span derived from `latency_ms` on
`Completed` — triple-counting the operation in Jaeger.

Correct mapping:

- The `Completed` event becomes the OTLP `Span` (per § A above,
  using `latency_ms` / `latency_ns`).
- The `Started` event becomes a `Span Event` (`Span.Events[]` in
  OTLP) attached to the same span via `(trace_id, span_id)`
  correlation. `Span Event.name = "started"`,
  `Span Event.attributes := env.labels`.
- Correlation requires both events to carry the **same**
  `(trace_id, span_id)` pair. `obs::scope!` makes this trivial
  because `span_id` is task-scoped; the runtime stamps the same
  id on both envelopes.

Naming convention: any schema whose `full_name` ends in `Started`
plus a sibling schema with the same prefix ending in `Completed`
is recognised as a span pair by `OtlpTraceSink`. Codegen sets a
`spans_paired_with: Option<&'static str>` const on the schema so
this is explicit, not pattern-inferred.

If only one half of the pair arrives (the other was dropped, or
they are emitted out of order across processes), `OtlpTraceSink`
emits the half it has as a point-in-time span (per § C below) and
increments `obs.runtime.v1.ObsSpanPairOrphaned` — this is
visibility, not error.

#### C. Schema with no duration and no pair — point-in-time span

For events with neither `FIELD_KIND_DURATION_NS` nor a paired
sibling, the span is point-in-time:

- `start_time_unix_nano = end_time_unix_nano = env.ts_ns`.

OTLP supports zero-duration spans; Jaeger/Tempo render them as
markers. This is the right shape for one-shot events like
`ObsUserSignedUp` or `ObsConfigReloaded` where there's no
operation-spanning interval.

Spans for the same `trace_id` are tied together by the OTel
collector; the SDK does not attempt span-tree reconstruction
in-process.

### 2.6 Trace context propagation

Cross-process correlation uses W3C Trace Context (and optionally
Baggage). The SDK does not implement HTTP middleware itself; it
exposes the propagator hook so HTTP/gRPC layers can use it. The
ready-made `tower::Layer` ships in [40-http-middleware.md](./40-http-middleware.md).

```rust
// At server boundary (e.g. tower::Layer or axum middleware):
let trace_ctx: ObsTraceCtx = obs::propagator()
    .extract_w3c(&http_headers)            // returns ObsTraceCtx::empty() if absent
    .or_else(ObsTraceCtx::generate);

let _scope = obs::scope!(
    trace_id  = trace_ctx.trace_id,
    span_id   = trace_ctx.span_id,           // becomes parent_span_id on emitted spans
    sampled   = trace_ctx.sampled,
);

// At outbound HTTP/gRPC client:
obs::propagator().inject_w3c(
    &obs::current_trace_ctx(),               // reads from active scope frame
    &mut outbound_request.headers_mut(),
);
```

`ObsTraceCtx` mirrors W3C `traceparent`: `trace_id: [u8; 16]`,
`span_id: [u8; 8]`, `flags: u8`.

### 2.7 Why we are not just an OTel SDK

OpenTelemetry's data model is *signal-shaped*: logs, metrics, traces
are peer concepts. A wide event is *operation-shaped*: one record
describes the whole operation, and the three signals are projections.
We project *into* OTel without forcing application code to think in
OTel.

## 3. Sink contract & catalogue

### 3.1 The `Sink` trait

```rust
pub trait Sink: Send + Sync + 'static {
    fn deliver(&self, env: ScrubbedEnvelope<'_>);
    fn flush(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
```

Sinks see a `ScrubbedEnvelope` — a wrapper produced by the per-tier
worker after running the payload scrubber. The wrapper carries the
envelope, the (possibly redacted) payload bytes, and a resolved
`Option<&'static dyn EventSchemaErased>` for sinks that need to
decode the typed payload (Parquet column population, OTLP body
expansion, JSON rendering, etc.). Sinks that only consume
`env.labels` — counters, OTel attribute writers, audit-filter sinks —
ignore the schema and pay no decode cost.

The contract for `EventSchemaErased`, the link-time schema registry
that resolves it, and the `ScrubbedEnvelope` lifetime story all
live in [14-schema-registry.md](./14-schema-registry.md). This spec
treats them as primitives.

The earlier `Sink::deliver(&self, env: &ObsEnvelope)` shape (showing
raw, possibly-unscrubbed payloads to sinks) was a soundness gap —
fixed in spec 14 and reflected here.

### 3.2 Built-in sinks

| Crate | Sink | Tier(s) | Notes |
| --- | --- | --- | --- |
| `obs-core` | `NoopSink` | any | Always present; returned when no observer installed |
| `obs-core` | `InMemorySink` | any | Test harness; bounded ring buffer with `iter()` |
| `obs-core` | `StdoutSink` | LOG (typically) | Human-readable line render; configurable formatter style |
| `obs-core` | `NdjsonFileSink` | LOG / AUDIT | Append-to-file with size + time rolling |
| `obs-otel` | `OtlpLogSink`    | LOG | OTLP/gRPC logs export |
| `obs-otel` | `OtlpMetricSink` | METRIC | OTLP/gRPC metrics export |
| `obs-otel` | `OtlpTraceSink`  | TRACE | OTLP/gRPC traces export |
| `obs-parquet` | `ParquetSink` | LOG | Single sparse table; rolls files. See [22-analytics-storage.md](./22-analytics-storage.md). |
| `obs-clickhouse` | `ClickHouseSink` | LOG | Single sparse table; live INSERT. See [22-analytics-storage.md](./22-analytics-storage.md). |
| `obs-tracing-bridge` | `ObsToTracingSink` | any | Mirrors envelopes back into `tracing`. See [30-tracing-bridge.md § 3](./30-tracing-bridge.md#3-direction-b--obs--tracing). |

### 3.3 The `MakeWriter` abstraction

Every text sink (Stdout, NDJSON file, custom) needs a *destination*.
Hardcoding stdout/files inside the sink type forces a new sink for
every new destination. The `MakeWriter` trait lets the same sink
implementation drop into different destinations:

```rust
pub trait MakeWriter: Send + Sync + 'static {
    type Writer: io::Write + Send + 'static;

    /// Returns a writer for one batch. The writer is dropped at end
    /// of batch; cheap to construct repeatedly.
    fn make_writer(&self) -> Self::Writer;

    /// Per-event writer factory; the default is `make_writer()`.
    /// Implementations may return a level-specific writer (e.g. ERROR
    /// goes to stderr).
    fn make_writer_for(&self, sev: Severity) -> Self::Writer { self.make_writer() }
}

pub struct StdoutWriter;
pub struct StderrWriter;
pub struct TeeWriter<A, B>(A, B);
pub struct LevelSplitWriter<A, B>(A, B);  // .ge(WARN) → B, else → A
pub struct RollingFileWriter { /* … */ }   // see § 3.4
pub struct NonBlockingWriter<W> { /* … */ } // background thread; see § 3.5
```

Built-in `MakeWriter` impls:

- `StdoutWriter` — `std::io::stdout()`
- `StderrWriter` — `std::io::stderr()`
- `LevelSplitWriter::new(stdout, stderr)` — INFO+ to stdout, WARN+ to
  stderr (the conventional shape for cargo binaries)
- `RollingFileWriter` — file appender with size + time rolling (§ 3.4)
- `NonBlockingWriter::new(inner, capacity)` — non-blocking writer with
  a worker thread (§ 3.5)
- `TeeWriter::new(a, b)` — write to both
- A user can implement `MakeWriter` for any custom destination
  (network sockets, in-memory buffers for tests, ringfs, …).

`StdoutSink` and `NdjsonFileSink` accept any `impl MakeWriter`:

```rust
StdoutSink::builder()
    .formatter(FormatterStyle::Json)
    .make_writer(LevelSplitWriter::new(StdoutWriter, StderrWriter))
    .build()?;
```

### 3.4 `RollingFileWriter` — size + time rolling

```rust
pub enum RollingPolicy {
    Never,
    SizeBased { max_bytes: u64 },
    Daily,
    Hourly,
    SizeOrAge { max_bytes: u64, max_age: Duration },
}

pub struct RollingFileWriterBuilder {
    pub fn directory(self, dir: impl Into<PathBuf>) -> Self;
    pub fn filename_prefix(self, p: impl Into<String>) -> Self;
    pub fn filename_suffix(self, s: impl Into<String>) -> Self;     // default ".ndjson"
    pub fn policy(self, p: RollingPolicy) -> Self;
    pub fn keep(self, n: usize) -> Self;                            // retain last N files
    pub fn build(self) -> io::Result<RollingFileWriter>;
}
```

File naming follows `prefix.YYYY-MM-DD.HH.suffix` for time-based, or
`prefix.NNNNNN.suffix` for size-based, or both for `SizeOrAge`. The
writer rotates at the boundary the policy declares. Old files are
deleted when `keep(N)` is set.

This replaces the size-only rotation in earlier drafts; daily/hourly
rotation is a real-world requirement for log shippers.

### 3.5 `NonBlockingWriter` — background flush thread

A slow disk should not stall a per-tier worker. `NonBlockingWriter`
wraps any `MakeWriter` with:

- a bounded `mpsc::SyncSender<Vec<u8>>` channel (default capacity 8192),
- one background thread draining the channel and calling
  `inner.write_all(...)`,
- on overflow: drop the line and increment
  `obs.runtime.v1.ObsSinkDropped{sink=writer_overflow}`.

`NonBlockingWriter::new(inner, capacity)` returns the writer plus a
`WorkerGuard` whose `Drop` flushes-and-joins the thread. Conventionally:

```rust
let (writer, _guard) = NonBlockingWriter::new(
    RollingFileWriter::builder().directory("/var/log/myapi").daily().build()?,
    8192,
);
StandardObserver::builder()
    .sink_for(Tier::Log, NdjsonFileSink::with_writer(writer))
    .build()?;
// _guard kept alive until process shutdown; flushes pending lines on drop.
```

Mirrors `tracing-appender::non_blocking`.

### 3.6 Formatter styles

`StdoutSink` accepts a formatter style:

```rust
pub enum FormatterStyle {
    Full,       // single line: ts level event labels=…; default
    Compact,    // abbreviated; field names elided when obvious from event name
    Pretty,     // multi-line; human-readable, dev-focused
    Json,       // newline-delimited JSON; production stdout for kubectl logs
}
```

`Json` is the production-stdout choice for `kubectl logs`-style
pipelines; `Pretty` is the dev choice. The format selection is a
per-sink config knob; not a global. Live reload changes apply on the
next event.

### 3.7 Sink chains and routing

The `SinkRouter` lives on `StandardObserver` and is configured via:

```rust
StandardObserver::builder()
    .sink_for(Tier::Log,    OtlpLogSink::from_env()?)
    .sink_for(Tier::Metric, OtlpMetricSink::from_env()?)
    .sink_for(Tier::Trace,  OtlpTraceSink::from_env()?)
    .sink_for(Tier::Audit,  AuditFileSink::new("/var/log/audit/")?)
    .sink_for_severity(Tier::Log, Severity::Warn,
                       NonBlockingWriter::new(StderrWriter, 1024))   // WARN+ to stderr
    .fallback_sink(StdoutSink::default())
    .build()?;
```

Multiple sinks can fan out the same tier; each sink sees every
matching envelope. Per-severity matching layers on top of tier match.

## 4. OTLP sink internals

### 4.1 Transport configuration

The OTLP sinks default to **gRPC over TLS** (`tonic` + `rustls` with
the `aws-lc-rs` crypto backend, per project policy). HTTP/protobuf
is available as an alternative for restricted networks:

```rust
OtlpLogSink::builder()
    .endpoint("https://otlp.example.com:4317")
    .protocol(OtlpProtocol::Grpc)            // default
    // .protocol(OtlpProtocol::HttpProtobuf) // POSTs to /v1/logs etc.
    .compression(OtlpCompression::Gzip)      // gzip per OTel spec
    .timeout(Duration::from_secs(10))
    .retry_policy(OtlpRetry::exponential(
        max_attempts: 5,
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(30),
    ))
    .header("authorization", "Bearer …")     // per-RPC metadata
    .build()?;
```

Standard OTel env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_EXPORTER_OTLP_COMPRESSION`, `OTEL_EXPORTER_OTLP_TIMEOUT`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME`) are honoured by
`from_env()`. So a 12-factor deployment needs no code.

### 4.2 Retry & backpressure

Non-retryable response codes (`4xx` other than `429`/`503`) drop the
batch and increment `ObsSinkFailed{sink=otlp_logs, reason=4xx}`.
Retryable failures back off exponentially; the queue between the
worker and the OTLP exporter is bounded — overflow drops with a
metric increment, never blocks.

#### Two-layer backpressure: where each drop is counted

There are **two** bounded queues in front of the network:

1. The per-tier `mpsc` between the emit thread and the worker
   ([11-runtime-core.md § 4](./11-runtime-core.md#4-per-tier-workers-and-sinks)).
   Default capacity 8192. Overflow drops on the emit side and
   increments `obs.runtime.v1.ObsSinkDropped{tier, reason=channel_full}`.
2. The OTLP exporter's retry queue inside the sink. Default
   capacity 16384 envelopes (or 64 batches × 256 envelopes/batch).
   Overflow drops on the worker side and increments
   `obs.runtime.v1.ObsSinkFailed{sink=otlp_*, reason=retry_queue_full}`.

The two counters distinguish "the application produced faster than
the worker could batch" (queue 1) from "the worker batched fine but
the network/backend is too slow" (queue 2). Operators size each
queue against a different signal; `obs doctor` shows recommended
sizes calibrated for "P99 sustained 10k events/s without drops with
1 s OTLP RTT".

Recommended defaults assume:
- per-tier mpsc capacity = `max(8192, expected_qps * 1s)`
- OTLP retry queue capacity = `max(16384, expected_qps * max_backoff)`

Document the relationship between the two so users do not "fix" one
queue while leaving the other under-sized.

### 4.3 Convenience constructor

```rust
let (logs, metrics, traces) = obs_otel::otlp_trio_from_env()?;
StandardObserver::builder()
    .sink_for(Tier::Log,    logs)
    .sink_for(Tier::Metric, metrics)
    .sink_for(Tier::Trace,  traces)
    ...
```

All three share a single Resource built from the observer's identity
plus optional `service.namespace`, `deployment.environment`, host
detection.

## 5. Build dependencies

| Depends on | Provides |
| --- | --- |
| [10-data-model.md](./10-data-model.md) | Envelope shape |
| [11-runtime-core.md](./11-runtime-core.md) | `Sink` trait, `ResourceAttrs`, sink router |

Sink implementations ship in the per-target crates: `obs-core` (built-
ins + `MakeWriter` family), `obs-otel`, `obs-tracing-bridge`. See
[61-crates-and-features.md](./61-crates-and-features.md).
