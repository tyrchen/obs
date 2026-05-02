# Design — Core Architecture

Status: draft v2 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [wide-events-prd.md](./wide-events-prd.md)

> v2 changes: switched proto runtime from `prost` to `buffa`; replaced
> per-event analytical tables with a single sparse columnar table;
> shortened envelope field names; added explicit "Key Design Decisions"
> section grounded in tracing-style ergonomics; introduced automatic
> trace correlation via `obs::scope!` task-locals; tightened the
> service-identity story.

## 1. Data model

The unit of observation is a **Wide Event**: one strongly-typed protobuf
message, emitted exactly once per logical operation, carrying every
dimension a downstream system might want to query.

### 1.1 Tier

A wide event declares one *tier* that selects its primary durable
destination. Tier is a routing hint — the same event may also fan out to
metric/trace sinks regardless of tier.

```proto
enum Tier {
  TIER_UNSPECIFIED = 0;
  TIER_LOG    = 1;  // Durable, queryable; default for most events
  TIER_METRIC = 2;  // Aggregated; payload may be discarded after counter inc
  TIER_TRACE  = 3;  // Spans; envelope.trace_id / span_id are required
  TIER_AUDIT  = 4;  // Compliance: separate retention, encryption, immutability
}
```

### 1.2 Severity

Six levels aligned with OTel `SeverityNumber` buckets:

```proto
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_TRACE = 1;
  SEVERITY_DEBUG = 2;
  SEVERITY_INFO  = 3;
  SEVERITY_WARN  = 4;
  SEVERITY_ERROR = 5;
  SEVERITY_FATAL = 6;
}
```

A schema declares `default_sev`; call sites may **escalate** but not
demote (`emit_at` clamps upward in release, `debug_assert!`s in debug).

### 1.3 Field roles

Every field on a wide event carries a `FieldKind` and a `Cardinality`.
These together drive code generation, OTel mapping, and compile-time
lints.

```proto
enum FieldKind {
  FIELD_KIND_UNSPECIFIED   = 0;
  FIELD_KIND_LABEL         = 1;  // Bounded dimension; safe as metric/span attribute
  FIELD_KIND_ATTRIBUTE     = 2;  // Free-form; never a metric dim; in log/span body
  FIELD_KIND_MEASUREMENT   = 3;  // Numeric; emitted as a metric data point
  FIELD_KIND_TRACE_ID      = 4;  // Lifted to envelope.trace_id
  FIELD_KIND_SPAN_ID       = 5;  // Lifted to envelope.span_id
  FIELD_KIND_PARENT_SPAN_ID = 6; // Lifted to envelope.parent_span_id
  FIELD_KIND_TIMESTAMP_NS  = 7;  // Overrides envelope.ts_ns
  FIELD_KIND_DURATION_NS   = 8;  // Drives span start/end derivation
  FIELD_KIND_FORENSIC      = 9;  // Opaque blob; never indexed; size-capped
}

enum Cardinality {
  CARDINALITY_UNSPECIFIED = 0;
  CARDINALITY_LOW       = 1;  // <  10  (status, boolean)
  CARDINALITY_MEDIUM    = 2;  // < 10k  (route, tenant)
  CARDINALITY_HIGH      = 3;  // <  1M  (user_id) — illegal for LABEL
  CARDINALITY_UNBOUNDED = 4;  // open    — illegal for LABEL/MEASUREMENT
}

enum Classification {
  CLASSIFICATION_UNSPECIFIED = 0;
  CLASSIFICATION_INTERNAL = 1;
  CLASSIFICATION_PII      = 2;  // Redactable; never on LABEL
  CLASSIFICATION_SECRET   = 3;  // Stripped before durable write; never on LOG/AUDIT tier
}

enum SamplingReason {
  SAMPLING_REASON_UNSPECIFIED = 0;
  SAMPLING_REASON_HEAD_RATE   = 1;  // Selected by head-rate roll
  SAMPLING_REASON_TAIL_ERROR  = 2;  // Flushed because a sibling event hit ERROR/FATAL
  SAMPLING_REASON_SLOW        = 3;  // `always_log_slower_than_ms` triggered
  SAMPLING_REASON_FORENSIC    = 4;  // Emitted by obs::forensic! (always retained)
  SAMPLING_REASON_AUDIT       = 5;  // AUDIT-tier event (always retained)
  SAMPLING_REASON_RUNTIME     = 6;  // SDK self-event (obs.runtime.v1.*)
  SAMPLING_REASON_OVERRIDE    = 7;  // Per-event head_rate=1.0 forces always-on
}
```

### 1.4 Envelope

Every emitted event is wrapped in a transport-neutral envelope. The
envelope is the contract between the SDK and any sink — sinks may
consume only the envelope (cheap, no descriptor needed) or descend into
the typed payload (expensive, schema-aware).

Envelope field names are deliberately short — these go on every event,
multiplied by request rate × service count, so character count matters
in `protoc --decode` output, dashboards, and ad-hoc CLI views.

```proto
message ObsEnvelope {
  string  full_name   = 1;   // "myapp.v1.ObsRequestCompleted"
  fixed64 schema_hash = 2;   // first 8 bytes of BLAKE3 over (full_name, tier, default_sev, FIELDS[])
  Tier    tier        = 3;
  Severity sev        = 4;
  fixed64 ts_ns       = 5;   // Unix epoch nanoseconds

  // Correlation (lifted from FIELD_KIND_*_ID fields by codegen, OR auto-filled
  // from the active obs::scope! task-local; see §5)
  string  trace_id        = 6;
  string  span_id         = 7;
  string  parent_span_id  = 8;

  // Service identity (set once at observer init; cheap atomic load on emit)
  string  service  = 9;
  string  instance = 10;
  string  version  = 11;

  // Buffa-encoded payload bytes
  bytes   payload  = 12;

  // Flat label projection: extracted at emit time so cheap sinks
  // (metric counter, OTel attribute writer) never decode the payload.
  map<string, string> labels = 13;

  // Sampling provenance (head-rate / tail-on-error / forensic / always)
  SamplingReason sampling_reason = 14;

  // Stable BLAKE3-derived id for the originating callsite (bridge / forensic /
  // span / instrument). 0 = no interning. When non-zero, downstream resolves
  // (target, file, line, template, field_names) via `ObsCallsiteRegistered`.
  // See callsite-interning-design.md.
  fixed64 callsite_id = 15;
}

message ObsBatch {
  uint32  format_ver = 1;
  string  batch_id   = 2;
  fixed64 started_ns = 3;
  fixed64 closed_ns  = 4;

  // schema_hash → fully qualified name lookup, deduplicated per batch
  map<string, string> schemas = 5;
  repeated ObsEnvelope events = 6;
}
```

`schema_hash` is the first 8 bytes of BLAKE3 over
`(full_name, tier, default_sev, FIELDS[])`, computed at build time and
stored as a `u64` constant. It lets a downstream consumer detect schema
evolution without registry lookup, and lets the batch dedupe schema
names.

64 bits is sized for the role this id plays — distinguishing schemas
across services and time, **not** authenticating payloads. Birthday
bound on accidental collision at 64 bits is ~4 × 10⁹ distinct schemas;
realistic workspaces have ≤ 10⁴, an entire industry's lifetime maybe
10⁸. The schema_hash is not a tamper-detection primitive (buffa
payloads are not signed); the only failure mode of a contrived
collision is "downstream picks the wrong typed view and produces
nonsense fields", which is contained by the existing classification
machinery. Saves 24 bytes per envelope vs the 32-byte BLAKE3-256 we
considered, and lines up uniformly with the 64-bit `callsite_id`
(see [callsite-interning-design.md](./callsite-interning-design.md))
so the runtime treats the two id namespaces the same way.

### 1.5 Naming convention: `Obs*` event types

User-defined event types **must** be named `Obs<EventName>`. Examples:

```
ObsRequestStarted   ObsRequestCompleted   ObsUpstreamFailed
ObsCheckoutStarted  ObsUserSignedUp       ObsForensicEvent
```

Why:

- **Visual distinction** — at a call site, `ObsCheckoutCompleted::builder()`
  is unambiguously an observability emission, never confused with a
  domain type called `CheckoutCompleted`.
- **Greppability** — `rg '\bObs[A-Z]'` finds every emit site in the repo
  in one shot.
- **Codegen pattern matching** — the codegen and CLI lints use the
  prefix as a sanity check; an event without it is a likely typo.

This is enforced by lint `L011` (warning by default, error under
`--strict`). The `Obs` proto namespace prefix is not required (events
can live in any package), only the message name.

## 2. Runtime topology

```
┌─────────────────────────┐
│ application code        │
│ ObsMyEvent::builder()…  │
│        .emit()          │
└──────────┬──────────────┘
           │ stack-allocated typed struct (buffa::Message)
           ▼
┌─────────────────────────┐
│ EventSchema::project()  │  generated; pushes labels & lifts trace/span ids
└──────────┬──────────────┘
           │ ObsEnvelope (payload still bytes; labels populated)
           ▼
┌─────────────────────────┐
│ Observer (global)       │  ArcSwap<dyn Observer>; default = NoopObserver
└──────────┬──────────────┘
           │
           ├─► Sampler / RateLimiter (config in ArcSwap; live-reloaded)
           │
           └─► Per-tier bounded mpsc channels
                      │
            ┌─────────┴─────────┬─────────────┬──────────────┐
            ▼                   ▼             ▼              ▼
     LogWorker (task)    MetricWorker   TraceWorker    AuditWorker
            │                   │             │              │
            │                   │             │              │
            └─► Sink chain      └─► Sink chain (per-worker; isolated failure domain)
```

### 2.1 The Observer trait

All emission goes through a single global function pointer. This is the
only runtime indirection on the hot path.

```rust
pub trait Observer: Send + Sync + 'static {
    /// Hot path; never blocks; never panics.
    fn emit_envelope(&self, env: ObsEnvelope);

    /// Cheap callsite filter check; called by both emit forms before
    /// constructing the event payload.
    fn enabled(&self, callsite: &ObsCallsite) -> bool;

    /// Flush all sinks; awaits in-flight batches.
    fn flush(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Shutdown all sinks and join workers. Idempotent.
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Synchronous shutdown for use in panic hooks and `Drop` impls
    /// where awaiting is not possible. Best-effort within `timeout`.
    fn shutdown_blocking(&self, timeout: Duration);
}

static OBSERVER: ArcSwap<Box<dyn Observer>> = ...;  // default = NoopObserver

pub fn observer() -> Guard<Arc<Box<dyn Observer>>> { OBSERVER.load() }
pub fn install_observer<O: Observer>(o: O) { ... }
pub fn install_panic_hook() { ... }              // opt-in; see § 9.1
```

- Library crates depend on `obs-sdk` only — they never construct an
  observer, so a binary that does not initialize observability pays one
  atomic load per emit and nothing else.
- The observer can be swapped at runtime (test harnesses replace it
  with `InMemoryObserver` in `setup`; reset in `teardown`).
- `flush` and `shutdown` are async and idempotent. `shutdown` is
  required at process exit to avoid losing in-flight events.

### 2.2 Per-tier workers and sinks

Each tier owns a bounded `tokio::sync::mpsc` channel and a single
worker `tokio::task` that drains it. Sinks are children of workers.

```rust
pub trait Sink: Send + Sync + 'static {
    /// Called from the worker task — never the emit thread.
    /// Must be non-blocking; long IO must be queued internally.
    fn deliver(&self, env: &ObsEnvelope);

    fn flush(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
```

Why per-tier workers (not one shared worker):

- **Failure isolation.** A blocked Parquet writer cannot stall metric
  emission. A failing OTLP gRPC connection cannot stall LOG.
- **Backpressure granularity.** LOG tier may run at 100k events/s while
  AUDIT tier runs at 100/s; each gets a channel sized for its own load.
- **Sink lifecycle.** Per-worker `shutdown()` lets us flush an OTLP sink
  before tearing down a Parquet sink, in a deterministic order.

The router applies tier matching at observer level:

```rust
pub struct SinkRouter {
    routes: Vec<(TierMatcher, SeverityMatcher, Arc<dyn Sink>)>,
    fallback: Arc<dyn Sink>,
}
```

#### Built-in sinks (ship in v1)

| Crate | Sink | Notes |
| --- | --- | --- |
| `obs-core` | `NoopSink` | Always present; returned when no observer installed |
| `obs-core` | `InMemorySink` | Test harnesses; bounded ring buffer with `iter()` |
| `obs-core` | `StdoutSink` (dev) | Human-readable line render; toggled by `OBS_DEV=1` |
| `obs-core` | `NdjsonFileSink` | Append to a file; rotates on size |
| `obs-otel` | `OtlpLogSink`    | OTLP/gRPC logs export |
| `obs-otel` | `OtlpMetricSink` | OTLP/gRPC metrics export |
| `obs-otel` | `OtlpTraceSink`  | OTLP/gRPC traces export |
| `obs-parquet` | `ParquetSink` | One sparse table for all events; rolls files |
| `obs-clickhouse` | `ClickHouseSink` | One sparse table; live INSERT |

### 2.3 Hot path: zero allocations beyond the event itself

For an event with `N` fields:

1. The `ObsMyEvent { … }` literal is one stack allocation; no heap.
2. `EventSchema::encode_payload(&mut buf)` writes into a thread-local
   `BytesMut` reused across emissions; the resulting `Bytes` is
   reference-counted, no copy.
3. `EventSchema::project(&mut env)` writes labels into the envelope's
   `SmallVec<[(&'static str, String); 8]>`; only `String`s for label
   *values* allocate, and label values are typically already in scope.
4. The envelope is moved into the per-tier `mpsc` channel. The channel
   is bounded (default 8192); on full, the SDK increments
   `obs_dropped_total{tier}` and discards rather than block.

Background workers per tier own their sinks and drain in batches. Sinks
may further batch internally (Parquet, ClickHouse).

### 2.4 Sampling

Two-stage sampling, both configured live via `ArcSwap<EventsConfig>`:

- **Head sampling**: per `(event_full_name, severity)` rate. Fast path,
  one `f64` comparison.
- **Tail-on-error**: a per-scope ring buffer (capacity 64) holds recent
  events; if any subsequent event in the same `obs::scope!` is
  `>= ERROR`, the buffer is flushed. Otherwise the buffer is dropped
  when the scope guard is dropped.

The tail buffer lives in a `tokio::task_local!` storage; entering an
`obs::scope!` macro pushes a new buffer onto the per-task stack. The
scope guard's `Drop` impl is what triggers either flush or discard —
**there is no "request_end()" call to forget**. This is a deliberate
fix to a known footgun in scope-based observability designs that key
buffers by `request_id: String` and leak when the handler short-circuits.

```rust
pub async fn handle_request(req: Request) -> Response {
    let _scope = obs::scope!(trace_id = req.id.clone());

    ObsRequestStarted::builder().route(route_of(&req)).emit();
    let r = process(req).await;          // may emit ObsUpstreamFailed (ERROR)
    ObsRequestCompleted::builder()
        .route(route_of(&req))
        .status(if r.is_ok() { Status::Ok } else { Status::ServerError })
        .latency_ms(r.elapsed_ms())
        .emit();

    r
    // _scope dropped (including on async cancel — see § 8.1): if any ERROR
    // seen, flush full buffer; else discard.
}
```

### 2.5 Backpressure

Every channel is bounded. On overflow, the SDK increments
`obs_dropped_total{tier, reason=channel_full}` and drops the envelope.
We never block the emit thread, and we never silently spool to disk in
the default config — spooling is a feature of specific sinks (Parquet,
audit) and is opt-in.

## 3. Storage model — single sparse table

The default analytical store is **one sparse columnar table** that
contains every event type, not one table per type.

### 3.1 Table shape

| Column group | Columns | Notes |
| --- | --- | --- |
| Envelope | `ts_ns`, `full_name`, `schema_hash`, `tier`, `sev`, `trace_id`, `span_id`, `parent_span_id`, `service`, `instance`, `version`, `sampling_reason` | One row per event |
| Common labels | `labels: map<string, string>` | All label fields, key by name |
| Per-event payload | `payload_<full_name_snake>: Struct<…>` | Sparse: only populated when `full_name` matches |
| Raw fallback | `payload_proto: bytes` | Original buffa-encoded bytes for unknown schemas |

Example (DDL elided):

```
ts_ns                | full_name                        | sev   | labels                                       | payload_myapp_v1_obs_request_completed                       | payload_myapp_v1_obs_user_signed_up
─────────────────────┼──────────────────────────────────┼───────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┼─────────────────────────────────────
1746150225123456000  | myapp.v1.ObsRequestCompleted     | INFO  | {route:list_users, status:ok, tenant:acme}   | {user_id: u-42, latency_ms: 48, bytes_out: 2048}             | NULL
1746150225789012000  | myapp.v1.ObsUserSignedUp         | INFO  | {channel:web, country:US}                    | NULL                                                         | {user_id: u-99, plan: pro}
```

Per-event payload columns are nullable structs; sparse columnar
storage (Parquet, ClickHouse) compresses the unused ones to ~1 byte
per row.

### 3.2 Why one table, not one-table-per-event

| Concern | Per-event tables | Single sparse table |
| --- | --- | --- |
| Cross-event time joins (`SELECT * WHERE trace_id = X`) | union N tables | one query |
| Adding a new event type | new table + new ingest pipeline + new schema migration | append a struct column; old rows have NULL |
| Schema evolution | per-table migration on every add | additive; existing rows unchanged |
| File count in object storage | `O(events × hours)` | `O(hours)` |
| Catalog clutter | tens to hundreds of tables | one table per service |
| Query "give me everything in the last 5 min" | union all tables | trivial |
| Compression of label keys | repeated per file | dictionary-encoded once |
| Operational analogue | Mixpanel/Amplitude with named tables | Honeycomb / Snowplow Atomic / Segment unified events table |

The single-table model is the original wide-events shape and is what
Honeycomb, Snowplow, and Segment converge on. It optimises for the
read patterns wide-events were designed to enable: ad-hoc OLAP across
the entire event stream.

Per-event tables remain available as an opt-in for very-high-volume
single-event-type workloads where partition pruning by `full_name` is
not enough. This is configured per-sink, not per-event:

```rust
ParquetSink::builder()
    .layout(ParquetLayout::Single)               // default
    // .layout(ParquetLayout::TablePerEvent)     // opt-in for high-volume splits
    .build()?
```

### 3.3 Analytics is a view, not a tier

Analytics is not a separate signal type — it is what falls out of the
single-table model. Every wide event is implicitly an "analytics event"
because:

- `full_name` is the event name (Mixpanel `event` column equivalent)
- `labels` are the dimensions
- numeric `MEASUREMENT` fields are the metrics
- `trace_id` is the session/journey key
- `ts_ns` is the timeline

A funnel query is `SELECT full_name, count(*) FROM obs_events WHERE
trace_id IN (…) GROUP BY full_name`; a cohort query is `WHERE
full_name = 'myapp.v1.ObsUserSignedUp' AND labels['country'] = 'US'`;
a retention query joins the table to itself on `labels['user_id']`.
There is no analytics-tier sink — `ParquetSink` and `ClickHouseSink`
*are* the analytics sinks.

## 4. OpenTelemetry mapping

The mapping is performed by the OTLP sinks, not the core; this keeps
the core free of OTel as a hard dependency. All mappings target OTLP
1.x (`opentelemetry-proto >= 1.4`).

### 4.1 OTel Resource (set once)

`service`, `instance`, `version` are **Resource** attributes in OTel,
not per-record attributes. The OTLP sinks build a single `Resource`
at construction and reuse it across every export RPC. The envelope
also carries them so non-OTLP sinks (Parquet, ClickHouse, NDJSON) get
the same identity without a separate Resource concept; the OTLP sinks
specifically **do not** also stamp them as per-LogRecord attributes
to avoid double-attribution.

| Envelope field | OTel Resource attribute (semconv) |
| --- | --- |
| `env.service`  | `service.name` |
| `env.instance` | `service.instance.id` |
| `env.version`  | `service.version` |
| (config)       | `service.namespace` (optional, from `obs.yaml`) |
| (config)       | `deployment.environment` (optional, from `obs.yaml`) |
| (config)       | `host.name`, `host.arch` (optional, auto-detected if `detect_host = true`) |

Resource `schema_url` is set to the OTel semconv URL the sink was
built against, e.g. `https://opentelemetry.io/schemas/1.27.0`.

### 4.2 Severity → OTLP `SeverityNumber`

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

### 4.3 To OTLP Logs

A wide event maps 1:1 to an OTel `LogRecord`:

| Wide event | OTLP `LogRecord` |
| --- | --- |
| `env.ts_ns` | `time_unix_nano` |
| `Instant::now()` at `emit_envelope` | `observed_time_unix_nano` |
| `env.sev` | `severity_number` + `severity_text` per § 4.2 |
| `env.full_name` | `attributes["event.name"]` (semconv `event.name`) |
| `env.schema_hash` | `attributes["obs.schema_hash"]` (as `int_value`, u64) |
| `env.trace_id` (16-byte hex → 16 raw bytes) | `trace_id` |
| `env.span_id`  (8-byte hex → 8 raw bytes)   | `span_id` |
| `env.parent_span_id` | `attributes["obs.parent_span_id"]` (OTLP LogRecord has no parent_span_id) |
| `env.labels[k]` | `attributes[k]` (as `string_value`) |
| `env.sampling_reason` | `attributes["obs.sampling_reason"]` |
| typed payload (decoded) | `body` as `KeyValueList` (opt-in; default off) |

If `body` decode is off, the LogRecord is emitted with
`body = bytes(env.payload)`; consumers that know the schema (via
`schema_hash`) can decode the buffa bytes themselves.

### 4.4 To OTLP Metrics

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

### 4.5 To OTLP Traces

If `env.trace_id` is non-empty:

- a `Span` is emitted with `name = env.full_name`,
- `start_time_unix_nano = end_time_unix_nano = env.ts_ns` (point-in-time
  span), unless the schema declares a `FIELD_KIND_DURATION_NS` field,
  in which case `start_time = ts_ns - duration`,
- `parent_span_id = env.parent_span_id`,
- `kind = SPAN_KIND_INTERNAL` by default; set on the schema via
  `option (obs.v1.event).span_kind = SPAN_KIND_SERVER` for inbound
  edge events,
- `status_code` derived from severity: `SEVERITY_ERROR | FATAL → STATUS_CODE_ERROR`,
  otherwise `STATUS_CODE_UNSET`,
- `attributes := env.labels` plus `event.name = env.full_name`.

Spans for the same `trace_id` are tied together by the OTel exporter;
the SDK does not attempt span-tree reconstruction in-process.

### 4.6 Trace context propagation

Cross-process correlation uses W3C Trace Context (and optionally
Baggage). The SDK does not implement HTTP middleware itself; it
exposes the propagator hook so HTTP/gRPC layers can use it.

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
`span_id: [u8; 8]`, `flags: u8`. `obs-tower` (companion crate, see
crates-design § 2.11) ships a ready-made `tower::Layer` that calls
`extract_w3c` on inbound and `inject_w3c` on outbound, plus emits
`ObsHttpRequestStarted` / `ObsHttpRequestCompleted`.

### 4.7 Why we are not just an OTel SDK

OpenTelemetry's data model is *signal-shaped*: logs, metrics, traces
are peer concepts. A wide event is *operation-shaped*: one record
describes the whole operation, and the three signals are projections.
We project *into* OTel without forcing application code to think in
OTel.

## 5. Tracing-style ergonomics

The `tracing` crate has set the bar for Rust observability ergonomics.
We adopt its core ergonomic primitives where they fit, and consciously
diverge where wide events demand a stronger contract.

### 5.1 What we adopt from `tracing`

- **Macro-driven call sites.** `obs::emit!` mirrors `tracing::event!`:
  static metadata at compile time, dynamic check + dispatch at runtime.
  Field setup is gated by `if observer().enabled(...)` so a noop
  observer pays only one atomic load + one branch.
- **RAII span guards.** `obs::scope!` returns a `Drop`-on-exit guard,
  matching `Span::enter()`. Nested scopes form a per-task stack.
- **Field inheritance.** Fields declared on an outer `obs::scope!` are
  available to events emitted inside it without re-passing them. This
  is how `trace_id` flows automatically (see § 5.4).
- **`#[obs::instrument]`** on functions/methods to wrap the body in a
  scope and emit `ObsFnEntered` / `ObsFnExited` events with `latency_ns`.
- **Layer composition** for the in-process pipeline (sampler, rate
  limiter, redactor, sink router) — though we expose a higher-level
  `StandardObserverBuilder` instead of the raw layer trait, for
  ergonomic simplicity.
- **Static `MacroCallsite`-style metadata.** Each `emit!` site compiles
  to a `static OBSERVER_CALLSITE: ObsCallsite` so per-callsite filtering
  (via `EnvFilter`-style DSL) is cheap.

### 5.2 What we deliberately diverge on

| `tracing` | `obs` | Why |
| --- | --- | --- |
| Untyped key=value at call site | Typed struct; field names compile-checked | Wide events demand a contract |
| String message body | No string body; the *event type* is the message | Forces schema discipline |
| Multiple subscribers via layers | Single global observer with composed sinks | Wide events have a single canonical envelope; subscribers cannot disagree on shape |
| Field inheritance is implicit through context | `obs::scope!` declares which fields propagate (allowlist) | Avoids accidental leakage of high-card fields from outer scopes |
| `#[instrument]` auto-captures all args | `#[obs::instrument]` opts in per arg | Args may be PII; making it opt-in is safer |

### 5.3 Emit forms: builder is canonical, macro is shorthand

`obs` ships two equivalent emit forms. **The chained builder is the
canonical form**; the `obs::emit!` macro is sugar for terse cases.
This split is deliberate — see [dev-ergonomics-design.md § 1.1](./dev-ergonomics-design.md#11-the-two-emit-forms-canonical-builder)
for the rationale (rust-analyzer chain-completion, pinpointed required-
field errors, refactor friendliness).

```rust
// PRIMARY (what dogfooding examples and codegen scaffolds use):
ObsRequestCompleted::builder()
    .route(Route::ListUsers)
    .status(Status::Ok)
    .latency_ms(48)
    .bytes_out(2048)
    .emit();                       // .emit_at(Severity::Warn) to escalate

// SHORTHAND (for one- or two-field events):
obs::emit!(ObsHelloEmitted { who: Audience::World });
obs::emit!(Severity::Warn, ObsUpstreamFailed { route, error_kind });
```

The `Severity` keyword form (`obs::emit!(WARN, …)`) is supported via
re-exported severity idents `obs::TRACE`, `obs::DEBUG`, `obs::INFO`,
`obs::WARN`, `obs::ERROR`, `obs::FATAL` — `obs::emit!(WARN, …)` and
`obs::emit!(Severity::Warn, …)` are the same call.

Both forms expand to the same callsite-gated dispatch:

```rust
// Generated by both .emit() and obs::emit!:
{
    static __CALLSITE: ObsCallsite = ObsCallsite::new(
        ObsRequestCompleted::FULL_NAME,
        ObsRequestCompleted::DEFAULT_SEV,
        module_path!(),
        file!(), line!(),
    );
    if obs::observer().enabled(&__CALLSITE) {
        let evt: ObsRequestCompleted = /* struct constructed by builder or literal */;
        let mut env = obs::__private::build_envelope(&__CALLSITE, &evt);
        evt.project(&mut env);
        obs::observer().emit_envelope(env);
    }
}
```

The builder's `.emit()` is implemented as a thin `inline(always)` over
the same callsite-gated dispatch (see schema-codegen § 3.3). The
callsite is keyed on the macro/builder source location, so each emit
site has a stable static `ObsCallsite` for filter caching.

### 5.4 The `obs::scope!` macro and automatic correlation

```rust
let _scope = obs::scope!(
    trace_id  = req.id.clone(),
    tenant_id = tenant.clone(),
);
```

Effects:

1. Push a `ScopeFrame { fields, tail_buffer: VecDeque::with_capacity(64), seen_error: false }`
   onto a `tokio::task_local!` stack (or thread-local for sync code).
2. Every subsequent `obs::emit!` first inspects the stack: if a field
   on the event schema is empty *and* a frame above declares a value
   for that field name, the value is auto-filled. So `trace_id` flows
   from the outermost scope to every event without manual threading.
3. The frame's `tail_buffer` records every emitted envelope at TRACE
   or DEBUG (head-sampled out by default) until either:
   - an event with `sev >= ERROR` is emitted → buffer is flushed
     (sampling_reason = `tail_on_error`), `seen_error = true`,
   - the scope guard is dropped → buffer is discarded.
4. When the scope guard is dropped, the frame is popped. **No
   `on_request_end()` call to forget.** This is a direct fix for the
   leak class found in scope-by-string designs.

`obs::scope!` is an **explicit allowlist**: only the fields named in
the macro propagate; nothing else from the surrounding context leaks
into events. A proc-macro cannot scan the entire binary at expansion
time, so we cannot prove at compile time that every named field is
actually consumed by some `EventSchema`. We do two things instead:

- **At observer init**, the SDK builds a global `BTreeSet<&'static str>`
  of field names declared as LABEL or TRACE_ID across every
  `EventSchema` registered in the binary (the codegen emits a
  `register_schema` call per type, collected by `inventory`).
- **In dev mode** (`OBS_DEV=1` or debug builds), the first emit inside
  a scope frame whose declared fields contain a name absent from that
  set issues a one-time `tracing::warn!` (or stderr line) naming the
  field. In release builds the check is skipped.

Auto-fill rule: a scope-declared field overrides an event field only
when the event field's value is the type's default sentinel
(`String::new()` for strings, `0` for numerics with `#[obs(trace_id)]`
or `#[obs(label)]` annotation marked `default-fillable`). Explicit
non-default values on the call site always win.

### 5.5 The `#[obs::instrument]` attribute

```rust
#[obs::instrument(
    enter = ObsFnEntered,
    exit  = ObsFnExited,
    skip  = (raw_body),
    fields(route, tenant_id),
)]
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    // ...
}
```

Expansion (sketch):

```rust
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    let _scope = obs::scope!(route = req.route(), tenant_id = req.tenant());
    obs::emit!(ObsFnEntered { fn_name: "handle_list_users".into() });
    let __started = std::time::Instant::now();
    let __res = async move { /* original body */ }.await;
    obs::emit!(ObsFnExited {
        fn_name: "handle_list_users".into(),
        latency_ns: __started.elapsed().as_nanos() as u64,
    });
    __res
}
```

Both `ObsFnEntered` and `ObsFnExited` are built-in events shipped in
`obs-proto`; they are LOG-tier, INFO-default-sev, and have one LABEL
field (`fn_name`) and one MEASUREMENT (`latency_ns`).

### 5.6 The `EnvFilter`-equivalent

We ship `obs::Filter` with a DSL inspired by `tracing-subscriber::EnvFilter`:

```
OBS_FILTER="info,myapp::auth=debug,myapp.v1.ObsRequestCompleted=trace"
```

Filters apply at the static `ObsCallsite` level so a filtered-out emit
costs only the atomic load + branch.

## 6. Configuration

A single YAML config file (or programmatic `EventsConfig` struct),
reloaded live via `ArcSwap`:

```yaml
service:
  name: my-api
  version: 1.4.0
  instance: ${HOSTNAME}

sampling:
  default:
    head_rate: 1.0
    tail_on_error: true
  per_event:
    "myapp.v1.ObsRequestCompleted":
      head_rate: 0.05            # 5% of successful requests
      always_log_slower_than_ms: 500
      tail_on_error: true

cardinality:
  enforce: strict
  max_label_value_bytes: 256

classification:
  pii_redaction: enabled
  secret_strip: enabled

filter: "info,myapp::auth=debug"

sinks:
  - type: stdout
    when: { dev: true }
  - type: otlp
    endpoint: http://localhost:4317
    signals: [logs, metrics, traces]
  - type: parquet
    when: { tier: log }
    base_dir: /var/lib/obs/parquet
    layout: single
    roll: { max_bytes: 268435456, max_age_secs: 300 }
```

Reloading semantics: each emit captures a `Guard<Arc<EventsConfig>>`;
the config never tears mid-decision.

### 6.1 Defaults table

Every key has a documented default; an empty config file is valid and
gives a working observer.

| Key | Default | Notes |
| --- | --- | --- |
| `service.name` | crate name from `CARGO_PKG_NAME` if unset | Required for OTLP Resource |
| `service.version` | `CARGO_PKG_VERSION` | |
| `service.instance` | `hostname` | Override for stateful sets |
| `sampling.default.head_rate` | `1.0` | Sample everything by default |
| `sampling.default.tail_on_error` | `true` | Flush on first ERROR in scope |
| `sampling.default.always_log_slower_than_ms` | unset | No slow-path override |
| `cardinality.enforce` | `strict` | `strict` panics in debug, drops in release; `permissive` only logs |
| `cardinality.max_label_value_bytes` | `256` | Per spec policy: bytes not chars |
| `classification.pii_redaction` | `enabled` | Sink-side PII scrubber |
| `classification.secret_strip` | `enabled` | LOG/AUDIT cannot carry SECRET |
| `limits.max_payload_bytes` | `262144` (256 KiB) | Per-event encoded cap |
| `limits.channel_capacity` | `8192` | Per-tier mpsc channel size |
| `limits.tail_buffer_capacity` | `64` | Per-scope tail-on-error ring buffer |
| `limits.forensic_rate_per_s` | `100` | `obs::forensic!` rate limit |
| `limits.forensic_daily_cap` | `100000` | Per-site daily cap |
| `filter` | `info` | Like `RUST_LOG=info` |
| `sinks` | `[stdout when dev]` | Dev: stdout pretty; prod: explicit list required |
| `sinks[type=otlp].protocol` | `grpc` | `grpc` or `http_protobuf` |
| `sinks[type=otlp].compression` | `gzip` | OTel-recommended |
| `sinks[type=otlp].timeout_secs` | `10` | |
| `sinks[type=otlp].retry.max_attempts` | `5` | exponential backoff |
| `sinks[type=parquet].layout` | `single` | `single` or `table_per_event` |
| `sinks[type=parquet].roll.max_bytes` | `268435456` (256 MiB) | |
| `sinks[type=parquet].roll.max_age_secs` | `300` (5 min) | |
| `sinks[type=parquet].compression` | `zstd` | level 3 |
| `sinks[type=clickhouse].batch_size` | `10000` | Inserts batched by row count |

## 7. Error handling

The SDK never panics on emit. Failure modes:

| Failure | Action |
| --- | --- |
| Channel full | Drop, increment `obs_dropped_total{tier, reason=channel_full}` |
| Sink IO error | Sink retries with backoff; after N retries, drop and increment `obs_sink_failed_total` |
| Payload encode failure (impossible with a valid struct) | `debug_assert!`; release: drop with metric |
| Observer not installed | Noop; one atomic load only |
| Config invalid at reload | Reject, keep previous config, surface via `ObsConfigReloadFailed` self-event |

## 8. Threading model

- **Emit thread**: any application thread, sync or async. Emit is
  `&self` on the global observer; no locks taken on the hot path.
- **Per-tier worker**: one tokio task per tier, owning a bounded
  `mpsc::Receiver`. Workers spawned at observer init via `tokio::spawn`;
  their `JoinHandle`s stored on the observer for `shutdown()`.
- **Background flush**: each sink has its own batching policy; OTLP
  sinks batch by 100 events or 1 s, Parquet by 256 MiB or 5 min.
- **Shutdown**: `obs::observer().shutdown().await` flushes all sinks
  and joins all worker tasks; safe to call from `tokio::main` exit hook.

### 8.1 Async cancellation

`obs::scope!` returns an RAII guard whose `Drop` runs the
flush-or-discard logic. Tokio guarantees `Drop` runs when a future is
cancelled (including during `select!`, `JoinSet::abort`, or task
cancellation), so the tail buffer is **not** leaked on cancellation.
A scope frame containing `seen_error = true` flushes its buffer in
`Drop` even when the future is being cancelled.

The frame's `Drop` must not `await`. It pushes the buffered envelopes
onto the per-tier mpsc channel (which is non-blocking). Channel-full
in `Drop` is treated like any other backpressure event and increments
`obs_dropped_total{tier, reason=channel_full_on_drop}`.

### 8.2 Runtime constraints

- **Async runtime: tokio only** (current_thread or multi_thread).
  Other runtimes (smol, async-std) are not supported in v1 because the
  scope task-local relies on `tokio::task_local!` semantics and the
  worker tasks are `tokio::spawn`'d. A future SDK split could abstract
  this behind a `Runtime` trait if demand appears.
- **`std::thread`-based code is supported** by falling back to a
  thread-local scope frame when no tokio task-local is present.
- **MSRV**: pinned in `rust-toolchain.toml` to current stable
  (`1.85+`). Minor releases bump MSRV freely; major releases only
  with a `migration.md` entry.
- **Targets**: `x86_64-{linux,darwin}`, `aarch64-{linux,darwin}` in
  CI. WASM and `no_std` are non-goals for v1; `obs-types` is
  `no_std`-clean for future use.

## 9. Production concerns

This section addresses operational details that surface only when
the SDK runs in a real service for real users.

### 9.1 Panic hook

A panic in user code should produce one final `ObsPanicked` event
before the process tears down. The SDK ships an opt-in panic hook:

```rust
// In main(), after install_observer():
obs::install_panic_hook();           // chains the existing std::panic::take_hook()
```

The hook captures `panic.message()`, `panic.location()`, the active
scope's `trace_id`/`span_id` (if any), and emits `ObsPanicked` (LOG
tier, FATAL severity, sampling_reason = `OVERRIDE`). Then it calls
`obs::observer().shutdown_blocking(Duration::from_secs(2))` to flush
in-flight sinks before letting the previous hook continue (which may
abort the process). Process-aborting hooks (e.g. for `panic = "abort"`
profiles) still get the event because the flush completes before the
chained hook fires.

### 9.2 Payload size cap

Wide events should not be megabytes. The runtime enforces a per-event
encoded-payload cap (default 256 KiB; configurable via
`EventsConfig.limits.max_payload_bytes`). Oversized payloads are
**dropped at emit time** with a metric increment
(`obs_oversized_total{full_name}`) and a one-shot stderr warning in
dev mode. This applies to forensic blobs as well — they are not a
free-pass to emit a 50 MiB heap dump.

### 9.3 Forensic rate limit

`obs::forensic!` is rate-limited per `(crate, site)` via a token
bucket:

- Default: 100 events/s/site, with a 1000-event burst.
- Daily cap: 100K events/site/day (rolling window).
- Configurable per-crate via `[package.metadata.obs] forensic_rate_per_s = 100`.

When the bucket empties, additional `forensic!` calls become noops
and increment `ObsForensicBudgetExceeded` (the SDK self-event). The
intent is "forensic data is precious; blackbox dumps are not".

### 9.4 OTLP transport

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
`OTEL_EXPORTER_OTLP_COMPRESSION`) are honoured by `from_env()`.

Non-retryable response codes (`4xx` other than `429`/`503`) drop the
batch and increment `ObsSinkFailed{sink=otlp_logs, reason=4xx}`.
Retryable failures back off exponentially; the queue between the
worker and the OTLP exporter is bounded — overflow drops with a
metric increment, never blocks.

### 9.5 Coexistence with `tokio-console`

`obs` does not replace `tokio-console`; the two are complementary.
`tokio-console` is for tracing async runtime behaviour (task
states, lock contention, runtime stats); `obs` is for application
events. Both can be installed in the same binary; they listen on
different channels (`tokio-console` uses its own `tracing` layer
on the `tokio=trace` target).

### 9.6 ClickHouse runtime cardinality

The single-table model uses `Map(LowCardinality(String), String)` for
labels (crates-design § 2.8). ClickHouse's `LowCardinality` type
shares a column-wide dictionary; if labels admit very many distinct
values at runtime (despite cardinality annotations being LOW/MEDIUM),
the dictionary degrades. The SDK does **not** enforce label-value
cardinality at runtime — that responsibility sits with the operator,
who can:

- watch the `obs.runtime.v1.ObsLabelCardinalityHigh` self-event
  (emitted when an HLL counter for a `(full_name, label_key)` pair
  estimates > the schema's declared cap), or
- run `obs query --since 1h --select 'distinct labels.tenant_id'` to
  audit a specific label.

### 9.7 Iceberg / Delta Lake position

`obs-parquet` emits plain Parquet files into a directory layout
designed to be a valid Iceberg/Delta-compatible warehouse table when
combined with downstream catalog metadata. We do not write Iceberg
manifests directly in v1; users wire `nessie`, `polaris`, or AWS Glue
on top. The Arrow schema is stable (additive only; see
schema-codegen § 5), so an external catalog is safe.

### 9.8 Multi-process / fork

The SDK assumes a single-process model. After `fork()` (rare in
tokio services), the child process should call `install_observer`
fresh; the per-tier worker tasks are not inherited cleanly. We do
not document a sanctioned fork path; if a user needs one, the
`unix-fork` feature flag (post-v1) would gate a `pre_fork()`/`post_fork()`
pair.

## 9. Service identity

`service`, `instance`, `version` are written **once** at observer init
and stored as `ArcSwap<ServiceIdentity>`. On every `emit_envelope`
they are read via a single `Guard` load and cloned into the envelope.

This matches the operational reality: the values change once per
process lifetime (or never), so spending allocations on them per-emit
is wasteful. Tests can swap them by re-installing the observer.

## 10. Key Design Decisions

The decisions below are the ones most likely to surprise a reader of
this spec, or the ones we expect future contributors to question. Each
includes the alternative we considered and why we rejected it.

### D1 — Wire format is buffa, not prost

`buffa` (and `buffa-build`, `buffa-reflect`) is the runtime, not
`prost`. Why:

- **Custom proto options first-class.** `(obs.v1.field) = { kind: LABEL,
  cardinality: LOW }` is parsed by `buffa-reflect`'s `DescriptorPool`
  directly from the FileDescriptorSet that `buffa-build` emits. With
  prost we would need either `prost-reflect` *or* a hand-rolled proto
  parser for the annotations; tok demonstrates the hand-rolled path is
  expensive to maintain.
- **Zero-copy views.** Buffa generates a `<Type>View<'a>` struct per
  message that borrows directly from the wire bytes. Sinks that only
  need a few fields (e.g. the OTLP attribute view that only needs
  `labels`) can skip the heap-allocating decode entirely.
- **No protoc dependency.** `buffa-build` ships a self-hosted parser
  (or accepts a precompiled `FileDescriptorSet`), so user builds do not
  depend on `protoc` on the path. Hermetic CI is one less variable.
- **`preserve_unknown_fields = true` by default.** A consumer reading
  an envelope built against an older schema does not lose unknown
  fields on re-encode; this is what lets old proxies safely round-trip
  events from newer producers.
- **Tested in production.** This stack is already in use elsewhere in
  the author's projects; we know its failure modes.

Trade-off: buffa's MSRV (1.85+) is tighter than prost's (1.70+). We
accept this because the project pins `rust-toolchain.toml` to current
stable.

### D2 — Single sparse table, not table-per-event

Default storage layout is one wide table with sparse per-event
struct columns (see § 3). The motivation is the `WHERE trace_id = X`
read pattern, additive schema evolution, and operational simplicity.
Per-event tables remain opt-in for the `ParquetLayout::TablePerEvent`
case; we did not want to make the high-volume case *impossible*, only
not the default.

### D3 — Per-tier mpsc workers, not a shared queue

Per-tier workers isolate failure domains (a hung Parquet writer cannot
stall metric emission), allow per-tier channel sizing, and make
shutdown ordering deterministic. Cost: more tokio tasks (4 by default).
This is negligible for any non-trivial service.

### D4 — Tail buffer scoped to `obs::scope!` Drop, not `request_id` string

Scope-keyed buffers in a `DashMap<String, RequestTrack>` (the obvious
implementation) leak when a handler short-circuits without calling
`on_request_end`. We tie the buffer's lifetime to the RAII guard that
`obs::scope!` returns; cleanup happens in `Drop` regardless of the
control-flow path that exits the scope. This is the same pattern
`tracing::Span::enter` uses, and is the right shape for Rust.

### D5 — Field inheritance is allowlisted via `obs::scope!`

`tracing` allows any subscriber-attached field to flow into nested
events implicitly. We require fields to be named in `obs::scope!(...)`
explicitly. This avoids two failure modes:

- A high-cardinality field accidentally inherited into a metric
  attribute set.
- A PII field flowing into a label without explicit declaration.

The validation happens at observer init (linker-level scan of all
`EventSchema` impls registered via `inventory`) and in dev mode at
runtime. We deliberately do **not** claim compile-time enforcement: a
proc-macro can't see the whole binary. See § 5.4 for the runtime
mechanic.

### D6 — Trace correlation is automatic via task-local, schema field is the contract

User code calls `obs::scope!(trace_id = req.id)`. The codegen for
`EventSchema::project` checks the active scope's field map for any
field annotated `FIELD_KIND_TRACE_ID` on the schema, and auto-fills it
if the call-site struct literal left it empty. This means:

- Users typically thread `trace_id` only at the scope boundary.
- A user who *wants* an event with a different trace_id can pass it
  explicitly on the struct literal; the explicit value wins.

This fixes a real ergonomic regression in earlier wide-event SDKs,
which required passing `request_id` at every emit site.

### D7 — Service identity set once, read via ArcSwap

`service`, `instance`, `version` change at most once per process
lifetime. Storing them per-emit is waste; storing them in static
atomics (the obvious answer) makes mid-test reset awkward. ArcSwap
splits the difference: cheap reads, simple replacement, no atomics
fighting.

### D8 — Schema hash is a build-time `u64`, not runtime, not 32 bytes

We BLAKE3 the descriptor at build time and truncate to a `u64`
constant. Runtime hashing on every emit would be wasteful and would
forfeit the ability to verify schema versions in CI without running
the binary. 64 bits is sized for accidental-collision avoidance at
realistic schema counts (~10⁴ per workspace, < 10⁸ industry-wide)
with multiple orders of magnitude of birthday-bound headroom; the
hash is not used as a tamper-detection primitive (buffa payloads are
not signed). Saves 24 bytes per envelope vs the 32-byte natural
BLAKE3-256 output and matches `callsite_id`'s shape so the two id
namespaces compose uniformly.

### D9 — No panic on emit, ever

Every error path in the emit pipeline (channel full, sink failure,
config reload error) drops the event with a metric increment.
`debug_assert!` is acceptable; `panic!` is not. An observability bug
that takes down the host process is a worse incident than a missing
event.

### D10 — Global observer, not contextual

OpenTelemetry's contextual propagation through every API is powerful
but adds friction to every library that wants to emit events. A global
observer (with `ArcSwap` for test isolation) makes a library emission
cost one atomic load. Cross-process trace propagation is still done
via OTel propagators at HTTP/gRPC boundaries; this is orthogonal to
the in-process observer.

### D11 — Labels projected at emit time, not at sink time

`EventSchema::project` writes labels into the envelope's `labels` map
once. Cheap sinks (metric counter, OTel attribute writer, audit
filter) iterate `labels` without touching the typed payload. Expensive
sinks (the Parquet writer that needs full row data) decode the
payload. This pays the projection cost once for many sinks rather than
re-extracting per sink.

### D12 — Forensic events are budgeted, not banned

Sometimes there is no schema yet for the data we need to log. The
`obs::forensic!` macro emits an `ObsForensicEvent` carrying free-form
data. Each crate has a `forensic_max` budget in `Cargo.toml`; the CLI
lints excess. The trend over time should be toward zero forensic
calls, but we provide the escape hatch rather than force an
"emergency PR for a new schema" workflow during incidents.

### D13 — `Obs*` prefix on event type names

See § 1.5. Convention enforced by lint, not the type system. Visual
distinction is the main payoff; greppability and codegen pattern
matching are bonuses.

### D14 — Builder is canonical, macro is shorthand

See § 5.3 and [dev-ergonomics-design.md § 1.1](./dev-ergonomics-design.md#11-the-two-emit-forms-canonical-builder).
The chained typed builder is the form scaffolding emits, docs lead
with, and AI prompts default to. `obs::emit!` exists as sugar for
terse one- or two-field events and severity escalation. Both expand
to the same callsite-gated dispatch.

### D15 — OTel identity on Resource, not LogRecord attributes

`service`/`instance`/`version` go on the OTel `Resource` once, not on
every `LogRecord`/`DataPoint`/`Span` attribute set. Per-record
duplication wastes wire bytes and confuses downstream queries (which
attribute key is canonical?). The envelope still carries them so
non-OTLP sinks (Parquet, ClickHouse) get the same identity.

### D16 — Panic hook is opt-in but officially documented

`obs::install_panic_hook()` is opt-in (calling `install_observer` does
not implicitly install it) because some users have a richer panic
hook already (e.g. Sentry). When opted in, it captures one
`ObsPanicked` and `shutdown_blocking()` before chaining the previous
hook — so even `panic = "abort"` profiles get the event flushed.

## 11. Test strategy

- **Unit tests** in each crate for codegen, mapping, sampling logic.
- **Integration tests** in `crates/obs-core/tests/` that install
  `InMemoryObserver`, emit events, and assert envelope contents.
- **OTLP conformance** in `crates/obs-otel/tests/` against a mock OTel
  collector (`tonic` server stub) — assert wire-level OTLP shape.
- **Property tests** with `proptest` on schema codegen: round-trip
  encode/decode (including `*View<'a>` borrow), projection idempotency,
  severity clamp.
- **Bench harness** in `crates/obs-core/benches/` with `criterion`,
  measuring P50/P99 of the emit hot path; CI gates on regression.

## 12. Observability of the SDK itself

The SDK emits its own internal events under `obs.runtime.v1`:

| Event | Tier | Purpose |
| --- | --- | --- |
| `obs.runtime.v1.ObsSinkDropped` | METRIC | counter of dropped envelopes per tier/sink |
| `obs.runtime.v1.ObsConfigReloaded` | LOG | new config hash + diff summary |
| `obs.runtime.v1.ObsConfigReloadFailed` | LOG (WARN) | reason; old config retained |
| `obs.runtime.v1.ObsSchemaUnknown` | LOG | observer received a schema it does not know (dev mode) |
| `obs.runtime.v1.ObsSinkFailed` | LOG (WARN) | sink IO failure with backoff state |
| `obs.runtime.v1.ObsForensicBudgetExceeded` | LOG (WARN) | crate exceeded its forensic budget |
| `obs.runtime.v1.ObsLabelCardinalityHigh` | LOG (WARN) | HLL counter for `(full_name, label_key)` exceeded declared cap |
| `obs.runtime.v1.ObsOversizedDropped` | LOG (WARN) | event dropped because encoded payload > `limits.max_payload_bytes` |
| `obs.runtime.v1.ObsPanicked` | LOG (FATAL) | emitted by panic hook before shutdown_blocking |
| `obs.runtime.v1.ObsBridgePiiSuspected` | LOG (WARN) | tracing-bridge name-pattern PII redactor fired (one-shot per field name) |
| `obs.runtime.v1.ObsBridgeMatcherConflict` | LOG (WARN) | two `register_typed` matchers matched the same callsite |
| `obs.runtime.v1.ObsBridgeLateSpanRecord` | LOG (WARN) | `Span::record` arrived after `ObsSpanCompleted` already emitted |
| `obs.runtime.v1.ObsBridgeNoDispatcher` | LOG (DEBUG) | `ObsToTracingSink` ran without a tracing default; rate-limited 1/min |
| `obs.v1.ObsSpanCompleted` | LOG (DEBUG) | bridged `tracing::Span::close` carries name + latency + fields (see tracing-interop § 2.3) |
| `obs.v1.ObsSpanEntered` | LOG (TRACE) | bridged `tracing::Span::new_span` (only when `SpanEventMode::Both`) |
| `obs.runtime.v1.ObsCallsiteRegistered` | LOG (DEBUG) | callsite interning broadcast: `(callsite_id, target, file, line, template, field_names)` (see callsite-interning § 3.4) |
| `obs.runtime.v1.ObsCallsiteHashCollision` | LOG (WARN) | distinct callsites computed the same `callsite_id`; second falls back to verbose |
| `obs.runtime.v1.ObsCallsiteRegistryConflict` | LOG (WARN) | two processes registered the same `callsite_id` with conflicting metadata |
| `obs.runtime.v1.ObsBridgeCallsiteUnresolved` | LOG (DEBUG) | Direction B sink received an interned envelope without a matching registry entry; rate-limited |
| `obs.v1.ObsTracingInternedEvent` | LOG (INFO) | bridged `tracing::Event` body when interning mode != Off |
| `obs.v1.ObsForensicInternedEvent` | LOG (INFO) | `obs::forensic!` body when interning mode != Off |

These events flow through the same observer, so they appear in the
same sinks as user events — there is exactly one signal channel.

## 13. Callsite interning (cross-reference)

Native `obs::emit!` is already "interned" in the defmt sense: the
compile-time `schema_hash` (`u64`, the first 8 bytes of BLAKE3 over
the descriptor, build-time const) is the analogue of defmt's interned
format-string id, and the buffa-encoded payload uses tag-numbered
fields rather than field names on the wire.

The opportunity for additional wire savings is on the **bridged
tracing path** and the **forensic path**, where today's envelopes
carry repeated literal `target` / `file:line` / message-template
strings. The optional `callsite_id = 15` envelope field plus the
`ObsCallsiteRegistered` self-event implement this. Interning is
**off by default in v1** and opt-in via `obs.yaml`'s `interning:`
block.

The full design — modes (`Off`/`Hybrid`/`Compact`), registry
lifecycle, re-emit cadence, downstream consumer story, OTel
mapping, CLI tooling, and the bidirectional bridge integration —
lives in [callsite-interning-design.md](./callsite-interning-design.md).

## 14. Project-policy compliance (CLAUDE.md)

This section records how the runtime design satisfies the project
policies in `CLAUDE.md` and where it takes documented exceptions.

### 14.1 Required at every workspace crate root

```rust
#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
```

- `forbid(unsafe_code)` is non-negotiable per CLAUDE.md § Rust Safety.
  No crate in this workspace uses `unsafe` blocks. The `Box::leak`
  patterns used for `'static Metadata` synthesis (tracing-interop
  § 3.3) and for static label keys are safe Rust.
- `missing_docs` enforces doc comments on every public item, satisfied
  by the per-event `///` doc comments emitted by codegen
  (schema-codegen § 3.3).
- `missing_debug_implementations` is satisfied by `#[derive(Debug)]`
  on every public type. Sensitive types (envelopes carrying PII or
  SECRET-classified payload) get a manual `Debug` that delegates to
  the existing scrubber (see § 14.4).

### 14.2 Concurrent maps: `DashMap`, never `RwLock<HashMap>`

CLAUDE.md § Async & Concurrency mandates `DashMap` for concurrent
hash maps. All caches in the runtime follow this:

| Cache | Type | Justification |
| --- | --- | --- |
| `ObsCallsiteRegistry` (callsite-interning § 3.2) | `DashMap<CallsiteId, Arc<CallsiteRecord>>` | write-once-per-key, read-many |
| Bridge metadata cache (tracing-interop § 3.3) | `DashMap<MetadataKey, &'static Metadata>` | same pattern |
| Per-callsite filter decisions (`obs::Filter`) | `DashMap<&'static ObsCallsite, FilterDecision>` | hot read on every emit |

For *infrequently-updated* shared data (the live-reloaded
`EventsConfig`, the `ServiceIdentity`), we use `ArcSwap` per
CLAUDE.md's parallel guidance — see § 6 and § 9.

### 14.3 Async traits — documented `async-trait` exception

`Observer` and `Sink` (§ 2.1, § 2.2) declare async methods via
`Pin<Box<dyn Future + Send + '_>>` rather than native `async fn`
because both traits are used as `dyn Trait`:

```rust
static OBSERVER: ArcSwap<Box<dyn Observer>> = …;
pub struct SinkRouter { fallback: Arc<dyn Sink>, … }
```

Native `async fn` in traits (stable since Rust 1.75) is not
object-safe with arbitrary returned futures. CLAUDE.md § Async &
Concurrency calls this out as an explicit exception: "When traits
require object safety (used with `dyn Trait` for dynamic dispatch
like `Arc<dyn TaskStorage>`), use `async-trait` crate and document
the reason in module-level docs." The `Pin<Box<dyn Future>>` form
is equivalent to the `async-trait` macro expansion; we use it
directly to avoid the proc-macro dependency on the SDK's hot path.

### 14.4 Error types — `thiserror` libraries, `anyhow` apps

| Crate | Error policy |
| --- | --- |
| `obs-types`, `obs-proto`, `obs-core`, `obs-build`, `obs-otel`, `obs-parquet`, `obs-clickhouse`, `obs-tracing-bridge`, `obs-tower`, `obs-macros`, `obs-sdk` | Library crates: domain-specific `enum` errors via `#[derive(thiserror::Error)]` with `#[source]` for chained causes. Public surface returns `Result<T, ThisCrateError>`; internal helpers use `?` with `From` impls between layers. |
| `apps/obs-cli`, `apps/server` | Application crates: `anyhow::Result<T>` at command/handler boundaries, with `.context()` / `.with_context()` for diagnostic breadcrumbs. |
| `build.rs` (any crate) | Application-shaped: `anyhow::Result<()>`. |

The runtime's `Observer::emit_envelope` is `&self -> ()` — it never
returns errors because emit is the hot path and CLAUDE.md § Error
Handling allows `panic!` only in unrecoverable cases. Emit failures
are dropped with metric increment (§ 7), not surfaced.

### 14.5 Public-type stability — `#[non_exhaustive]`

Every public enum and struct exported from a library crate carries
`#[non_exhaustive]` so future variants/fields don't constitute SemVer
breaks for downstream pattern-matchers:

```rust
#[non_exhaustive]
pub enum Tier { /* ... */ }

#[non_exhaustive]
pub struct ObsEnvelope { /* ... */ }
```

The proto-generated wire types are immune (proto3 is implicitly
non-exhaustive — unknown fields preserved per buffa's
`preserve_unknown_fields` default).

### 14.6 Secret handling — `secrecy::SecretString`

Fields with `Classification::Secret` are typed as `secrecy::SecretString`
(or `secrecy::SecretBox<T>` for non-string types) at the boundary
between user code and the SDK:

- The `#[derive(Event)]` codegen emits `pub field: SecretString` for
  any `#[obs(classification = "secret")]` field.
- `SecretString::expose_secret()` is required to read the value;
  this makes accidental `Debug`/`Display` printing impossible at the
  type level. `Debug` redacts to `[REDACTED]`.
- The payload scrubber (schema-codegen § 3.1) strips SECRET fields
  before durable sinks (LOG/AUDIT) write — defence-in-depth on top
  of the type-system guarantee.
- For tracing-bridged events with no declared classification, the
  `DefaultPiiPatternRedactor` (tracing-interop § 2.6) wraps matching
  values in `SecretString` before passing them through.

### 14.7 No `unwrap` / `expect` / `panic!` on the hot path

Verified by clippy lints (`-W clippy::unwrap_used -W clippy::expect_used
-W clippy::panic`) on every emit-path module, per CLAUDE.md § Rust
Safety. Hot-path code uses panic-free constructions:

- BLAKE3 truncation reads the typed `&[u8; 32]` and does a
  byte-by-byte `from_le_bytes` (callsite-interning § 3.1) — no
  `try_into().unwrap()`.
- Channel sends use `try_send` with explicit drop-on-overflow (§ 2.5),
  not `send().await` which would block the emit thread.
- Atomic loads on the `OBSERVER` `ArcSwap` are infallible by type.

Cold-path code (init, config reload) returns `Result` and propagates
via `?`.

### 14.8 Tokio features and runtime constraints

Per CLAUDE.md § Async & Concurrency, all tokio dependencies in
`[workspace.dependencies]` specify features explicitly:

```toml
[workspace.dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time", "signal"] }
```

The `signal` feature is required for `reload_on_sighup()` (§ 6).
`current_thread` runtime is supported but not the default; the
collector workers are spawned on whichever runtime is active.

### 14.9 Configuration via the `config` crate, YAML format

`EventsConfig` (§ 6) loads from YAML via the `config` crate, layered
over env-var overrides (`OBS_*`) — matches CLAUDE.md § Async &
Concurrency: "Always consider using config crate for configuration
management. Always use yaml format". Compile-time tuning lives in
`const` (e.g., `RING_BUFFER_CAP: usize = 64`); runtime tuning lives
in YAML.

### 14.10 `Bytes`, `SmallVec`, `Cow` per CLAUDE.md § Performance

- Payloads cross the SDK as `bytes::Bytes` (refcounted, zero-copy
  slicing) rather than `Vec<u8>`.
- Label projection uses `SmallVec<[(&'static str, String); 8]>` —
  most events have ≤ 8 labels; the inline buffer eliminates heap
  allocation for the common case.
- Filter and config values that may be borrowed or owned use
  `Cow<'static, str>` to avoid clones at parse time.

### 14.11 Logging & observability of the SDK itself

CLAUDE.md § Logging & Observability says "Use `tracing` for
structured logging". The SDK's own diagnostics (config-reload
failures, sink errors) use `tracing::*` macros — *not* `obs::emit!`
— because the SDK cannot depend on its own observer being installed
correctly to report its own bootstrap failures. SDK-internal
diagnostics flow through whichever `tracing::Subscriber` the user
installed (commonly `tracing-subscriber::fmt::layer()` in dev),
independent of the observer pipeline. User-facing self-events
(§ 12) flow through the observer normally.
