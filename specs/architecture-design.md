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
  bytes   schema_hash = 2;   // BLAKE3-256 of (full_name, tier, default_sev, FIELDS[])
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

`schema_hash` is BLAKE3 over `(full_name, tier, default_sev, FIELDS[])`,
computed at build time and stored as a `[u8; 32]` constant. It lets a
downstream consumer detect schema evolution without registry lookup, and
lets the batch dedupe schema names.

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
    fn emit_envelope(&self, env: ObsEnvelope);
    fn flush(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    fn shutdown(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

static OBSERVER: ArcSwap<Box<dyn Observer>> = ...;  // default = NoopObserver

pub fn observer() -> Guard<Arc<Box<dyn Observer>>> { OBSERVER.load() }
pub fn install_observer<O: Observer>(o: O) { ... }
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
    ObsRequestCompleted::builder()...emit();

    r
    // _scope dropped: if any ERROR seen, flush full buffer; else discard.
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
the core free of OTel as a hard dependency.

### 4.1 To OTLP Logs

A wide event maps 1:1 to an OTel `LogRecord`:

| Wide event | OTLP LogRecord |
| --- | --- |
| `env.ts_ns` | `time_unix_nano` |
| `env.sev` | `severity_number` (mapped to OTel scale) + `severity_text` |
| `env.full_name` | `attributes["event.name"]` |
| `env.trace_id` / `span_id` | `trace_id` / `span_id` |
| `env.labels` | `attributes[*]` (as strings) |
| typed payload (decoded) | `body` as `KeyValueList` |

Payload decode is opt-in per sink; for high-volume LOG-tier events the
sink can ship `body = bytes(payload)` and rely on the consumer to
decode using the schema registry.

### 4.2 To OTLP Metrics

For each `FIELD_KIND_MEASUREMENT` field on a schema, a metric data
point is generated whose attribute set is the union of `env.labels`:

| Schema field | Metric instrument |
| --- | --- |
| `uint64 / int64` w/ `metric: counter` | `Sum` (monotonic, delta) |
| `uint64 / int64` w/ `metric: gauge`   | `Gauge` |
| `uint64 / int64 / double` w/ `metric: histogram` | `Histogram` (bounds in annotation) |
| no `metric:` annotation | not emitted; lives in payload only |

Because all LABEL fields are by construction `Low | Medium`
cardinality, the generated metric's attribute set is bounded at
compile time.

### 4.3 To OTLP Traces

If `env.trace_id` is non-empty:

- a `Span` is emitted with `name = full_name`,
- `start_time = end_time = env.ts_ns` (event-as-span), unless the
  schema declares a `FIELD_KIND_DURATION_NS` field, in which case
  `start_time = ts_ns - duration`,
- attributes := `env.labels`.

Spans for the same `trace_id` are tied together by the OTel exporter;
the SDK does not attempt span-tree reconstruction in-process.

### 4.4 Why we are not just an OTel SDK

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

### 5.3 The `obs::emit!` macro

```rust
// Default severity (from schema's default_sev):
obs::emit!(ObsRequestCompleted {
    route: Route::ListUsers,
    status: Status::Ok,
    tenant_id: "acme".into(),
    latency_ms: 48,
    bytes_out: 2048,
});

// Escalated severity:
obs::emit!(WARN, ObsUpstreamFailed {
    route: Route::ListUsers,
    error_kind: ErrorKind::Timeout,
});

// Builder form (preferred for many fields; works the same):
ObsRequestCompleted::builder()
    .route(Route::ListUsers)
    .status(Status::Ok)
    .latency_ms(48)
    .emit();
```

Expansion sketch:

```rust
{
    static __CALLSITE: ObsCallsite = ObsCallsite::new(
        ObsRequestCompleted::FULL_NAME,
        ObsRequestCompleted::DEFAULT_SEV,
        module_path!(),
        file!(), line!(),
    );
    if obs::observer().enabled(&__CALLSITE) {
        let evt = ObsRequestCompleted { /* fields */ };
        let mut env = obs::__private::build_envelope(&__CALLSITE, &evt);
        evt.project(&mut env);
        obs::observer().emit_envelope(env);
    }
}
```

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

`obs::scope!` accepts only fields that are LABEL-class on at least one
event schema in the program (the macro checks at compile time). This
is what makes inheritance an allowlist, not implicit context.

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
and the macro checks at compile time that each named field is LABEL-
or TRACE_ID-class on at least one event in the binary. This avoids
two failure modes:

- A high-cardinality field accidentally inherited into a metric
  attribute set.
- A PII field flowing into a label without explicit declaration.

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

### D8 — Schema hash is a build-time constant, not runtime

We BLAKE3 the descriptor at build time and store a `[u8; 32]` const.
Runtime hashing on every emit would be wasteful and would forfeit the
ability to verify schema versions in CI without running the binary.

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

These events flow through the same observer, so they appear in the
same sinks as user events — there is exactly one signal channel.
