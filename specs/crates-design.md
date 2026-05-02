# Design — Workspace Crates & Public API

Status: draft v2 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [architecture-design.md](./architecture-design.md), [schema-codegen-design.md](./schema-codegen-design.md)

> v2 changes: switched proto runtime to `buffa`; analytics sinks
> default to single-table layout; envelope/builder examples use the
> `Obs*` prefix; shorter envelope field names; added `obs-cli` and
> `obs-build` dep notes for buffa-reflect.

## 1. Workspace layout

```
obs/
├── Cargo.toml                    # [workspace] resolver = "3"
├── crates/
│   ├── obs-types/                # leaf: enums (Tier, Severity, FieldKind, ...)
│   ├── obs-proto/                # built-in obs/v1/options.proto + generated wire types
│   ├── obs-macros/               # proc-macro: #[derive(Event)], emit!, scope!, forensic!, instrument
│   ├── obs-core/                 # runtime: Observer, Sink, Collector, sampling, config
│   ├── obs-build/                # build-time codegen helpers (used in user build.rs)
│   ├── obs-otel/                 # OTLP/gRPC sinks for logs, metrics, traces
│   ├── obs-parquet/              # Single-table Arrow/Parquet sink
│   ├── obs-clickhouse/           # Single-table ClickHouse sink
│   ├── obs-tracing-bridge/       # bidirectional tracing ↔ obs (see tracing-interop-design.md)
│   ├── obs-tower/                # tower::Layer for HTTP propagation + ObsHttpRequest events
│   └── obs-sdk/                  # façade: re-exports the everyday API
└── apps/
    ├── obs-cli/                  # `obs` binary: lint, validate, codegen, query, diff
    └── server/                   # example service using obs-sdk
```

### 1.1 Dependency graph

```
                       obs-types
                          │
            ┌─────────────┼──────────────┐
            ▼             ▼              ▼
        obs-proto    obs-macros      obs-build
       (uses buffa) (proc-macro2)  (uses buffa-build,
            │             │            buffa-reflect)
            └──────┬──────┘              │
                   ▼                     │
                obs-core ◄───────────────┘
                   ▲
        ┌──────────┼──────────────────────────┐
        │          │           │              │
    obs-otel  obs-parquet  obs-clickhouse  obs-tracing-bridge
        │          │           │              │
        └──────────┴─────┬─────┴──────────────┘
                         ▼
                      obs-sdk
                         ▲
                         │
                     user crate / obs-cli
```

Rules:

- `obs-types` has zero deps beyond `buffa` (for enum encode/decode).
- `obs-core` does not depend on `obs-otel` / sinks; sinks are pluggable.
- `obs-sdk` re-exports the common subset; users typically only depend on it.
- App-level `obs-cli` may depend on every other crate.

## 2. Per-crate API

### 2.1 `obs-types`

The leaf type crate. Every other crate depends on it.

```rust
pub use crate::tier::Tier;
pub use crate::severity::Severity;
pub use crate::field_kind::FieldKind;
pub use crate::cardinality::Cardinality;
pub use crate::classification::Classification;
pub use crate::metric_kind::MetricKind;
pub use crate::sampling::SamplingReason;
```

Each enum:
- derives `Copy`, `Clone`, `Debug`, `Eq`, `Hash`,
- implements `buffa::Enumeration` so it can live in protobuf,
- exposes `const fn` helpers (`is_label_compatible`, `cap`, `as_str`).

`#![forbid(unsafe_code)]`. Vocabulary changes here cause an envelope
`format_ver` bump; that's the intended forcing function.

### 2.2 `obs-proto`

Owns the canonical `obs/v1/*.proto` files and their generated Rust
types (via `buffa-build`).

```
crates/obs-proto/
├── proto/obs/v1/
│   ├── options.proto      # MessageOptions / FieldOptions extensions
│   ├── envelope.proto     # ObsEnvelope, ObsBatch
│   ├── enums.proto        # Tier, Severity, FieldKind, Cardinality, ...
│   └── builtin.proto      # ObsForensicEvent, ObsTracingForensicEvent,
│                          # ObsFnEntered, ObsFnExited, runtime self-events
└── build.rs               # buffa-build over the above; emits FDS
```

Public API:

```rust
pub mod obs::v1 {
    pub use crate::pb::*;     // ObsEnvelope, ObsBatch, ObsForensicEvent, ...
    pub use crate::pb_view::*; // zero-copy *View<'a> types from buffa
}
```

User crates do not normally import this directly; they access the
same types via `obs::ObsEnvelope` re-export from `obs-sdk`.

### 2.3 `obs-macros`

Procedural macros only. Every macro is documented with a doc test.

```rust
/// Derive macro for Rust-first authoring.
///
/// ```ignore
/// #[derive(obs::Event)]
/// #[event(tier = "log", default_sev = "info")]
/// pub struct ObsLoggedIn {
///     #[obs(label, cardinality = "low")]    pub method: AuthMethod,
///     #[obs(attribute, classification = "pii")] pub user_id: String,
/// }
/// ```
#[proc_macro_derive(Event, attributes(event, obs))]
pub fn derive_event(item: TokenStream) -> TokenStream { ... }

/// Companion derive for enums used as LABEL fields. Emits `Display`,
/// `FromStr`, and the `obs::__private::EnumCount` impl that lint L005
/// reads. Variants render in `snake_case` by default; override per
/// variant with `#[obs(rename = "...")]`.
///
/// ```ignore
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, obs::EnumLabel)]
/// pub enum AuthMethod { Password, OAuthGoogle, OAuthGithub }
/// // Display: "password" | "oauth_google" | "oauth_github"
/// // EnumCount::COUNT == 3
/// ```
#[proc_macro_derive(EnumLabel, attributes(obs))]
pub fn derive_enum_label(item: TokenStream) -> TokenStream { ... }

/// Function-like macro for one-shot event emission.
///
/// `obs::emit!(ObsLoggedIn { method: AuthMethod::Password, user_id })`
/// `obs::emit!(WARN, ObsLoggedIn { method, user_id })`
#[proc_macro] pub fn emit(item: TokenStream) -> TokenStream { ... }

/// RAII attribute scope for trace correlation and tail buffer.
///
/// `let _g = obs::scope!(trace_id = req_id, tenant_id = tenant);`
#[proc_macro] pub fn scope(item: TokenStream) -> TokenStream { ... }

/// Forensic escape hatch.
#[proc_macro] pub fn forensic(item: TokenStream) -> TokenStream { ... }

/// Function/method instrumentation.
///
/// `#[obs::instrument(fields(route, tenant_id), skip(raw_body))]`
#[proc_macro_attribute] pub fn instrument(...) -> TokenStream { ... }
```

`obs-macros` enforces:

- compile-time cardinality lint (see schema-codegen § 3.4)
- emits `EventSchema` impl
- emits `EnumCount` impl when an enum is used as a LABEL field
- enforces `Obs*` event-name prefix (L011)
- compile-time check that fields named in `obs::scope!` are LABEL or
  TRACE_ID class on at least one schema in the binary

### 2.4 `obs-core`

The runtime. This is the biggest crate.

#### Modules

```
src/
├── lib.rs
├── observer/
│   ├── mod.rs              # `pub trait Observer`, `install_observer`, `observer()`
│   ├── noop.rs             # `NoopObserver` (default)
│   ├── standard.rs         # `StandardObserver` with sink router + per-tier workers
│   └── in_memory.rs        # test harness observer
├── envelope/
│   ├── builder.rs          # build_envelope<E: EventSchema>(...)
│   └── projection.rs       # Label projection helper
├── callsite.rs             # ObsCallsite static metadata + filter cache
├── sink/
│   ├── mod.rs              # `pub trait Sink`
│   ├── stdout.rs           # human-readable dev sink
│   ├── ndjson.rs           # NDJSON file sink
│   ├── memory.rs           # bounded ring buffer for tests
│   ├── router.rs           # SinkRouter
│   └── batch.rs            # generic Batcher used by sinks
├── sampling/
│   ├── mod.rs              # head + tail sampling
│   ├── tail_buffer.rs      # tokio::task_local ring buffer
│   └── rate.rs             # per-event rate limiter
├── scope/                  # obs::scope! support, ScopeFrame, task-local stack
├── filter/                 # EnvFilter-style DSL (Obs::Filter)
├── config/
│   ├── mod.rs              # `EventsConfig` (serde, ArcSwap)
│   ├── reload.rs           # SIGHUP / file watcher
│   └── schema.rs           # YAML schema
└── error.rs
```

#### Key public types

```rust
pub use obs_proto::obs::v1::{ObsEnvelope, ObsBatch};
pub use obs_types::*;

pub trait EventSchema: ... { ... }       // (defined here; codegen targets it)

pub trait Emit: EventSchema + Sized {
    fn emit(self) { ... }
    fn emit_at(self, sev: Severity) { ... }
}
impl<E: EventSchema + Sized> Emit for E {}

pub trait Observer: Send + Sync + 'static { ... }
pub trait Sink: Send + Sync + 'static { ... }

pub fn install_observer<O: Observer>(o: O);
pub fn observer() -> arc_swap::Guard<Arc<Box<dyn Observer>>>;

pub struct StandardObserverBuilder { ... }
pub struct EventsConfig { ... }
```

#### `StandardObserverBuilder`

```rust
let observer = StandardObserver::builder()
    .service("my-api", env!("CARGO_PKG_VERSION"))
    .instance(hostname::get()?.to_string_lossy().into_owned())
    .sink_for(Tier::Log,    NdjsonFileSink::new("/var/log/myapi.ndjson")?)
    .sink_for(Tier::Metric, otel::OtlpMetricSink::from_env()?)
    .sink_for(Tier::Trace,  otel::OtlpTraceSink::from_env()?)
    .config_from_yaml_path("/etc/myapi/obs.yaml")?
    .reload_on_sighup()
    .build()?;

obs::install_observer(observer);

// At process exit:
obs::observer().shutdown().await;
```

#### Test harness

```rust
let (observer, handle) = InMemoryObserver::new();
obs::install_observer(observer);

ObsLoggedIn::builder()
    .method(AuthMethod::Password)
    .user_id("u-1")
    .emit();

let events = handle.drain();
assert_eq!(events.len(), 1);
assert_eq!(events[0].full_name, "myapp.v1.ObsLoggedIn");
assert_eq!(events[0].labels.get("method"), Some(&"password".to_string()));
```

### 2.5 `obs-build`

Build-script helpers. Public API is one struct:

```rust
pub struct Config { ... }

impl Config {
    pub fn new() -> Self;
    pub fn files(self, files: &[impl AsRef<Path>]) -> Self;
    pub fn include(self, dir: impl AsRef<Path>) -> Self;
    pub fn out_dir(self, dir: impl AsRef<Path>) -> Self;

    pub fn include_obs_options(self) -> Self;
    pub fn extern_path(self, proto_path: &str, rust_path: &str) -> Self;

    pub fn with_arrow_schema(self, on: bool) -> Self;
    pub fn with_json_render(self, on: bool) -> Self;
    pub fn with_payload_scrub(self, on: bool) -> Self;
    pub fn with_otel_attribute_view(self, on: bool) -> Self;

    pub fn descriptor_source(self, src: DescriptorSource) -> Self;  // pass-through to buffa-build

    pub fn compile(self) -> Result<(), CodegenError>;
}

pub enum DescriptorSource { Protoc, Buf, Precompiled(PathBuf) }
```

Internally:

1. Builds a `buffa_build::Config`, mirrors codegen toggles, captures
   the `FileDescriptorSet` path.
2. Loads the FDS into `buffa_reflect::DescriptorPool`.
3. Walks messages, reads `(obs.v1.event)` and `(obs.v1.field)`
   extensions, and emits the `obs/*.rs` files described in
   [schema-codegen-design.md § 3.1](./schema-codegen-design.md#31-generated-artifacts-per-proto-file).

User crates include the output via:

```rust
obs::include_schemas!("myapp.v1");
```

The `include_schemas!` macro lives in `obs-macros`; it expands to
`include!` calls under `OUT_DIR`.

### 2.6 `obs-otel`

OpenTelemetry sinks. Depends on `opentelemetry`, `opentelemetry-otlp`,
`tonic`, `rustls` (with `aws-lc-rs` crypto backend per project
policy). The OTLP `Resource` is built once at sink construction
(`service`, `instance`, `version` from the observer plus optional
`service.namespace`, `deployment.environment`, host detection) and
reused across exports — see architecture-design § 4.1.

```rust
pub struct OtlpLogSink { ... }
pub struct OtlpMetricSink { ... }
pub struct OtlpTraceSink { ... }

pub enum OtlpProtocol { Grpc, HttpProtobuf }
pub enum OtlpCompression { None, Gzip, Zstd }

impl OtlpLogSink {
    pub fn from_env() -> Result<Self>;       // OTEL_EXPORTER_OTLP_*
    pub fn builder() -> OtlpLogSinkBuilder;
}

pub struct OtlpLogSinkBuilder { /* ... */ }
impl OtlpLogSinkBuilder {
    pub fn endpoint(self, url: impl Into<String>) -> Self;
    pub fn protocol(self, p: OtlpProtocol) -> Self;        // default Grpc
    pub fn compression(self, c: OtlpCompression) -> Self;  // default Gzip
    pub fn timeout(self, d: Duration) -> Self;             // default 10s
    pub fn header(self, k: &str, v: &str) -> Self;         // repeatable
    pub fn retry_policy(self, p: OtlpRetry) -> Self;
    pub fn resource_attr(self, k: &str, v: &str) -> Self;  // extras like deployment.environment
    pub fn detect_host(self, on: bool) -> Self;            // host.name etc.
    pub fn schema_url(self, url: &str) -> Self;            // default current semconv
    pub fn build(self) -> Result<OtlpLogSink>;
}
// Same shape for OtlpMetricSink (adds .push_interval()) and OtlpTraceSink.

impl Sink for OtlpLogSink {
    fn deliver(&self, env: &ObsEnvelope) {
        // Map per architecture-design §4.3.
        // Reuses the prebuilt Resource; never re-stamps service/instance/version
        // as per-LogRecord attributes.
    }
    fn flush(&self) -> ... { ... }
    fn shutdown(&self) -> ... { ... }
}
```

Convenience constructor wires all three with a shared Resource:

```rust
let (logs, metrics, traces) = obs_otel::otlp_trio_from_env()?;
StandardObserver::builder()
    .sink_for(Tier::Log,    logs)
    .sink_for(Tier::Metric, metrics)
    .sink_for(Tier::Trace,  traces)
    ...
```

Standard env var support: `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_PROTOCOL`, `OTEL_EXPORTER_OTLP_HEADERS`,
`OTEL_EXPORTER_OTLP_COMPRESSION`, `OTEL_EXPORTER_OTLP_TIMEOUT`,
`OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME`. The `from_env()`
constructor reads these so a 12-factor deployment needs no code.

### 2.7 `obs-parquet`

Writes batches as Parquet files using a **single Arrow schema** that
contains all event types as sparse struct columns (per
[architecture-design.md § 3](./architecture-design.md#3-storage-model--single-sparse-table)).

```rust
pub struct ParquetSink { ... }

impl ParquetSink {
    pub fn builder() -> ParquetSinkBuilder;
}

pub enum ParquetLayout {
    /// Single sparse table; all events written to obs_events.parquet
    /// with per-event-type struct columns. Default.
    Single,
    /// One file per event type. Opt-in for very-high-volume splits.
    TablePerEvent,
}

pub struct ParquetSinkBuilder {
    pub fn base_dir(self, dir: impl Into<PathBuf>) -> Self;
    pub fn layout(self, l: ParquetLayout) -> Self;          // default Single
    pub fn roll_max_bytes(self, n: u64) -> Self;
    pub fn roll_max_age(self, d: Duration) -> Self;
    pub fn compression(self, c: ParquetCompression) -> Self;
    pub fn partition_by(self, fields: &[&str]) -> Self;     // e.g. ["service", "date"]
    pub fn build(self) -> Result<ParquetSink>;
}
```

File path on disk:

```
base_dir/service=my-api/date=2026-05-02/hour=14/obs_events-{batch_id}.parquet
```

Schema discovery is automatic — the sink reads the `EventSchema`
registry populated at observer init and combines all per-event Arrow
field fragments into one table schema.

### 2.8 `obs-clickhouse`

Native ClickHouse insertion into a single `obs_events` table per
service.

```rust
pub struct ClickHouseSink { ... }

impl ClickHouseSink {
    pub fn builder() -> ClickHouseSinkBuilder;
}

pub struct ClickHouseSinkBuilder {
    pub fn url(self, url: impl Into<String>) -> Self;
    pub fn database(self, db: impl Into<String>) -> Self;
    pub fn table(self, name: impl Into<String>) -> Self;     // default "obs_events"
    pub fn auto_migrate(self, on: bool) -> Self;             // default false (CI step instead)
    pub fn batch_size(self, n: usize) -> Self;
    pub fn build(self) -> Result<ClickHouseSink>;
}
```

The CLI `obs migrate clickhouse` (see [cli-design.md](./cli-design.md))
emits the DDL for the schemas at build time so production DBs are
migrated by an explicit step, not at runtime.

DDL strategy (one table):

```sql
CREATE TABLE obs_events (
    ts_ns                            DateTime64(9),
    full_name                        LowCardinality(String),
    schema_hash                      UInt64,
    sev                              LowCardinality(String),
    trace_id                         String,
    span_id                          String,
    parent_span_id                   String,
    service                          LowCardinality(String),
    instance                         LowCardinality(String),
    version                          LowCardinality(String),
    sampling_reason                  LowCardinality(String),
    labels                           Map(LowCardinality(String), String),
    payload_myapp_v1_obs_request_completed  Nested( route LowCardinality(String),
                                                    status LowCardinality(String),
                                                    /* ... */ ),
    payload_myapp_v1_obs_user_signed_up     Nested( /* ... */ ),
    payload_proto                    String CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_ns)
ORDER BY (ts_ns, full_name, trace_id);
```

### 2.9 `obs-tracing-bridge`

Bidirectional bridge between `tracing` and `obs`. The full design,
loop-avoidance proof, performance budget, migration playbook, and
key decisions live in [tracing-interop-design.md](./tracing-interop-design.md).
This entry summarises the public API surface.

```rust
// =================================================================
// Direction A: tracing → obs
// =================================================================

/// Default form. Composes with tracing-subscriber::registry().
pub struct TracingToObsLayer { /* ... */ }

impl TracingToObsLayer {
    pub fn new() -> Self;
    pub fn with_field_promotions(self, p: FieldPromotions) -> Self;
    pub fn with_redactor(self, r: Arc<dyn Redactor>) -> Self;
    pub fn with_span_events(self, mode: SpanEventMode) -> Self;
    pub fn with_filter(self, f: tracing_subscriber::EnvFilter) -> Self;
    pub fn with_interning(self, mode: InterningMode) -> Self;  // see callsite-interning-design.md

    /// Promote a matching tracing callsite into a typed obs event
    /// instead of `ObsTracingForensicEvent`. Cached per callsite id.
    pub fn register_typed<E: EventSchema>(
        self,
        matcher: TypedMatcher,
        promote: impl Fn(&tracing::Event<'_>, &SpanCtx<'_>) -> E + Send + Sync + 'static,
    ) -> Self;
}

impl<S> tracing_subscriber::Layer<S> for TracingToObsLayer
where S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> { ... }

/// Standalone subscriber for binaries that have no other tracing layers.
pub struct TracingToObsSubscriber { /* ... */ }
impl tracing::Subscriber for TracingToObsSubscriber { ... }

/// Predicate over `tracing::Metadata` + recorded fields.
pub struct TypedMatcher { /* ... */ }
impl TypedMatcher {
    pub fn new() -> Self;
    pub fn target(self, t: &'static str) -> Self;
    pub fn target_regex(self, r: &str) -> Result<Self, regex::Error>;
    pub fn name(self, n: &'static str) -> Self;
    pub fn level_at_least(self, l: tracing::Level) -> Self;
    pub fn field(self, f: &'static str) -> Self;
}

/// Allowlist of tracing-field names that get promoted to envelope labels.
/// Per-name runtime cardinality budget enforced via HLL counter.
pub struct FieldPromotions { /* ... */ }
impl FieldPromotions {
    pub fn new() -> Self;
    pub fn promote(self, name: &'static str, cap: Cardinality) -> Self;
}

/// Span emission mode for Direction A. Default: Off.
pub enum SpanEventMode {
    Off,                 // bridge spans for context only; emit `ObsSpanCompleted` on close
    Both,                // also emit `ObsSpanEntered` on new_span
    Suppressed,          // never emit any span events; only thread context
}

/// Built-in name-pattern PII redactor + user-pluggable redaction.
pub trait Redactor: Send + Sync {
    fn redact(&self, target: &str, field: &str, value: &mut String) -> RedactAction;
}
pub enum RedactAction { Keep, Replaced, Drop }

/// Default Redactor; matches /(?i)password|secret|token|api[_-]?key|
/// authorization|cookie|ssn|credit[_-]?card|bearer/ on field names.
pub struct DefaultPiiPatternRedactor;

/// Callsite interning mode (see callsite-interning-design.md). v1 default: Off.
pub enum InterningMode { Off, Hybrid, Compact }

// =================================================================
// Direction B: obs → tracing
// =================================================================

/// Sink that synthesises tracing::Event from each ObsEnvelope.
pub struct ObsToTracingSink { /* ... */ }

impl ObsToTracingSink {
    pub fn new() -> Self;
    pub fn with_payload_decode(self, m: PayloadDecodeMode) -> Self;
    pub fn with_span_emission(self, m: SpanEmissionMode) -> Self;
}

impl Sink for ObsToTracingSink { /* loop-guarded; cached metadata */ }

pub enum PayloadDecodeMode {
    Off,                              // default — labels only
    DecodeKnown,                      // dev — every payload field as tracing field
    DecodeKnownAttributesOnly,        // dev — ATTRIBUTE-class fields only
}

pub enum SpanEmissionMode {
    Off,                              // default — no tracing span per obs::scope!
    OnScope,                          // open ephemeral tracing span on obs::scope! enter
}

// =================================================================
// Typical wiring (both directions; loop-break and span correlation
// per tracing-interop-design.md § 4)
// =================================================================

fn init() -> anyhow::Result<()> {
    // 1. obs first; ObsToTracingSink fans events back out into tracing.
    let observer = StandardObserver::builder()
        .service("my-api", env!("CARGO_PKG_VERSION"))
        .sink(obs_tracing_bridge::ObsToTracingSink::new())
        .sink_for(Tier::Log, otel::OtlpLogSink::from_env()?)
        .build()?;
    obs::install_observer(observer);
    obs::install_panic_hook();

    // 2. tracing next; bridge layer lifts every tracing event into obs.
    tracing_log::LogTracer::init()?;     // optional: lift `log` crate too
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())          // dev pretty
        .with(obs_tracing_bridge::TracingToObsLayer::new()
                .with_field_promotions(
                    obs_tracing_bridge::FieldPromotions::new()
                        .promote("tenant_id", Cardinality::Medium)
                        .promote("route",     Cardinality::Medium)))
        .init();
    Ok(())
}
```

### 2.10 `obs-tower`

A `tower::Layer` companion for HTTP services. Ships:

- `ObsHttpLayer` — extracts W3C `traceparent`/`tracestate` from
  inbound headers, opens an `obs::scope!`, emits
  `ObsHttpRequestStarted` and `ObsHttpRequestCompleted` (both built-in
  in `obs-proto`), and propagates the context to outbound requests
  via the matching `ObsHttpClientLayer`.
- Works with `axum`, `hyper`, `tonic`, anything tower-compatible.

```rust
let app = axum::Router::new()
    .route("/api/users", get(list_users))
    .layer(obs_tower::ObsHttpLayer::server()
        .with_route_extractor(|req| route_for(req)));
```

`ObsHttpRequestStarted`/`Completed` schemas are LOG-tier with LABEL
fields `route`, `method`, `status_class`, MEASUREMENT `latency_ms`
and `bytes_out`. Override the route extractor for framework-specific
routing.

### 2.11 `obs-sdk`

The single dependency a typical app pulls in.

```toml
[dependencies]
obs-sdk = { version = "0.1", features = ["otel", "parquet"] }
```

```rust
// Re-exports from each crate, organized so users rarely look up paths.
pub use obs_macros::{Event, emit, scope, forensic, instrument, include_schemas};
pub use obs_core::{
    Emit, EventSchema, ObsEnvelope, ObsBatch,
    Observer, Sink, install_observer, observer,
    StandardObserver, InMemoryObserver, EventsConfig,
    StdoutSink, NdjsonFileSink, NoopSink, InMemorySink,
};
pub use obs_types::{
    Tier, Severity, FieldKind, Cardinality, Classification, MetricKind, SamplingReason,
};
#[cfg(feature = "otel")]      pub use obs_otel as otel;
#[cfg(feature = "parquet")]   pub use obs_parquet as parquet;
#[cfg(feature = "clickhouse")] pub use obs_clickhouse as clickhouse;
#[cfg(feature = "tracing-bridge")] pub use obs_tracing_bridge as tracing_bridge;
```

#### Features

| Feature | Pulls in | Default? | Purpose |
| --- | --- | --- | --- |
| `dev` | `StdoutSink` pretty renderer | **yes** | Local development output |
| `otel` | `obs-otel` (OTLP gRPC + HTTP) | **yes** | Production export to any OTLP backend |
| `panic-hook` | `obs::install_panic_hook` | **yes** | Capture `ObsPanicked` before tear-down |
| `parquet` | `obs-parquet` | no | Single-table analytics on local/object store |
| `clickhouse` | `obs-clickhouse` | no | Single-table analytics on ClickHouse |
| `tracing-bridge` | `obs-tracing-bridge` | no | Bidirectional `tracing` interop |
| `tower` | `obs-tower` | no | HTTP middleware layer |
| `test` | `InMemoryObserver`, `assert_emitted!`, `#[obs::test]` | no | Test ergonomics; cfg-only crate, free at release |

Each downstream feature gates its sink and re-exports. `dev`, `otel`,
and `panic-hook` are default because they map to "what most services
want". A library crate that wants minimum deps overrides
`default-features = false`.

## 3. End-to-end usage example

A user crate `myapp`:

```toml
# myapp/Cargo.toml
[dependencies]
obs-sdk = { version = "0.1", features = ["otel", "parquet"] }
typed-builder = "0.20"

[build-dependencies]
obs-build = "0.1"

[package.metadata.obs]
schema-source = "proto"
proto-root    = "proto"
forensic_max  = 5
```

```rust
// myapp/build.rs
fn main() -> anyhow::Result<()> {
    // build.rs is application-shaped, not library-shaped, so anyhow per
    // CLAUDE.md § Error Handling. obs_build::Config::compile returns
    // a thiserror enum (CodegenError) which `?`-propagates cleanly.
    obs_build::Config::new()
        .files(&["proto/myapp/v1/events.proto"])
        .include("proto")
        .include_obs_options()
        .out_dir(std::env::var("OUT_DIR")?)
        .compile()?;
    Ok(())
}
```

```rust
// myapp/src/lib.rs
use obs_sdk::*;

include_schemas!("myapp.v1");
pub use generated::*;
```

```rust
// myapp/src/main.rs
use myapp::*;
use obs_sdk::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let observer = StandardObserver::builder()
        .service("myapp", env!("CARGO_PKG_VERSION"))
        .instance(hostname::get()?.to_string_lossy().into_owned())
        .sink_for(Tier::Log,    NdjsonFileSink::new("./events.ndjson")?)
        .sink_for(Tier::Metric, otel::OtlpMetricSink::from_env()?)
        .sink_for(Tier::Trace,  otel::OtlpTraceSink::from_env()?)
        .config_from_yaml_path("./obs.yaml")?
        .reload_on_sighup()
        .build()?;
    install_observer(observer);

    let req_id = uuid::Uuid::new_v4().to_string();
    let _scope = scope!(trace_id = req_id, tenant_id = "acme".to_string());

    ObsRequestStarted::builder()
        .route(Route::ListUsers)
        .emit();
    // trace_id auto-filled from scope; tenant_id auto-filled from scope.

    let started = std::time::Instant::now();
    let result = handle_request().await;
    let elapsed_ms = started.elapsed().as_millis() as u64;

    ObsRequestCompleted::builder()
        .route(Route::ListUsers)
        .status(if result.is_ok() { Status::Ok } else { Status::ServerError })
        .latency_ms(elapsed_ms)
        .bytes_out(result.as_ref().map(|r| r.bytes).unwrap_or(0))
        .emit();

    observer().shutdown().await;
    Ok(())
}
```

The key ergonomic property: this code is roughly the same length as
equivalent `tracing` + `metrics` + manual OTel span code, every label
is type-checked, every dimension is bounded, **trace_id is threaded
once at the scope boundary**, and one emit produces all three signals
plus a row in `obs_events`.

## 4. Versioning policy

- All `obs-*` crates version in lock-step via `[workspace.package].version`.
- Breaking changes to `obs-types` enums are minor only between `0.x`
  versions and require a major bump in `1.x+`.
- The envelope `format_ver` field on `ObsBatch` is bumped any time the
  wire shape changes.
- The CLI ships an `obs version --schema` subcommand that prints the
  supported envelope formats for consumer compatibility checks.
- Buffa upstream pins live in workspace deps; we do not float across
  buffa minor releases without an explicit upgrade PR + integration
  test pass.
