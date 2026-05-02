# Design — `tracing` ↔ `obs` Bridge

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [11-runtime-core.md](./11-runtime-core.md), [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md), [20-otel-and-sinks.md](./20-otel-and-sinks.md)

> v3 changes: cross-references retargeted to the post-split spec
> structure; previously hand-waved `SpanCtx<'a>` and `FieldCapture`
> now defined in [12-schema-and-codegen.md § 3.6](./12-schema-and-codegen.md#36-auxiliary-trait-surface);
> bridge metadata cache uses the same atomic-Interest model as native
> emit ([11-runtime-core.md § 2](./11-runtime-core.md#2-the-obscallsite-and-atomic-interest-cache)).

This spec defines the bidirectional bridge between the `tracing`
ecosystem and `obs`. It is the canonical design for the
`obs-tracing-bridge` crate.

## 1. Motivation

`tracing` is the de facto Rust diagnostics ecosystem. Two facts shape
this design:

1. **Most third-party libraries emit through `tracing`.** axum,
   hyper, sqlx, tower-http, reqwest, deadpool, tokio's own
   instrumentation — all of them log via `tracing::event!`. A
   service that wants those signals in its observability store needs
   them lifted into `obs`.
2. **Many developer-facing tools subscribe to `tracing`.** Pretty
   formatters (`tracing-subscriber::fmt`), OTel integration
   (`tracing-opentelemetry`), IDE log viewers, the `console-subscriber`
   for tokio runtime introspection. A service that wants its `obs`
   events to show up in `cargo run` output needs them dispatched into
   `tracing`.

Bidirectional interop is therefore not a polish item. Without it, the
SDK either (a) ignores third-party diagnostics or (b) is invisible to
the surrounding ecosystem. Both are unacceptable.

The bridge has two halves:

- **Direction A**: `tracing` → `obs` via a `tracing-subscriber::Layer`
  (default) and an opt-in standalone `Subscriber`.
- **Direction B**: `obs` → `tracing` via an `obs::Sink` that
  synthesises `tracing::Event`s and dispatches them.

Both halves can be installed simultaneously; § 4 specifies the
loop-break.

## 2. Direction A — `tracing` → `obs`

### 2.1 `TracingToObsLayer` (default) and `TracingToObsSubscriber` (opt-in)

We ship **both**. The `Layer` is what 99 % of users want; the
`Subscriber` exists for binaries that have no other tracing layers
and want a single root.

```rust
pub struct TracingToObsLayer { /* ... */ }

impl TracingToObsLayer {
    pub fn new() -> Self;
    pub fn with_field_promotions(self, promotions: FieldPromotions) -> Self;
    pub fn with_redactor(self, r: Arc<dyn Redactor>) -> Self;
    pub fn register_typed<E: EventSchema>(
        self,
        matcher: TypedMatcher,
        promote: impl Fn(&tracing::Event<'_>, &SpanCtx<'_>) -> E + Send + Sync + 'static,
    ) -> Self;
    pub fn with_span_events(self, mode: SpanEventMode) -> Self;
}

impl<S> tracing_subscriber::Layer<S> for TracingToObsLayer
where S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> {
    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>);
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: ...);
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, ctx: ...);
    fn on_close(&self, id: tracing::span::Id, ctx: ...);
    fn enabled(&self, metadata: &tracing::Metadata<'_>, ctx: ...) -> bool;
}
```

Why Layer is the default:

- Composes with `tracing-subscriber::registry()` so existing
  `EnvFilter`, `fmt::layer()`, `tracing-opentelemetry`, and
  `console-subscriber` continue to work.
- Per-span state lives in the registry's extension storage —
  the canonical place — instead of a parallel `DashMap`.

The Subscriber form (`TracingToObsSubscriber::new()`) takes over the
`tracing::dispatcher::set_default` slot and serves the same translation
without needing `tracing-subscriber`. It's a one-liner for binaries
that don't want any of the registry composition machinery.

#### Init patterns

```rust
// LAYER (recommended) — composes with everything else:
tracing_subscriber::registry()
    .with(EnvFilter::from_default_env())
    .with(tracing_subscriber::fmt::layer())          // dev pretty output
    .with(obs_tracing_bridge::TracingToObsLayer::new())
    .init();

// SUBSCRIBER (advanced) — root, no other tracing layers:
let sub = obs_tracing_bridge::TracingToObsSubscriber::new()
    .with_filter("info,myapp=debug".parse()?);
tracing::subscriber::set_global_default(sub)?;
```

### 2.2 Mapping `tracing::Event` → `ObsEnvelope`

By default, every `tracing::Event` becomes one
`obs.v1.ObsTracingForensicEvent`. The conversion is mechanical:

| `tracing` source | `ObsEnvelope` / payload |
| --- | --- |
| `metadata.level()` | `env.sev` per § 2.2.1 |
| `metadata.target()` | `payload.target` (LABEL, MEDIUM) |
| `metadata.name()` | `payload.callsite_name` (ATTRIBUTE) — usually `event src/foo.rs:42`, not user-meaningful |
| `metadata.module_path()` | `payload.module` (LABEL, MEDIUM) |
| `metadata.file()` + `line()` | `payload.source_loc` (ATTRIBUTE), only in dev mode |
| field `message` (`%message` / format-string) | `payload.message` (ATTRIBUTE) |
| other fields | `payload.attrs: map<string, string>` |
| span ancestor `Span::name`s, oldest first | `payload.span_path: string` (e.g. `request:auth:db_query`) |
| ancestor span fields with promotable names (§ 2.4) | lifted to `env.labels` |
| `env.full_name` | `obs.v1.ObsTracingForensicEvent` (default) or auto-typed schema's `FULL_NAME` (§ 2.5) |
| `env.tier` | `Tier::Log` (forensic always; auto-typed events use their schema's tier) |
| `env.ts_ns` | `Instant::now()` at `on_event` |
| `env.trace_id` / `env.span_id` / `env.parent_span_id` | from active `obs::scope!` frame; if none, from current `tracing::Span::current()` (§ 2.3) |
| `env.sampling_reason` | `SamplingReason::HeadRate` |

#### 2.2.1 `Level` → `Severity`

| `tracing::Level` | `obs::Severity` | OTLP `SeverityNumber` (per arch § 4.2) |
| --- | --- | --- |
| `TRACE` | `Trace` | 1 |
| `DEBUG` | `Debug` | 5 |
| `INFO`  | `Info`  | 9 |
| `WARN`  | `Warn`  | 13 |
| `ERROR` | `Error` | 17 |
| (none) | `Fatal` | 21 — `obs` synthesises only via `install_panic_hook`; tracing has no FATAL |

### 2.3 Span mapping

`tracing::Span` is a half-open lifetime: opened by `new_span`, may be
entered and exited multiple times, may have fields recorded between
enter and close, and finally closed. The bridge collapses this into
`obs`'s scope/event model:

| Mode | Behaviour |
| --- | --- |
| `SpanEventMode::Off` (**default**) | Span is **silent on open** but used for context (trace_id propagation). On `on_close`, emit one `ObsSpanCompleted` carrying span name, latency, and the union of `Span::record`'d fields. |
| `SpanEventMode::Both` | Emit `ObsSpanEntered` on `on_new_span`, `ObsSpanCompleted` on `on_close`. Used in dev mode where every span boundary is interesting. |
| `SpanEventMode::Suppressed` | Never emit span events; only thread context for child events. |

`ObsSpanCompleted` and `ObsSpanEntered` are built-in events shipped in
`obs-proto`. `ObsSpanCompleted` has fields:

```proto
message ObsSpanCompleted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_DEBUG };
  string  name        = 1 [(obs.v1.field) = { kind: LABEL, cardinality: MEDIUM }];
  string  target      = 2 [(obs.v1.field) = { kind: LABEL, cardinality: MEDIUM }];
  uint64  latency_ns  = 3 [(obs.v1.field) = { kind: MEASUREMENT,
                                              metric: { kind: HISTOGRAM, unit: "ns" } }];
  string  trace_id    = 4 [(obs.v1.field) = { kind: TRACE_ID }];
  string  span_id     = 5 [(obs.v1.field) = { kind: SPAN_ID }];
  string  parent_span_id = 6 [(obs.v1.field) = { kind: PARENT_SPAN_ID }];
  map<string, string> fields = 7 [(obs.v1.field) = { kind: ATTRIBUTE }];
}
```

Per-span state (start `Instant`, accumulated fields) lives in the
registry's `extensions::insert/get_mut` so multiple Layers can
coexist without storing parallel DashMaps.

#### Cross-system span correlation

Both subsystems ultimately need ONE `(trace_id, span_id)` per logical
operation per task. The rule:

- When `TracingToObsLayer::on_new_span` fires AND no `obs::scope!`
  frame is active in the current task, the bridge opens an implicit
  obs scope that uses the tracing span's id as `span_id`. The
  `trace_id` is sourced in this priority order:
  1. From `tracing-opentelemetry`'s `OtelData` extension if present
     (mature OTel-aware spans).
  2. From `Span::field("trace_id")` if recorded.
  3. From the parent span's trace_id if any.
  4. Otherwise generated as a 16-byte BLAKE3 of `(span_id, ts_ns)`.
- When `obs::scope!(trace_id = ..., span_id = ...)` is active AND a
  `tracing::span!` opens inside, the bridge **does not** create a new
  obs scope; the tracing span inherits the obs scope's ids. The
  bridge stamps the tracing span's extension storage with the obs
  scope handle so `on_close` can find it.
- When both are entered concurrently in the same task, the **first**
  one to push wins; the second one observes the existing context and
  attaches to it.

The user-visible contract: trace correlation Just Works regardless of
which subsystem opens the scope first.

### 2.4 Field promotion to labels

By default, all bridged fields land in `payload.attrs` (no label
explosion risk because attrs are payload-only). A configurable
allowlist promotes named fields to `env.labels` so OTel metric
exporters and the analytical store can group by them:

```rust
let promotions = FieldPromotions::new()
    .promote("tenant_id",  Cardinality::Medium)
    .promote("route",      Cardinality::Medium)
    .promote("status",     Cardinality::Low)
    .promote("error_kind", Cardinality::Low);

TracingToObsLayer::new().with_field_promotions(promotions);
```

The promotion enforces a runtime cardinality budget per
`(tracing_target, field_name)` pair using a streaming HLL counter.
If a promoted field exceeds its declared cap (e.g. someone shoves
`user_id` into `tenant_id`), the bridge:

1. Stops promoting that field for the rest of the process lifetime
   (it falls back to attrs).
2. Emits one `obs.runtime.v1.ObsLabelCardinalityHigh` warning event.

This is the runtime equivalent of the compile-time cardinality lints
on first-class `obs` events.

### 2.5 Auto-typing — promoting tracing events to typed `Obs*` events

When the user knows that a particular tracing callsite (e.g.
`tower_http::trace::on_response`) carries a stable shape, they can
register a promoter so the bridge produces a typed `ObsXxx` instead
of `ObsTracingForensicEvent`:

```rust
pub struct TypedMatcher {
    target_eq:    Option<&'static str>,
    target_re:    Option<regex::Regex>,
    name_eq:      Option<&'static str>,
    level_min:    Option<tracing::Level>,
    require_fields: Vec<&'static str>,
}

impl TypedMatcher {
    pub fn new() -> Self;
    pub fn target(self, t: &'static str) -> Self;
    pub fn target_regex(self, r: &str) -> Result<Self, regex::Error>;
    pub fn name(self, n: &'static str) -> Self;
    pub fn level_at_least(self, l: tracing::Level) -> Self;
    pub fn field(self, f: &'static str) -> Self;
}
```

Three concrete examples:

```rust
// 1. tower-http access logs → ObsHttpRequestCompleted
let layer = TracingToObsLayer::new()
    .register_typed::<ObsHttpRequestCompleted>(
        TypedMatcher::new()
            .target("tower_http::trace::on_response")
            .field("status")
            .field("latency"),
        |event, ctx| {
            let mut cap = FieldCapture::default();
            event.record(&mut cap);
            ObsHttpRequestCompleted::builder()
                .route(ctx.label("http.route").unwrap_or("unknown".into()))
                .method(ctx.label("http.method").and_then(parse_method).unwrap_or(Method::Other))
                .status_class(cap.u64("status").map(status_class).unwrap_or(StatusClass::Other))
                .latency_ms(cap.duration("latency").map(|d| d.as_millis() as u64).unwrap_or(0))
                .build()
        },
    );

// 2. Anything at ERROR with an `error` field → ObsErrorReported
let layer = layer.register_typed::<ObsErrorReported>(
    TypedMatcher::new().level_at_least(Level::ERROR).field("error"),
    |event, _| {
        let mut cap = FieldCapture::default();
        event.record(&mut cap);
        ObsErrorReported::builder()
            .source(event.metadata().target())
            .message(cap.string("error")
                .or_else(|| cap.string("message"))
                .unwrap_or_default())
            .build()
    },
);

// 3. sqlx queries → ObsDbQueryExecuted
let layer = layer.register_typed::<ObsDbQueryExecuted>(
    TypedMatcher::new().target("sqlx::query"),
    |event, _| {
        let mut cap = FieldCapture::default();
        event.record(&mut cap);
        ObsDbQueryExecuted::builder()
            .driver(Driver::Postgres)
            .rows(cap.u64("rows_affected").unwrap_or(0))
            .elapsed_ms(cap.f64("elapsed_secs").map(|s| (s * 1000.0) as u64).unwrap_or(0))
            .build()
    },
);
```

Matcher dispatch is hot-path: it's checked against `Metadata`, which
is a `'static` pointer. The bridge keeps a
`HashMap<callsite::Identifier, ArcedPromoter>` populated **on first
sight** (lazy initialisation), so subsequent events from the same
callsite are an O(1) lookup.

If multiple matchers match the same callsite, the **first registered
wins** and a one-shot warning is logged.

### 2.6 PII / classification

Bridged events carry no declared classification — we don't know what
third-party libraries put in their fields. The bridge applies three
layers of defence:

1. **Default classification = `Internal`**, kind = `Attribute`. So
   bridged values never become metric labels and never auto-promote
   to LOG-tier-only fields by accident.
2. **Built-in name-pattern redactor** (default-on, can be disabled):
   field names matching `(?i)password|secret|token|api[_-]?key|authorization|cookie|ssn|credit[_-]?card|bearer` are replaced with the literal `"[REDACTED:bridge_pattern]"` before being placed into `payload.attrs`. The bridge emits one `obs.runtime.v1.ObsBridgePiiSuspected` event the first time each unique field name is redacted, so operators can audit.
3. **Configurable redactor**: `with_redactor(Arc<dyn Redactor>)`
   gives full programmatic control:

   ```rust
   trait Redactor: Send + Sync {
       fn redact(&self, target: &str, field: &str, value: &mut String) -> RedactAction;
   }
   enum RedactAction { Keep, Replaced, Drop }
   ```

For the auto-typed path (§ 2.5), the typed schema's classification
machinery applies normally — if the promoter routes a `password`
field into a `Classification::Secret` field of a typed event, the
existing payload scrubber (schema-codegen § 3.1) strips it before
the durable sink writes it.

### 2.7 Filter integration

`RUST_LOG` controls the bridge — i.e. the standard
`tracing-subscriber::EnvFilter` set on the registry gates events at
source. Reasoning:

- Filter at source is what every `tracing` user already knows.
- Events not passing `EnvFilter` never reach `Layer::on_event`, so
  the bridge is genuinely free for filtered-out targets.
- A separate filter would be confusing — "I set RUST_LOG=info but
  obs is showing TRACE events from this crate".

If the user wants tighter filtering on the bridge specifically,
`TracingToObsLayer::new().with_filter("warn,my_noisy_crate=off")` adds
a per-layer `EnvFilter` *after* the registry's. `OBS_FILTER` does not
apply to the bridge directly; it filters native `obs::emit!` calls
elsewhere in the binary.

End-to-end:

```rust
// RUST_LOG=info,sqlx=warn cargo run
tracing_subscriber::registry()
    .with(EnvFilter::from_default_env())          // gates RUST_LOG
    .with(tracing_subscriber::fmt::layer())       // dev formatter
    .with(TracingToObsLayer::new()                // bridge
            .with_field_promotions(...)
            .register_typed::<ObsHttpRequestCompleted>(...))
    .init();

obs::install_observer(StandardObserver::builder()
    .service("api", env!("CARGO_PKG_VERSION"))
    .config_from_yaml_path("./obs.yaml")?         // OBS_FILTER lives here
    .sink_for(Tier::Log, OtlpLogSink::from_env()?)
    .build()?);
```

### 2.8 `tracing-log` interaction

`log::info!(...)` → `tracing-log::LogTracer::init()` → `tracing::Event`
→ registry → `TracingToObsLayer` → `obs`. No special handling
required. The bridge documentation calls out the one-line
`LogTracer::init()` requirement so users don't lose `log`-using
crates' output silently.

```rust
tracing_log::LogTracer::init()?;            // log → tracing
tracing_subscriber::registry()
    .with(TracingToObsLayer::new())
    .init();
// Now `log::info!()` from any dep flows into obs as ObsTracingForensicEvent.
```

### 2.9 Callsite interning (opt-in, off by default)

The bridge is the highest-leverage place in the SDK to apply
`defmt`-style callsite interning: the per-event `target` /
`module_path` / `file:line` / message-template strings are
literally repeated on every emission of the same call site.
Interning collapses ~280 B per event to ~30 B at the cost of a
downstream registry lookup.

```rust
TracingToObsLayer::new()
    .with_interning(InterningMode::Hybrid)        // off | hybrid | compact
    .with_field_promotions(...)
    .register_typed::<ObsHttpRequestCompleted>(...);
```

When `with_interning != Off`:

- On first sight of a `tracing::callsite::Identifier`, the bridge
  computes a stable `callsite_id = BLAKE3(target, file, line, level,
  field_names)` (truncated to 64 bits), inserts into the
  `ObsCallsiteRegistry`, and synchronously emits one
  `obs.runtime.v1.ObsCallsiteRegistered` envelope.
- The data envelope's `env.callsite_id` carries that id. The
  payload becomes either `ObsTracingInternedEvent` (Hybrid mode —
  retains the rendered message + dynamic args) or a compact
  args-only payload (Compact mode).
- Subsequent emissions from the same callsite skip registration;
  one DashMap lookup determines the `callsite_id`.
- Re-emit cadence (default 10 min / 10 k events) refreshes
  `ObsCallsiteRegistered` so late or rebatched downstreams catch
  up.

Auto-typed promotions (§ 2.5) DO NOT interact with interning —
they already have a stable `schema_hash` carrying the same
information. Interning kicks in only on the forensic / non-typed
path.

The full design — modes, lifecycle, wire-size analysis, CLI
tooling, hash-collision handling — lives in
[31-callsite-interning.md](./31-callsite-interning.md).

## 3. Direction B — `obs` → `tracing`

### 3.1 Why this matters

A user installs `obs` and a third-party library installed `tracing-subscriber::fmt::layer()` for development output. Without
Direction B:

- Their `obs::emit!` calls are invisible in `cargo run` stdout.
- `tracing-opentelemetry` doesn't see span context from `obs::scope!`.
- IDE log panels (e.g. RustRover, VS Code rust-analyzer) don't
  surface obs events.
- Debugging features like `tracing-tree` and `tokio-console` (which
  hook on tracing events) miss obs activity entirely.

The bridge in this direction is `ObsToTracingSink`: a normal `obs::Sink`
registered on the `StandardObserver` like any other.

### 3.2 The `ObsToTracingSink`

```rust
pub struct ObsToTracingSink {
    // Per project CLAUDE.md: prefer DashMap over RwLock<HashMap> for concurrent
    // maps. Cache is write-once-per-key on cold path, read-many on hot path.
    cache: DashMap<MetadataKey, &'static tracing::Metadata<'static>>,
    payload_decode: PayloadDecodeMode,
    span_emission: SpanEmissionMode,
}

/// Cache key — `&'static str` for non-interned envelopes (keyed by `full_name`),
/// `u64` for interned envelopes (keyed by `callsite_id`).
enum MetadataKey { ByFullName(&'static str), ByCallsiteId(u64) }

impl ObsToTracingSink {
    pub fn new() -> Self;                                   // sensible defaults
    pub fn with_payload_decode(self, m: PayloadDecodeMode) -> Self;
    pub fn with_span_emission(self, m: SpanEmissionMode) -> Self;
}

pub enum PayloadDecodeMode {
    Off,                       // default — no decode; envelope.labels only
    DecodeKnown,               // dev — decode + dispatch every payload field as tracing field
    DecodeKnownAttributesOnly, // dev — decode + dispatch ATTRIBUTE-class fields
}

pub enum SpanEmissionMode {
    Off,                       // default — never open tracing spans on obs's behalf
    OnScope,                   // open a tracing span when obs::scope! enters; close on drop
}

impl Sink for ObsToTracingSink {
    fn deliver(&self, env: &ObsEnvelope) {
        if loop_guard::IN_TRACING_BRIDGE.get() { return; }    // see § 4.1
        loop_guard::IN_OBS_BRIDGE.set(true);

        let meta = self.metadata_for(env);                    // cached or synthesised
        tracing::dispatcher::get_default(|d| {
            if !d.enabled(meta) { return; }
            let valueset = self.build_valueset(meta, env);
            let event = tracing::Event::new(meta, &valueset);
            d.event(&event);
        });

        loop_guard::IN_OBS_BRIDGE.set(false);
    }
}
```

When no tracing dispatcher is installed, `get_default` invokes the
`Dispatch::none()` no-op — so `ObsToTracingSink` is free at zero
configuration. We don't add `tracing` as a runtime dependency; it's
already installed by anyone who pulls `obs-tracing-bridge`.

### 3.3 Synthesising `Metadata`

`tracing::Event::new` requires `&'static Metadata`. We can't get a
literal static for an unknown event type, so we **leak** one per
distinct `(full_name, sev)`:

```rust
fn metadata_for(&self, env: &ObsEnvelope) -> &'static tracing::Metadata<'static> {
    let key = if env.callsite_id != 0 {
        MetadataKey::ByCallsiteId(env.callsite_id)
    } else {
        MetadataKey::ByFullName(intern_static_str(&env.full_name))
    };
    // DashMap's `entry().or_insert_with` is atomic insert-if-absent.
    // The closure runs at most once per key (per shard write-lock); on the
    // hot path subsequent reads take only a shard read-lock.
    *self.cache.entry(key)
        .or_insert_with(|| synthesize_metadata(env))
}

fn synthesize_metadata(env: &ObsEnvelope) -> &'static tracing::Metadata<'static> {
    // Box::leak gives 'static; bounded by total distinct event types in
    // the binary (typically <1k). See § 8 for callsite-leak budget.
    let full_name: &'static str = intern_static_str(&env.full_name);
    let fields: &'static [&'static str] = Box::leak(
        env.labels.keys()
            .chain(["obs.trace_id", "obs.span_id", "obs.full_name", "message"].iter().copied())
            .map(|s| intern_static_str(s) as &'static str)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    );
    let callsite: &'static dyn tracing_core::Callsite =
        Box::leak(Box::new(BridgeCallsite::new(full_name, severity_to_level(env.sev))));
    tracing_core::callsite::register(callsite);
    Box::leak(Box::new(tracing::Metadata::new(
        full_name,                    // name
        "obs.bridge",                 // target — reserved (see § 4.1)
        severity_to_level(env.sev),
        None,                         // file
        None,                         // line
        None,                         // module path
        tracing_core::field::FieldSet::new(fields, callsite.identifier()),
        tracing_core::Kind::EVENT,
    )))
}
```

Cold-path nuance: `DashMap::entry().or_insert_with(...)` holds the
shard's write-lock for the duration of the closure. On the cold path
that closure does several `Box::leak`s and a `tracing_core::callsite::register`.
For our access pattern (cold writes, ~10³ first-sights total per
process, all in startup) this is fine. If a future workload generates
sustained high-rate cold inserts, switch to a get-or-compute pattern
that races outside the lock.

Field set membership is taken from `env.labels.keys()` plus a small
fixed prefix. The first-time payload structure determines the
permanent FieldSet for that schema; later events with the same
`full_name` use the cached metadata. Since `env.labels` for a given
schema always carries the same keys (codegen-derived), this is
stable.

### 3.4 Field mapping

| `ObsEnvelope` source | `tracing::Event` field |
| --- | --- |
| `env.full_name` | `target = "obs.bridge"` (loop guard) and field `obs.full_name` |
| `env.sev` | `Level` per § 2.2.1 (Fatal collapses to ERROR — tracing has no FATAL) |
| `env.trace_id` | field `obs.trace_id` (hex) |
| `env.span_id` | field `obs.span_id` (hex) |
| `env.parent_span_id` | field `obs.parent_span_id` (hex) |
| `env.labels[k]` | tracing field `k` (string) |
| `env.payload` | not dispatched unless `PayloadDecodeMode != Off` |

`tracing-opentelemetry` reads OTel context from the active tracing
span, not from event fields. To make obs's `trace_id` flow into OTel
spans, use `SpanEmissionMode::OnScope` (§ 3.5).

### 3.5 Span emission mode

The tricky case. `obs::emit!` is a point event; `tracing` spans
have a lifetime. If we materialise a tracing span per emit we leak
state and break formatters.

Two modes:

- **`SpanEmissionMode::Off` (default)**: Never open a tracing span
  from `ObsToTracingSink`. Events are emitted at root level (or
  under whatever tracing span was already current — typically the
  one opened by Direction A bridging some upstream tracing layer).
  `obs.trace_id`/`obs.span_id` are dispatched as fields so that
  bespoke layers can correlate.

- **`SpanEmissionMode::OnScope`**: When `obs::scope!` enters and the
  scope frame has no associated tracing span yet (i.e. the scope
  was opened by user code, not bridged from an upstream tracing
  span), the bridge opens a `tracing::span!(Level::INFO, "obs_scope",
  trace_id = …, …)` and stores the span guard in the scope frame's
  extension slot. When the obs scope drops, the tracing span is
  dropped too. Now `tracing-opentelemetry` sees a tracing span with
  the right context for every obs scope, and obs events emitted
  inside the scope dispatch under that span.

Span emission is opt-in because:

- Many users set up `obs::scope!` per request, and creating an
  ephemeral tracing span per request through the bridge would
  double the work of `tracing-opentelemetry`'s own span machinery.
- Users with `tracing-opentelemetry` typically already have
  `#[tracing::instrument]` on handlers; the bridge lets the existing
  tracing span propagate into obs (Direction A § 2.3) instead.

### 3.6 `#[obs::instrument]` vs `#[tracing::instrument]`

Both can decorate the same function. The expansion order is:

1. `#[tracing::instrument]` opens a tracing span around the body.
2. Direction A bridge sees `on_new_span` and pushes an obs scope
   inheriting the tracing span's `(trace_id, span_id)`.
3. `#[obs::instrument]` opens an obs scope around the body.
4. The obs scope sees that an outer obs scope (from step 2) already
   exists with the same task-local key and **inherits**, not
   replaces, the trace context.
5. `ObsFnEntered` and `ObsFnExited` events emit with the matching ids.
6. Inside the body, `obs::emit!` and `tracing::info!` both see the
   shared context.
7. `#[obs::instrument]` body exits, popping its scope (no-op for
   trace_id, since we inherited).
8. `#[tracing::instrument]` body exits, closing the tracing span.
9. Direction A bridge sees `on_close`, drops the bridged obs scope.

In practice users should pick one. We document `#[obs::instrument]`
as preferred when `obs` is installed; the dual-decoration scenario
above is the safety contract for migration.

### 3.7 Reconstituting interned envelopes for `tracing`

When the producer side has interning enabled (§ 2.9), the data
envelopes carry `env.callsite_id != 0` and a stripped payload.
`ObsToTracingSink` reconstitutes the **original** tracing event
shape so downstream layers (`fmt::layer`, `tracing-opentelemetry`,
IDE viewers) see no difference between "never interned" and
"round-tripped through interning":

1. Look up `env.callsite_id` in the same in-process
   `ObsCallsiteRegistry` populated by Direction A.
2. Use the registered `(target, name, module_path, file, line,
   field_names)` to synthesise a `&'static tracing::Metadata`
   (cached by `MetadataKey::ByCallsiteId(id)` in the same `DashMap`
   used for non-interned envelopes; see § 3.3).
3. Build a `tracing::Event` whose `target = registered.target`
   (NOT `"obs.bridge"` as for non-interned envelopes) so existing
   target-based filters and IDE colourisation continue to apply.
4. Field set is the union of `registered.field_names` (rendered
   from the args payload) plus `obs.callsite_id` and the
   loop-guard markers.

Registry miss (impossible in single-process; defensive only):
emit a degraded tracing event with `target = "obs.bridge.unresolved"`
and increment `obs.runtime.v1.ObsBridgeCallsiteUnresolved`,
rate-limited.

The full design lives in
[31-callsite-interning.md](./31-callsite-interning.md).

## 4. Coexistence

### 4.1 Loop avoidance

Two thread-local guards, plus a reserved tracing target as
defence-in-depth:

```rust
mod loop_guard {
    use std::cell::Cell;
    thread_local! {
        pub static IN_TRACING_BRIDGE: Cell<bool> = const { Cell::new(false) };
        pub static IN_OBS_BRIDGE:     Cell<bool> = const { Cell::new(false) };
    }
}
```

- `TracingToObsLayer::on_event` returns immediately if
  `IN_OBS_BRIDGE.get()` (the current event is a re-entry from the
  obs sink, dispatched while we were emitting an obs envelope).
- `ObsToTracingSink::deliver` returns immediately if
  `IN_TRACING_BRIDGE.get()` (the current envelope is a re-entry from
  the tracing layer, dispatched while we were emitting a tracing
  event).
- Both set their respective flag for the duration of their work, then
  clear it.

Defence-in-depth: `ObsToTracingSink` synthesises tracing `Metadata`
with `target = "obs.bridge"`. `TracingToObsLayer::on_event` filters
out events whose target equals or starts with `obs.bridge`. So even
if a thread-local guard were ever bypassed (e.g. by a tracing
formatter that re-dispatches), the loop is broken.

The two guards are **synchronous**. Bridge work runs in the
calling task's thread without `await` between guard set and clear,
so thread-local visibility is guaranteed. `tokio` task migration
across `await` points doesn't apply because no `await` happens
inside the guarded section.

### 4.2 Filter composition

Three filters in a fully-installed binary:

1. `RUST_LOG`'s `EnvFilter` on the tracing registry — gates which
   tracing events reach `Layer::on_event`. This is the **primary**
   filter for the Direction A bridge.
2. `OBS_FILTER` (or `obs.yaml`'s `filter` key) — gates which
   `obs::emit!` callsites dispatch. Applies to native obs events
   and to bridged events synthesised from tracing (per § 2.7).
3. The `ObsToTracingSink` — dispatches every envelope it receives,
   trusting that step 1 already filtered. There is no third filter
   on the obs→tracing direction.

This layering means a user who sets `RUST_LOG=warn` and
`OBS_FILTER=info` gets:

- Tracing events at WARN+ flow into obs (via Direction A).
- Native obs events at INFO+ emit normally and dispatch into tracing
  (via Direction B), where the registry's `EnvFilter` will filter
  them again — events under the `obs.bridge` target are typically
  let through by `RUST_LOG=warn,obs.bridge=trace` if the user wants
  obs events visible in the formatter.

### 4.3 The recommended canonical init

```rust
use obs_sdk::{install_observer, install_panic_hook, StandardObserver, Tier};
use obs_sdk::otel::OtlpLogSink;
use obs_tracing_bridge::{ObsToTracingSink, TracingToObsLayer, FieldPromotions};
use tracing_subscriber::{registry, prelude::*, EnvFilter, fmt};

fn init() -> anyhow::Result<()> {
    // 1. obs first; one of its sinks fans events back out into tracing.
    let observer = StandardObserver::builder()
        .service("my-api", env!("CARGO_PKG_VERSION"))
        .sink_for(Tier::Log,    OtlpLogSink::from_env()?)
        .sink_for(Tier::Metric, obs_sdk::otel::OtlpMetricSink::from_env()?)
        .sink(ObsToTracingSink::new())
        .config_from_yaml_path("/etc/my-api/obs.yaml")?
        .reload_on_sighup()
        .build()?;
    install_observer(observer);
    install_panic_hook();

    // 2. tracing next; the bridge layer lifts every tracing event into obs.
    tracing_log::LogTracer::init()?;
    registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer())                                // dev pretty
        .with(TracingToObsLayer::new()
                .with_field_promotions(
                    FieldPromotions::new()
                        .promote("tenant_id", obs::Cardinality::Medium)
                        .promote("route",     obs::Cardinality::Medium)))
        .init();

    Ok(())
}
```

Why no infinite loop:

- A `tracing::info!` enters `TracingToObsLayer::on_event`. The guard
  `IN_OBS_BRIDGE` is **false** (no obs work in flight). The layer sets
  `IN_TRACING_BRIDGE = true`, calls
  `obs::observer().emit_envelope(env)` → `ObsToTracingSink::deliver`
  reads `IN_TRACING_BRIDGE = true` → returns. Other sinks (OtlpLogSink,
  etc.) deliver normally. Layer clears `IN_TRACING_BRIDGE`. Done.
- An `ObsRequestCompleted::builder().emit()` enters
  `ObsToTracingSink::deliver`. The guard `IN_TRACING_BRIDGE` is
  **false**. The sink sets `IN_OBS_BRIDGE = true`, calls
  `tracing::dispatcher::get_default(|d| d.event(...))` → registry
  dispatches to `TracingToObsLayer::on_event` → reads
  `IN_OBS_BRIDGE = true` → returns. The fmt layer formats the event
  and writes to stdout. Sink clears `IN_OBS_BRIDGE`. Done.

In both directions, every event is delivered to **every** sink/layer
exactly once.

## 5. Migration playbook

The bridge is designed for incremental adoption. Three stages:

### Stage 1 — All-tracing app, drop in the bridge

Existing app uses only `tracing` + `tracing-subscriber`. Add:

```rust
tracing_subscriber::registry()
    .with(EnvFilter::from_default_env())
    .with(fmt::layer())                                  // existing
    .with(obs_tracing_bridge::TracingToObsLayer::new())  // new
    .init();

obs::install_observer(StandardObserver::builder()
    .sink_for(Tier::Log, OtlpLogSink::from_env()?)
    .build()?);
```

Result: every existing `tracing::info!()` flows into `obs` as
`ObsTracingForensicEvent` and lands in the OTLP backend's logs view.
No user code changes. Useful for surfacing third-party crate logs
(sqlx, hyper, etc.) in the analytical store.

### Stage 2 — Type up the high-volume targets

Run `obs audit` weekly. The report ranks tracing-bridge volume by
target:

```
Tracing-bridge events emitted last 7 days:
  api::handlers     1.2M  ← typed candidate
  db::pool          430K  ← typed candidate
  hyper::client     180K
  sqlx::query       120K  ← typed candidate
  ... 67 more targets
```

For each candidate, either:

(a) Replace the `tracing::info!()` calls with `obs::emit!` against a
    new typed schema:

```rust
// before
tracing::info!(target: "api::handlers", route, status, latency_ms,
               "request completed");

// after
ObsRequestCompleted::builder()
    .route(route)
    .status(status)
    .latency_ms(latency_ms)
    .emit();
```

(b) Or, leave the third-party calls alone and register an auto-typed
    promoter so the bridge produces the typed event:

```rust
TracingToObsLayer::new()
    .register_typed::<ObsHttpRequestCompleted>(
        TypedMatcher::new()
            .target("tower_http::trace::on_response")
            .field("status").field("latency"),
        |event, ctx| { /* extract fields, build typed event */ },
    )
```

Stage 2 typically converts 80 % of bridged volume into typed events
within a few weeks.

### Stage 3 — Add Direction B and (optionally) drop Direction A

Once most tracing volume is typed:

```rust
let observer = StandardObserver::builder()
    .sink(ObsToTracingSink::new())                       // ← add
    .sink_for(Tier::Log, OtlpLogSink::from_env()?)
    .build()?;
install_observer(observer);
```

Now obs events show up in `cargo run` stdout via the existing
`fmt::layer()`, in `tracing-opentelemetry`-bridged Jaeger spans,
in IDE log viewers — without any changes to those tools.

If the workspace has fully migrated and no third-party `tracing`
volume remains worth lifting, `TracingToObsLayer` can be removed.
Most production workspaces keep both directions installed forever
because the cost is negligible and the third-party coverage is
valuable.

## 6. Test strategy

The bridge ships its own test harness in
`crates/obs-tracing-bridge/tests/`:

- **`tracing_to_obs_basic.rs`** — emits one tracing event per
  level, asserts the resulting `ObsTracingForensicEvent` envelope
  carries the expected `sev`, `payload.target`, `payload.message`,
  and field set. Uses the `InMemoryObserver` test harness from
  `obs-core`.
- **`obs_to_tracing_basic.rs`** — installs a `tracing-subscriber::fmt`
  layer with a writer collecting into a `Vec<u8>`, emits one obs
  event per severity, asserts the formatter output contains the
  expected target / level / fields.
- **`roundtrip_property.rs`** — `proptest`-driven: for arbitrary
  `(level, target, fields)`, build a `tracing::Event` → bridge →
  `ObsEnvelope` → bridge back → `tracing::Event`. Assert: target
  preserved (modulo `obs.bridge` for synthesised events), severity
  preserved, all string-typed fields preserved verbatim, numeric
  fields preserved within type-conversion limits.
- **`no_infinite_loop.rs`** — installs both directions, emits one
  obs event and one tracing event, asserts each appears exactly
  once in each subscriber/sink. Runs for 1000 iterations under
  `cargo test --release` to surface any cyclic dispatch.
- **`span_correlation.rs`** — opens a tracing span, emits an obs
  event under it, asserts the obs envelope's `trace_id` matches the
  bridged tracing span's id. Conversely, opens an `obs::scope!`,
  emits a tracing event, asserts the tracing event reaches a
  collector layer with the obs scope's `trace_id` as a field.
- **`pii_redaction.rs`** — emits tracing events with field names
  matching the built-in pattern (`password`, `api_key`, etc.),
  asserts the resulting payload values are `[REDACTED:bridge_pattern]`
  and that one `ObsBridgePiiSuspected` self-event is emitted per
  unique field name.
- **`auto_typed_promotion.rs`** — registers an
  `ObsHttpRequestCompleted` matcher, emits a matching tracing event,
  asserts the resulting envelope's `full_name` is the typed schema
  (not `ObsTracingForensicEvent`) and that all expected fields are
  populated.
- **`benchmarks/`** — `criterion` benches:
  - `bench_tracing_to_obs_overhead` — `tracing::info!` baseline vs
    bridged emit. CI gate: ≤ 2 µs delta.
  - `bench_obs_to_tracing_overhead` — `obs::emit!` baseline vs
    bridged dispatch. CI gate: ≤ 1.5 µs delta.

## 7. Performance budget

Targets, in addition to the native emit budgets in
[71-performance-budgets.md § 3.2](./71-performance-budgets.md#32-bridge):

| Path | Budget P50 | Notes |
| --- | --- | --- |
| `tracing::info!` → obs envelope (forensic mode) | ≤ 3 µs | native obs emit ~1 µs + bridge overhead ≤ 2 µs |
| `tracing::info!` → obs envelope (auto-typed mode) | ≤ 3 µs | same as above; matcher lookup is cached |
| `obs::emit!` → tracing event | ≤ 2.5 µs | native obs emit ~1 µs + bridge overhead ≤ 1.5 µs |
| `tracing::span!` → obs scope (Direction A on_new_span + on_close) | ≤ 4 µs total | spans are amortised over events emitted under them |

How we achieve the bridge overhead:

- **Field capture without per-call heap**: a thread-local
  `FieldCapture { strings: Vec<(String, String)>, scratch: BytesMut }`
  reused across calls. The `Vec` is `clear()`ed (preserves capacity)
  after each event. Net: zero allocations on the steady state.
- **Cached metadata lookup**: `DashMap<MetadataKey, &'static Metadata>`.
  Hot path is one shard read-lock + HashMap::get ≈ 60–80 ns. Project
  policy (CLAUDE.md § Async & Concurrency) mandates DashMap over
  `RwLock<HashMap>`; the write-once-per-key access pattern fits the
  DashMap idiom exactly.
- **Cached promoter dispatch**: typed-promotion lookup is keyed on
  `tracing_core::callsite::Identifier`, which is a `Copy` pointer.
  HashMap lookup ~30 ns.
- **Static loop-guard cells**: thread-local `Cell<bool>` is ~5 ns to
  read/write.

## 8. Failure modes

| Failure | Bridge behaviour |
| --- | --- |
| obs channel full when `TracingToObsLayer` tries to push | Same as native emit overflow: drop, increment `obs_dropped_total{tier, reason=channel_full}`. Tracing event is otherwise unaffected; other layers still receive it. |
| Tracing dispatcher missing when `ObsToTracingSink` dispatches | `tracing::dispatcher::get_default` invokes `Dispatch::none()` no-op. Bridge increments `obs_bridge_no_dispatcher_total` once per minute (rate-limited). |
| `Span::record` arrives after the bridge already emitted `ObsSpanCompleted` (race in `SpanEventMode::Both` mode) | Update is dropped. The bridge emits `ObsBridgeLateSpanRecord` self-event with the span name. In default `Off` mode, all records are aggregated until `on_close` so this race doesn't exist. |
| Synthesised callsite leak under high event-type churn | Bounded by total distinct schemas in the binary (set by the codegen, ≤ 10⁴ in pathological cases, typically ≤ 10²). Memory cost negligible (≤ 10 MiB worst case). |
| Tracing subscriber stack swapped at runtime | `ObsToTracingSink` dispatches to the *current* default; if the user calls `set_global_default` again, future events route to the new dispatcher. No re-init on the obs side required. |
| User registers two typed promoters that match the same callsite | First-registered wins; bridge emits one-shot `ObsBridgeMatcherConflict` self-event naming both candidate event types. |
| Tracing field with the same name as a recognised label | The bridge prefers the field value over the inherited scope value (matches `tracing` user expectation). Documented. |

## 9. Key Design Decisions

### KD1 — Layer is the default; Subscriber is the escape hatch

Layer composes with the existing `tracing-subscriber::registry()`
ecosystem; replacing the subscriber would break every other
`tracing` integration the user has installed. Subscriber form is
shipped only for the unusual case of a tracing-free binary.

### KD2 — Forensic by default; auto-typing is opt-in

Every bridged event lands in `ObsTracingForensicEvent` until the
user explicitly registers a typed promoter. This ensures the bridge
is useful out-of-the-box without forcing the user to declare a
schema for every third-party log line.

### KD3 — Field promotions are configurable, defaults conservative

No fields auto-promote to labels by default. The user names the
small set of promotable fields (`tenant_id`, `route`, …) explicitly
because those names are the ones their dashboards care about.

### KD4 — PII redaction is opinionated and on by default

A name-pattern redactor runs by default. Some users will dislike
the pattern list; they can disable or override it. The harm of
leaking a `password` field into the analytical store is far worse
than the harm of one unnecessary `[REDACTED]`.

### KD5 — Loop break uses thread-local guards plus a reserved target

Two-guard scheme works because bridge work is synchronous within
a single dispatch. The `obs.bridge` reserved target is
defence-in-depth for the case where a future code change might
introduce an `await` between guard set and clear.

### KD6 — Span correlation is task-local, not envelope-stamped

Trace context flows through the same `tokio::task_local!` that
`obs::scope!` and Direction A both write to. There is one source
of truth per task, regardless of which subsystem opened the
context. This avoids the failure mode where two subsystems disagree
on `trace_id`.

### KD7 — `RUST_LOG` controls the bridge, not `OBS_FILTER`

Filtering at the tracing source is what users already know. A
bridge-specific filter (`with_filter` per layer) is available for
tighter control but defaults to inheriting the registry's filter.

### KD8 — Metadata is leaked, never freed

`tracing` requires `&'static`. Per-process leakage is bounded by
the number of distinct event schemas. Acceptable cost for not
needing arena allocators or unsafe lifetime extension.

### KD9 — `SpanEmissionMode::Off` is the default for Direction B

Materialising tracing spans on every `obs::scope!` doubles the work
of users who already have `tracing-opentelemetry` installed. The
default is the cheaper one; opt in for the dev-mode case.

### KD10 — `#[obs::instrument]` is preferred when both can be used

When `obs` is installed, the typed `ObsFnEntered`/`ObsFnExited`
events plus the typed scope are richer than what
`#[tracing::instrument]` produces. Both are documented to coexist
without breakage, but the `obs` form is the one we recommend.

## 10. Open questions / risks

- **Tracing's `Visit` API is allocator-unfriendly for non-string
  values.** `Visit::record_debug(&self, _: &dyn Debug)` requires
  formatting via `format!`. We accept this; benchmark shows ~150 ns
  per debug field, within budget. If it becomes a hotspot, we could
  add a fast path for `record_str/u64/i64/bool/f64` that skips the
  Debug formatting.
- **Span field updates between `enter` and `close` in the default
  mode**: aggregation is correct but consumers expecting timely
  field updates may see stale values until close. Documented; the
  alternative (emit on every `record`) is far too noisy.
- **`tracing-opentelemetry` interaction in Direction B's
  `SpanEmissionMode::OnScope`**: opening a tracing span from within
  an `obs::scope!` enters `tracing-opentelemetry`'s span pipeline,
  which builds its own OTel span. We then ALSO have our own
  `OtlpTraceSink` from `obs-otel`. Two OTel spans for the same
  logical operation. We need either (a) a flag to disable
  `OtlpTraceSink` when `OnScope` is on, or (b) a span-id
  deduplication mechanism. **Resolution deferred** to v1.1; for now,
  document the interaction and recommend OnScope only in
  development.
- **Tracing callsite registration limits**: `tracing-core` has no
  documented hard limit, but the registration HashMap grows
  unboundedly. In practice, our cap (distinct event schemas) is
  small, so this is fine. If a user has thousands of distinct event
  types, the cost is one-time.
- **`Subscriber` form duplicate-implementation cost**: maintaining
  both Layer and Subscriber doubles the surface area. We accept
  this because the Subscriber form is small (~200 LoC) and the
  Layer form delegates to the same translator core.
- **WASM**: the bridge depends on `tracing-subscriber`'s registry
  which is `Send + Sync`-bound. WASM single-threaded targets need
  their own bridge. Out of scope for v1; matches obs's overall
  no-WASM stance.

## 11. Tracing-bridge built-in events

The bridge ships these built-ins in `obs-proto/proto/obs/v1/builtin.proto`:

| Event | Tier | Default sev | Purpose |
| --- | --- | --- | --- |
| `obs.v1.ObsTracingForensicEvent` | LOG | INFO | Default destination for unmatched `tracing::Event` (Direction A) |
| `obs.v1.ObsSpanCompleted` | LOG | DEBUG | Bridged `tracing::Span::close` (Direction A) |
| `obs.v1.ObsSpanEntered` | LOG | TRACE | Bridged `tracing::Span::new_span` (Direction A, optional via `SpanEventMode::Both`) |
| `obs.runtime.v1.ObsBridgePiiSuspected` | LOG | WARN | Pattern-matched PII redaction triggered (one-shot per field name) |
| `obs.runtime.v1.ObsBridgeMatcherConflict` | LOG | WARN | Two typed promoters matched the same callsite |
| `obs.runtime.v1.ObsBridgeLateSpanRecord` | LOG | WARN | `Span::record` after `ObsSpanCompleted` already emitted |
| `obs.runtime.v1.ObsBridgeNoDispatcher` | LOG | DEBUG | `ObsToTracingSink` ran without a tracing default; rate-limited 1/min |

These events flow through the same observer as user events, so they
appear in the same dashboards / queries.
