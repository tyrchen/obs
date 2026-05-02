# Design — Schema Definition & Code Generation

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [10-data-model.md](./10-data-model.md), [11-runtime-core.md](./11-runtime-core.md)

> v3 changes: pulled in foundational types from the new
> [10-data-model.md](./10-data-model.md); defined the previously
> hand-waved `MetricEmitter`, `BuildableTo`, `FieldCapture`, and
> `EnumCount` traits explicitly (§ 3.7); split off the Rust-first
> `#[derive(Event)]` rendering rules into § 3.5; cross-references
> retargeted from the old `architecture-design.md` to the new
> per-stage specs.
>
> v2 changes: replaced `prost`/`prost-build` with `buffa`/`buffa-build`/
> `buffa-reflect`; codegen now uses the `FileDescriptorSet` emitted by
> `buffa-build` and walks it with `buffa-reflect` instead of a hand-rolled
> proto parser; added `Obs*` naming lint; clarified single-table Arrow
> schema; dropped the speculative multi-language section (Rust-only in v1).

## 1. Authoring modes

A user can define wide-event schemas in either of two equivalent ways. Both
produce the same generated artifacts.

### 1.1 Proto-first (recommended for shared schemas)

```proto
// myapp/proto/myapp/v1/events.proto
syntax = "proto3";

package myapp.v1;

import "obs/v1/options.proto";

message ObsRequestCompleted {
  option (obs.v1.event) = {
    tier: TIER_LOG,
    default_sev: SEVERITY_INFO,
  };

  string  route        = 1 [(obs.v1.field) = { kind: LABEL,        cardinality: MEDIUM }];
  Status  status       = 2 [(obs.v1.field) = { kind: LABEL,        cardinality: LOW    }];
  string  tenant_id    = 3 [(obs.v1.field) = { kind: LABEL,        cardinality: MEDIUM }];
  string  user_id      = 4 [(obs.v1.field) = { kind: ATTRIBUTE,    cardinality: HIGH,  classification: PII }];
  string  trace_id     = 5 [(obs.v1.field) = { kind: TRACE_ID                                              }];
  uint64  latency_ms   = 6 [(obs.v1.field) = { kind: MEASUREMENT,  metric: { kind: HISTOGRAM, unit: "ms",
                                              bounds: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000] } }];
  uint64  bytes_out    = 7 [(obs.v1.field) = { kind: MEASUREMENT,  metric: { kind: COUNTER, unit: "By" } }];
}

enum Status { OK = 0; CLIENT_ERROR = 1; SERVER_ERROR = 2; }
```

The build script invokes `obs-build` (a thin layer over `buffa-build`)
which emits Rust wire types **and** the `EventSchema` impls.

### 1.2 Rust-first (recommended for single-crate apps)

```rust
use obs::Event;

#[derive(Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsRequestCompleted {
    #[obs(label, cardinality = "medium")]
    pub route: Route,                 // enum implementing Display + EnumCount

    #[obs(label, cardinality = "low")]
    pub status: Status,

    #[obs(label, cardinality = "medium")]
    pub tenant_id: TenantId,

    #[obs(attribute, cardinality = "high", classification = "pii")]
    pub user_id: UserId,

    #[obs(trace_id)]
    pub trace_id: String,

    #[obs(measurement, metric(histogram, unit = "ms",
        bounds = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000]))]
    pub latency_ms: u64,

    #[obs(measurement, metric(counter, unit = "By"))]
    pub bytes_out: u64,
}
```

The `#[derive(Event)]` proc-macro emits the same `EventSchema` impl. It
also emits a `.proto` file under `OUT_DIR` so the schema is available
as a contract for non-Rust consumers (manual or future SDKs).

**Single source of truth**: a crate picks one mode in `Cargo.toml`:

```toml
[package.metadata.obs]
schema-source = "proto"      # or "rust"
proto-root    = "proto"      # if "proto"
```

Mixing modes within a crate is a build-time error.

## 2. The `obs.v1.options` proto extensions

Custom options live in their own crate-distributed proto file, parsed
by `buffa-reflect`'s `DescriptorPool`:

```proto
// crates/obs-proto/proto/obs/v1/options.proto
syntax = "proto3";

package obs.v1;

import "google/protobuf/descriptor.proto";

extend google.protobuf.MessageOptions {
  EventMeta event = 80001;
}

extend google.protobuf.FieldOptions {
  FieldMeta field = 80002;
}

message EventMeta {
  Tier     tier        = 1;
  Severity default_sev = 2;
}

message FieldMeta {
  FieldKind      kind           = 1;
  Cardinality    cardinality    = 2;
  Classification classification = 3;
  MetricSpec     metric         = 4;  // when kind == MEASUREMENT
}

message MetricSpec {
  MetricKind     kind   = 1;
  string         unit   = 2;          // UCUM units: "ms", "By", "1"
  repeated double bounds = 3;         // for histograms
}

enum MetricKind {
  METRIC_KIND_UNSPECIFIED = 0;
  METRIC_KIND_COUNTER     = 1;
  METRIC_KIND_GAUGE       = 2;
  METRIC_KIND_HISTOGRAM   = 3;
}
```

Field number range `80000–89999` is reserved for `obs.v1.*` options.
This is in the global option-extension space; we do not consume it
from third-party schemas without explicit `import`.

## 3. Build-time pipeline (proto-first)

```
┌───────────────────────────────────────┐
│ user crate's build.rs:                │
│   obs_build::Config::new()…compile() │
└──────────────┬────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│ Stage 1: buffa-build                         │
│   .proto → src/pb/{pkg}.rs                  │
│   Wire types (Message + View) + Default     │
│   FileDescriptorSet emitted as a side effect │
│   No protoc required (Buf or precompiled FDS)│
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│ Stage 2: obs codegen                         │
│   Load FDS via buffa-reflect::DescriptorPool│
│   Walk messages + read (obs.v1.event/field) │
│   → impl EventSchema for each ObsXxx         │
│   → typed builder structs                    │
│   → Arrow schema fragments per event         │
│   → JSON renderer dispatch table             │
│   → secret/PII redaction dispatch            │
│   → static cardinality / Obs-prefix lints    │
└──────────────────────────────────────────────┘
```

Stage 1 and Stage 2 are both pure Rust; no `protoc` binary on PATH is
required. Hermetic CI is preserved.

### 3.1 Generated artifacts (per .proto file)

For an input `myapp/v1/events.proto`:

```
$OUT_DIR/
├── pb/
│   └── myapp.v1.rs               # buffa wire types + zero-copy views
└── obs/
    ├── schemas.rs                # impl EventSchema for each ObsXxx
    ├── builders.rs               # typed builder structs and From impls
    ├── arrow_schema.rs           # Arrow Field fragments contributing to the global obs_events schema
    ├── render.rs                 # full_name → decode-to-JSON dispatcher
    ├── scrub.rs                  # full_name → PII/SECRET redaction dispatcher
    └── lints.rs                  # const _: () = { ... static asserts ... }
```

The user includes them with one macro:

```rust
// src/lib.rs
obs::include_schemas!("myapp.v1");   // wires up all generated files
```

### 3.2 The `EventSchema` trait

```rust
pub trait EventSchema: Send + Sync + Sized + 'static {
    const FULL_NAME: &'static str;
    const TIER: Tier;
    const DEFAULT_SEV: Severity;
    const FIELDS: &'static [FieldMeta];

    /// First 8 bytes of BLAKE3 over (FULL_NAME, TIER, DEFAULT_SEV, FIELDS);
    /// a build-time const. 64 bits is sized for accidental-collision avoidance
    /// at realistic schema counts; this is an identifier, not a tamper-
    /// detection primitive. See [10-data-model.md § 6](./10-data-model.md#6-envelope)
    /// and [99-key-decisions.md § D2](./99-key-decisions.md).
    const SCHEMA_HASH: u64;

    /// Encode this event's payload using buffa's encoder into a reused buffer.
    fn encode_payload(&self, buf: &mut bytes::BytesMut);

    /// Project labels and lift trace/span ids onto the envelope.
    /// Generated; never hand-written.
    fn project(&self, env: &mut ObsEnvelope);

    /// For MEASUREMENT-annotated fields, emit metric data points.
    /// Generated; called by metric sinks.
    fn project_metrics(&self, sink: &mut dyn MetricEmitter);
}
```

### 3.3 Generated builder

For `ObsRequestCompleted` the codegen emits:

```rust
#[derive(typed_builder::TypedBuilder)]
#[builder(build_method(vis = "pub", name = build))]
pub struct ObsRequestCompletedArgs {
    #[builder(setter(into))] pub route: Route,
    pub status: Status,
    #[builder(setter(into))] pub tenant_id: TenantId,
    #[builder(setter(into, strip_option), default)] pub user_id: Option<UserId>,
    #[builder(setter(into), default)] pub trace_id: String,   // auto-filled from obs::scope!
    pub latency_ms: u64,
    pub bytes_out: u64,
}

impl ObsRequestCompletedArgs {
    pub fn into_event(self) -> ObsRequestCompleted { /* field move */ }
}

impl ObsRequestCompleted {
    pub fn builder() -> ObsRequestCompletedArgsBuilder<()> {
        ObsRequestCompletedArgs::builder()
    }
}

// Magic that lets `.emit()` work on the builder directly:
impl<S> ObsRequestCompletedArgsBuilder<S>
where Self: BuildableTo<ObsRequestCompletedArgs> {
    pub fn emit(self) { self.build().into_event().emit() }
    pub fn emit_at(self, sev: Severity) { self.build().into_event().emit_at(sev) }
}
```

Required vs optional is decided by the proto: a non-`optional` field
is required at the builder; an `optional` field defaults to `None`.
TRACE_ID-class fields are always given a `default` setter so the user
can rely on `obs::scope!` auto-fill.

### 3.4 Generated lint module (the safety net)

```rust
// $OUT_DIR/obs/lints.rs (excerpt for ObsRequestCompleted)
use obs::__private::{Cardinality, Classification, FieldKind, Tier};

const _: () = {
    // L001: every LABEL field must be Low or Medium cardinality
    assert!(Cardinality::Medium.is_label_compatible(),
        "obs L001: field `route` is LABEL but cardinality is High/Unbounded");
    assert!(Cardinality::Low.is_label_compatible(),
        "obs L001: field `status` is LABEL but cardinality is High/Unbounded");
    assert!(Cardinality::Medium.is_label_compatible(),
        "obs L001: field `tenant_id` is LABEL but cardinality is High/Unbounded");

    // L002: PII fields must not be LABEL
    // (user_id classification=PII; kind=ATTRIBUTE, so OK)

    // L003: SECRET fields must not exist on LOG/AUDIT tier events
    // (no SECRET fields here; nothing to check)

    // L004: MEASUREMENT fields must have a MetricSpec
    // (latency_ms ✓, bytes_out ✓)

    // L005: enum-typed LABELs have variant_count() ≤ Cardinality cap
    assert!(<Status as obs::__private::EnumCount>::COUNT
                <= Cardinality::Low.cap() as usize,
        "obs L005: enum `Status` has more variants than its declared LABEL cardinality");

    // L011: event message name starts with `Obs`
    assert!(obs::__private::starts_with_obs("ObsRequestCompleted"),
        "obs L011: event type name `ObsRequestCompleted` must start with `Obs`");
};
```

These constants are evaluated at `cargo build` time. A violation is a
hard compile error with a message naming the offending field.

The proc-macro / codegen also emits the `EnumCount` impl for any enum
used as a LABEL, using a generated `const COUNT: usize = N` pulled from
the descriptor — no nightly `variant_count` required.

### 3.5 Schema hash is a build-time artifact too

`SCHEMA_HASH` is computed at build time (we know `FIELDS` statically)
and stored as a `u64` constant — the first 8 bytes of
`blake3::hash(canonical_descriptor_bytes).as_bytes()`, read as
little-endian via `<[u8; 8]>::try_from(&hash[..8])` (panic-free per
CLAUDE.md `clippy::indexing_slicing`). There is no runtime hashing.

`EnumLabel` rendering: `#[derive(EnumLabel)]` walks variant names and
renders them as `snake_case` via the `heck` crate
(`AuthMethod::OAuthGoogle → "oauth_google"`); per-variant override
through `#[obs(rename = "auth-google")]`. The codegen also emits a
`const COUNT: usize = N` constant via the generated `EnumCount` impl
(see § 3.7), so lint L005 is checkable without nightly's
`variant_count`.

### 3.6 Auxiliary trait surface

The codegen and runtime share a small set of auxiliary traits.
Earlier drafts referenced these without defining them; this section
is the contract.

> **See also**: [`EventSchemaErased`](./14-schema-registry.md#2-the-eventschemaerased-trait)
> is the object-safe complement to `EventSchema` that this codegen
> also emits, alongside the `linkme`-collected registration into
> `obs_core::registry::EVENT_SCHEMAS`. It belongs to the schema-
> registry contract; it is referenced here so a reader of this spec
> knows the codegen produces *both* `EventSchema` (typed, generic)
> and `EventSchemaErased` (object-safe, registry-bound) for each
> event.

```rust
// ─── Builder state contract ─────────────────────────────────────────

/// Marker trait implemented by `typed_builder` for the
/// "all-required-fields-set" builder state. The codegen emits a
/// blanket impl over the parameter shape `typed_builder` produces, so
/// `.emit()` only compiles when every required setter has been
/// called. The `obs::emit!` macro relies on the same shape.
pub trait BuildableTo<Args> {
    fn build(self) -> Args;
}

// ─── Metric emission contract ───────────────────────────────────────

/// Implemented by metric sinks (OTLP metrics, Prometheus exporters);
/// generated `EventSchema::project_metrics` calls one method per
/// `MEASUREMENT` field on the event. The trait is `&mut self` so
/// implementations can hold transient state (e.g. an attribute set
/// being assembled). The `Sink` itself is `&self`; the trait is
/// invoked from inside `Sink::deliver` with a per-call mutable view.
pub trait MetricEmitter {
    fn record_counter(&mut self, instrument: &'static str, value: u64,
                      unit: Option<&'static str>);
    fn record_gauge_u64(&mut self, instrument: &'static str, value: u64,
                        unit: Option<&'static str>);
    fn record_gauge_f64(&mut self, instrument: &'static str, value: f64,
                        unit: Option<&'static str>);
    fn record_histogram(&mut self, instrument: &'static str, value: f64,
                        unit: Option<&'static str>, bounds: &'static [f64]);
    /// Attribute set carried into every record_* on the same event.
    fn with_attributes(&mut self, attrs: &[(&'static str, &str)]);
}

// ─── Bridge field capture (used by tracing→obs auto-typing) ─────────

/// Visitor used by tracing's `Event::record(visitor)` to extract
/// typed values into a thread-local scratch space; reused across
/// emissions (zero per-event allocation in the steady state).
pub struct FieldCapture {
    strings:  Vec<(&'static str, String)>,
    u64s:     Vec<(&'static str, u64)>,
    i64s:     Vec<(&'static str, i64)>,
    f64s:     Vec<(&'static str, f64)>,
    bools:    Vec<(&'static str, bool)>,
    /// Reused encoder scratch for `record_debug` / `record_display`.
    scratch:  bytes::BytesMut,
}

impl FieldCapture {
    pub fn default() -> Self;
    pub fn clear(&mut self);              // preserves capacity
    pub fn string(&self, name: &str) -> Option<&str>;
    pub fn u64(&self, name: &str)    -> Option<u64>;
    pub fn i64(&self, name: &str)    -> Option<i64>;
    pub fn f64(&self, name: &str)    -> Option<f64>;
    pub fn bool(&self, name: &str)   -> Option<bool>;
    pub fn duration(&self, name: &str) -> Option<Duration>;
}

impl tracing::field::Visit for FieldCapture { /* … */ }

// ─── Span context (used by tracing→obs auto-typing) ─────────────────

/// Read-only view of the active scope/span context that
/// `register_typed`-style closures receive. Carries the labels the
/// user has named in `obs::scope!` plus the span ancestry (for
/// `tracing` source spans).
pub struct SpanCtx<'a> {
    /// Labels from the active `obs::scope!` allowlist, in
    /// outermost-first order.
    pub labels: &'a [(&'static str, &'a str)],
    /// Tracing span ancestry, oldest first; empty if this `SpanCtx`
    /// originates from a non-bridge path.
    pub spans:  &'a [SpanFrame<'a>],
}

pub struct SpanFrame<'a> {
    pub name:   &'a str,
    pub target: &'a str,
}

impl<'a> SpanCtx<'a> {
    pub fn label(&self, name: &str) -> Option<&'a str>;
    pub fn span_path(&self) -> Cow<'a, str>;   // "request:auth:db_query"
    pub fn target(&self) -> Option<&'a str>;
}

// ─── Enum cardinality (used by lint L005) ────────────────────────────

/// Compile-time variant count for any enum used as a LABEL field.
/// Generated by `#[derive(EnumLabel)]`.
pub trait EnumCount {
    const COUNT: usize;
}
```

These traits are re-exported from `obs-sdk` under the `__private`
module — they are intended for codegen consumers, not human
implementers, but they are documented because reading the generated
output is a routine debugging activity.

### 3.7 The single-table Arrow schema

The Parquet/ClickHouse sinks emit into one wide table per service. The
codegen contributes a per-event-type Arrow `Field` (a struct of the
event's payload columns) to a global schema:

```rust
// $OUT_DIR/obs/arrow_schema.rs
pub fn payload_struct_for(full_name: &str) -> Option<Arc<arrow_schema::Field>> {
    match full_name {
        "myapp.v1.ObsRequestCompleted" => Some(Arc::clone(&FIELD_OBS_REQUEST_COMPLETED)),
        "myapp.v1.ObsUserSignedUp"     => Some(Arc::clone(&FIELD_OBS_USER_SIGNED_UP)),
        _ => None,
    }
}

pub fn all_payload_fields() -> &'static [Arc<arrow_schema::Field>] {
    &[ Arc::clone(&FIELD_OBS_REQUEST_COMPLETED),
       Arc::clone(&FIELD_OBS_USER_SIGNED_UP) ]
}
```

The `obs-parquet` / `obs-clickhouse` sinks call `all_payload_fields()`
at startup to assemble the table schema, and `payload_struct_for(name)`
on each row to choose which struct column to populate. All other
struct columns on that row are written as NULL — sparse columnar
storage handles this efficiently.

## 4. The `obs-build` API

```rust
// build.rs — anyhow per CLAUDE.md § Error Handling (build scripts are
// application-shaped, not library-shaped).
fn main() -> anyhow::Result<()> {
    obs_build::Config::new()
        .files(&["proto/myapp/v1/events.proto"])
        .include("proto")
        .include_obs_options()    // pulls obs/v1/options.proto from the crate
        .out_dir(std::env::var("OUT_DIR")?)

        // Codegen toggles:
        .with_arrow_schema(true)
        .with_json_render(true)
        .with_payload_scrub(true)
        .with_otel_attribute_view(true)

        // Map proto enums to existing Rust types (escape hatch):
        .extern_path(".myapp.v1.Status", "::myapp::Status")

        // Pass-through to buffa-build:
        .descriptor_source(obs_build::DescriptorSource::Buf)   // optional

        .compile()?;
    Ok(())
}
```

Defaults are conservative: arrow + scrub on, json render off (consumers
can turn it on if they ship a CLI that needs it).

Internally `obs-build`:

1. Calls `buffa_build::Config::new()...generate_views(true).compile()`,
   capturing the emitted `FileDescriptorSet` path (via
   `descriptor_set(...)` if the user did not supply one).
2. Loads the FDS into `buffa_reflect::DescriptorPool::decode(...)`.
3. Iterates `pool.all_messages()`, reads each message's
   `(obs.v1.event)` extension and each field's `(obs.v1.field)`
   extension via the descriptor pool's extension API.
4. Generates `obs/schemas.rs`, `obs/builders.rs`, `obs/arrow_schema.rs`,
   `obs/render.rs`, `obs/scrub.rs`, `obs/lints.rs`.

This replaces the hand-rolled proto parser pattern. There is no
custom proto-text grammar in `obs-build`.

## 5. Schema evolution & versioning

Rules enforced by `obs schema diff` (CLI, see [50-cli.md § 3.6](./50-cli.md#36-obs-diff)):

| Change | Verdict |
| --- | --- |
| Add new event type | ✅ allowed |
| Add new field with new tag | ✅ allowed (default value used by old consumers) |
| Reuse a deleted tag | ❌ banned |
| Change field type | ❌ banned (use a new field) |
| Tighten cardinality (HIGH → MEDIUM) | ⚠ allowed; warns; may break label budget |
| Loosen cardinality (LOW → MEDIUM) | ✅ allowed |
| Change `kind` (LABEL ↔ ATTRIBUTE) | ❌ banned (changes downstream schema) |
| Promote field classification (INTERNAL → PII) | ✅ allowed; redaction kicks in |
| Demote classification (PII → INTERNAL) | ❌ banned |
| Change `tier` | ❌ banned (use a new event type) |
| Rename event without `Obs` prefix | ❌ banned (L011) |

Schemas under `proto/` should be reviewed with the diff tool in CI;
the diff tool emits a structured report and a non-zero exit code on
`❌` changes.

The `SCHEMA_HASH` is **not** a substitute for the diff: a hash change
tells you *something* changed, but the diff tells you *what* and
whether it is breaking.

## 6. The forensic escape hatch

Sometimes an emergency requires logging structured data that has no
schema yet. We provide `obs::forensic!` for this case:

```rust
obs::forensic!(
    site = "billing::reconcile",
    message = "unexpected ledger state",
    {
        "ledger_id" => ledger_id,
        "expected"  => expected_balance,
        "actual"    => actual_balance,
    }
);
```

This emits `obs.v1.ObsForensicEvent` with `site`, `message`, and a
`map<string, string>` payload. It is rate-limited per-site, per-process,
governed by `[package.metadata.obs] forensic_max = N` and validated by
`obs lint`. Forensic events are **always** flushed regardless of
sampling, so emergency data is never lost.

The intent is that forensic usage trends to zero over time as schemas
mature. The CLI surfaces "how many forensic events emitted last week
per site" as a work-driving signal for engineering teams.

## 7. Tracing-bridge codegen

`obs-tracing-bridge` ships a `tracing::Subscriber` that converts every
`tracing` event into `obs.v1.ObsTracingForensicEvent`. The mapping is:

| `tracing` | `ObsTracingForensicEvent` |
| --- | --- |
| `event.metadata().level()` | `Severity` |
| `event.metadata().target()` | `target` (LABEL, MEDIUM) |
| Each field | entry in `attrs: map<string, string>` |
| Span context | `span_path: string` (e.g. `request:auth:db_query`) |

This bridge has zero codegen requirements on the user; it lifts
existing `tracing` calls into the wide-event stream so a single
ingestion pipeline can consume both during migration. Removing the
bridge once a crate is fully schema-instrumented is a one-line change.

## 8. Codegen performance budget

Per-crate codegen must satisfy:

- ≤ 100 ms wall time for ≤ 50 events
- ≤ 1 s for ≤ 500 events
- Output cached by `OUT_DIR` and content-hashed; no work if `.proto`
  files unchanged
- No use of `Span::call_site` for emitted code paths (which can cause
  recompiles on unrelated changes)

A `cargo bench` in `crates/obs-build/benches/` measures these.

## 9. v1 scope: Rust only

The `.proto` schemas are the single source of truth and are
multi-language by nature, but `obs-build`, `obs-macros`, and the entire
SDK ship only Rust artifacts in v1. Cross-language SDK work is
deferred to post-1.0 and gated on actual demand from a non-Rust
consumer of these schemas. The CLI is also Rust-only — it does not
attempt to manage Go/Python/TypeScript code generation.
