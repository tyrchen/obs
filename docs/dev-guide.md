# obs — Developer Guide

> **Audience:** contributors to the `obs` workspace, sink/bridge
> implementers, and library authors integrating deeply with the SDK.
> **Goal:** explain *why* the runtime is shaped the way it is, the
> contracts each layer must honour, and the workflow expected of
> changes that touch the public API.

Other guides:
[User Guide](./user-guide.md) ·
[Migration from `tracing`](./migration-from-tracing.md) ·
[中文开发者指南](./dev-guide.zh-CN.md) ·
[Specs index](../specs/index.md)

---

## Table of contents

1. [Workspace map](#1-workspace-map)
2. [Architecture in one diagram](#2-architecture-in-one-diagram)
3. [The `Observer` trait and three-tier resolution](#3-the-observer-trait-and-three-tier-resolution)
4. [The hot-path callsite cache](#4-the-hot-path-callsite-cache)
5. [Per-tier workers and pipeline order](#5-per-tier-workers-and-pipeline-order)
6. [The schema registry](#6-the-schema-registry)
7. [Codegen pipeline](#7-codegen-pipeline)
8. [Scopes, scope frames, and auto-fill](#8-scopes-scope-frames-and-auto-fill)
9. [Filter DSL](#9-filter-dsl)
10. [Sampling order and `traceparent.sampled`](#10-sampling-order-and-traceparentsampled)
11. [Sinks: the `ScrubbedEnvelope` contract](#11-sinks-the-scrubbedenvelope-contract)
12. [The AUDIT spool](#12-the-audit-spool)
13. [The `tracing` bridge](#13-the-tracing-bridge)
14. [Callsite interning](#14-callsite-interning)
15. [Security: scrubber, classification, secrecy](#15-security-scrubber-classification-secrecy)
16. [Performance budgets and bench harness](#16-performance-budgets-and-bench-harness)
17. [Testing strategy](#17-testing-strategy)
18. [Contributing checklist](#18-contributing-checklist)
19. [Release workflow](#19-release-workflow)
20. [Reference: load-bearing decisions](#20-reference-load-bearing-decisions)

---

## 1. Workspace map

```
crates/
  obs-types          # 7 vocabulary enums (Tier, Severity, FieldKind, …); leaf
  obs-proto          # envelope.proto / builtin.proto + buffa codegen
  obs-core           # Observer, sinks, registry, scope, sampler, scrubber, config
  obs-macros         # #[derive(Event)], obs::emit!, #[obs::test], …
  obs-build          # build.rs codegen for proto-first authoring
  obs-sdk            # façade re-export — the only crate users normally touch
  obs-otel           # OTLP log / metric / trace sinks
  obs-parquet        # ParquetSink (single sparse obs_events table)
  obs-clickhouse     # ClickHouseSink + DDL emitter
  obs-tower          # HTTP server + client middleware
  obs-tracing-bridge # bidirectional tracing ↔ obs bridge
apps/
  obs-cli            # the `obs` developer CLI (clap v4)
  server             # demo: hello-world emit
  server-proto       # demo: proto-first authoring
  soak               # 50 k events/sec soak harness (spec 90 § M4)
examples/            # four runnable example services (todomvc, interop pair, sinks-showcase)
specs/               # design specs — read 00-prd.md → 99-key-decisions.md
docs/                # this guide, user guide, migration guide, research memos
```

### Dependency graph (must stay acyclic)

```
                ┌──────────────┐
                │  obs-types   │
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │  obs-proto   │
                └──────┬───────┘
                       │
       ┌───────────────┼───────────────┐
       │               │               │
┌──────▼───┐    ┌──────▼───────┐  ┌────▼────────┐
│ obs-core │◄───│  obs-macros  │  │  obs-build  │
└──────┬───┘    └──────────────┘  └─────────────┘
       │
   sinks │ (no obs-core knowledge of any sink crate;
       │  sinks pull obs-core, never vice-versa)
       ▼
┌────────────┐ ┌────────────┐ ┌────────────────┐
│  obs-otel  │ │ obs-parquet│ │ obs-clickhouse │
└────────────┘ └────────────┘ └────────────────┘

┌────────────────────┐  ┌────────────┐
│ obs-tracing-bridge │  │ obs-tower  │
└────────────────────┘  └────────────┘

                 obs-sdk re-exports
                  the common subset
```

`obs-core` is forbidden from depending on any sink crate; sinks always
pull `obs-core`. Only `apps/obs-cli` may depend on every other crate.

---

## 2. Architecture in one diagram

```
                                              user code
                                               │
                                               │  ObsXxx::builder().…().emit()
                                               │  obs::emit!(WARN, ObsXxx { … })
                                               ▼
                                       ┌──────────────────┐
                                       │  ObsCallsite     │  (static; per-call-site)
                                       │  enabled() check │   AtomicU8 + AtomicU32 generation
                                       └────────┬─────────┘
                       (Never) ◄───────────────┐│
                                               │▼
                                  ┌──────────────────────┐
                                  │ Observer::resolve()  │  per-task → per-thread → global
                                  └──────────┬───────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │ EventSchema.project │  payload bytes + label projection
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  Head sampler       │  rate(full_name, sev) → keep/drop
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │ Tail-on-error push  │  per-scope ring buffer
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  mpsc::try_send     │  one channel per Tier
                                  └──────────┬──────────┘
                                             │ — emit thread returns here —
        ────────────────────────────────────────────────────────
                                             │ (worker task)
                                  ┌──────────▼──────────┐
                                  │  Scrubber           │  PII redact, SECRET strip
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │ ScrubbedEnvelope    │  type-system handoff
                                  └──────────┬──────────┘
                                             │
                                  ┌──────────▼──────────┐
                                  │  SinkRouter         │  per-tier sink chain
                                  └──────────┬──────────┘
                                             │
                  ┌──────────────────────────┼──────────────────────────┐
                  ▼                          ▼                          ▼
         StdoutSink / NDJSON       OtlpLog / Metric / Trace      Parquet / ClickHouse
```

Two design rules govern the whole picture:

1. **The emit thread never blocks for a sink** (the AUDIT tier is the
   only deliberate exception, and even there it falls through to a
   disk spool after a bounded wait).
2. **The emit thread never sees PII or SECRETs in payload form** — the
   scrubber runs in the worker, before any sink sees the envelope.

---

## 3. The `Observer` trait and three-tier resolution

```rust
pub trait Observer: Send + Sync + 'static {
    fn emit_envelope(&self, env: ObsEnvelope);
    fn enabled(&self, callsite: &ObsCallsite) -> Interest;
    fn generation(&self) -> u32;
    fn reload_filter(&self, filter: Filter);
    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn shutdown<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn shutdown_blocking(&self);
}
```

Trait-async would force `dyn Observer` to be non-object-safe; we
deliberately use `Pin<Box<dyn Future>>` here (CLAUDE.md async-trait
exception). Same applies to `Sink`.

### 3.1 Resolution order

```rust
pub fn observer() -> Arc<dyn Observer> {
    if OVERRIDE_COUNT.load(Ordering::Relaxed) == 0 {
        return OBSERVER_GLOBAL.load_full();
    }
    if let Ok(per_task) = OBSERVER_TASK.try_with(|o| o.clone()) { return per_task; }
    if let Some(per_thread) = OBSERVER_THREAD.with(|c| c.borrow().clone()) { return per_thread; }
    OBSERVER_GLOBAL.load_full()
}
```

- **`OVERRIDE_COUNT` short-circuit** keeps single-tenant production at
  one atomic load + `ArcSwap::load_full` (~15 ns).
- **per-task** uses `tokio::task_local!` — survives across `.await`.
- **per-thread** uses `thread_local!` with a `RefCell` — sync-only, must
  not be held across `.await`.
- The `with_observer_thread_local` API is named verbosely on purpose —
  reaching for it inside async code should look wrong at the call site
  (decision D47).

### 3.2 Carrying overrides across `.await`

```rust
tokio::spawn(
    handle_request(req)
        .instrument(scope)                    // scope frame
        .with_observer(observer_for_tenant),  // observer override
);
```

Both adapters layer onto the same `Instrumented<F>` type via two
orthogonal slots. Without explicit propagation, `tokio::spawn` orphans
both — the spawned task sees the global observer with no scope.

### 3.3 Re-entry guard

A `CAN_ENTER: Cell<bool>` thread-local prevents recursion when a sink
itself emits an event (e.g., bridge sink synthesising a `tracing::Event`
that comes back into the layer). When `CAN_ENTER == false`, the
observer no-ops and the sink's emit is dropped silently. Mirrors the
`State::can_enter` pattern in `tracing-core`.

---

## 4. The hot-path callsite cache

Every emit site has a `static ObsCallsite`:

```rust
pub struct ObsCallsite {
    pub full_name:  &'static str,
    pub default_sev: Severity,
    interest:    AtomicU8,    // 0 unknown / 1 Never / 2 Sometimes / 3 Always
    generation:  AtomicU32,   // matches Observer.generation() when valid
}
```

### 4.1 Hot-path check

```rust
fn enabled(&self, cur_gen: u32) -> Option<bool> {
    if self.generation.load(Relaxed) == cur_gen {
        match self.interest.load(Relaxed) {
            1 => return Some(false),    // never
            3 => return Some(true),     // always
            _ => {}
        }
    }
    None  // re-query observer
}
```

- **`Never`** short-circuits to false in ~25 ns.
- **`Always`** short-circuits to true; resolves observer + projects
  payload in ~110 ns even with no observer installed (the noop case).
- **`Sometimes`** falls through to the observer call (more expensive
  but only when filter cares).

### 4.2 Cache invalidation

`Observer::reload_filter()` bumps `generation` atomically. The next
`enabled()` call sees the mismatch, re-queries, and CASes the new
state in. **Never block on the observer side** — the cache may briefly
be stale across a reload; that's deliberate and acceptable.

### 4.3 Why static, not DashMap

Earlier drafts kept the cache in a process-wide `DashMap` keyed by call
site. Inlining the `Atomic*` pair onto the static `ObsCallsite` removes
a hash + lookup from the hot path entirely (decision D11). Match
parity with `tracing::Interest` is intentional — anyone who has read
`tracing-core` recognises the pattern instantly.

---

## 5. Per-tier workers and pipeline order

### 5.1 One worker per tier

`StandardObserver::build()` spawns four bounded mpsc channels (one per
`Tier::{Log, Metric, Trace, Audit}`), each owned by a single tokio task
that drives the tier's sink chain. Default channel capacity 8192;
configurable in `obs.yaml` `queues.*`. Failure isolation: an OTLP
endpoint blocking the LOG tier never touches METRIC.

### 5.2 The full pipeline

For each emit:

| Step | Where | Cost (typical) |
| --- | --- | --- |
| `ObsCallsite::enabled` | static | 25 ns |
| `Observer::enabled` (only if Sometimes) | global / per-task / per-thread | 50 ns |
| `EventSchema::project` (build payload + label map; auto-fill from scope) | emit thread | 200–500 ns |
| Head sampler decision | emit thread | < 50 ns |
| Tail-on-error push | emit thread | < 30 ns |
| `mpsc::try_send` | emit thread | ~100 ns |
| **emit returns** | | |
| Scrubber (per-event redaction) | worker | 100 ns – 1.5 µs |
| Build `ScrubbedEnvelope<'_>` | worker | 0 (lifetime cast) |
| `SinkRouter::deliver` per sink | worker | sink-specific |

### 5.3 Backpressure

- LOG / METRIC / TRACE: when the channel is full, `try_send` drops the
  envelope and emits `ObsSinkDropped { tier, reason: "channel_full" }`.
- AUDIT: never silent-drops. See § 12.
- OTLP retry queue: separate cap (16384 default) so operators can tell
  "app too fast" (`channel_full`) from "network too slow"
  (`retry_queue_full`).

---

## 6. The schema registry

### 6.1 `EventSchema` (typed) vs `EventSchemaErased` (object-safe)

User code derives `EventSchema`:

```rust
pub trait EventSchema {
    const FULL_NAME:   &'static str;
    const TIER:        Tier;
    const DEFAULT_SEV: Severity;
    const FIELDS:      &'static [FieldMeta];
    const SCHEMA_HASH: u64;       // build-time constant
    fn encode_payload(&self, out: &mut Vec<u8>);
    fn project(&self, env: &mut ObsEnvelope, scope: Option<&ScopeFrame>);
    fn project_metrics(&self, mp: &mut MetricEmitter);
}
```

Sinks see the object-safe `EventSchemaErased`:

```rust
#[non_exhaustive]
pub trait EventSchemaErased: Send + Sync + 'static {
    fn full_name(&self) -> &'static str;
    fn schema_hash(&self) -> u64;
    fn tier(&self) -> Tier;
    fn default_sev(&self) -> Severity;
    fn fields(&self) -> &'static [FieldMeta];
    fn project_metrics(&self, env: &ObsEnvelope, mp: &mut MetricEmitter);
    fn decode_to_arrow_struct(&self, env: &ObsEnvelope) -> Option<arrow_array::StructArray>;
    fn decode_to_otlp_kv(&self, env: &ObsEnvelope) -> SmallVec<[KeyValue; 8]>;
    fn render_json(&self, env: &ObsEnvelope, out: &mut String);
    fn scrub_for_log(&self, env: &mut ObsEnvelope, scratch: &mut Vec<u8>) -> Result<()>;
    fn otel_attribute_view<'a>(&'a self, env: &'a ObsEnvelope) -> AttributeView<'a>;
}
```

The trait is sealed and `#[non_exhaustive]` — only codegen may
implement it (decision D49). This lets us add methods without breaking
hand-rolled implementations.

### 6.2 `linkme`-based registration

Each generated schema impl registers itself at link time:

```rust
#[linkme::distributed_slice(obs_core::registry::EVENT_SCHEMAS)]
#[linkme(crate = obs_core::__private::linkme)]
static __SCHEMA_OBS_REQUEST_COMPLETED: &dyn EventSchemaErased = &ObsRequestCompletedSchema;
```

Why `linkme` over `inventory`:

- Compile-time error on duplicate `full_name` (best possible failure mode).
- Reliable on musl-static, WASM, and stripped binaries — `inventory`
  walks ctor tables which can be GC'd.
- Zero startup walk; the slice is laid out by the linker.

### 6.3 The `SchemaRegistry`

Built once at observer init from `EVENT_SCHEMAS`:

```rust
pub struct SchemaRegistry {
    by_name: HashMap<&'static str, &'static dyn EventSchemaErased>,
    by_hash: HashMap<u64, &'static dyn EventSchemaErased>,
    arrow:   Arc<arrow_schema::Schema>,
}
```

Sinks look up `schema_hash` first (8-byte u64), `full_name` fallback
(decision D40). The fallback covers *foreign-producer envelopes* — the
CLI decoding batches from another service that linked different schemas.

### 6.4 LTO stripping

Aggressive LTO can drop the `EVENT_SCHEMAS` slice if nothing references
it. `obs::include_schemas!("myapi.v1")` exists to anchor the slice from
your crate root; `StandardObserver::build()` returns an error if the
slice is empty *and* events get emitted, telling you to call the macro.

### 6.5 Sink lookup miss path

When `lookup` misses (foreign-producer envelope, schema not linked into
this binary):

- OTLP body becomes `bytes(payload)`.
- Arrow row writes `payload_proto: bytes`.
- JSON renderer prints `{ "_unknown_schema": true, "schema_hash": ..., "payload_b64": "..." }`.
- Rate-limited `ObsSchemaUnknown` self-event surfaces it.

This path is for inspection (`obs decode`) only — production sink
chains assume schemas are linked.

---

## 7. Codegen pipeline

### 7.1 Two stages

```
.proto → buffa-build → wire types + FileDescriptorSet
                   ↓
                   ↓ (FDS)
                   ↓
         obs-build (walks DescriptorPool via buffa-reflect)
                   ↓
EventSchema impls + builders + Arrow fragments
+ JSON renderer + scrub dispatch + lint asserts
```

No `protoc` required; both stages are pure Rust. `obs-build` is the
*only* place where field annotations (`obs.v1.field`, `obs.v1.event`)
become Rust code.

### 7.2 Lints as `const _: () = assert!(...)`

Every L001..L013 lint is emitted as a `const _: () = { ... assert!(...) }`
block in the generated file, so violations fail `cargo build`. The
`obs lint` CLI command runs the same logic on `.proto` files outside
the build to give an early signal in CI.

### 7.3 Generated builder

`typed-builder` provides the marker-state machinery. Codegen calls into
it with a curated `#[builder(...)]` config so missing-required-field
errors point at `.emit()` rather than into the generated module.

### 7.4 Schema evolution

Enforced by `obs schema diff`:

| Change | Allowed? |
| --- | --- |
| Add a field | ✅ (additive — old rows NULL) |
| Remove a field | ⚠ deprecated; tag becomes reserved |
| Reuse a tag previously used | ❌ break (L008) |
| Change field type | ❌ break |
| Change `FieldKind` | ❌ break |
| Demote `Classification` (PII → INTERNAL) | ❌ banned |
| Promote `Classification` (INTERNAL → PII) | ✅ |
| Change `Tier` | ❌ break |

`obs diff base..HEAD` exits 2 on any breaking change. Wire this in CI.

### 7.5 Auxiliary traits

| Trait | Generated for | Purpose |
| --- | --- | --- |
| `BuildableTo<Args>` | every event | Enables `.emit()` on the typed-builder marker chain |
| `MetricEmitter` | every event | One closure per `MEASUREMENT` field |
| `FieldCapture` | bridge promoters | tracing-visitor adapter |
| `SpanCtx` | trace-context fields | Auto-fills `trace_id`/`span_id`/`parent_span_id` from scope |
| `EnumCount` | every `EnumLabel` enum | Compile-time variant count for L005 |

---

## 8. Scopes, scope frames, and auto-fill

### 8.1 The `ScopeFrame`

```rust
pub struct ScopeFrame {
    fields:        SmallVec<[ScopeField; 8]>,
    tail_buffer:   Option<RingBuffer<ObsEnvelope>>,   // 64-deep, scope! only
    parent_id:     Option<NonZeroU64>,
    span_id:       NonZeroU64,
    trace_id:      [u8; 16],
    sampled:       bool,
    started_ns:    u64,
}
```

- **`obs::scope!`** binds fields *and* a tail buffer.
- **`obs::context!`** binds fields only (no buffer).

Both are RAII guards — `Drop` pops the frame off the per-task scope
stack. On `Drop`, if any envelope in the tail buffer was tagged with
`Severity::Error` or higher, the buffer flushes to the observer.

### 8.2 Auto-fill mechanics

Auto-fill is **not** a builder default. Codegen routes default-fillable
fields through internal `Option<T>`:

```rust
// generated
fn project(&self, env: &mut ObsEnvelope, scope: Option<&ScopeFrame>) {
    let trace_id = self.trace_id_opt
        .clone()
        .unwrap_or_else(|| scope.and_then(|s| s.trace_id_str()).unwrap_or_default());
    ...
}
```

The user-facing setter is `impl Into<T>`, so explicit `.trace_id("")`
produces `Some("")` and **bypasses** scope. Omitted setter produces
`None` and inherits. This pinpoints the moment a developer chose to
override scope.

### 8.3 Scope is **not** a `tracing::Span`

- No start/end timestamps on the frame itself (you can derive them from
  Started/Completed event pairs if you want them).
- No `enter`/`exit` cycle.
- No post-hoc `Span::record`.
- No span tree maintained by `obs` — the relationship between scopes is
  *parent → child by stack position*, not by ID linkage.

For span-with-duration semantics, use `#[obs::instrument]` (which emits
`ObsFnExecuted`) or a Started/Completed event pair (which the OTLP
trace sink collapses into one OTel span).

---

## 9. Filter DSL

`obs::Filter` ports `tracing-subscriber::EnvFilter` grammar verbatim:

```
target_or_event[span_field=val]?[=level]?
```

Examples:

```
info
info,myapi::auth=debug
myapi.v1.ObsRequestCompleted=trace
myapi.v1.ObsRequestCompleted[route=admin]=trace
info,myapi::cache=trace,myapi.v1.ObsHealthcheck=off
```

### 9.1 Statics vs Dynamics

Just like `EnvFilter`, the parser splits clauses into:

- **Statics** — compiled to a flat lookup table at parse time; consulted
  on every callsite enabled-check.
- **Dynamics** — clauses with a field-value matcher. Evaluated against
  the projected envelope labels.

### 9.2 Field clauses

`[field=value]` matches against envelope **`labels` only** (LABEL kind).
ATTRIBUTE-kind fields can never be matched — `obs lint` warns when you
write a filter that names one. Enforcing this prevents a confusing
class of "filter looks right but matches nothing" bugs.

### 9.3 Precedence

```
tracing::EnvFilter (RUST_LOG, only if bridge installed)
    ↓
obs::Filter        (OBS_FILTER / obs.yaml `filter:`)
    ↓
per-sink filter    (rare; e.g. STDOUT only WARN+, OTLP everything)
```

---

## 10. Sampling order and `traceparent.sampled`

For each emit, in order:

1. **Inbound W3C `traceparent.sampled`** — if the active scope has a
   propagated sampled bit set, honour it (always emit if set, always
   drop if cleared). Mirrors OTel `ParentBasedSampler`. Opt out via
   `sampling.honour_traceparent_sampled = false`.
2. **Head sampler** — single `f64` compare per `(full_name, severity)`.
   Deterministic seed for reproducibility.
3. **Tail-on-error** — per-scope ring buffer; flushed when any sibling
   event hits ERROR or FATAL.

The order matters: tail-on-error only sees events that survived the
head sampler. It's a quality booster on top of head sampling, not a
replacement.

---

## 11. Sinks: the `ScrubbedEnvelope` contract

```rust
#[non_exhaustive]
pub trait Sink: Send + Sync + 'static {
    fn deliver<'a>(
        &'a self,
        env: ScrubbedEnvelope<'a>,
        registry: &'a SchemaRegistry,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

    fn flush<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }

    fn shutdown<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
}
```

### 11.1 `ScrubbedEnvelope<'_>`

```rust
pub struct ScrubbedEnvelope<'a> {
    inner:  &'a ObsEnvelope,
    scratch: &'a [u8],          // optional redacted-payload view
}
```

The lifetime ties the redacted payload to a worker-owned scratch
buffer. **Sinks must not extend the lifetime**. If a sink needs the
envelope longer than one `deliver` call (e.g. batched ClickHouse
INSERT), it must clone the relevant fields.

### 11.2 What a sink may and may not assume

✅ **May:**
- Look up `EventSchemaErased` by `schema_hash` or `full_name`.
- Decode payload to Arrow / OTLP key-values / JSON via the registry.
- Read all envelope fields (labels, severity, trace context, …).
- Buffer / batch / retry; sinks own their own backpressure.

❌ **Must not:**
- Hold `ScrubbedEnvelope<'_>` past the `deliver` call.
- Block the worker thread on synchronous IO; use tokio.
- Call `Observer::emit_envelope` directly (use `obs::emit!`; the
  re-entry guard makes synthesised events safe).
- Assume schema is registered — the lookup-miss path exists for a
  reason.

### 11.3 Writing a custom sink

```rust
use obs_core::{ScrubbedEnvelope, SchemaRegistry, Sink};
use std::pin::Pin;
use std::future::Future;

pub struct MyVendorSink { client: reqwest::Client, endpoint: String }

impl Sink for MyVendorSink {
    fn deliver<'a>(
        &'a self,
        env: ScrubbedEnvelope<'a>,
        registry: &'a SchemaRegistry,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let body = registry
            .by_hash(env.schema_hash())
            .map(|schema| {
                let mut s = String::new();
                schema.render_json(env.inner(), &mut s);
                s
            })
            .unwrap_or_else(|| serde_json::to_string(env.inner()).unwrap_or_default());

        Box::pin(async move {
            if let Err(e) = self.client.post(&self.endpoint).body(body).send().await {
                obs_sdk::emit!(WARN, ObsSinkFailed {
                    sink: "my_vendor".into(),
                    reason: e.to_string(),
                });
            }
        })
    }
}
```

Tests: write a `RecordingTransport`-style fake (see
`obs-clickhouse::transport::RecordingTransport` for a worked example).

---

## 12. The AUDIT spool

AUDIT events must never silent-drop. The flow:

1. **In-channel, fast path.** `try_send` to the AUDIT mpsc (cap
   `audit.channel_capacity`, default 1024).
2. **Bounded blocking.** If full, **block up to `audit.block_ms_max`**
   (100 ms default) waiting for capacity.
3. **Spool to disk.** If still full after the block, write the envelope
   to `${audit.spool_dir}/${YYYYMMDD-HHMMSS}-${pid}.audit.bin`. Format:
   length-prefixed buffa with per-record CRC32C. Emit `ObsAuditSpooled`.
4. **On observer init**, scan `spool_dir` for `*.audit.bin` files,
   replay them through the worker (FIFO by mtime), then delete. Emit
   `ObsAuditSpoolRecovered { count }`.
5. **If the spool itself is unwritable** (disk full, permission), the
   `audit.on_failure` policy fires:
   - `panic` (default) — `panic!()` so a supervised restart picks it up.
   - `abort` — `std::process::abort()` (no unwinding; fastest).
   - `warn_only` — emit `ObsAuditSpoolFailed`, **drop the AUDIT event**,
     and continue. Use only when an outer pipeline guarantees durability.

Spool format (binary, per record):

```
| 4-byte LE length | 4-byte LE CRC32C of payload | <length> bytes buffa-encoded ObsEnvelope |
```

CLI inspection:

```bash
obs decode --audit-spool /var/lib/myapi/audit-spool > audit.ndjson
```

### 12.1 Implementation notes

- The spool writer is `obs-audit-spool` (top-level crate). It opens
  files with `O_APPEND` and `fsync`s on every write; correctness over
  throughput. Throughput is acceptable because AUDIT is rare by design.
- Recovery skips files older than `audit.spool_max_age` (default 7d)
  and emits `ObsAuditSpoolDropped`; operator action required.

---

## 13. The `tracing` bridge

`obs-tracing-bridge` ships two halves; both can be installed
simultaneously.

### 13.1 Direction A: `tracing → obs`

`TracingToObsLayer` (default, layered) and
`TracingToObsSubscriber` (escape hatch when you can't use `Registry`).

The layer uses `tracing-subscriber::registry()` extensions for per-span
state — no parallel DashMap. Each `tracing::Event` becomes either:

- `ObsTracingForensicEvent` (default) — carries `target`, `module`,
  `source_loc`, `message`, and field map.
- A typed `Obs*` event — when a `register_typed::<E>(matcher, promoter)`
  was registered for the callsite identifier.

```rust
TracingToObsLayer::new()
    .with_field_promotions(
        FieldPromotions::new()
            .promote("tenant_id", Cardinality::Medium)
            .promote("route", Cardinality::Medium))
    .register_typed::<ObsHttpRequestCompleted>(
        TypedMatcher::new()
            .target("tower_http::trace::on_response")
            .field("status").field("latency"),
        |event, ctx, cap| {
            ObsHttpRequestCompleted::builder()
                .route(cap.string("route").unwrap_or_default())
                .status_class(parse_status(cap.u64("status").unwrap_or(0)))
                .latency_ms(cap.u64("latency").unwrap_or(0))
                .build()
        });
```

Keys on `tracing_core::callsite::Identifier`: O(1) cached; first
matcher to register wins on conflict (and emits
`ObsBridgeMatcherConflict`).

### 13.2 Direction B: `obs → tracing`

`ObsToTracingSink` is a normal `Sink` that synthesises a
`tracing::Event` per envelope. Useful when:

- You're adopting `obs` inside an existing `tracing-subscriber::fmt`
  host and want the typed events to flow through the same pretty
  printer.
- You want `tracing-opentelemetry` or `console-subscriber` to also see
  obs-emitted events.

`PayloadDecodeMode`:

- `Off` — only envelope fields synthesised.
- `DecodeKnown` — payload decoded for events whose schema is in the
  registry.
- `DecodeKnownAttributesOnly` — same but skip MEASUREMENT fields.

`SpanEmissionMode::OnScope` synthesises a span per `obs::scope!` frame.
**Combining `OnScope` + `OtlpTraceSink` produces duplicate OTel spans**
— the bridge logs `ObsConfigInconsistent` at init.

### 13.3 Loop break

Both layers tag synthesised events with target `obs.bridge` and set a
thread-local `IN_BRIDGE: Cell<bool>`. The other half checks both before
re-emitting. Combined with the `CAN_ENTER` re-entry guard, the loop
cannot self-amplify.

---

## 14. Callsite interning

`obs.yaml`:

```yaml
interning:
  mode: hybrid              # off | hybrid | compact
  refresh_interval_secs: 600
  refresh_event_count: 10000
```

Tokenises the bridge path's `target`/`module_path`/`file:line`/template
strings to a `callsite_id: u64` (`fixed64` field 15 on `ObsEnvelope`).

| Mode | Wire size vs Off | Decoder requirements |
| --- | --- | --- |
| `Off` | 100 % (full strings) | None |
| `Hybrid` | ~50 % | Optional registry; rendered message kept |
| `Compact` | ~25 % | Registry required to decode |

### 14.1 `callsite_id` generation

First 8 bytes of `BLAKE3(source, target, file, line, level, field_names, template)`.

- Deterministic across processes and restarts.
- Collision-resistant to <2⁻⁴⁴ for 1 M call sites.
- ~80 ns to compute.
- **`callsite_id == 0` is reserved** ("not interned"); the BLAKE3
  perturbation re-rolls from bytes 8..16 if the truncation hits zero
  (1-in-2⁶⁴).

### 14.2 The `ObsCallsiteRegistry`

Process-local DashMap keyed by `callsite_id`. Registration is
**synchronous on first sight**: emits `ObsCallsiteRegistered` (with
`SamplingReason::OVERRIDE`, bypasses sampler) before the data envelope.
Re-emission cadence (10 min OR 10 k events per callsite) keeps
downstream consumers in sync after restarts.

### 14.3 Direction-B reconstitution

When `ObsToTracingSink` reads an interned envelope, it looks up
`callsite_id` in the registry and synthesises `tracing::Metadata` with
the **original** `target` (not `obs.bridge`) so external tooling sees
no difference between "never interned" and "round-tripped" events.

---

## 15. Security: scrubber, classification, secrecy

### 15.1 Threat model

`obs` is a **boundary library**: in-process producers are trusted, but
downstream sinks (network OTLP, durable Parquet/ClickHouse) are not.
Compile-time annotations + runtime scrubbing keep SECRET / PII off
durable disk; operators must be able to audit what was redacted and
why.

### 15.2 Classification levels

| Level | Behaviour |
| --- | --- |
| `INTERNAL` (default) | No redaction. |
| `PII` | Redacted to `"[REDACTED:pii]"` keeping schema/row shape stable. **Cannot** be on a `LABEL` field (L002). |
| `SECRET` | Stripped entirely. **Cannot** appear on a `LOG` or `AUDIT` tier event (L003). Field type **must** be `secrecy::SecretString` / `SecretBox<T>` so the value cannot leak via `Debug`. |

### 15.3 The scrubber

`EventSchemaErased::scrub_for_log(env, scratch) -> Result<()>` runs in
the **worker** between sampler and sink chain:

- Writes redacted payload bytes into worker-owned `scratch` (no extra
  allocation in steady state).
- Original `env.payload` left untouched for non-durable sinks (e.g.
  metric counters that read only labels).
- Failure drops the envelope at the worker; unscrubbed payloads never
  reach a sink.

### 15.4 Bridge-side pattern redactor

For tracing events with no declared classification, the bridge runs a
default name-pattern redactor:

```
(?i)password|secret|token|api[_-]?key|authorization|cookie|ssn|credit[_-]?card|bearer
```

Matched field values become `"[REDACTED:bridge_pattern]"`. One-shot
`ObsBridgePiiSuspected` per field name.

### 15.5 Custom redactor

```rust
pub trait Redactor: Send + Sync + 'static {
    fn redact(&self, target: &str, field: &str, value: &str) -> RedactAction;
}

pub enum RedactAction {
    Keep,
    Replaced(Cow<'static, str>),
    Drop,
}
```

Plug via `TracingToObsLayer::with_redactor(...)`. Use for domain-specific
patterns (e.g. `phone_number`, internal account IDs).

### 15.6 What `obs` does NOT defend against

- Malicious in-process code that lies about classification — schema
  author trust is assumed.
- TLS to OTLP backends — that's the user's responsibility.
- Sink-side encryption at rest — operator decision per backend.

---

## 16. Performance budgets and bench harness

### 16.1 Budgets

Per-emit budgets on a 2024-class laptop (criterion gates fail on >10 %
regression):

| Bench | Path | Budget |
| --- | --- | --- |
| `bench_emit_noop` | No observer installed | ≤ 110 ns |
| `bench_emit_filtered` | Filter says drop | ≤ 25 ns |
| `bench_observer_resolution` | Three-tier, no override | ≤ 15 ns |
| `bench_with_observer_poll` | per-task override on every poll | ≤ 30 ns |
| `bench_emit_inmemory` | Full path, sink no-op | ≤ 1 µs P50 |
| `bench_emit_ndjson` | Full path, NDJSON sink | ≤ 1.5 µs P50 |
| `bench_scope_enter_exit` | scope! ↔ Drop | ≤ 100 ns |
| `bench_encode_payload` | buffa, 10 fields | ≤ 5 µs |
| `bench_registry_lookup` | by `schema_hash` | ≤ 15 ns |
| `bench_registry_lookup_by_name` | fallback | ≤ 60 ns |
| `bench_registry_lookup_miss` | foreign producer | ≤ 80 ns |
| `bench_registry_init` | 1000 schemas | ≤ 10 ms |
| `bench_scrub_for_log` | Clean | ≤ 100 ns |
| `bench_scrub_for_log` | 5 redact fields | ≤ 1.5 µs |
| `bench_bridge_to_obs` | tracing → obs | ≤ 3 µs |
| `bench_bridge_from_obs` | obs → tracing | ≤ 2.5 µs |
| `bench_intern_cold` | First sight | ≤ 4 µs |
| `bench_intern_warm` | Cached lookup | ≤ 2.5 µs |

Benches live in `crates/obs-core/benches/` and
`crates/obs-tracing-bridge/benches/`. Baselines committed as
`benches/baseline.json`. Update on release prep with `make bench-update`.

### 16.2 Profiling toolchain

```bash
cargo flamegraph --bench bench_emit_inmemory
samply record cargo bench --bench bench_emit_inmemory
cargo asm --rust 'obs_core::observer::ObsCallsite::enabled' > crates/obs-core/asm/enabled.s
```

PRs touching the macro-expansion path **must** update the asm snapshot
in `crates/obs-core/asm/` so reviewers can see register/instruction
changes.

### 16.3 Soak harness

`apps/soak` drives 50 k events/sec for 30 s (`make soak`) or 24 h
(`make soak-24h`). Asserts:

- `ObsSinkDropped` count == 0
- AUDIT spool count == 0 (assuming default sink config)
- Resident memory stays bounded (no monotonic growth past warm-up)

---

## 17. Testing strategy

### 17.1 Test pyramid

- **Unit** in `#[cfg(test)] mod tests` next to the code.
- **Integration** under `tests/` per crate.
- **Property tests** with `proptest` — proto roundtrip, scope trace_id
  propagation, codegen determinism, bridge roundtrip preserves
  target/level/string-fields.
- **Trybuild compile-error fixtures** — every L001..L013 lint has paired
  `.rs` + `.stderr` snapshot. CI fails on drift; `TRYBUILD=overwrite`
  regenerates.
- **Fuzz harnesses** under `crates/<crate>/fuzz/` (cargo-fuzz, nightly,
  excluded from the workspace `cargo build`).
- **Bench harnesses** under `crates/<crate>/benches/` (criterion).
- **Mock OTLP collector** in `obs_otel::test::MockOtelCollector` — a
  real `tonic` server that asserts on the OTLP wire shape, not SDK
  internals.

### 17.2 The dev-ergonomics suite

`crates/obs-sdk/tests/dev_ergonomics/`:

- `test_quickstart_60s.rs` — scaffolds + builds + runs a fixture crate.
- `test_compile_errors.rs` — trybuild snapshots for each lint.
- `test_no_observer_noop.rs` — no panic, no allocation beyond one
  atomic load.
- `test_hot_reload.rs` — write `obs.yaml`, send `SIGHUP`, verify next
  emit picks up the new sampling rate.
- `test_in_memory_observer.rs` — `assert_emitted!` matches partial
  fields; `wait_for` times out cleanly.
- `test_tracing_bridge.rs` — install bridge; emit `tracing::info!`;
  assert the result is an `ObsTracingForensicEvent`.
- `test_parallel_obs_test.rs` — 32 concurrent `#[obs::test]`s;
  per-thread / per-task slot prevents contamination.

CI runs this suite on every PR; failure here is treated as severely as
a clippy regression.

### 17.3 Conventions

- Tests must use `EventsConfig::builder()`, not load fixture YAML
  (file IO breaks `#[obs::test]` parallelism on Windows CI).
- Tests must not call `obs::install_observer` directly — `#[obs::test]`
  installs an `InMemoryObserver` automatically.
- For multi-tenant tests, build per-tenant observers and use
  `Future::with_observer` — never `with_observer_thread_local` across
  `.await`.

---

## 18. Contributing checklist

Before opening a PR:

```bash
cargo build --workspace --all-features
cargo test  --workspace --all-features
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
make lint-strict       # cargo clippy -W clippy::pedantic with curated allows
make audit             # cargo deny check advisories
make deny              # cargo deny check (advisories + bans + licenses + sources)
make check-format-ver  # envelope wire-shape lock (spec 90 § 3.3)
```

For changes that touch the hot path:

```bash
cargo bench --bench bench_emit_inmemory -- --save-baseline pr-NN
# compare against benches/baseline.json; > 10% regression blocks merge
```

For changes that touch generated code or the macro:

- Update `crates/obs-core/asm/enabled.s` (`cargo asm`).
- Update trybuild fixtures (`TRYBUILD=overwrite cargo test --test compile`).
- Re-run the dev-ergonomics suite end-to-end.

For schema changes (envelope, builtin events):

- Update `format_ver` in `obs-proto` if wire shape changes.
- Run `make check-format-ver`.
- Add the change to the migration matrix in `specs/15-config.md` if it
  affects reload semantics.

For new public API:

- Add `///` doc comments with examples.
- Re-export from `obs-sdk` if user-facing.
- Add a section to `docs/user-guide.md` and `docs/user-guide.zh-CN.md`.
- Update `docs/index.md`.

Project policy summary (full text in `/Users/tchen/projects/mycode/rust/obs/CLAUDE.md`):

- `#![forbid(unsafe_code)]` workspace-wide.
- No `unwrap()` / `expect()` / `panic!()` on user input paths
  (clippy denies workspace-wide on emit modules).
- No `tokio::fs` / `tokio::process` (CLI exempts itself locally).
- `thiserror` for library errors, `anyhow` for application errors.
- Tokio-only async runtime in v1.

---

## 19. Release workflow

1. **Open a release PR** that bumps `[workspace.package].version`.
2. **Run the full validation set:**
   ```bash
   make ci-full      # build + test + fmt + clippy + audit + deny + soak (30s)
   make soak-24h     # only before stamping a major release
   ```
3. **Update `CHANGELOG.md`** using `git-cliff` (`cliff.toml` is configured).
4. **Tag and publish** in dependency order: `obs-types` → `obs-proto`
   → `obs-macros` → `obs-core` → `obs-build` → sinks → bridge → tower
   → `obs-sdk` → `obs-cli`. The Makefile's `make publish-dry-run`
   target validates each `Cargo.toml` for missing fields before any
   `cargo publish`.
5. **Tag the repo** — `git tag v0.X.Y && git push --tags`.
6. **Update `docs/migration-from-tracing.md`** with anything user-facing
   that changed.

---

## 20. Reference: load-bearing decisions

The 49 numbered design decisions live in
[`specs/99-key-decisions.md`](../specs/99-key-decisions.md). The ones
most likely to surprise newcomers:

| # | Decision | Why |
| --- | --- | --- |
| **D1** | `buffa` over `prost` | First-class custom-option support; no `protoc`; faster reflective walks. |
| **D5** | Single sparse `obs_events` table | Cross-event joins are one query; new events append a column with NULLs in old rows. |
| **D7** | Three-tier observer (per-task → per-thread → global) | Multi-tenant correctness in async without a per-emit allocation. |
| **D9** | AUDIT may block ≤100ms then spool, never silent-drop | The whole reason AUDIT exists is durability; silent-drop would defeat it. |
| **D11** | Atomic Interest cache on the static callsite | tracing parity; removes the DashMap from the hot path. |
| **D14** | `Pin<Box<dyn Future>>` for `Observer`/`Sink` async | Required for object-safety; documented async-trait exception. |
| **D15** | Builder canonical, macro shorthand | Builder fits rust-analyzer chain-completion; macro fits 1–2 field events. |
| **D16** | `obs::scope!` is **not** a `tracing::Span` | A scope is a field allowlist + tail buffer, not a span tree node. |
| **D22** | Tail buffer scoped to scope! Drop, not request_id strings | Avoids leak class where a string key never gets cleaned up. |
| **D38** | `linkme` over `inventory` | Compile-time duplicate detection; reliable on musl/WASM. |
| **D39** | `ScrubbedEnvelope<'_>` as the worker→sink handoff | The type system guarantees sinks cannot see unscrubbed payload. |
| **D42** | Inbound `traceparent.sampled` honoured before local sampler | OTel `ParentBasedSampler` parity. |
| **D44** | Filter DSL ports EnvFilter grammar verbatim | `RUST_LOG`-style strings work unchanged. |
| **D47** | `with_observer_thread_local` is named verbosely | The async foot-gun should look wrong at the call site. |
| **D49** | `EventSchemaErased` is sealed `#[non_exhaustive]` | Allows method additions without breaking hand-rolled impls. |

Read all 49 before merging anything that changes a public-facing
contract.
