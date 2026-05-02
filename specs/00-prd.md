# PRD — Wide-Event Observability for Rust

Status: draft v2 · Owner: obs-core · Last updated: 2026-05-02

> v2 changes: promotes **analytics** from a side-effect to a first-class
> goal alongside log/metric/trace; adopts the `Obs*` event-name
> convention; tightens scope to Rust-only for v1; switches the proto
> runtime story to `buffa`.

## 1. Problem

Rust backends today reach for four uncoordinated tools:

- **`tracing`** for diagnostic events and spans — untyped at the wire
  boundary, serialized as ad-hoc JSON, with no contract on field
  names, types, or cardinality. Field drift across services is
  endemic; the same concept is variously `user_id`, `userId`, `uid`.
- **`metrics` / `prometheus`** for counters and histograms — strong on
  aggregation, but every label is a cardinality risk that the type
  system cannot catch. High-cardinality labels (`user_id`, `request_id`)
  leaking into a label set is a recurring production-outage pattern.
- **`opentelemetry-rust`** for spans/metrics/logs — solves transport,
  but the data model is still untyped at the call site
  (`KeyValue::new(key, value)`), and a single business operation
  produces three separate OTLP payloads that share no schema.
- **A separate analytics SDK** (Mixpanel, Amplitude, Segment, or a
  hand-rolled "events" pipeline) for product analytics — yet another
  emission API, yet another schema, yet another set of dashboards
  that drift from the operational ones.

The result is the **"Great Observability Lie"**: a service emits
gigabytes of signals, the dashboards are green, the analytics funnel
looks fine, and yet on incident a responder cannot join a slow log
line to its metric, its trace, *or* its analytics row because the
dimensions don't line up.

## 2. Vision

A single, schema-first SDK in which **one event** is the unit of
observation:

```rust
// Canonical emit form: typed builder, RA-friendly, refactor-trivial.
ObsRequestCompleted::builder()
    .route(Route::ListUsers)        // LABEL — bounded enum, becomes metric dim
    .status(Status::Ok)             // LABEL — bounded enum
    .tenant_id(tenant)              // LABEL — bounded by config
    .user_id(uid)                   // ATTRIBUTE — high card, NOT a metric dim
    .latency_ms(elapsed.as_millis() as u64)  // MEASUREMENT
    // .trace_id auto-filled from obs::scope!
    .emit();

// Shorthand for terse events:
obs::emit!(ObsHelloEmitted { who: Audience::World });
obs::emit!(Severity::Warn, ObsUpstreamFailed { route, error_kind });
```

That single call:

- writes one structured log record (LOGS sink),
- increments `request_completed_total{route, status, tenant_id}`
  (METRIC sink, cardinality-safe by construction),
- emits a span `request.completed` with attributes (TRACE sink),
- writes one row of the unified `obs_events` table for OLAP analytics.

The schema is **defined once in `.proto`** (or in a Rust struct via
`#[derive(Event)]`), with field-level annotations that the build
system enforces at compile time. Adding a high-cardinality label to a
metric dimension is a build error, not a 3 a.m. page.

## 3. Goals

| # | Goal | Measure |
| --- | --- | --- |
| G1 | Match `tracing` ergonomics at the call site | `≤ 2 lines of code` to emit a typed event vs `1 line` for `info!()`; `obs::scope!` ≈ `tracing::span` |
| G2 | Make illegal observability states unrepresentable | Cardinality, PII, and tier violations fail `cargo build`, not at runtime |
| G3 | Native OpenTelemetry interop | A wide event projects into OTLP logs/metrics/traces with zero conversion code in user services |
| G4 | First-class analytics | Auto-generated unified Arrow/Parquet schema for the `obs_events` table; one event ≈ one row; OLAP queries are first-class, not an afterthought |
| G5 | Pluggable sinks without app changes | Swap OTLP → ClickHouse → Parquet via config; user code unchanged |
| G6 | Hot path overhead ≤ `tracing` + `serde_json` | < 1 µs P50 to build, project, and enqueue an event on a modern x86 core |
| G7 | AI-friendly authoring | Schemas are self-describing; codegen output is deterministic; lint errors are explicit and actionable; `Obs*` prefix gives unambiguous identity to events in code |
| G8 | Single emission ≡ analytics row | Engineering and product analytics share one schema and one storage table; no separate "events SDK" required |

## 4. Non-goals

- **Not a storage backend.** We emit into existing systems (OTLP
  collectors, ClickHouse, GreptimeDB, Parquet on object stores). We do
  not build a TSDB.
- **Not a UI.** Visualisation is the backend's job (Grafana, Honeycomb,
  etc.).
- **Not an agent.** The Vector / OpenTelemetry Collector ecosystem
  already does this well. We are the **emit-side SDK** and a small set
  of **direct sinks**.
- **Not a `tracing` killer in disguise.** `tracing` remains valid for
  ad-hoc diagnostics; we ship a one-way bridge that lifts `tracing`
  events into `ObsTracingForensicEvent` for migration, but coexistence
  is a first-class state.
- **No JSON-on-the-wire as a default.** Internal transport is
  protobuf (via `buffa`); human-readable JSON is a *renderer*, not a
  serialization.
- **Not multi-language in v1.** Schemas are language-neutral; the SDK
  and toolchain are Rust-only. Cross-language SDK work is gated on
  demonstrated demand from a non-Rust consumer.

## 5. Users & use cases

### 5.1 Application engineer (primary)

Wants to instrument a request handler in a few lines and have it Just
Work across logs, metrics, traces, and analytics. Today reaches for
`info!()` + `metrics!()` + a separate analytics SDK and accepts the
duplication. With `obs`, declares the event schema once in `.proto`,
calls `.emit()`, and gets all four outputs.

### 5.2 Platform/SRE engineer

Wants a global lever to (a) cap cardinality, (b) sample by event type
or severity, (c) redact PII, (d) drop SECRET-classified fields before
they hit durable storage. Edits a YAML config; the running service
reloads via `ArcSwap` without restart.

### 5.3 Data engineer / analyst

Wants typed Parquet rows in object storage to query with DuckDB /
Trino / Iceberg / ClickHouse, *and* wants those rows to share a
schema with the operational stream, not drift from it. With `obs`,
the unified `obs_events` Arrow schema is generated from the same
`.proto`, and product analytics queries hit the same data the SRE
sees in Grafana.

### 5.4 Product analyst

Wants funnels, retention, cohorts. With `obs`, those queries are SQL
against `obs_events` filtered by `full_name` and `labels`. There is
no "track this event" plumbing to install — every emit is already an
analytics row.

### 5.5 Library author

Wants to instrument a library crate without forcing a full
observability stack on downstream users. With `obs`, depends on
`obs-sdk` only; if no observer is installed the emissions are no-ops
with the cost of one atomic load.

### 5.6 AI coding agent (explicit)

Generates instrumentation by reading the `.proto` schema. Strong
types, deterministic codegen, and the `Obs*` naming convention mean
the agent cannot invent a field name or accidentally add an
unbounded label — the build fails and the agent self-corrects. This
is designed for, not retrofitted to.

## 6. Success metrics

| Metric | Baseline (`tracing` + `metrics` + OTLP + Mixpanel-class SDK) | Target |
| --- | --- | --- |
| LOC per emit site (log + metric + analytics) | 4 (one per signal) | 1 |
| Mean wire bytes per event @ 30 fields | ~650 (JSON over OTLP/HTTP) | < 250 (buffa over OTLP/gRPC) |
| Hot-path P50 (build + project + enqueue) | 1.5–3 µs | < 1 µs |
| Time-to-detect schema drift in CI | hours/days (post-deploy) | seconds (`cargo build` failure) |
| Cardinality incidents per quarter (label-explosion-induced OOM) | > 0 (industry baseline) | 0 |
| Analytics tables to migrate when adding an event type | 1 per type | 0 (additive struct column) |

## 7. Constraints

- **Rust 2024, MSRV pinned to current stable (1.85+).** No nightly
  features.
- **`#![forbid(unsafe_code)]`** in every workspace crate (per project
  CLAUDE.md).
- **Tokio only** as the async runtime in v1; sinks must be
  non-blocking on the emit path. (smol/async-std support is gated on
  demand.)
- **`buffa`** for proto wire types and `buffa-reflect` for descriptor
  walking (custom-option support is first-class).
- **OpenTelemetry data-model compatibility**, not just transport.
  Service identity goes on the OTel `Resource` (set once); per-event
  attributes never duplicate it. Severity maps onto OTLP
  `SeverityNumber` 1–24 buckets. Histogram bounds are honoured as
  explicit boundaries.
- **W3C Trace Context** is the cross-process correlation contract
  (`traceparent`, `tracestate`); HTTP middleware lives in `obs-tower`.
- **Backward-compatible schema evolution.** Adding a field is
  non-breaking; removing or retyping is a build-time error against
  committed schemas.
- **Single sparse table** is the default analytical layout; per-event
  tables are opt-in.
- **Builder-first ergonomics.** `Type::builder().xxx().emit()` is the
  canonical call form; `obs::emit!(...)` is shorthand for terse cases.

## 8. Out-of-scope (for v1)

- Cross-language SDKs (Go, Python, TypeScript). The `.proto` schemas
  are portable; the `obs::emit` ergonomics are Rust-only in v1.
- Built-in distributed sampling coordinator. Per-process head + tail
  sampling ships in v1; cluster-wide sampling agreement is a roadmap
  item.
- A bundled UI / query frontend. We integrate with what exists.
- Built-in network egress to proprietary vendors. OTLP is the
  universal contract; vendor-specific sinks live in third-party
  crates.

## 9. Risks & mitigations

| Risk | Mitigation |
| --- | --- |
| Up-front schema cost feels heavy vs. `info!()` | (a) `#[derive(Event)]` on a plain Rust struct as an alternative to writing `.proto` by hand; (b) AI-driven schema authoring is well-supported by the strict contract; (c) `obs::forensic!()` escape hatch with per-crate budget; (d) dedicated dev-ergonomics spec |
| OTel data model is a moving target | Pin to OTLP `1.x`; map at the sink layer, not the core; keep proto schema independent of OTLP shape |
| Codegen explosion in build times | `buffa-build` is fast (no protoc); outputs cached in `target/`; benchmarks gate any regression > 10% |
| Single-table sparseness explodes column count | New events are additive; ClickHouse/Parquet handle hundreds of sparse columns efficiently; opt-in per-event tables exist for the worst-case |
| Lock-in to our wire format | Internal transport is plain protobuf; users can read raw batches with any proto-aware tool |
| Adoption friction in mixed `tracing` codebases | `obs-tracing-bridge` lifts `tracing` events into `ObsTracingForensicEvent` for incremental migration |
| `Obs*` prefix feels obtrusive | Lint defaults to warning, becomes error under `--strict`; teams can disable via `--allow L011`; the visual-greppability win compounds at scale |

## 10. Open questions

- Default sampling policy out of the box: head-only, or head +
  tail-on-error? (Leaning: tail-on-error by default, configurable
  off.)
- Is there a place for a thin `tracing-subscriber`-shaped facade for
  users who want `obs` data plane but `tracing` macros? (Leaning:
  yes, behind a feature flag, but not the recommended path.)
- For the analytics use case, is DuckDB-as-a-sink worth shipping
  in-tree, or does Parquet + external DuckDB cover it? (Leaning:
  Parquet only in v1; revisit with usage data.)
