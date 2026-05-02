# Roadmap — Incremental Delivery

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: every spec in this directory

> v3 changes: re-organised around the post-split spec structure; each
> stage maps 1:1 to a small set of specs; cross-references retargeted;
> adopted the atomic-Interest cache, per-thread observer override,
> AUDIT-tier delivery policy, time-based rolling, `MakeWriter`,
> `Instrumented<F>`, single-event `#[obs::instrument]`, `SpanTrace`,
> filter-cache invalidation, and the `callsite_id == 0` fix into the
> milestone exit criteria.
>
> v2 changes: switched proto runtime to `buffa` / `buffa-build` /
> `buffa-reflect`; analytics sinks default to single sparse table.

## 0. Principles

- **Always shippable.** Every milestone leaves `cargo build`,
  `cargo test`, `cargo +nightly fmt --check`,
  `cargo clippy -- -D warnings` green.
- **Type-safety first.** Each milestone may defer features but never
  relaxes compile-time guarantees. We never ship a release that lets a
  HIGH-cardinality LABEL slip through.
- **Dogfood internally.** `apps/server` is updated alongside the SDK;
  if a milestone makes the example more painful, the design is wrong.
- **No incomplete code.** Per project CLAUDE.md: no `TODO`, no
  `unimplemented!`, no half-finished modules.
- **One milestone, one stack of specs.** Each stage below names the
  specs it implements; reading them in order gives a self-contained
  build target.
- **Honest calibration.** Earlier drafts said M0–M3 in 10 weeks for
  one developer. That estimate was wrong by 2–3×. The current
  estimates below are realistic for one focused developer; pad by
  another 50% for "developer also doing reviews / on-call /
  meetings". Stakeholders should plan for a v1 in 6 months, not 10
  weeks.

## 1. Build-order graph

```
                   00-prd
                     │
                     ▼
               10-data-model
                     │
                     ▼
               11-runtime-core
                     │
        ┌────────────┼────────────┬───────────────────┐
        ▼            ▼            ▼                   ▼
  12-schema-     13-emit-     14-schema-         70-security-and-
  and-codegen    scope-and-   registry           classification
                 filter           │
                     │            ▼
                     │      20-otel-and-sinks
                     │            │
                     │            ▼
                     │      22-analytics-storage
                     │            │
                     ▼            │
              30-tracing-bridge   │
                     │            │
                     ▼            │
              31-callsite-        │
              interning           │
                     │            │
        ┌────────────┴───┬────────┴────┐
        ▼                ▼             ▼
  40-http-          50-cli         71-performance-
  middleware                       budgets
                                       │
                                       ▼
                                 72-testing-strategy
                                       │
                                       ▼
                                 60-dev-ergonomics
                                       │
                                       ▼
                                 61-crates-and-features
```

Reference docs (read alongside, not in order): [80-glossary.md](./80-glossary.md),
[99-key-decisions.md](./99-key-decisions.md).

## 2. Milestones

### M-1 — Spec hardening + risk spikes (week 1, *before* coding)

Address the design gaps found in the architectural review: things
that would cost weeks to retrofit if discovered during M0–M3
implementation.

**Specs touched** (already done in this revision):
- New: [14-schema-registry.md](./14-schema-registry.md) — the
  object-safe schema registry that every sink depends on.
- Updated: [11-runtime-core.md](./11-runtime-core.md) (ArcSwap shape,
  pipeline order, AUDIT spool format + recovery, `WeakObserver`,
  cross-platform reload, forensic crate identification).
- Updated: [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md)
  (auto-fill is runtime, W3C `traceparent.sampled` propagation,
  EnvFilter grammar port).
- Updated: [20-otel-and-sinks.md](./20-otel-and-sinks.md) (OTLP Span
  for Started/Completed pairs, single-source Resource attrs, two-layer
  backpressure, `Sink::deliver(ScrubbedEnvelope<'_>)`).
- Updated: [22-analytics-storage.md](./22-analytics-storage.md)
  (atomic-rename Parquet, ClickHouse durability bound).
- Updated: [30-tracing-bridge.md](./30-tracing-bridge.md)
  (`register_typed` takes `&mut FieldCapture`, lossiness disclosure,
  `OnScope`+`OtlpTraceSink` warning).
- Updated: [31-callsite-interning.md](./31-callsite-interning.md)
  (startup pre-warm).
- Updated: [10-data-model.md](./10-data-model.md) (configurable prefix).
- Updated: [50-cli.md](./50-cli.md) (auth model, `--audit-spool`).
- Updated: [99-key-decisions.md](./99-key-decisions.md) (D38–D46).

**Risk spikes** (a half-day each; goal is ruling out unknowns):

- [ ] `buffa-reflect` custom-option reading: confirm the FDS walk
      addresses extension `(obs.v1.event)` ergonomically. Fallback
      plan: parse `.proto` text via `buffa-build`'s parser hook.
- [ ] `linkme` distributed-slice on macOS arm64, Linux x86_64-musl,
      and stripped release builds. Validate that stripped binaries
      still produce a non-empty `EVENT_SCHEMAS` slice.
- [ ] `ArcSwap<Arc<dyn Trait>>` shape compiles with `Lazy` and
      `from_pointee`; benchmark `observer()` returning `Arc<dyn>`
      vs returning a `Guard`. Decide on the public signature.
- [ ] `tokio::task_local!` propagation behaviour with
      `tokio::select!` + cancellation. Confirm the `Drop`-runs-on-
      cancel guarantee documented in [11-runtime-core.md § 8.1](./11-runtime-core.md#81-async-cancellation).
- [ ] `notify` file-watcher reliability on macOS APFS (a
      historically flaky combination). Decide whether to ship it as
      default or behind a feature flag.

**Exit criteria**: each spike yields a 1-page memo committed under
`./docs/research/`. If any spike fails, the relevant spec gets a
revised section before M0 begins.

**Estimate**: 1 week, calendar.

### M0 — Foundations (week 2–4)

**Specs implemented**: [10-data-model.md](./10-data-model.md),
[11-runtime-core.md](./11-runtime-core.md) (subset),
[12-schema-and-codegen.md](./12-schema-and-codegen.md) (proc-macro
MVP), [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md)
(emit + scope only).

**Exit criteria**: a "hello world" event compiles, emits, and renders
to stdout. No sinks beyond `Stdout` / `InMemory`. Buffa codegen
pipeline is wired and proves out custom-option reading. Atomic
`Interest` cache works on the static callsite. Per-thread observer
override slot enables parallel `cargo test`.

- [ ] Workspace skeleton; pin `rust-toolchain.toml` to current stable.
- [ ] `obs-types`: enums (Tier, Severity, FieldKind, Cardinality,
      Classification, MetricKind, SamplingReason). All
      `#![forbid(unsafe_code)]`. Implement `buffa::Enumeration` for
      each.
- [ ] `obs-proto`: `obs/v1/options.proto`, `envelope.proto`,
      `enums.proto`, `builtin.proto`. `build.rs` invokes
      `buffa_build::Config`; capture FDS via `descriptor_set(...)`.
      `ObsBatch.schemas` is `map<fixed64, string>`.
- [ ] `obs-core`:
  - `EventSchema` trait with `SCHEMA_HASH` const.
  - **`EventSchemaErased` object-safe trait + `linkme`-collected
    `EVENT_SCHEMAS` distributed slice + `SchemaRegistry`** ([14-schema-registry.md](./14-schema-registry.md)).
    Sinks added in M2/M3 will not work without this; it must land in M0.
  - `ObsCallsite` with `interest: AtomicU8`, `generation: AtomicU32`.
  - `ObsEnvelope` builder + projection helper.
  - `Observer` trait, `OBSERVER_GLOBAL` (`Lazy<ArcSwap<Arc<dyn Observer>>>`)
    + `OBSERVER_LOCAL` (`RefCell<Option<Arc<dyn Observer>>>`) +
    `with_test_observer` + `WeakObserver`.
  - `NoopObserver`, `InMemoryObserver`.
  - `StandardObserver` shell with `SinkRouter` (single-tier wired);
    `Sink::deliver(ScrubbedEnvelope<'_>)` from day one (no migration
    later).
  - `StdoutSink` (dev pretty-printer; `FormatterStyle::Full`).
  - `InMemorySink` (test harness).
  - `EventsConfig` + `ArcSwap` reload + `Observer::reload_filter()`
    that bumps `generation` (filter cache invalidation, see
    [13-emit-scope-and-filter.md § 7.3](./13-emit-scope-and-filter.md#73-cache-invalidation-on-reload)).
- [ ] `obs-macros`: `#[derive(Event)]` MVP
  - parses `#[event(...)]` and `#[obs(...)]`,
  - emits `EventSchema` impl,
  - emits `EventSchemaErased` impl + `#[linkme::distributed_slice(EVENT_SCHEMAS)]`
    registration ([14-schema-registry.md § 7](./14-schema-registry.md#7-codegen-contract-what-obs-build-emits)),
  - emits typed builder via `typed-builder`,
  - emits compile-time lints L001 (cardinality), L002 (PII on LABEL),
    L003 (SECRET on LOG/AUDIT), L011 (configurable-prefix naming;
    reads `[workspace.metadata.obs] event_prefix` per [10-data-model.md § 7.1](./10-data-model.md#71-configuring-the-prefix)).
- [ ] `obs-sdk` façade with `dev` feature; `StandardObserver::dev()`
      shortcut.
- [ ] `apps/server`: hello-world handler emitting `ObsHelloEmitted`.
- [ ] CI: `cargo build`, `cargo test`, `cargo clippy -D warnings`,
      `cargo +nightly fmt --check`, `cargo deny check`.

**Risks**: both retired in M-1 spikes (`buffa-reflect` extension
reads, `linkme` distributed slice reliability). M0 inherits no
unknown-unknowns from upstream tooling.

### M1 — Schema-first authoring + dev-erg (week 5–7)

**Specs implemented**: [12-schema-and-codegen.md](./12-schema-and-codegen.md)
(complete), [60-dev-ergonomics.md](./60-dev-ergonomics.md),
[72-testing-strategy.md](./72-testing-strategy.md) (trybuild + dev-
erg suite layout).

**Exit criteria**: a user can write `.proto` with `obs` annotations
and run `obs-build` in `build.rs` to generate Rust code, including
all lints. `obs init` scaffolds a working crate. trybuild fixtures
pin every lint message. `#[obs::test]` works under cargo's parallel
runner without `serial_test`.

- [ ] `obs-build`:
  - `Config` builder (files, includes, out_dir, extern_path,
    toggles, descriptor_source pass-through).
  - calls `buffa-build` for wire types + FDS.
  - reads custom options via `buffa-reflect::DescriptorPool`.
  - emits `obs/schemas.rs`, `obs/builders.rs`, `obs/lints.rs`,
    `obs/arrow_schema.rs` (fragments only at this stage).
  - schema hash baked in as `u64` constant ([10-data-model.md § 6](./10-data-model.md#6-envelope)).
- [ ] `obs-macros::include_schemas!` macro.
- [ ] Auxiliary trait surface ([12-schema-and-codegen.md § 3.6](./12-schema-and-codegen.md#36-auxiliary-trait-surface)):
      `BuildableTo`, `MetricEmitter`, `FieldCapture`, `SpanCtx`,
      `EnumCount`.
- [ ] `apps/obs-cli`:
  - `obs init` (proto-first and rust-first scaffold).
  - `obs validate <file>...`.
  - `obs lint --root <dir>`.
  - `obs schema show <full_name>`.
  - `obs version`.
  - `obs completions <shell>`.
- [ ] Compile-error quality work:
  - L001/L002/L003/L011 emit messages matching the format in
    [60-dev-ergonomics.md § 6](./60-dev-ergonomics.md#6-compile-error-quality).
  - trybuild cases pin the messages
    ([72-testing-strategy.md § 4](./72-testing-strategy.md#4-compile-error-fixtures-trybuild)).
- [ ] Test ergonomics:
  - `obs::test::assert_emitted!` macro.
  - `#[obs::test]` attribute that uses `with_test_observer` for
    parallel-safe per-thread observer ([72-testing-strategy.md § 3](./72-testing-strategy.md#3-the-obstest-attribute-and-parallel-test-ergonomics)).
- [ ] `crates/obs-sdk/tests/dev_ergonomics/`:
  - `test_quickstart_60s.rs`,
  - `test_compile_errors.rs`,
  - `test_no_observer_noop.rs`,
  - `test_in_memory_observer.rs`,
  - `test_parallel_tests.rs` (32 concurrent `#[obs::test]`s).
- [ ] Update `apps/server` to author one event in `.proto` and one
      via `#[derive(Event)]` to prove parity.

**Risks**: custom-option descriptor walking with `buffa-reflect` —
the spike from M0 confirms feasibility; this milestone makes it
ergonomic. If extension reads turn out to be brittle, fall back to
parsing the `.proto` text via `buffa-build`'s parser hook.

### M2 — Sinks, sampling, OTel parity (week 8–13)

**Specs implemented**: [11-runtime-core.md](./11-runtime-core.md)
(complete: workers + AUDIT policy + panic hook + payload caps),
[13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md)
(complete: scope + Instrumented + filter + forensic + SpanTrace),
[20-otel-and-sinks.md](./20-otel-and-sinks.md), part of
[30-tracing-bridge.md](./30-tracing-bridge.md) (Direction A only),
[71-performance-budgets.md](./71-performance-budgets.md) (bench
harness wired).

**Exit criteria**: running `apps/server` against a local OTel
collector produces logs, metrics, and traces that show up in any
OTel-compatible backend. `obs::scope!` + `Instrumented<F>` provide
automatic trace correlation including across `tokio::spawn`.
AUDIT-tier overflow spools to disk. Hot reload changes filter
without restart.

- [ ] Per-tier mpsc workers in `StandardObserver` ([11-runtime-core.md § 4](./11-runtime-core.md#4-per-tier-workers-and-sinks)):
  - one bounded channel + worker per tier,
  - drop counters on overflow + `obs.runtime.v1.ObsSinkDropped`,
  - AUDIT delivery policy: bounded blocking + spool ([11-runtime-core.md § 6.4](./11-runtime-core.md#64-audit-tier-delivery-policy)).
- [ ] Sampling ([13-emit-scope-and-filter.md § 6](./13-emit-scope-and-filter.md#6-sampling)):
  - head sampling per `(full_name, severity)` from config,
  - tail-on-error: `tokio::task_local!` ring buffer (capacity 64),
    bound to the `obs::scope!` `Drop` guard,
  - `obs::scope!` macro with field allowlist + automatic field
    propagation,
  - `obs::context!` macro (broadcasting only),
  - rate limiting per event (token bucket via `governor`).
- [ ] `obs::Instrumented<F>` future adapter ([13-emit-scope-and-filter.md § 3](./13-emit-scope-and-filter.md#3-obsinstrumentedf--async-scope-adapter)).
- [ ] `obs::Filter` (EnvFilter-equivalent DSL) + `OBS_FILTER` env var
      + filter-cache invalidation via `Observer::generation`.
- [ ] `obs::SpanTrace` for error capture ([13-emit-scope-and-filter.md § 9](./13-emit-scope-and-filter.md#9-obsspantrace--error-capture-with-scope-context)).
- [ ] Sinks ([20-otel-and-sinks.md § 3](./20-otel-and-sinks.md#3-sink-contract--catalogue)):
  - `MakeWriter` trait + `StdoutWriter` / `StderrWriter` /
    `LevelSplitWriter` / `TeeWriter`,
  - `RollingFileWriter` (size + time-based rolling),
  - `NonBlockingWriter` (background flush thread),
  - `StdoutSink` with `FormatterStyle::{Full,Compact,Pretty,Json}`,
  - `NdjsonFileSink` migrated onto `RollingFileWriter`.
- [ ] `obs-otel`:
  - `OtlpLogSink` (mapping per [20-otel-and-sinks.md § 2.3](./20-otel-and-sinks.md#23-to-otlp-logs)),
  - `OtlpMetricSink` (per § 2.4; enum LABELs become bounded
    attribute sets),
  - `OtlpTraceSink` (per § 2.5),
  - `otlp_trio_from_env()` convenience,
  - `ResourceAttrs` propagation ([11-runtime-core.md § 7](./11-runtime-core.md#7-service-identity)).
- [ ] `obs-tracing-bridge` Direction A — minimal viable
      ([30-tracing-bridge.md § 2](./30-tracing-bridge.md#2-direction-a--tracing--obs)):
  - `TracingToObsLayer` with default forensic mapping,
  - `Level → Severity` table,
  - `FieldPromotions` allowlist with HLL cardinality enforcement,
  - `DefaultPiiPatternRedactor` on by default,
  - `SpanEventMode::Off` default; `ObsSpanCompleted` on close.
- [ ] `#[obs::instrument]` attribute macro: single-event default
      (`ObsFnExecuted`); opt-in `enter = true` for two-event mode
      ([13-emit-scope-and-filter.md § 5](./13-emit-scope-and-filter.md#5-the-obsinstrument-attribute)).
- [ ] Panic hook ([11-runtime-core.md § 6.1](./11-runtime-core.md#61-panic-hook)).
- [ ] CLI:
  - `obs decode` (binary `ObsBatch` → NDJSON),
  - `obs tail --file | --stdin | --otlp`,
  - `obs query --from path/file.ndjson`,
  - `obs doctor`.
- [ ] Bench harness ([71-performance-budgets.md § 4](./71-performance-budgets.md#4-bench-harness)):
  - emit P50/P99 budget; CI gates 10% regression,
  - comparison against `tracing` + `serde_json` baseline.
- [ ] Dev-erg additions:
  - `test_hot_reload.rs`,
  - `test_tracing_bridge.rs`,
  - `test_panic_hook.rs`.

**Risks**: OTLP wire-shape conformance. Mitigation: integration test
suite runs against an in-process `tonic` mock OTel collector ([72-testing-strategy.md § 6](./72-testing-strategy.md#6-mock-otlp-collector)).

### M3 — Analytics, governance, full bridge, interning (week 14–22)

**Specs implemented**: [22-analytics-storage.md](./22-analytics-storage.md),
remainder of [30-tracing-bridge.md](./30-tracing-bridge.md)
(Direction B + auto-typing), [31-callsite-interning.md](./31-callsite-interning.md),
[40-http-middleware.md](./40-http-middleware.md),
[50-cli.md](./50-cli.md), [70-security-and-classification.md](./70-security-and-classification.md).

**Exit criteria**: schemas migrate into ClickHouse / Parquet via the
CLI, both targeting the **single sparse `obs_events` table**; CI
rejects breaking proto changes; forensic budget enforced; `obs query`
runs against ClickHouse and S3-backed Parquet; bridge round-trips
events through both directions without loops; callsite interning is
opt-in and reduces wire bytes per the budget table.

- [ ] `obs-parquet`:
  - generated unified Arrow schema (envelope + per-event struct
    fragments combined at observer init),
  - `ParquetSink` with `ParquetLayout::Single` default, rolling
    files, partitioning by `service` + `date`,
  - `opendal` integration for object-store targets,
  - opt-in `ParquetLayout::TablePerEvent`.
- [ ] `obs-clickhouse`:
  - `ClickHouseSink` writing to a single `obs_events` table per
    service,
  - DDL emitter for CLI consumption (single CREATE TABLE with
    sparse `Nested(...)` per event type),
  - `auto_migrate` opt-in (dev only).
- [ ] CLI:
  - `obs diff <baseline> <head>` with breaking-change exit code 2,
  - `obs audit` (forensic budget rollup, tracing-bridge usage,
    audit-tier coverage),
  - `obs migrate clickhouse` (single CREATE TABLE; ALTER on diff),
  - `obs migrate parquet` (unified Arrow schema JSON),
  - `obs query --from clickhouse://` and `--from s3://` (behind
    features),
  - `obs callsites dump | load | show <id>`,
  - `obs query --callsite <id>`.
- [ ] `obs-macros`:
  - lint L004 (MEASUREMENT missing metric annotation),
  - lint L005 (enum variants exceed declared cardinality cap),
  - lint L010 (forensic budget enforcement),
  - lint L013 (LABEL definition conflict across crates).
- [ ] `obs.v1.ObsForensicEvent` formalised; `obs::forensic!` macro.
- [ ] `obs-tracing-bridge` Direction B + advanced features
      ([30-tracing-bridge.md § 3](./30-tracing-bridge.md#3-direction-b--obs--tracing)):
  - `ObsToTracingSink` with `DashMap<MetadataKey, &'static Metadata>`
    cache,
  - Two thread-local loop guards + `obs.bridge` reserved target,
  - `SpanEmissionMode::Off` (default) + `OnScope` opt-in,
  - `PayloadDecodeMode::{Off, DecodeKnown, DecodeKnownAttributesOnly}`.
- [ ] `obs-tracing-bridge` auto-typing path:
  - `TypedMatcher` predicate API,
  - `register_typed::<E>(matcher, promote)` with cached
    per-callsite-id dispatch,
  - `FieldCapture` thread-local visitor,
  - `Redactor` trait + `DefaultPiiPatternRedactor`.
- [ ] `obs-tracing-bridge` test suite ([72-testing-strategy.md § 1](./72-testing-strategy.md#1-test-pyramid-by-crate)):
      `tracing_to_obs_basic`, `obs_to_tracing_basic`,
      `roundtrip_property` (proptest), `no_infinite_loop` (1000-iter
      release stress), `span_correlation`, `pii_redaction`,
      `auto_typed_promotion`.
- [ ] `obs-tracing-bridge` benches with CI gates per [71-performance-budgets.md § 3.2](./71-performance-budgets.md#32-bridge).
- [ ] Bridge built-in events shipped in `obs-proto/builtin.proto`:
      `ObsTracingForensicEvent`, `ObsSpanCompleted`, `ObsSpanEntered`,
      `ObsBridgePiiSuspected`, `ObsBridgeMatcherConflict`,
      `ObsBridgeLateSpanRecord`, `ObsBridgeNoDispatcher`.
- [ ] Callsite interning ([31-callsite-interning.md](./31-callsite-interning.md)):
  - `fixed64 callsite_id = 15;` on `ObsEnvelope`,
  - `0` reserved; hashing path perturbs to non-zero ([31-callsite-interning.md § 3.1](./31-callsite-interning.md#31-id-generation-blake3-truncated-to-64-bits)),
  - `ObsCallsiteRegistry` (DashMap-based) on `StandardObserver`,
  - `ObsCallsiteRegistered` self-event with `SamplingReason::OVERRIDE`,
  - `ObsTracingInternedEvent` + `ObsForensicInternedEvent` payload
    types,
  - `TracingToObsLayer::with_interning(InterningMode::{Off,Hybrid,Compact})`,
  - reconstitution path in `ObsToTracingSink`,
  - default mode is `Off` in v1.
- [ ] `obs-tower` ([40-http-middleware.md](./40-http-middleware.md)):
  - `ObsHttpLayer::server()` and `ObsHttpClientLayer::new()`,
  - `ObsHttpRequestStarted` / `ObsHttpRequestCompleted` /
    `ObsHttpClientStarted` / `ObsHttpClientCompleted` schemas.
- [ ] End-to-end integration: `apps/server` with realistic handler
      emitting `ObsRequestStarted` / `ObsRequestCompleted` /
      `ObsUpstreamFailed`, sinks routed to OTLP + Parquet +
      ClickHouse + `ObsToTracingSink`, third-party `tracing` events
      from `tower-http` and `sqlx` lifted via `register_typed` to
      `ObsHttpRequestCompleted` / `ObsDbQueryExecuted`, all
      dashboards verified.
- [ ] Final dev-erg pass: re-run all dev-erg tests including
      `assert_emitted!` patterns and quickstart timing.

**Risks**: proto schema diff requires deterministic comparison;
depend on the FDS round-trip via `buffa-reflect` and golden-file
tests under `crates/obs-cli/tests/diff/`.

### M4 — Hardening + soak (week 23–24)

The cushion between "code complete" and v1.0. Without it, the
project ships with N latent issues that show up under sustained
load.

- [ ] Run `apps/server` under realistic load: 50 k events/sec,
      100+ distinct event types, **24-hour soak**, all sinks active
      (OTLP + Parquet + ClickHouse + bridge in both directions).
- [ ] Watch the SDK self-events; fix the top 5 anomalies.
- [ ] Validate `obs.runtime.v1.ObsSinkDropped` stays at zero in the
      steady state with the recommended queue defaults.
- [ ] `cargo audit`, `cargo deny check`, `cargo clippy -D warnings -W clippy::pedantic`
      clean across the workspace.
- [ ] Pre-built CLI binaries for `darwin-{x86_64,arm64}` and
      `linux-{x86_64,arm64}` via GitHub Releases.
- [ ] Lock the envelope `format_ver = 1` and freeze the wire shape;
      fail CI on any change to `obs-proto/proto/obs/v1/envelope.proto`
      without a `format_ver` bump.
- [ ] Documentation pass: every public item has a `///` doc; every
      crate has a module `//!` doc; the top-level `README.md`
      reflects the real install + emit + tail flow on a 2024-class
      laptop with cold cache.
- [ ] Migration guide for `tracing` users (5-page doc) committed
      under `./docs/migration-from-tracing.md`.
- [ ] Public RFC: solicit feedback on the API surface before
      stamping v1.0 (4-week comment window).

**Estimate**: 2 weeks calendar, plus 4 weeks RFC comment window.

### M-future — Out-of-scope for v1, tracked

| Item | Trigger |
| --- | --- |
| Cross-language SDKs (Go, Python, TypeScript) | adoption signal from at least one team |
| Cluster-wide sampling agreement | sampling overhead becomes a real bottleneck |
| Schema registry HTTP service | > 5 services sharing the same schemas |
| `obs query` against Iceberg | analytics team request |
| GUI for `obs schema show` / `obs diff` | request from non-Rust users |
| In-tree DuckDB sink | usage data justifies it |
| Cross-process callsite registry sharing (Unix socket) | sustained per-process registration storms |
| Default interning mode flip Off → Hybrid | v1.1 once registry-snapshot tooling has soaked |

## 3. Cross-cutting concerns

### 3.1 Performance gates

Per [71-performance-budgets.md § 5](./71-performance-budgets.md#5-ci-gates):
`cargo bench --bench emit_hot_path` runs against
`benches/baseline.json`; > 10% regression on any path fails the job.

### 3.2 Documentation

Every milestone closes its docs as part of "done":

- module-level `//!` docs that explain the crate's role,
- public types / functions have `///` doc comments with `# Examples`,
- the `apps/server` README walks through emit, scope, config,
- the top-level `README.md` reflects the latest user-facing API once
  M2 lands,
- the dev-ergonomics doc is kept consistent with what actually
  compiles in `crates/obs-sdk/tests/dev_ergonomics/`.

### 3.3 Compatibility & versioning

- Pre-`1.0`: minor bumps may break any API; the changelog calls them
  out.
- The envelope `format_ver` field is bumped only when the wire shape
  changes. M0 → M3 expectation: stays at `1`.
- `obs-types` enum additions are non-breaking; reordering / removing
  variants requires a major bump and a `migration.md` entry.
- Buffa upstream is pinned in `[workspace.dependencies]`; we do not
  float across buffa minor releases without an integration test pass.

## 4. Risks & open decisions

| Risk / decision | Status | Notes |
| --- | --- | --- |
| `buffa-reflect` custom-option ergonomics on extensions | open | Spike scheduled in M0 day 1 |
| ArcSwap vs `tokio::sync::watch` for config | locked | ArcSwap for sync-only readers |
| Stable enum count vs nightly `variant_count` | locked | Codegen emits `const COUNT: usize = N` from descriptor |
| Whether to ship a Prom-direct sink in M2 | deferred | OTLP → Prom collector is the supported path |
| Tail-buffer memory pressure under burst | open | Cap configurable; default 64 envelopes per scope |
| Naming of `obs.v1.options` field-number range | locked | 80000–89999 reserved |
| Single-table column count under wide-event explosion | open | Bench at M3 with 100+ event types |
| `Obs*` prefix lint default level | open | Defaults to **error** under `--strict`, warning otherwise |
| `SpanEmissionMode::OnScope` + `OtlpTraceSink` double-OTel-span | deferred to v1.1 | Document recommends OnScope only in dev. See [99-key-decisions.md](./99-key-decisions.md) "Open / deferred" |
| Bridge `Visit::record_debug` allocator cost | accepted | ~150 ns/field via `format!`; within budget |
| Callsite interning default mode | locked for v1 | `Off`. Flip-default is a v1.1 question |
| `u64` id collision under > 1 M ids | open | Birthday bound is < 2⁻⁴⁴ at 1 M; CLI lint warns at > 10⁵ |
| `ObsCallsiteRegistered` re-emit storm at startup | accepted | Bounded by per-tier mpsc rate-limiting |
| Cross-process registry sharing | deferred to v1.1 | Considered a Unix-socket sidecar registry |
| AUDIT spool unwritable behaviour | locked | `EventsConfig.audit.on_failure ∈ {panic, abort, warn_only}` ([11-runtime-core.md § 6.4](./11-runtime-core.md#64-audit-tier-delivery-policy)) |

## 5. Definition of done (v1.0)

- Every spec doc in this directory has at least one corresponding
  test or example demonstrating the described behaviour.
- A new contributor can `git clone`,
  `cargo run -p obs-cli -- init demo`, `cargo run -p server`, see
  events in stdout, point at OTLP endpoint, see them in Jaeger /
  Prometheus / Loki, and read a Parquet file with DuckDB.
- The `apps/server` example is the canonical reference and is
  exercised in every CI run.
- The CLI ships pre-built binaries for darwin-{x86_64,arm64} and
  linux-{x86_64,arm64} via GitHub Releases.
- `cargo audit`, `cargo deny check`, `cargo clippy -D warnings -W clippy::pedantic`
  all pass.
- `crates/obs-sdk/tests/dev_ergonomics/` is green; the timing
  assertions in `test_quickstart_60s.rs` are met on a 2024-class
  laptop without warm caches.
