# Implementation Plan — Dependency-Ordered Build

Status: draft v1 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: every spec in this directory

This document is the *implementation* counterpart to
[90-roadmap.md](./90-roadmap.md). The roadmap is organised by
**user-visible feature** (M0 = "hello world emit", M1 = "schema-first
authoring", M2 = "sinks + OTel", …) with milestone exit criteria. This
plan is organised by **dependency order** — what blocks what, what can
be parallelised, and what has to be settled before code is written.

The two are complementary: the roadmap defines *when a user gets the
feature*; this plan defines *what an engineer builds in what order so
that no later phase requires retrofitting the foundation*.

> **When to read which?** Stakeholders read 90 ("when do I get
> ClickHouse?"). Engineers read 91 ("what do I implement next, and
> why?"). PMs read both.

## 0. Readiness assessment

The spec set is **mostly ready** to be implemented:

- Internally consistent (every cross-reference resolves; every
  `Sink::deliver` site agrees; every key decision has a paper trail).
- Every load-bearing contract is written down (envelope shape,
  `Observer` resolution, `EventSchemaErased`, `ScrubbedEnvelope`,
  pipeline order, AUDIT spool format).
- Design decisions have rationale (D1–D49 in
  [99-key-decisions.md](./99-key-decisions.md)).

Two caveats hold up "ready for implementation":

### 0.1 The M-1 risk spikes still need to run

Five assumptions in the spec set are *spec-shaped* until validated by
running code. They are listed in [90-roadmap.md § 2 M-1](./90-roadmap.md#m-1--spec-hardening--risk-spikes-week-1-before-coding)
and recapped here:

| Spike | What's at stake | Half-day cost |
| --- | --- | --- |
| `buffa-reflect` custom-option ergonomics on the FDS | Spec 12's authoring story; fallback is text parsing | 0.5 d |
| `linkme` distributed-slice on macOS arm64, Linux x86_64-musl, stripped release builds | Spec 14's entire registry mechanism | 0.5 d |
| `ArcSwap<Arc<dyn Trait>>` shape compiles with `Lazy::from_pointee`; benchmark | Spec 11's hot path | 0.5 d |
| `tokio::task_local!` cancellation behaviour with `select!` + `Drop` | Spec 11 § 8.1, spec 13 § 3 | 0.5 d |
| `notify` file-watcher reliability on macOS APFS | Spec 11 § 3.2 cross-platform reload | 0.5 d |

Each yields a 1-page memo committed under `./docs/research/`. If a
spike fails, the relevant spec gets a revised section *before* M0
begins. Skipping this is the highest-leverage way to cause weeks of
rework.

### 0.2 Two specs are referenced but never authored

These pieces are mentioned by other specs without ever being
formally defined:

- **`obs.yaml` schema** — referenced by 11 (config reload), 13
  (filter), 20 (sinks), 22 (sink configs), 60 (dev). Format is
  implied to be YAML mapping `EventsConfig` fields, but the canonical
  schema lives nowhere. A user can't ship without one.
- **The `EventsConfig` Rust type** — same problem; mentioned 8+
  times across specs but its `serde` shape is not pinned anywhere.

**Action**: write `15-config.md` *during* Phase 0 (alongside the
spikes). It is a 1-day spec; it unblocks every sink's config story.
Without it, every sink author will improvise their own config shape
and they will drift.

Beyond these two: ready.

## 1. Why dependency-ordering matters

The roadmap's milestone shape ("M0 = hello world", "M1 = authoring",
"M2 = sinks + OTel") is **right for stakeholder communication** but
**wrong for engineering execution**. Three concrete examples where
the dependency-correct order differs from the feature order:

1. **Schema registry must land in M0, not M2.** Every sink (M2/M3)
   consumes `&dyn EventSchemaErased` to decode payloads. Building
   `OtlpLogSink` in M2 against a not-yet-implemented registry means
   the sink's contract is provisional and gets retrofitted. Better:
   land the registry in M0 alongside the bare `Observer` so that
   even `StdoutSink` consumes it from day one.

2. **Three-tier observer resolution must land in M0, not be
   deferred.** Adding the per-task tier later is a refactor of every
   emit site (the `observer()` function changes signature implicitly
   when a new probe step is introduced). Pay the design cost once,
   in the foundation, not after 50 callers exist.

3. **`obs-tracing-bridge` Direction A motivates `obs-otel`.** Spec
   30 § 2.5 (auto-typing) is what justifies the typed-promotion
   path through OTLP. Building `obs-otel` first gives no signal on
   whether the typed shapes are right; building bridge first gives
   real third-party events to validate the OTLP mapping against.

The plan below applies these principles strictly.

## 2. Estimated total effort

**22–24 weeks of focused work for one senior developer.** Two
developers working in parallel from Phase 4 onward can compress to
~14 weeks, but the earlier phases serialise on the foundation and do
not parallelise cleanly.

Per CLAUDE.md project policy, "always shippable" means every phase
ends with `cargo build && cargo test && cargo clippy -D warnings &&
cargo +nightly fmt --check && cargo deny check` green. The estimates
below assume that overhead.

## 3. Phase 0 — risk retirement (week 1)

Single goal: settle the assumptions before writing production code.

| # | Deliverable | Lands in |
| --- | --- | --- |
| 0.1 | All five M-1 spikes from § 0.1 above | `./docs/research/spike-*.md` |
| 0.2 | `15-config.md` — `EventsConfig` Rust type, `obs.yaml` schema | new spec |
| 0.3 | Updated specs reflecting any spike findings (e.g. if `linkme` fails on a target, revise spec 14 to add the fallback) | inline edits |

**Exit gate**: every spike memo committed; specs updated; CI is
green on `obs-types` if you've started it (optional, see § 4 step 1
which can begin in week 1 if a developer has time).

## 4. Phase 1 — foundation (weeks 2–4)

Build the spine in strict dependency order. Each item blocks
everything underneath it.

```
                    obs-types  ◄── leaf, zero deps
                       │
                       ▼
                    obs-proto  ◄── envelope.proto, builtin.proto (all self-events)
                       │
                       ▼
                    obs-core spine
                    ┌──────────────────────────────────────┐
                    │ EventSchema trait                    │
                    │ EventSchemaErased + linkme registry  │ ← spec 14
                    │ ObsCallsite + atomic Interest        │
                    │ Three-tier observer resolution       │ ← spec 11 § 3.1
                    │ Observer / Sink / ScrubbedEnvelope   │
                    │ NoopObserver / InMemoryObserver      │
                    │ StandardObserver shell (single tier) │
                    └──────────────────────────────────────┘
                       │
            ┌──────────┴───────────┐
            ▼                      ▼
     obs-macros              obs-build
     #[derive(Event)]        proto-first codegen
            │                      │
            └──────────┬───────────┘
                       ▼
              StdoutSink (FormatterStyle::Full)
                       │
                       ▼
              apps/server hello world emit
                       │
                       ▼
              trybuild fixtures + bench_emit_noop
```

### Tasks in order

| # | Task | Spec | Effort |
| --- | --- | --- | --- |
| 1.1 | Workspace skeleton; `rust-toolchain.toml` pinned; `[workspace]` Cargo.toml; `#![forbid(unsafe_code)]` + `#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]` everywhere | 61 § 1 | 1 d |
| 1.2 | `obs-types` — all 7 enums (`Tier`, `Severity`, `FieldKind`, `Cardinality`, `Classification`, `MetricKind`, `SamplingReason`); const helpers (`is_label_compatible`, `cap`, `as_str`); `buffa::Enumeration` impls | 10, 61 § 2.1 | 2 d |
| 1.3 | `obs-proto` — `obs/v1/options.proto`, `envelope.proto`, `enums.proto`, `builtin.proto` (**all** self-events from 11 § 10, even those whose runtime emission lands in M2/M3); `build.rs` invokes `buffa-build`; FDS captured via `descriptor_set(...)` | 10, 61 § 2.2 | 3 d |
| 1.4 | `obs-core` registry — `EventSchemaErased` (sealed + `#[non_exhaustive]`); `linkme::distributed_slice EVENT_SCHEMAS`; `SchemaRegistry` (by_name + by_hash); `ScrubbedEnvelope<'_>` worker handoff | 14 | 3 d |
| 1.5 | `obs-core` callsite — `ObsCallsite { interest: AtomicU8, generation: AtomicU32 }`; `enabled(cur_gen)` short-circuit; const-fn constructor | 11 § 2 | 1 d |
| 1.6 | `obs-core` observer — three-tier resolution (`OBSERVER_GLOBAL` `Lazy<ArcSwap<Arc<dyn Observer>>>`, `OBSERVER_THREAD` `RefCell<Option<Arc<dyn Observer>>>`, `OBSERVER_TASK` `tokio::task_local!`); `OVERRIDE_COUNT` fast-flag; `CAN_ENTER` re-entry guard; `observer()`, `observer_weak()`, `WeakObserver`; `with_observer_thread_local`, `with_test_observer`; `WithObserver` trait + `Future::with_observer` returning `Instrumented<F>` | 11 § 3, 13 § 3 | 4 d |
| 1.7 | `obs-core` envelope — builder helper, projection helper (with auto-fill from active scope frame in `EventSchema::project` per 13 § 2.1) | 11 § 5 | 1 d |
| 1.8 | `obs-core` sinks scaffolding — `Sink` trait (`deliver(ScrubbedEnvelope<'_>)`); `NoopSink`; `InMemorySink` (bounded ring buffer + `handle.drain` / `wait_for` / `count`); `StandardObserver` with `SinkRouter` (single-tier wired, no AUDIT yet); `EventsConfig` shell with `ArcSwap` reload + `Observer::reload_filter` bumping `generation` | 11 § 4, 14 § 5 | 3 d |
| 1.9 | `obs-macros` — `#[derive(Event)]` MVP: parse `#[event(...)]` + `#[obs(...)]`; emit `EventSchema` impl + `EventSchemaErased` impl + `linkme::distributed_slice` registration + `typed-builder`-derived builder + L001/L002/L003/L011 const-eval lints (L011 reads `[workspace.metadata.obs] event_prefix`) | 12, 14 § 7 | 5 d |
| 1.10 | `obs-build` MVP — `Config` builder; calls `buffa-build`; reads custom options via `buffa-reflect::DescriptorPool`; emits the same `EventSchema`/`EventSchemaErased` artefacts as the macro path. **Byte-identical output for the same schema in either authoring mode** is the test bar. | 12 § 4, 14 § 7 | 5 d |
| 1.11 | `StdoutSink` with `FormatterStyle::Full` only | 20 § 3.6 | 2 d |
| 1.12 | `obs-sdk` façade with `dev` feature; `StandardObserver::dev()` shortcut | 61 § 2.11 | 1 d |
| 1.13 | `apps/server` — emits one `ObsHelloEmitted`, prints to stdout | 60 § 2 | 0.5 d |
| 1.14 | trybuild fixtures L001/L002/L003/L011; `bench_emit_noop` with CI gate at 50 ns | 71 § 4, 72 § 4 | 2 d |
| 1.15 | CI: `cargo build`, `cargo test`, `cargo clippy -D warnings`, `cargo +nightly fmt --check`, `cargo deny check` | CLAUDE.md | 1 d |

**Total Phase 1: 14–15 working days** (3 calendar weeks at 5 days /
week). Tasks 1.9 and 1.10 may run in parallel if a second developer
is available, shaving 5 days off the calendar.

**Exit criteria**: a fresh user runs `cargo run -p obs-server`,
sees an event on stdout. `cargo bench` shows ≤ 50 ns noop emit.
trybuild snapshots are green and committed. The atomic Interest
cache works. The three-tier observer resolution works (verified by
a unit test that installs different observers at each tier and
asserts which one wins).

## 5. Phase 2 — schema-first authoring & dev-ergonomics (weeks 5–7)

Closes the codegen and tooling story so a real user can adopt obs.

| # | Task | Spec | Effort |
| --- | --- | --- | --- |
| 2.1 | `obs-build` complete — all generated files (`schemas.rs`, `builders.rs`, `lints.rs`, `arrow_schema.rs` *fragments only* — Parquet schema assembly waits for Phase 4); schema hash baked as `u64` const | 12 § 3, 14 § 7 | 4 d |
| 2.2 | `obs::include_schemas!` macro | 61 § 2.5 | 1 d |
| 2.3 | Auxiliary trait surface — `BuildableTo`, `MetricEmitter`, `FieldCapture`, `SpanCtx`, `EnumCount` | 12 § 3.6 | 2 d |
| 2.4 | `apps/obs-cli` minimum — `init` (proto-first + rust-first scaffold), `validate`, `lint`, `schema show`, `version`, `completions`. The `--schemas`/`--schemas-fds` runtime descriptor-pool path per spec 14 § 10.1 so users can decode foreign batches | 50 §§ 3.1–3.5, 14 § 10.1 | 5 d |
| 2.5 | Compile-error quality — L001/L002/L003/L011 messages match the format in 60 § 6; trybuild snapshots pin them | 60 § 6, 72 § 4 | 3 d |
| 2.6 | `#[obs::test]` attribute — uses per-thread for sync, per-task for async (via `WithObserver::with_observer`); supports `Result<T, E>` returns; nested calls stack LIFO; documented spawn-from-test gotcha per 72 § 3 | 72 § 3 | 2 d |
| 2.7 | `assert_emitted!` partial-match macro | 60 § 8 | 1 d |
| 2.8 | Dev-erg test suite — `test_quickstart_60s`, `test_compile_errors`, `test_no_observer_noop`, `test_in_memory_observer`, `test_parallel_tests`, `test_registry_init`, `test_scrubbed_envelope`, `test_multi_tenant_observer` | 60 § 13, 72 § 7 | 4 d |

**Total Phase 2: 22 working days** (~4.5 calendar weeks).

**Exit criteria**: a fresh user can `cargo install obs-cli && obs
init demo && cd demo && cargo run` and see a typed event in 60
seconds. Compile errors for cardinality / PII violations are
pinpoint with `help:` lines. `cargo test` runs `#[obs::test]`
attributes in parallel without `serial_test`. The test that spawns
a task and forwards `obs::observer()` passes.

## 6. Phase 3 — sinks, sampling, OTel (weeks 8–13)

The largest phase. Eleven tasks, mostly serial because each depends
on the per-tier worker landing first.

| # | Task | Spec | Effort |
| --- | --- | --- | --- |
| 3.1 | Per-tier mpsc workers in `StandardObserver` — bounded channels per tier, drop counters + `ObsSinkDropped`, `Sink::flush` / `shutdown` lifecycle. **Skip AUDIT initially** (its own week below). | 11 § 4 | 4 d |
| 3.2 | Pipeline order complete — emit-thread steps (callsite check → project + auto-fill → head sampler → tail buffer → mpsc) and worker-thread steps (scrubber → ScrubbedEnvelope → SinkRouter → Sink::deliver). Each step gets a unit test. | 11 § 4.1 | 3 d |
| 3.3 | `obs::scope!`, `obs::context!`, `obs::Instrumented<F>` (carries scope + observer override jointly per 13 § 3); test the propagation matrix from 11 § 3.1 | 13 §§ 2, 3 | 4 d |
| 3.4 | Tail-on-error ring buffer in scope frame (capacity 64); RAII `Drop` flush-or-discard; async-cancellation safe per 11 § 8.1 | 13 § 6 | 2 d |
| 3.5 | Head sampler (per `(full_name, sev)` config); inbound W3C `traceparent.sampled` honoured before local sampler per 13 § 6 | 13 § 6 | 2 d |
| 3.6 | `obs::Filter` — port `tracing-subscriber::EnvFilter` parser (Statics/Dynamics split); field-value clauses match envelope `labels` per 13 § 7.1; `obs lint` warns on filter expressions referencing non-LABEL fields | 13 § 7 | 5 d |
| 3.7 | Sinks layer — `MakeWriter` trait + `StdoutWriter` / `StderrWriter` / `LevelSplitWriter` / `TeeWriter`; `RollingFileWriter` (size + time); `NonBlockingWriter` (background thread + `WorkerGuard`); `StdoutSink` with all four `FormatterStyle`s; `NdjsonFileSink` migrated onto `RollingFileWriter` | 20 §§ 3.3–3.6 | 6 d |
| 3.8 | `obs-otel` — `OtlpLogSink` (mapping per 20 § 2.3); `OtlpMetricSink` (per § 2.4 with bounded enum-LABEL attribute sets); `OtlpTraceSink` with the **three-pattern Span mapping** for Started/Completed pairs per 20 § 2.5; `from_env()` honours standard OTel env vars; `otlp_trio_from_env()`; mock OTLP collector for tests; two-layer backpressure (per-tier mpsc + retry queue) per 20 § 4.2 | 20, 61 § 2.6 | 8 d |
| 3.9 | `obs-tracing-bridge` Direction A — `TracingToObsLayer` (default forensic mapping); Level→Severity table; `FieldPromotions` allowlist with HLL cardinality enforcement; `DefaultPiiPatternRedactor` on by default; `SpanEventMode::Off` default with `ObsSpanCompleted` on close. **Direction B is deferred to Phase 4.** | 30 § 2 | 6 d |
| 3.10 | `#[obs::instrument]` attribute macro — single-event default (`ObsFnExecuted`); opt-in `enter = true` for two-event mode; respects `Instrumented<F>` for async fns | 13 § 5 | 3 d |
| 3.11 | Panic hook (`install_panic_hook()`); `obs::SpanTrace` for error capture | 11 § 6.1, 13 § 9 | 2 d |
| 3.12 | **AUDIT path** — bounded blocking + binary length-prefixed buffa spool format per 11 § 6.4; CRC32C tail-of-write recovery; FIFO drain ordering; recovery on observer init via `ObsAuditSpoolRecovered`. Test with deliberate `kill -9` between batches. | 11 § 6.4 | 5 d |
| 3.13 | CLI additions — `decode` (including `--audit-spool`), `tail` (`--file` / `--stdin` / `--otlp`), `query --from path/file.ndjson`, `doctor` | 50 §§ 3.7–3.10 | 4 d |
| 3.14 | Bench harness wired — every bench from 71 § 4; CI gate at 10% regression | 71 § 4 | 2 d |
| 3.15 | Dev-erg additions — `test_hot_reload`, `test_tracing_bridge`, `test_panic_hook`, `test_audit_spool_recovery` | 72 § 7 | 2 d |

**Total Phase 3: 58 working days** (~12 calendar weeks for one
developer; some sub-tasks within 3.7 / 3.8 / 3.9 parallelise across
two developers).

**Exit criteria**: `apps/server` running against a local OTel
collector produces logs/metrics/traces visible in Jaeger +
Prometheus + Loki. `tracing::info!()` from `tower-http` and `sqlx`
appears as `ObsTracingForensicEvent` in the same OTLP backend. Hot
reload via SIGHUP changes filter without restart. Per-tenant
observer via `Future::with_observer` works end-to-end across three
test tenants.

## 7. Phase 4 — analytics, governance, full bridge, interning (weeks 14–22)

Two tracks. With one developer, run them serially in the order
below. With two, run them in parallel.

### Track A — analytics & governance

| # | Task | Spec | Effort |
| --- | --- | --- | --- |
| 4A.1 | Generated unified Arrow schema (envelope + per-event struct fragments combined at observer init from `SchemaRegistry::arrow_schema()`) | 14 § 4, 22 | 3 d |
| 4A.2 | `obs-parquet` — `ParquetSink` with `ParquetLayout::Single` default, atomic `*.tmp` rename per 22 § 2.0a, partitioning by `service` + `date`, `opendal` for object stores, sweep `*.tmp` at observer init | 22, 61 § 2.7 | 6 d |
| 4A.3 | `obs-clickhouse` — `ClickHouseSink` with single-table DDL per 22 § 3, batched INSERTs, retry policy, `auto_migrate` opt-in (dev only) | 22, 61 § 2.8 | 5 d |
| 4A.4 | CLI — `diff`, `audit`, `migrate clickhouse`, `migrate parquet`, `query --from clickhouse://`, `query --from s3://`. Credential model from 50 § 3.9 (ambient env, never `obs.yaml`) | 50 §§ 3.6, 3.7, 3.9, 3.12, 3.13 | 6 d |
| 4A.5 | Lints L004 (MEASUREMENT no metric), L005 (enum overflow), L010 (forensic budget), L013 (LABEL conflict across crates) | 12 § 3.4, 50 § 3.4 | 4 d |
| 4A.6 | `obs::forensic!` macro formalised; `ObsForensicEvent` schema; per-callsite `governor::DefaultDirectRateLimiter`; `obs lint --strict` walks every callsite per 11 § 6.3 | 11 § 6.3, 12 § 6, 13 § 8 | 3 d |

### Track B — bridge completion + interning + tower

| # | Task | Spec | Effort |
| --- | --- | --- | --- |
| 4B.1 | `obs-tracing-bridge` Direction B — `ObsToTracingSink` consuming `ScrubbedEnvelope` per 30 § 3.2 (corrected signature); `DashMap<MetadataKey, &'static Metadata>` cache; two thread-local loop guards + `obs.bridge` reserved target; `SpanEmissionMode::Off` default, `OnScope` opt-in (with `ObsConfigInconsistent` warning when `OtlpTraceSink` also installed per 30 § 3.5); `PayloadDecodeMode` variants | 30 § 3 | 6 d |
| 4B.2 | Auto-typing — `TypedMatcher` predicate API; `register_typed::<E>(matcher, promote)` taking `&mut FieldCapture` per 30 § 2.5 (zero-alloc steady state); `Redactor` trait + `DefaultPiiPatternRedactor` | 30 §§ 2.5, 2.6 | 5 d |
| 4B.3 | Pre-warm registry — built-in list of well-known third-party callsites per 31 § 3.3; `with_prewarm(false)` opt-out | 31 § 3.3 | 2 d |
| 4B.4 | Callsite interning — `fixed64 callsite_id = 15` on envelope; `0` reserved; perturb-to-non-zero hashing path; `ObsCallsiteRegistry` (DashMap-based) on `StandardObserver`; `ObsCallsiteRegistered` self-event with `SamplingReason::OVERRIDE`; `ObsTracingInternedEvent` + `ObsForensicInternedEvent` payload types; `TracingToObsLayer::with_interning(InterningMode::{Off,Hybrid,Compact})`; reconstitution path in `ObsToTracingSink`; default `Off` in v1 | 31 | 8 d |
| 4B.5 | Bridge built-in events shipped in `builtin.proto` (already added in M0; verify they are emitted at runtime now): `ObsTracingForensicEvent`, `ObsSpanCompleted`, `ObsSpanEntered`, `ObsBridgePiiSuspected`, `ObsBridgeMatcherConflict`, `ObsBridgeLateSpanRecord`, `ObsBridgeNoDispatcher`, `ObsBridgeCallsiteUnresolved` | 30 § 11, 31 § 3.4 | 1 d |
| 4B.6 | Bridge test suite — `tracing_to_obs_basic`, `obs_to_tracing_basic`, `roundtrip_property` (proptest with the lossiness preconditions from 30 § 6), `no_infinite_loop` (1000-iter `--release` stress), `span_correlation`, `pii_redaction`, `auto_typed_promotion` | 30 § 6 | 4 d |
| 4B.7 | Bridge benches with CI gates per 30 § 7 / 71 § 3.2 | 30 § 7 | 2 d |
| 4B.8 | `obs-tower` — `ObsHttpLayer::server()`, `ObsHttpClientLayer::new()`, `ObsHttpRequestStarted` / `Completed` / `ClientStarted` / `ClientCompleted` schemas; `with_per_request_observer` for multi-tenant per 40 § 3.1; W3C propagator integration | 40, 61 § 2.10 | 5 d |
| 4B.9 | End-to-end multi-tenant integration test — three tenants, each with own OTLP endpoint + Parquet bucket; HTTP layer dispatches via header; assert events route correctly even under tokio task migration | 40 § 3.1 | 3 d |

**Track A total**: 27 working days. **Track B total**: 36 working
days. Serial: 63 days (~13 calendar weeks). Parallel: 36 days (~7
calendar weeks).

**Exit criteria**: end-to-end multi-sink demo with all features
active. `obs query` against ClickHouse and S3-backed Parquet.
Bridge round-trips events without loops (1000-iter `--release`
stress test green). Multi-tenant per-task observer demo running
with three tenants, distinct OTLP endpoints per tenant, validated
end-to-end.

## 8. Phase 5 — hardening, soak, RFC (weeks 23–26+)

| # | Task | Spec | Effort |
| --- | --- | --- | --- |
| 5.1 | 24-hour soak with `apps/server` at 50 k events/sec; 100+ distinct event types; all sinks active. Watch self-events; fix top 5 anomalies | 90 M4 | 5 d |
| 5.2 | Validate `ObsSinkDropped` stays at zero in steady state with recommended queue defaults | 90 M4 | 1 d |
| 5.3 | `cargo audit`, `cargo deny check`, `cargo clippy -D warnings -W clippy::pedantic` clean across workspace | CLAUDE.md | 2 d |
| 5.4 | Pre-built CLI binaries for `darwin-{x86_64,arm64}` and `linux-{x86_64,arm64}` via GitHub Releases | 90 M4 | 2 d |
| 5.5 | Lock envelope `format_ver = 1`; CI fails on any change to `obs-proto/proto/obs/v1/envelope.proto` without a `format_ver` bump | 90 § 3.3 | 1 d |
| 5.6 | Documentation pass — every public item has `///` doc; every crate has `//!` module doc; top-level `README.md` reflects the real install + emit + tail flow on a 2024-class laptop with cold cache | CLAUDE.md | 4 d |
| 5.7 | Migration guide for `tracing` users — `./docs/migration-from-tracing.md`, ~5 pages | 60 § 10 | 2 d |
| 5.8 | Public RFC, 4-week comment window before v1.0 stamp | 90 M4 | (calendar wait) |

**Total Phase 5: 17 working days + 4-week RFC wait.**

## 9. What makes this order *correct*, not just plausible

Three principles drove the ordering:

### 9.1 Schema registry before sinks

Every sink consumes `EventSchemaErased`. Building `OtlpLogSink`
(or even `ParquetSink`) before the registry means the sink's
contract is provisional — it gets retrofitted when the registry
lands and the `Sink::deliver` signature shifts. By landing spec 14
in Phase 1 (alongside the bare `Observer`), even `StdoutSink` is
written against the final `ScrubbedEnvelope<'_>` shape from day
one. No retrofits.

### 9.2 Three-tier observer + atomic Interest cache from day one

Both shape the hot path. Adding the per-task tier later is a
refactor of every emit site (the `observer()` resolver gets a new
probe step). Adding `OVERRIDE_COUNT` later means the perf baseline
shifts under your feet. Pay the design cost once, in M0, when
there's no callsite count to refactor.

### 9.3 AUDIT spool is its own week, late in M2

The AUDIT path looks like a small feature ("write a file when the
channel's full") but the recovery semantics, CRC validation, drain
ordering, and crash recovery are subtle. Bundling it with the
LOG/METRIC/TRACE workers either drags those down or hides bugs for
months. Task 3.12 isolates it.

### Corollary: skip nothing in Phase 1

The temptation in Phase 1 is to defer the schema registry and the
per-task observer tier as "advanced features." Both are
foundational. The cost of skipping them is paid 3× in Phase 3 when
every sink and every Instrumented future has to be reworked. **If
you find yourself wanting to skip task 1.4 or 1.6, reconsider the
whole plan rather than the tasks.**

## 10. When to break the dependency order

Three cases where deviating is acceptable:

1. **Trivially-parallelisable tasks** that don't touch the foundation
   (e.g. CLI subcommand `obs version` can be written any time after
   the workspace skeleton). These can be assigned to a second
   developer in any phase.

2. **Tooling that aids debugging** (e.g. `obs decode` for inspecting
   a binary `ObsBatch` is useful in M2 even though it's nominally an
   M3 task). Pull it forward when the time saved on debugging beats
   the time spent context-switching.

3. **Doc-only spec edits** in response to spike findings or user
   feedback. Always allowed, never blocked by a phase boundary.

Cases where deviating is NOT acceptable:

- **Skipping `linkme` for `inventory` "to save a day"**. The
  link-time-error-on-duplicates property is load-bearing for the
  multi-binary workspace story (spec 14 KD1). `inventory`'s silent
  last-write-wins is a different design.
- **Deferring the per-task observer tier "until multi-tenancy is a
  customer ask"**. The whole-process global is a one-way door; the
  per-task tier is a supported deployment shape from v1 because
  changing the resolver signature post-v1 is a major bump.
- **Building `OtlpLogSink` against a not-yet-implemented registry**.
  See § 9.1.

## 11. Status reporting

Per CLAUDE.md "Always shippable", every Friday produce:

- `cargo test` results across the workspace
- `cargo bench` deltas vs `benches/baseline.json` (10% regression
  fails the report, not just the CI job)
- spec changes committed during the week (for stakeholder visibility)
- the next phase's first uncompleted task

This is a half-page report per week. It is not a meeting; it is a
markdown commit under `./docs/status/YYYY-WW.md`. Stakeholders read
asynchronously.

## 12. Build dependencies

| Depends on | Provides |
| --- | --- |
| Every spec in this directory | Engineering execution order |
| [90-roadmap.md](./90-roadmap.md) | Stakeholder-facing milestone communication (this doc maps phases ↔ milestones one-to-one) |

Phase ↔ milestone mapping for stakeholders:

| Engineering phase | Roadmap milestone | Calendar weeks |
| --- | --- | --- |
| Phase 0 (risk retirement + 15-config.md) | M-1 | week 1 |
| Phase 1 (foundation) | M0 | weeks 2–4 |
| Phase 2 (authoring + dev-erg) | M1 | weeks 5–7 |
| Phase 3 (sinks + sampling + OTel + bridge A) | M2 | weeks 8–13 |
| Phase 4 (analytics + bridge B + interning + tower) | M3 | weeks 14–22 |
| Phase 5 (hardening + soak + RFC) | M4 | weeks 23–26+ |

Use roadmap names with stakeholders; use phase names with engineers.
