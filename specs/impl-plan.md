# Impl Plan — Phased Delivery

Status: draft v2 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: all design docs in this directory

> v2 changes: switched proto runtime to `buffa` / `buffa-build` /
> `buffa-reflect`; analytics sinks default to single sparse table;
> dev-ergonomics surfacing is a first-class M1 deliverable
> (scaffolding, error quality, in-memory observer); milestone exit
> criteria reference the new dev-erg test suite.

## 0. Principles

- **Always shippable.** Every milestone leaves `cargo build`,
  `cargo test`, `cargo +nightly fmt`, `cargo clippy -- -D warnings`
  green.
- **Type-safety first.** Each milestone may defer features but never
  relaxes compile-time guarantees. We never ship a release that lets a
  HIGH-cardinality LABEL slip through.
- **Dogfood internally.** `apps/server` is updated alongside the SDK;
  if a milestone makes the example more painful, the design is wrong.
- **No incomplete code.** Per project CLAUDE.md: no `TODO`, no
  `unimplemented!`, no half-finished modules. Either a feature is in
  or it isn't.
- **Dev-erg is acceptance criteria, not a wish.** Each milestone has
  named entries in `crates/obs-sdk/tests/dev_ergonomics/` that pass.

## 1. Milestones

### M0 — Foundations (week 1–2)

**Exit criteria:** a "hello world" event compiles, emits, and renders
to stdout. No sinks beyond `Stdout` / `InMemory`. Buffa codegen
pipeline is wired and proves out custom-option reading.

- [ ] Workspace skeleton; pin `rust-toolchain.toml` to current stable.
- [ ] `obs-types`: enums (Tier, Severity, FieldKind, Cardinality,
      Classification, MetricKind, SamplingReason). All
      `#![forbid(unsafe_code)]`. Implement `buffa::Enumeration` for
      each.
- [ ] `obs-proto`: `obs/v1/options.proto`, `envelope.proto`,
      `enums.proto`, `builtin.proto`. `build.rs` invokes
      `buffa_build::Config`; capture FDS via `descriptor_set(...)`.
- [ ] `obs-core`:
  - `EventSchema` trait with `SCHEMA_HASH` const
  - `ObsEnvelope` builder + projection helper
  - `Observer` trait + `NoopObserver`, `InMemoryObserver`
  - `StandardObserver` shell with `SinkRouter` (single-tier wired)
  - `StdoutSink` (dev pretty-printer)
  - `InMemorySink` (test harness)
  - `EventsConfig` + `ArcSwap` reload
- [ ] `obs-macros`: `#[derive(Event)]` MVP
  - parses `#[event(...)]` and `#[obs(...)]`
  - emits `EventSchema` impl
  - emits the typed builder via `typed-builder`
  - emits compile-time lints L001 (cardinality), L002 (PII on LABEL),
        L003 (SECRET on LOG/AUDIT), L011 (`Obs*` naming)
- [ ] `obs-sdk` façade with `dev` feature; `StandardObserver::dev()`
      shortcut
- [ ] `apps/server`: hello-world handler emitting `ObsHelloEmitted`
- [ ] CI: `cargo build`, `cargo test`, `cargo clippy -D warnings`,
      `cargo +nightly fmt --check`, `cargo deny check`

**Risks:** `buffa-reflect` extension reads on the FDS — verify in a
spike on day 1 that the extension number `(obs.v1.event)` is
addressable from a `DescriptorPool` walk.

### M1 — Schema-first authoring + dev erg (week 3–4)

**Exit criteria:** a user can write `.proto` with `obs` annotations
and run `obs-build` in `build.rs` to generate Rust code, including
all lints. `obs init` scaffolds a working crate. Dev-erg test
fixtures pass.

- [ ] `obs-build`:
  - `Config` builder (files, includes, out_dir, extern_path,
        toggles, descriptor_source pass-through)
  - calls `buffa-build` for wire types + FDS
  - reads custom options via `buffa-reflect::DescriptorPool`
  - emits `obs/schemas.rs`, `obs/builders.rs`, `obs/lints.rs`,
        `obs/arrow_schema.rs` (fragments only at this stage)
  - schema hash baked in as `u64` constant (first 8 bytes of BLAKE3
        over the canonical descriptor; see architecture-design § 1.4)
- [ ] `obs-macros::include_schemas!` macro
- [ ] `apps/obs-cli`:
  - `obs init` (proto-first and rust-first scaffold)
  - `obs validate <file>...`
  - `obs lint --root <dir>`
  - `obs schema show <full_name>`
  - `obs version`
  - `obs completions <shell>`
- [ ] Compile-error quality work:
  - L001/L002/L003/L011 emit messages matching the format in
        [dev-ergonomics-design.md § 6](./dev-ergonomics-design.md#6-compile-error-quality)
  - `trybuild` cases pin the messages
- [ ] `obs::test::assert_emitted!` macro + `#[obs::test]` attribute
- [ ] `crates/obs-sdk/tests/dev_ergonomics/`:
  - `test_quickstart_60s.rs`
  - `test_compile_errors.rs`
  - `test_no_observer_noop.rs`
  - `test_in_memory_observer.rs`
- [ ] Update `apps/server` to author one event in `.proto` and one via
      `#[derive(Event)]` to prove parity

**Risks:** custom-option descriptor walking with `buffa-reflect` —
the spike from M0 confirms feasibility; this milestone makes it
ergonomic. If extension reads turn out to be brittle, fall back to
parsing the `.proto` text via `buffa-build`'s parser hook (which
exposes the parsed AST).

### M2 — Sinks, sampling, OTel parity (week 5–7)

**Exit criteria:** running `apps/server` against a local OTel
collector produces logs, metrics, and traces that show up in any
OTel-compatible backend (Jaeger / Prometheus / Loki tested in CI via
docker-compose). `obs::scope!` provides automatic trace correlation.

- [ ] Per-tier mpsc workers in `StandardObserver`:
  - one bounded channel + worker per tier
  - drop counters on overflow + `obs.runtime.v1.ObsSinkDropped`
        self-event
- [ ] Sampling:
  - head sampling per `(full_name, severity)` from config
  - tail-on-error: `tokio::task_local!` ring buffer (capacity 64),
        bound to the `obs::scope!` `Drop` guard (NOT keyed by
        `request_id` string — see [architecture-design.md § D4](./architecture-design.md#d4--tail-buffer-scoped-to-obsscope-drop-not-request_id-string))
  - `obs::scope!` macro with field allowlist + automatic field
        propagation into events
  - rate limiting per event (token bucket via `governor`)
- [ ] `obs::Filter` (EnvFilter-equivalent DSL) + `OBS_FILTER` env var
- [ ] `obs-core::NdjsonFileSink` with size-based rotation
- [ ] `obs-otel`:
  - `OtlpLogSink` (mapping per architecture-design § 4.1)
  - `OtlpMetricSink` (per § 4.2; enum LABELs become bounded
        attribute sets)
  - `OtlpTraceSink` (per § 4.3)
  - `otlp_trio_from_env()` convenience
- [ ] `obs-tracing-bridge` Direction A — minimal viable
      (per [tracing-interop-design.md § 2](./tracing-interop-design.md#2-direction-a--tracing--obs)):
  - `TracingToObsLayer` with default forensic mapping
  - `Level → Severity` table; `metadata.target/name/module` → payload
  - `FieldPromotions` allowlist with HLL cardinality enforcement
  - `DefaultPiiPatternRedactor` on by default (`password`, `secret`,
        `token`, `api_key`, `authorization`, `cookie`, `ssn`,
        `credit_card`, `bearer`)
  - `SpanEventMode::Off` default; `ObsSpanCompleted` on close,
        aggregating `Span::record` updates
  - Cross-system span correlation via the shared
        `tokio::task_local!` scope frame (architecture § 5.4)
  - `tracing-log` interaction documented + smoke-tested
- [ ] `#[obs::instrument]` attribute macro
- [ ] CLI:
  - `obs decode` (binary `ObsBatch` → NDJSON)
  - `obs tail --file | --stdin | --otlp`
  - `obs query --from path/file.ndjson` (filters + projection over
        the sparse-row schema)
  - `obs doctor` (per-crate setup diagnosis)
- [ ] Bench harness in `crates/obs-core/benches`:
  - emit P50/P99 budget; CI gates 10% regression
  - comparison against `tracing` + `serde_json` baseline
- [ ] Dev-erg additions:
  - `test_hot_reload.rs`
  - `test_tracing_bridge.rs`

**Risks:** OTLP wire-shape conformance. Mitigation: integration test
suite runs against an in-process `tonic` mock OTel collector that
asserts the OTLP proto messages received are well-formed and contain
expected attributes.

### M3 — Analytics + governance (week 8–10)

**Exit criteria:** schemas can be migrated into ClickHouse / Parquet
via the CLI, both targeting the **single sparse `obs_events` table**;
a CI job rejects breaking proto changes; forensic budget enforced;
`obs query` runs against ClickHouse and S3-backed Parquet.

- [ ] `obs-parquet`:
  - generated unified Arrow schema (envelope + per-event struct
        fragments combined at observer init)
  - `ParquetSink` with `ParquetLayout::Single` default, rolling
        files, partitioning by `service` + `date`
  - `opendal` integration for object-store targets
  - opt-in `ParquetLayout::TablePerEvent`
- [ ] `obs-clickhouse`:
  - `ClickHouseSink` writing to a single `obs_events` table per
        service via the `clickhouse` crate
  - DDL emitter for CLI consumption (single CREATE TABLE with
        sparse `Nested(...)` per event type)
  - `auto_migrate` opt-in (dev only)
- [ ] CLI:
  - `obs diff <baseline> <head>` with breaking-change exit code 2
  - `obs audit` (forensic budget rollup, tracing-bridge usage
        rollup, audit-tier coverage)
  - `obs migrate clickhouse` (single CREATE TABLE; ALTER on diff)
  - `obs migrate parquet` (unified Arrow schema JSON)
  - `obs query --from clickhouse://` and `--from s3://` (behind
        features)
- [ ] `obs-macros`:
  - lint L004 (MEASUREMENT missing metric annotation)
  - lint L005 (enum variants exceed declared cardinality cap)
  - lint L010 (forensic budget enforcement)
  - lint L013 (LABEL definition conflict across crates)
- [ ] `obs.v1.ObsForensicEvent` formalised; `obs::forensic!` macro
- [ ] `obs-tracing-bridge` Direction B + advanced features
      (per [tracing-interop-design.md § 3](./tracing-interop-design.md#3-direction-b--obs--tracing)):
  - `ObsToTracingSink` with `DashMap<MetadataKey, &'static Metadata>`
        cache (Box::leak per distinct `full_name` / `callsite_id`)
  - Two thread-local loop guards (`IN_TRACING_BRIDGE`,
        `IN_OBS_BRIDGE`) + `obs.bridge` reserved target
        (defence-in-depth)
  - `SpanEmissionMode::Off` (default) + `OnScope` opt-in
  - `PayloadDecodeMode::{Off, DecodeKnown, DecodeKnownAttributesOnly}`
- [ ] `obs-tracing-bridge` auto-typing path:
  - `TypedMatcher` predicate API (target/regex/name/level/field)
  - `register_typed::<E>(matcher, promote)` with cached
        per-callsite-id dispatch
  - `FieldCapture` thread-local visitor (zero per-event allocation
        on steady state)
  - `Redactor` trait + `DefaultPiiPatternRedactor` bundle
- [ ] `obs-tracing-bridge` test suite (per
      [tracing-interop-design.md § 6](./tracing-interop-design.md#6-test-strategy)):
  `tracing_to_obs_basic`, `obs_to_tracing_basic`,
  `roundtrip_property` (proptest), `no_infinite_loop` (1000-iter
  release stress), `span_correlation`, `pii_redaction`,
  `auto_typed_promotion`
- [ ] `obs-tracing-bridge` benches with CI gates: ≤ 2 µs forward
      overhead (tracing → obs), ≤ 1.5 µs reverse overhead
      (obs → tracing)
- [ ] Bridge built-in events shipped in `obs-proto/builtin.proto`:
      `ObsTracingForensicEvent`, `ObsSpanCompleted`, `ObsSpanEntered`,
      `ObsBridgePiiSuspected`, `ObsBridgeMatcherConflict`,
      `ObsBridgeLateSpanRecord`, `ObsBridgeNoDispatcher`
- [ ] Callsite interning (per
      [callsite-interning-design.md](./callsite-interning-design.md)):
  - `fixed64 callsite_id = 15;` added to `ObsEnvelope`
        (proto-additive)
  - `ObsCallsiteRegistry` (DashMap-based) on `StandardObserver`
  - `ObsCallsiteRegistered` self-event with `SamplingReason::OVERRIDE`
        (always retained); synchronous on first sight + cadence
        re-emit (`refresh_interval_secs` / `refresh_event_count`)
  - `ObsTracingInternedEvent` + `ObsForensicInternedEvent` payload
        types
  - `TracingToObsLayer::with_interning(InterningMode::{Off,Hybrid,Compact})`
        wired up; reconstitution path in `ObsToTracingSink`
  - CLI: `obs callsites dump | load | show <id>`, `obs query
        --callsite <id>`
  - Default mode is `Off` in v1; flip-default decision deferred to v1.1
- [ ] Callsite-interning self-events shipped:
      `ObsCallsiteRegistered`, `ObsCallsiteHashCollision`,
      `ObsCallsiteRegistryConflict`, `ObsBridgeCallsiteUnresolved`
- [ ] End-to-end integration: `apps/server` with realistic handler
      emitting `ObsRequestStarted` / `ObsRequestCompleted` /
      `ObsUpstreamFailed`, sinks routed to OTLP + Parquet +
      ClickHouse + `ObsToTracingSink`, third-party `tracing` events
      from `tower-http` and `sqlx` lifted via `register_typed` to
      `ObsHttpRequestCompleted` / `ObsDbQueryExecuted`, all
      dashboards verified
- [ ] Final dev-erg pass: re-run all dev-erg tests including
      `assert_emitted!` patterns and quickstart timing

**Risks:** proto schema diff requires deterministic comparison;
depend on the FDS round-trip via `buffa-reflect` and golden-file
tests under `crates/obs-cli/tests/diff/`.

### M-future — Out-of-scope for v1, tracked

| Item | Trigger |
| --- | --- |
| Cross-language SDKs (Go, Python, TypeScript) | adoption signal from at least one team |
| Cluster-wide sampling agreement | sampling overhead becomes a real bottleneck |
| Schema registry HTTP service | > 5 services sharing the same schemas |
| `obs query` against Iceberg | analytics team request |
| GUI for `obs schema show` / `obs diff` | request from non-Rust users |
| In-tree DuckDB sink | usage data justifies it (Parquet + external DuckDB covers v1) |

## 2. Cross-cutting concerns

### 2.1 Testing matrix

| Crate | Unit | Integration | Property | Bench |
| --- | --- | --- | --- | --- |
| `obs-types` | enums, const fns | — | — | — |
| `obs-proto` | encode/decode round-trip + view | — | proto round-trip | — |
| `obs-macros` | parse + emit | trybuild for bad inputs | — | — |
| `obs-build` | parser, codegen | end-to-end with fixture proto | — | codegen wall time |
| `obs-core` | observer, sink, sampler, scope, filter | InMemoryObserver | env. round-trip | emit hot path |
| `obs-otel` | mappers | mock OTel collector | — | — |
| `obs-parquet` | unified-schema gen | round-trip via `arrow` reader | — | batch write |
| `obs-clickhouse` | DDL gen | docker-compose CH | — | insert throughput |
| `obs-cli` | per-subcommand | trycmd against fixtures | — | — |
| `obs-tracing-bridge` | Layer/Sink/matcher/redactor | full bridge suite (loop, span correlation, PII, auto-typed) | event/envelope round-trip | forward + reverse overhead |
| `obs-tower` | layer factory | axum/hyper end-to-end | — | per-request overhead |
| `obs-sdk` | feature gating | dev-ergonomics suite | — | — |

### 2.2 Performance gates

CI runs `cargo bench --bench emit_hot_path` and compares against the
baseline stored in `benches/baseline.json`. > 10% regression fails
the job.

Targets:

- `emit` (no-op observer): ≤ 50 ns
- `emit` (StandardObserver, all sinks no-op): ≤ 1 µs P50
- `emit` (with NdjsonFileSink batched): ≤ 1.5 µs P50
- Scope `enter` + `exit`: ≤ 100 ns
- OTLP encode of one envelope (10 fields): ≤ 5 µs
- Bridge forward (`tracing::info!` → obs envelope, forensic mode): ≤ 3 µs P50 (≤ 2 µs delta over native obs emit)
- Bridge forward (auto-typed mode): ≤ 3 µs P50
- Bridge reverse (`obs::emit!` → tracing event, cached metadata): ≤ 2.5 µs P50 (≤ 1.5 µs delta over native obs emit)
- Bridge span lifecycle (`tracing::span!` → `ObsSpanCompleted`): ≤ 4 µs total amortised across child events
- Bridge interning Hybrid (cold callsite): ≤ 4 µs P50; (warm): ≤ 2.5 µs P50
- Direction B sink rendering interned envelope (cold): ≤ 3 µs; (warm): ≤ 1.5 µs
- BLAKE3 callsite hash (~200 input bytes): ≤ 80 ns

### 2.3 Documentation

Every milestone closes its docs as part of "done":

- module-level `//!` docs that explain the crate's role
- public types / functions have `///` doc comments with `# Examples`
- the `apps/server` README walks through emit, scope, config
- the top-level `README.md` reflects the latest user-facing API once
  M2 lands
- the dev-ergonomics doc is kept consistent with what actually
  compiles in `crates/obs-sdk/tests/dev_ergonomics/`

### 2.4 Compatibility & versioning

- Pre-`1.0`: minor bumps may break any API; the changelog calls them
  out.
- The envelope `format_ver` field is bumped only when the wire shape
  changes. M0 → M3 expectation: stays at `1`.
- `obs-types` enum additions are non-breaking; reordering / removing
  variants requires a major bump and a `migration.md` entry.
- Buffa upstream is pinned in `[workspace.dependencies]`; we do not
  float across buffa minor releases without an integration test pass.

## 3. Risks & open decisions

| Risk / decision | Status | Notes |
| --- | --- | --- |
| `buffa-reflect` custom-option ergonomics on extensions | open | Spike scheduled in M0 day 1 |
| ArcSwap vs `tokio::sync::watch` for config | locked | ArcSwap for sync-only readers |
| Stable enum count vs nightly `variant_count` | locked | Codegen emits `const COUNT: usize = N` from descriptor; no nightly |
| Whether to ship a Prom-direct sink in M2 | deferred | OTLP → Prom collector is the supported path; Prom direct can come later |
| Tail-buffer memory pressure under burst | open | Cap configurable; default 64 envelopes per scope |
| Naming of `obs.v1.options` field-number range | locked | 80000–89999 reserved |
| Single-table column count under wide-event explosion | open | Bench at M3 with 100+ event types; per-event-table fallback exists |
| `Obs*` prefix lint default level | open | Defaults to **error** under `--strict`, warning otherwise; may revisit if friction surfaces in beta |
| `SpanEmissionMode::OnScope` + `OtlpTraceSink` double-OTel-span | deferred to v1.1 | Document recommends OnScope only in dev; resolution path is either (a) auto-disable `OtlpTraceSink` when OnScope is on, or (b) span-id dedup at the OTel layer. See [tracing-interop-design.md § 10](./tracing-interop-design.md#10-open-questions--risks) |
| Bridge `Visit::record_debug` allocator cost | accepted | ~150 ns/field via `format!`; within budget. Fast-path opt-in if profiling demands it |
| Callsite interning default mode | locked for v1 | `Off` is the v1 default; `Hybrid` ships behind a config flag. Flip-default decision is a v1.1 question once registry-snapshot tooling has soaked |
| `u64` id collision under > 1 M ids (schema_hash + callsite_id share this concern) | open | Birthday bound is < 2⁻⁴⁴ at 1 M for either id; realistic workspaces have ≤ 10⁴ of each. A workspace genuinely above should widen to 96 or 128 bits via feature flag (no wire-format break). CLI lint warns at > 10⁵ |
| `ObsCallsiteRegistered` re-emit storm at startup | accepted | Bounded by per-tier mpsc channel rate-limiting; ~1 s of self-emit traffic for 10⁴ callsites |
| Cross-process registry sharing | deferred to v1.1 | Considered a Unix-socket sidecar registry; today registry is per-process |

## 4. Definition of done (v1.0)

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
