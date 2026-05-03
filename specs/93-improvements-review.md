# 93 — Implementation Review & Improvement Spec

Type: review + impl-plan delta
Date: 2026-05-02
Status: proposed
Scope: full audit of the `obs` workspace (33 799 LoC Rust) against
specs 10–72 (11 378 lines of spec).

This document records the gaps between the v1 specs and the current
implementation, sorted by severity and rolled up per spec. Each item
carries a concrete file:line citation and a fix shape. The intent is
to be the single backlog driver for closing v1.

It supersedes ad-hoc TODOs scattered through review commits
(`2ea95ff`, `db9da62`, `df32a34`, `25e81c8`, `7dd47e1`).

---

## 0. Executive verdict

The skeleton matches the spec. Most types, traits, builders, and
worker pipelines exist with correct shapes. Several load-bearing
guarantees are stubbed:

- The **runtime payload scrubber is a default passthrough** —
  `Classification::Pii`/`Secret` fields reach every durable sink
  unredacted (spec 70 § 4 invariant violated).
- The **`#[derive(Event)]` payload encoder writes JSON bytes** into a
  buffa-shaped slot — the two authoring paths produce
  *incompatible* wire formats (spec 12 § 1.2 broken).
- The **OTLP transport ships `StdoutDebugExporter` only** — there is
  no tonic / opentelemetry-otlp / HTTP exporter (spec 20 § 4 / spec
  61 § 2.6 broken).
- **Per-emit dynamic `Filter::event_allowed` is dead code** — the
  `[field=value]=level` clause from spec 13 § 7.1 never gates an
  emit.
- **Bridged tracing events lose `trace_id`/`span_id`** — Direction A
  never reads `tracing` span ancestry; the spec 30 § 2.3 promise
  "trace correlation Just Works" is unmet.
- **9 of 13 lints (L004–L010, L012, L013) are missing**; spec 60 § 6
  promises stable IDs L001–L013.
- **Most spec 11 § 10 self-events are not emitted.**

These are the v1 freeze blockers. Everything below is sized against
that.

---

## 1. P0 — Production-blocking correctness gaps

These violate explicit spec invariants and silently corrupt data,
leak secrets, or block downstream consumption. They must close before
the v1 freeze.

### P0-1 — Payload scrubber is a no-op
**Spec**: 14 § 5, 70 § 4 ("unscrubbed envelope is never delivered to
a sink").
**Code**: `crates/obs-core/src/registry/erased.rs:113-127` (default
passthrough); `crates/obs-core/src/registry/scrubbed.rs:57` (only
caller); zero overrides in `crates/obs-build/src/codegen.rs` or
`crates/obs-macros/src/derive_event.rs`.
**Impact**: Every `Classification::Pii`/`Secret` field flows to OTLP,
NDJSON, Parquet, ClickHouse with the original bytes. The build-time
lints L002/L003 are the only defence; runtime data carries the
classified field unchanged.
**Fix**:
1. Codegen emits `fn scrub_for_log(payload, scratch) -> &[u8]` per
   schema. For each field: if `classification ∈ {Pii, Secret}` and
   `tier ∈ {Log, Audit}`, decode the buffa tag, drop or replace the
   value with a `"<redacted-{name}>"` marker, re-encode into
   `scratch`.
2. Reuse `scratch: &mut BytesMut` so the steady-state path keeps the
   spec 71 zero-alloc budget when the schema has no classified
   fields.
3. Add an integration test that emits `ObsCustomerCreated{email:
   "foo@bar"}` (PII) and asserts the field is absent from a Parquet
   row and a ClickHouse insert body.

### P0-2 — Two payload encoders, two wire formats
**Spec**: 12 § 1.2 ("Rust-first and proto-first paths produce
byte-identical output").
**Code**: `crates/obs-macros/src/derive_event.rs:443-465` writes
JSON; `crates/obs-build/src/codegen.rs:152-159` writes buffa.
**Impact**: Sinks decode `payload` as buffa and misinterpret JSON
bytes (Parquet/OTLP/ClickHouse paths). The `EventSchemaErased::
decode_to_*` family will error on every Rust-first emit once
implemented (P0-4).
**Fix**: Replace `encode_payload_impl` with a real buffa encoder.
Generate per-field tag emission keyed on `f.kind`; reuse the same
helpers `obs-build` uses.

### P0-3 — Bridged tracing loses trace correlation
**Spec**: 30 § 2.3 / KD6.
**Code**: `crates/obs-tracing-bridge/src/direction_a.rs:200-214`
(no `trace_id` lookup); `345-356` (`on_new_span` stamps `Instant`
only); `377-407` (`ObsSpanCompleted` emit lacks
`trace_id/span_id/parent_span_id`); `crates/obs-proto/proto/obs/v1/
builtin.proto:42-51` (proto missing those fields too).
**Impact**: `tracing::info!(...)` events bridged into obs cannot be
correlated with sibling `obs::emit!` events from the same request.
The "single trace_id from edge to analytics" property collapses for
any caller still using `tracing::*` macros.
**Fix**:
1. Extend `ObsSpanCompleted` proto with `trace_id/span_id/
   parent_span_id/fields` (spec 30 § 2.3 snippet).
2. Direction A: in `on_new_span`, source `trace_id` from (a) span
   field `trace_id` if present, else (b) `OtelData` extension if
   `tracing-opentelemetry` is on the dispatcher, else (c) parent's
   `trace_id`, else (d) BLAKE3-128 of `(callsite, now_ns)` truncated.
   Stash on the span extension.
3. Open `obs::scope!(trace_id=…, span_id=…)` on `on_new_span` and
   stash the `ScopeGuard` in the span extension; drop on `on_close`.
   Sibling `obs::emit!` calls inside the `tracing::span` scope inherit
   automatically.
4. `on_event`: read `trace_id`/`span_id` from active obs scope (now
   present thanks to step 3) and stamp on the envelope.

### P0-4 — `EventSchemaErased::decode_to_*` returns errors
**Spec**: 14 § 8 ("sink-side fallback uses `render_json` for unknown
schemas, never errors out").
**Code**: `crates/obs-core/src/registry/erased.rs:67-93` returns
`DecodeError::Invariant("Phase 2 codegen not yet emitted")` for
`decode_to_arrow_struct` and `decode_to_otlp_kv`.
**Impact**: Once P0-2 lands and payloads become decodable, the
analytical sinks will error per envelope. Today it is masked because
ParquetSink only writes `payload_proto` as raw bytes.
**Fix**: Codegen for both authoring paths emits the per-field
projection. For unknown schemas, the *default* impl falls back to
walking the buffa tags via `buffa-reflect` and emitting
`(field_name → string-render)` pairs. Never error.

### P0-5 — Per-emit dynamic filter directives are dead code
**Spec**: 13 § 7.1 (`my_app::events::ObsRequestCompleted[route=
admin]=trace`).
**Code**: `crates/obs-core/src/filter.rs:133-145`
(`event_allowed` exists); `crates/obs-core/src/observer/standard.rs
:390-396` never calls it; `enabled` uses `||` instead of `&&`
between severity floor and dynamic predicate (logic bug regardless).
**Impact**: Operators cannot tune emit volume for a single label
value; debugging by `OBS_FILTER='*=info,my_app::Foo[user=alice]
=trace'` silently no-ops.
**Fix**:
1. In `StandardObserver::enabled`, evaluate `Filter::event_allowed`
   when callsite cache says `Sometimes`, then write the result back
   into `__CALLSITE.cache(generation, outcome)` so subsequent emits
   short-circuit.
2. Replace the `||` with `&&` in the static-floor check.
3. Add a parser test that `[route=admin]=trace` matches an envelope
   with `labels["route"]="admin"` only.

### P0-6 — OTLP has no real transport
**Spec**: 20 § 4.1 / 61 § 2.6.
**Code**: `crates/obs-otel/Cargo.toml` (no tonic / opentelemetry-otlp
/ rustls); `crates/obs-otel/src/sink.rs:60-90` (`StdoutDebugExporter`
is the only impl); endpoint/compression/retry fields stored, never
consulted.
**Impact**: Cannot ship to a real collector. The spec 61 § 2.11
default-feature `otel="yes"` claim is hollow.
**Fix**:
1. Add workspace deps: `tonic = "0.13"`, `opentelemetry-proto =
   "0.31"`, `tonic-codegen` for the three OTLP services, `rustls
   = "0.23"` with `aws-lc-rs` feature, `prost`.
2. `GrpcOtlpExporter` for both `OtlpProtocol::Grpc` and
   `OtlpProtocol::HttpProtobuf`. Honour all of `endpoint /
   compression / timeout / headers / retry_policy`.
3. Keep `StdoutDebugExporter` as opt-in for tests.

### P0-7 — AUDIT spool durability gap
**Spec**: 11 § 6.4 ("`O_APPEND` + fsync per batch").
**Code**: `crates/obs-core/src/audit_spool.rs:128-136`: `write_all`
+ `flush()` only — no `sync_data()` on `bin`/`crc`, no parent-dir
fsync.
**Impact**: A `kill -9` between `flush` and the next op loses
recently-appended AUDIT records. Spec 70 § 7 + spec 11 § 5
("AUDIT-tier never silent-dropped") are violated under host crash.
**Fix**: Mirror `crates/obs-parquet/src/writer.rs:238-251`. After
each batch, `bin.sync_data()` + `crc.sync_data()`. After segment
rotation, `parent_dir.sync_all()`. Gate behind `audit.fsync_mode
∈ {none, per_batch, per_record}`; **default `per_batch`** per
decision D6-5.

### P0-8 — Sinks ship `env.payload`, ignoring `ScrubbedEnvelope`
**Spec**: 14 § 5 / 70 § 4.
**Code**: `crates/obs-clickhouse/src/sink.rs:357-360`,
`crates/obs-parquet/src/writer.rs:110-114`,
`crates/obs-otel/src/mapping.rs:159` and `sink.rs:208/370/522`.
Each clones `env.envelope().clone()` rather than constructing an
outbound envelope with `payload = env.payload().to_vec()`.
**Impact**: Even if P0-1 lands, sinks bypass it because they ignore
the scrubbed slice.
**Fix**: Each sink builds its outbound row from
`(env.envelope_view(), env.payload())` where `payload()` is the
post-scrub slice. Add a regression test per sink that asserts a
`Classification::Secret` field never reaches the rendered output.

### P0-9 — `obs.yaml` config loader, env overlay, watcher all missing
**Spec**: 15 § 5.1 / § 5.3.
**Code**: `crates/obs-core/src/config.rs:317-379` ships only a
programmatic builder. No `from_yaml_path`, no `merged_with_env`, no
`notify`-based watcher, no SIGHUP hook. `OBS_FILTER` is the only env
var honoured (`crates/obs-core/src/observer/standard.rs:599`).
**Impact**: The deployment story is "edit Rust code, recompile". The
spec 15 § 5.3 SIGHUP/file-watcher reload is impossible.
**Fix**:
1. `EventsConfig::from_yaml_path<P>(p) -> Result<Self>` using
   `serde_yaml`; reject unknown keys (already covered by
   `deny_unknown_fields`).
2. `${VAR}` expansion via `shellexpand::env`.
3. `EventsConfig::merged_with_env(prefix="OBS")` walks env vars,
   `__` separator, applies last-wins overlay.
4. `notify` watcher with 200 ms debounce; rebuilds config and calls
   `Observer::reload_config(...)`.
5. Replace `serde_json::Value` sub-fields under `SinksConfig` with
   typed sub-structs (`StdoutSinkConfig`, `OtlpSinksConfig`,
   `NdjsonSinkConfig`, `ParquetSinkConfig`, `ClickHouseSinkConfig`)
   so YAML typos like `stylee` fail loudly.

---

## 2. P1 — Major missing functionality

These are spec'd surfaces that compile but do not implement the
stated behaviour. v1 is incomplete without them but they do not
silently corrupt data.

### P1-1 — 9 of 13 lints missing
**Spec**: 12 § 3.4 (L001–L013).
**Code**: `crates/obs-macros/src/derive_event.rs:474-544` and
`crates/obs-build/src/codegen.rs:397-481` implement L001, L002,
L003, L011 only.
**Action**: Land L004 (MEASUREMENT requires MetricSpec), L005 (enum
LABEL ≤ cardinality cap via `EnumCount::COUNT`), L006 (TIER_AUDIT
forbids PII/SECRET), L007 (snake_case), L008 (tag-reuse history),
L009 (event has no fields), L010, L012 (envelope-name shadow:
`ts_ns`, `service`, `instance`, `trace_id`, …), L013 (cross-event
schema_hash uniqueness, currently checked at runtime in
`registry/mod.rs:393` instead of at lint time). Trybuild fixtures
for each.

### P1-2 — Spec 11 § 10 self-event catalogue mostly empty
**Spec**: 11 § 10.
**Code**: only `ObsAuditSpoolRecovered`, `ObsPanicked`,
`ObsForensicBudgetExceeded` exist.
**Action**: Emit `ObsRegistryInitialized` (post-`build()`),
`ObsConfigReloaded` / `ObsConfigReloadFailed` (in `reload_config`),
`ObsSchemaUnknown` (in `SchemaRegistry::lookup` miss),
`ObsOversizedDropped` (in `auto_fill_envelope` size cap path),
`ObsAuditSpooled` / `ObsAuditSpoolFailed` (in `audit_spool.rs`),
`ObsLabelCardinalityHigh` (worker-side detection), `ObsSinkDropped`
+ `ObsSinkFailed` with `{tier, reason}` labels at every drop site
(`workers.rs`, `RetryQueue`, `NonBlockingWriter::dropped_total`,
`ParquetSink::encode_failures`, `ClickHouseSink::failures`).

### P1-3 — Bridge Direction A fidelity
**Spec**: 30 § 2.1 / § 2.2 / § 2.5.
**Code**: see audit ⚠️/❌ rows in agent report.
**Action**:
1. Populate `ObsTracingForensicEvent`'s typed proto fields
   (`target/message/span_path/attrs`) instead of flattening
   everything to `env.labels`.
2. Walk `ctx.span(id).scope()` to build `span_path` and to lift
   ancestor labels.
3. Add `register_callsite` so EnvFilter caches Interest.
4. `with_filter("warn,my_noisy_crate=off")` accepting
   `tracing_subscriber::EnvFilter` syntax.
5. `register_typed::<E: EventSchema>(matcher, |fields| -> E)` —
   route through the schema's own `project()` and scrubber.
6. Per-callsite matcher cache `DashMap<callsite::Identifier,
   ArcedPromoter>`; first-registered-wins with a one-shot
   `ObsBridgeMatcherConflict` warning.

### P1-4 — Bridge Direction B Metadata reconstitution
**Spec**: 30 § 3.7 / KD9.
**Code**: `crates/obs-tracing-bridge/src/direction_b.rs:398-474`
ships five static `Callsite`s with `target = "obs.bridge"` and a
fixed 8-name `FieldSet`.
**Action**: Synthesize one `Metadata` per `(full_name, sev)`:
`target = full_name`, fields = `env.labels.keys() ∪ {obs.trace_id,
obs.span_id, obs.full_name, message}`, store in
`DashMap<MetadataKey, &'static Metadata>` via `Box::leak` exactly
once. Restore the `obs.bridge` static as a fallback for
`SpanEmissionMode::Off`.

### P1-5 — OTLP Resource attribute set + W3C propagator
**Spec**: 20 § 2.1 / § 2.6.
**Code**: `crates/obs-otel/src/env_config.rs:62-70` carries only
`service_name/service_version/extra`; no `service.namespace /
service.instance.id / deployment.environment / host.*` first-class
fields. No `obs::propagator()` / `extract_w3c` / `inject_w3c` exist
in `obs-core`; `crates/obs-tower/src/propagator.rs` re-implements a
private W3C parser.
**Action**:
1. Extend `OtlpResourceAttrs` with the six semconv Resource keys;
   parse `OTEL_RESOURCE_ATTRIBUTES` per OTel spec.
2. Single source of truth: `StandardObserver` holds
   `ArcSwap<ResourceAttrs>`; sinks read from it; the OTLP builder
   updates it.
3. Add `obs_core::propagator::{W3cPropagator, ObsTraceCtx,
   extract_w3c, inject_w3c}`. `obs-tower` consumes those instead of
   re-implementing.
4. Encode `trace_id`/`span_id` as raw 16-/8-byte fields on the OTLP
   wire, not hex strings (`crates/obs-otel/src/mapping.rs:62-65`).

### P1-6 — OTLP metrics projection is a placeholder
**Spec**: 20 § 2.4.
**Code**: `crates/obs-otel/src/metrics.rs:33-48` emits
`<full_name>.count = 1` for every envelope.
**Action**: Codegen `EventSchema::project_metrics(payload, emitter)`
that walks each `FIELD_KIND_MEASUREMENT` field, dispatches by
`MetricKind::{Counter, Gauge, Histogram}` with the spec'd unit and
bounds, and forwards to `MetricEmitter`. Wire `MetricEmitter`'s
output to OTLP `Sum`/`Gauge`/`Histogram` data points with the
spec'd temporality.

### P1-7 — Span-pair tracker
**Spec**: 20 § 2.5 B.
**Code**: `crates/obs-otel/src/traces.rs:30-101` matches by suffix
`"Started"/"Completed"`; never emits `ObsSpanPairOrphaned`.
**Action**: Codegen sets `EventSchema::SPANS_PAIRED_WITH:
Option<&'static str>`. `OtlpTraceSink` matches on that constant.
Emit `ObsSpanPairOrphaned` after `pair_timeout` (default 60s).

### P1-8 — ParquetSink schema completeness
**Spec**: 22 § 1.1.
**Code**: `crates/obs-parquet/src/writer.rs:45-67`,
`crates/obs-clickhouse/src/ddl.rs:32-54` both lack
`service_namespace`, `deployment_environment`, `host_name`,
`host_arch`. ParquetSink writes `payload_proto: Binary` only — no
per-event `Struct` columns.
**Action**:
1. Extend the unified envelope schema with the four Resource
   columns.
2. `obs-build` emits `Arc<arrow_schema::Field>` fragments per
   schema. `ArrowSchemaModel::union(&[Schema])` produces the sparse
   `obs_events` schema. ParquetSink writes `payload_<full_name>:
   Struct<...>` columns.
3. Honour `ParquetLayout::TablePerEvent` (currently ignored —
   `crates/obs-parquet/src/sink.rs:97-121`).
4. Configure `set_max_row_group_size`, `set_dictionary_enabled`,
   `set_data_page_size_limit`, `set_statistics_enabled`.

### P1-9 — CLI surface gaps
**Spec**: 50.
**Code**: see `apps/obs-cli/src/cmd/`.
**Action**:
1. Add `obs generate` (top-level, spec § 3.2).
2. Add global flags `--root / --config / --format / --no-color /
   --quiet / -v / -vv` on the root `Cli` struct; today they live
   per-subcommand inconsistently.
3. `obs query`: support Parquet (read into Arrow) and ClickHouse
   (HTTP); add `--until / --trace / --grep / --select / --order-by`
   and TTY table rendering.
4. `obs tail --otlp` real implementation (currently `bail!`).
5. `obs migrate clickhouse` — populate per-event `schema_hash`
   (today hard-coded `0` at `migrate.rs:95`); generate the
   schema-applied registry table.
6. `obs decode --schemas proto/`: load schemas, decode payloads to
   JSON, render `ts` as ISO-8601, `sev` as text.
7. `obs lint` lands the 9 missing lints (P1-1) and routes summary
   to stderr per spec § 4.

### P1-10 — `obs-tower` does not open `obs::scope!`
**Spec**: 40 § 1 step 2.
**Code**: `crates/obs-tower/src/server.rs` never opens a scope —
handler emits cannot inherit `trace_id/span_id`; tail-on-error flush
cannot fire. Client (`crates/obs-tower/src/client.rs:140-145`)
always sends an empty `tracestate` and a fresh `trace_id` because
there is no active scope to read.
**Action**:
1. Server: open `obs::scope!(trace_id=ctx.trace_id, span_id=
   ctx.span_id, sampled=ctx.flags.sampled)` for the request future
   via `Instrumented<F>::instrument`.
2. Client: read the active scope's `trace_id`/`span_id` to populate
   the outbound `traceparent`; preserve incoming `tracestate`.
3. Replace `DefaultHasher`-seeded id generation
   (`propagator.rs:98-128`) with `getrandom`-backed CSPRNG (KD: spec
   71 randomness rule + CLAUDE.md security §).
4. Emit `latency_ms` as MEASUREMENT histogram + `bytes_out` as
   counter (today: string labels). Flip envelope severity on 5xx so
   tail-on-error fires.

### P1-11 — `obs init` produces a project that fails to evolve
**Spec**: 60 § 13.
**Code**: `apps/obs-cli/src/cmd/init.rs:107,121` writes literal
version pins `obs-sdk = "0.1"`, `obs-build = "0.1"`.
**Action**: Pin to the workspace version at build time, or read
`OBS_SCAFFOLD_VERSION` env (set by the release pipeline).

### P1-12 — `obs-otel` ships no `MockOtelCollector`
**Spec**: 72 § 6.
**Code**: no `MockOtelCollector` symbol exists; `obs-otel` has no
`tests/` directory.
**Action**: Land `obs_otel::test::MockOtelCollector` as part of P0-6
(needs the real OTLP transport to test against). gRPC server using
`tonic` + `tower::ServiceBuilder`; handle `Export*ServiceRequest`
for Logs/Metrics/Traces; capture into `Vec<ResourceLogs>` etc.;
expose `take_logs() / take_metrics() / take_traces()`.

---

## 3. P2 — Partial implementations & hot-path corrections

### P2-1 — Hot-path allocation in `project()` for LABEL fields
**Spec**: 71 § 1 ("no allocations on the steady-state emit path
beyond the typed event struct itself").
**Code**: `crates/obs-build/src/codegen.rs:204-205` and
`crates/obs-macros/src/derive_event.rs:418-441` both call
`ToString::to_string(&self.<field>)` per LABEL field per emit, plus
`#name.to_string()` per key. Two `String` allocations per LABEL.
**Action**: Switch the projection to `Cow<'static, str>` keys (the
field name is `&'static str` already, just stop the
`.to_string()`); for `String`-typed values, take ownership from the
event struct; for `&'static str`-typed values, store
`Cow::Borrowed`. The wire-format requirement to materialise to
`String` is unavoidable in `ObsEnvelope.labels`, but most fields can
borrow until the encode step.

### P2-2 — Bridge Direction A allocates per event
**Spec**: 71 § 7 / § 10.
**Code**: `crates/obs-tracing-bridge/src/direction_a.rs:262-265`
freshly constructs `FieldVisitor.pairs: Vec<(&'static str, String)>`
each `on_event`; per-attr `format!("attr.{name}")` allocates again.
**Action**: Thread-local `RefCell<FieldCapture>` rented per event;
reset, fill, drain. Avoid per-attr `format!`.

### P2-3 — `RollingFileWriter` does not fsync; filename layout
deviates from spec
**Spec**: 20 § 3.4 / § 3.5.
**Code**: `crates/obs-core/src/sink/writer.rs:366-368,430-433`.
**Action**:
1. After each `flush`, `file.sync_data()`. After rotation,
   parent-dir `sync_all()`.
2. Time-based: name files `prefix.YYYY-MM-DD.HH.suffix`; size-based:
   `prefix.NNNNNN.suffix` (per spec § 3.4).
3. Surface `NonBlockingWriter::dropped_total` as
   `ObsSinkDropped{sink=writer_overflow}` (P1-2).

### P2-4 — `StandardObserver::dispatch_audit` busy-waits with
`std::thread::sleep`
**Spec**: 11 § 6.4.
**Code**: `crates/obs-core/src/observer/standard.rs:253-261`.
**Action**: When inside a tokio runtime (detect via
`tokio::runtime::Handle::try_current`), use the existing
`TierWorker::send_with_timeout` async path so the executor stays
cooperative. Outside tokio, retain the busy-wait.

### P2-5 — `ClickHouseSink` blocks worker thread on retry
**Spec**: 22 § 3 (async insert).
**Code**: `crates/obs-clickhouse/src/sink.rs:120` calls
`std::thread::sleep`.
**Action**: Replace with `tokio::time::sleep`. Consider `keep-alive`
on the HTTP transport (currently a fresh `TcpStream` per request,
`transport.rs:186-251`) and TLS support.

### P2-6 — Filter parser doesn't match EnvFilter precisely
**Spec**: 13 § 7 ("ports EnvFilter grammar verbatim"); CLAUDE.md
("prefer winnow").
**Code**: `crates/obs-core/src/filter.rs:42-275` is a hand-rolled
parser that over-matches (`starts_with` on full name segments).
**Action**: Port using `winnow` (workspace dep); test against the
EnvFilter test suite vendored under `vendors/tracing/
tracing-subscriber/src/filter/env/`.

### P2-7 — `forensic!` runs through the head sampler
**Spec**: 13 § 6 ("always flushed regardless of sampling").
**Code**: `crates/obs-core/src/sampling.rs:43-73` does not
special-case `SamplingReason::Forensic`.
**Action**: First check in `decide()`: if reason ==
`Forensic`/`Override`/`TailError`, return `Sampled` unconditionally.

### P2-8 — `#[obs::instrument]` does not adapt the future
**Spec**: 13 § 5.
**Code**: `crates/obs-macros/src/instrument_attr.rs:71-77` keeps
the scope guard on the local stack rather than wrapping the future
in `Instrumented<F>`.
**Action**: For `async fn`, expand to
`obs::Instrumented::instrument(scope, async move { #body })
.await` so the scope re-enters per `poll`.

### P2-9 — Schema registry collision detection
**Spec**: 14 § 8 row 2 (`ObsCallsiteHashCollision`); spec 31 § 10
(`ObsCallsiteRegistryConflict`).
**Code**: `crates/obs-core/src/registry/mod.rs:80-114` overwrites
in `by_hash` silently.
**Action**: On insert, if `by_hash` already has a different
`full_name`, emit `ObsCallsiteHashCollision` once and keep the
first.

### P2-10 — `EventsConfig` `limits.max_payload_bytes` /
`max_label_value_bytes` not enforced
**Spec**: 11 § 6.2.
**Code**: declared in `crates/obs-core/src/config.rs:83-94`, never
read in `auto_fill_envelope` or `project()`.
**Action**: At envelope materialisation, measure
`payload.len()`/per-label byte length; on overflow, drop the emit
and increment `ObsOversizedDropped{reason=payload|label,
schema=full_name}` (P1-2).

### P2-11 — Bridge interning Hybrid vs Compact wire shape
**Spec**: 31 § 4 + § 8.
**Code**: `crates/obs-tracing-bridge/src/direction_a.rs:250-255`:
modes differ only in whether `target/module` are dropped.
**Action**: Hybrid emits `string message + map<string,string>
args` in a typed proto; Compact emits a length-prefixed buffa-encoded
args list, no rendered message. Without this the wire-size budget
in spec 31 § 8 is unreachable.

### P2-12 — ClickHouse / Parquet do not consume `ScrubbedEnvelope`
**Spec**: 14 § 5.
**Already covered by P0-8.** Listed here because there is also a
secondary correctness issue in `serialize_rows`: `ts_ns` is sent as
a JSON string (`crates/obs-clickhouse/src/sink.rs:323`); ClickHouse
forces a string parse server-side, sacrificing nanosecond resolution.
Fix while in the file: send as JSON number.

### P2-13 — Worker re-entry guard never accounts for drop
**Spec**: 11 § 10 — `ObsSinkDropped{reason=reentry}`.
**Code**: `crates/obs-core/src/observer/mod.rs:201-208` correctly
restores re-entry state but silently swallows the inner emit.
**Action**: Increment a counter; emit `ObsSinkDropped{reason=
reentry}` (P1-2).

### P2-14 — `obs-types` `no_std` claim is false
**Spec**: 61 § 2.1.
**Code**: `crates/obs-types/Cargo.toml:15-19` pulls in `serde` +
`thiserror` + `String`/`format!`.
**Action**: Either declare `#![no_std]` and feature-gate `serde` /
`alloc` users (pure semver bump), or drop the `no_std`-clean claim
from spec 61 § 2.1.

### P2-15 — `obs-sdk` default features
**Spec**: 61 § 2.11 (otel default = yes).
**Code**: `crates/obs-sdk/Cargo.toml:15-24` defaults to `dev,
panic-hook` only.
**Action**: Either add `otel` to default (after P0-6 lands a real
transport), or correct spec 61 § 2.11.

### P2-16 — `assert_emitted!` cannot match payload fields
**Spec**: 60 § 8.
**Code**: `crates/obs-core/src/test.rs:191-202` matches
`env.labels` only.
**Action**: After P0-2 lands a working buffa decoder, use
`EventSchemaErased::render_json` to extract payload fields and
match.

### P2-17 — `tracing-bridge` `init()` helper missing
**Spec**: 30 § 4.3.
**Code**: no `init()` function in `crates/obs-tracing-bridge/src/
lib.rs`.
**Action**: One-call helper that builds Registry + EnvFilter +
LogTracer + the bridge layer + a default `StandardObserver`.

### P2-18 — Trybuild fixtures cover only 4 of 13 lints
**Spec**: 72 § 4.
**Code**: `crates/obs-macros/tests/trybuild/fail/` lists L001/L002/
L003/L011 only.
**Action**: Add fixtures for L004–L010, L012, L013 alongside P1-1.

### P2-19 — `obs-otel` Resource attrs don't propagate through
`ArcSwap`
**Spec**: 20 § 2.1 sub-section "Single source of truth".
**Code**: builders set sink-local `OtlpResourceAttrs`
(`sink.rs:160-163`); changing identity post-init does not
propagate.
**Action**: Already covered in P1-5 — add a workspace-shared
`Arc<ResourceAttrs>` held under `ArcSwap` on the observer.

### P2-20 — Bench coverage well below spec 71 § 4
**Spec**: 71 § 4 (11 obs-core benches, 4 bridge benches).
**Code**: only `emit_noop`, `scope_overhead`, `bridge_overhead`.
**Action**: Land `bench_emit_filtered`, `bench_emit_inmemory`,
`bench_emit_ndjson`, `bench_with_observer_poll`,
`bench_encode_payload`, `bench_registry_lookup`,
`bench_registry_init`, `bench_scrub_for_log`,
`bench_interning_cold/warm`, `bench_blake3_callsite`. Ship a
`baseline.json` and CI gate that fails on > 10% regression.

---

## 4. P3 — Polish / hygiene / spec drift

| ID | Item | Code | Fix |
| --- | --- | --- | --- |
| P3-1 | `ObsForensicBudgetExceeded` built by hand without `EventSchema::project` | `crates/obs-macros/src/forensic_macro.rs:33-53` | Use the generated builder once `forensic!` becomes typed. |
| P3-2 | `StandardObserver::recover_audit_spool` runs before global observer install | `crates/obs-core/src/observer/standard.rs:643-664` | Stash recovery deltas; emit `ObsAuditSpoolRecovered` immediately after `Observer::set_global`. |
| P3-3 | `ObsCallsiteRegistered` proto missing `source/name/module_path/sev`; `CallsiteSource` proto enum missing | `crates/obs-proto/proto/obs/v1/builtin.proto:126-135` | Extend proto; codegen the enum. |
| P3-4 | Bridge `IN_OBS_BRIDGE` flag is dead code | `crates/obs-tracing-bridge/src/direction_b.rs:570-573` | Either use it for symmetric defence, or delete it and rely on the reserved-target check. Document the choice. |
| P3-5 | `ClickHouseSink` hand-rolled `base64` and `url_encode` | `crates/obs-clickhouse/src/sink.rs:372-402, transport.rs:268-279` | Use `base64` and `percent-encoding` workspace deps. |
| P3-6 | `obs-cli` `lint`/`audit` duplicate `scan_forensic_count` walker | `apps/obs-cli/src/cmd/lint.rs:281-304, audit.rs:89-110` | Extract to `cmd::scan` helper. |
| P3-7 | Stub `MockOtelCollector` and any test fixtures referenced by spec but absent | various | Track via P1-12. |
| P3-8 | `OBS_DEV` env var (spec 60 § 7) not wired | n/a | Either implement or drop from spec. |
| P3-9 | `obs schema show` lacks JSON output flag | `apps/obs-cli/src/cmd/schema.rs:29-66` | Add `--json` for AI consumers. |
| P3-10 | Hot-path `Mutex<ErasedWriterMaker>` per emit on stdout sink | `crates/obs-core/src/sink/stdout.rs:164-165` | Pre-render the writer once per worker; avoid the per-event mutex. |
| P3-11 | `Batch::push` is age-driven only on push, not on a timer | `crates/obs-otel/src/batch.rs:38-47` | Background `tokio::time::interval` per tier. |
| P3-12 | `RetryQueue` drains only at shutdown | `crates/obs-otel/src/sink.rs:230-232` | Background replayer with exponential backoff per `OtlpRetry::initial_backoff_ms`. |
| P3-13 | Scope dev-mode "field never used" warning missing | n/a | spec 13 § 2.3. |
| P3-14 | `RollingFileWriter` filename ignores `RollingPolicy` discriminant | `crates/obs-core/src/sink/writer.rs:430-433` | Track via P2-3. |

---

## 5. Spec text errata

These are mismatches where the *spec* needs updating, not the code.

| ID | Spec | Issue |
| --- | --- | --- |
| E-1 | 14 § 2 | "`#[non_exhaustive]` on the trait" — Rust does not have that. Reword to "sealed via supertrait `Sealed`". |
| E-2 | 50 | Locates `obs-cli` under `crates/`; reality is `apps/obs-cli/`. Pick one. |
| E-3 | 61 § 2.1 | "`obs-types` is `no_std`-clean" is false (uses `serde` + `format!`). Update or fix. |
| E-4 | 61 § 2.11 | `otel = default` claim is unmet; either land P0-6 + flip default, or update the table. |
| E-5 | 90 § M3 | "OTLP exporter ships in M3" — no transport exists; M3 exit criterion not met. |

---

## 6. Recommended implementation order

Optimised for least risk and earliest end-to-end correctness. Each
phase ends with a passing soak run and an updated spec 90 milestone.

### Phase 6.1 — Wire correctness foundation (1–2 weeks)
1. **P0-2** — buffa payload encoder for `#[derive(Event)]`.
2. **P0-4** — codegen `decode_to_arrow_struct` / `decode_to_otlp_kv`
   / `render_json` overrides; default falls back to buffa-reflect
   walk, never errors.
3. **P0-1** — codegen `scrub_for_log` overrides for any classified
   field.
4. **P0-8** — sinks read `ScrubbedEnvelope::payload()`.
5. Ship integration test asserting `Pii`/`Secret` fields are absent
   from Parquet rows, ClickHouse inserts, OTLP body.

Exit: a roundtrip test (`emit Pii field → flush → decode parquet`)
shows redaction.

### Phase 6.2 — OTLP transport (1 week)
1. **P0-6** — tonic / opentelemetry-proto / rustls; both gRPC and
   HTTP/protobuf.
2. **P1-5** — full Resource attrs + `obs::propagator()`.
3. **P1-12** — `MockOtelCollector` for tests.
4. **P1-6** — real metric projection.
5. **P1-7** — span-pair tracker driven by `SPANS_PAIRED_WITH`.

Exit: a soak run streams 1 M envelopes to a real
`opentelemetry-collector` and no non-zero `ObsSinkDropped`.

### Phase 6.3 — Filter, durability, observability (1 week)
1. **P0-5** — wire `Filter::event_allowed` into the emit hot path.
2. **P0-7** — fsync the AUDIT spool.
3. **P0-9** — `obs.yaml` loader, env overlay, watcher.
4. **P1-2** — emit the spec 11 § 10 self-event catalogue.
5. **P2-7, P2-8, P2-10, P2-13** — sampler/forensic/instrument/limits
   correctness.

Exit: a YAML-only operator can change filter / sampling / sinks at
runtime via SIGHUP and see `ObsConfigReloaded`.

### Phase 6.4 — Bridge fidelity (1–2 weeks)
1. **P0-3** — Direction A trace correlation.
2. **P1-3** — Direction A typed promoter, register_callsite,
   with_filter, matcher cache.
3. **P1-4** — Direction B per-(full_name, sev) Metadata
   reconstitution.
4. **P2-11** — Hybrid/Compact wire-shape distinction.
5. **P2-17** — `init()` helper.

Exit: `cargo run --example bridged_axum_app` produces correlated
traces in Jaeger via OTLP, even when the handler uses
`tracing::info!`.

### Phase 6.5 — Lint catalogue + CLI completeness (1 week)
1. **P1-1** + **P2-18** — land L004–L010, L012, L013 + trybuild
   fixtures.
2. **P1-9** — CLI generate / global flags / query / migrate /
   decode / lint surface.
3. **P3-6** — DRY the lint/audit walker.

Exit: `obs lint` rejects every spec 12 § 3.4 example; `obs query
--from clickhouse://...` returns rows.

### Phase 6.6 — Performance & hygiene (1 week)
1. **P2-1, P2-2, P3-10** — kill per-emit allocations.
2. **P2-3, P2-5, P2-6** — file fsync + winnow filter parser +
   tokio-friendly retry.
3. **P2-20** — bench coverage + CI gate.
4. **P3-1 … P3-14** — polish.

Exit: spec 71 P50/P99 budgets hold; CI fails on > 10% regression.

---

## 7. Decisions

The team has resolved the prior open-questions list. The decisions
below are binding for Phase 6 implementation.

### D6-1 — Bump wire format to `format_ver = 2` alongside P0-2
The bridge spec (extended `ObsSpanCompleted` with `trace_id /
span_id / parent_span_id / fields`, `CallsiteSource` enum, distinct
Hybrid vs Compact interned variants) plus the move from JSON to
buffa for the `#[derive(Event)]` payload are not safely additive
under `format_ver = 1` — old readers would treat new payload bytes
as malformed JSON. Bump the lock to `2` in lockstep with P0-2.
Update `scripts/check-format-ver.sh` and the `format_ver` constant
in `crates/obs-proto/src/lib.rs`. Document the bump in
`CHANGELOG.md`.

### D6-2 — Ship typed `secrecy::SecretBox<T>` / `SecretString` for
classified fields
Add `secrecy = { version = "0.10", features = ["serde"] }` to the
workspace. Codegen for any field carrying `Classification::Secret`:

- Field type rewrite: `String → SecretString`, `Vec<u8> →
  SecretBox<Vec<u8>>`, anything else → `SecretBox<T>`.
- Builder setter accepts `impl Into<SecretString>` /
  `impl Into<SecretBox<T>>` so call-sites stay ergonomic.
- Generated `Debug` impl prints `<redacted>` for the field; add a
  unit test asserting `format!("{evt:?}")` does not contain the
  secret value.
- `project()` and the buffa encoder call `expose_secret()` only at
  the moment of writing the payload bytes; the resulting bytes are
  then handed to the runtime scrubber (P0-1) which redacts them.

For `Classification::Pii`: keep the field as a plain typed value
(no secrecy wrapper) — runtime redaction (P0-1) is sufficient and
PII is not the same threat model as a credential.

### D6-3 — Ports EnvFilter dialect verbatim, extends with
`[field=value]=level` as a strict superset
Implement the parser in `winnow`. The base grammar is exactly
EnvFilter's (target, span name, level, field predicates per
`tracing-subscriber/src/filter/env/directive.rs`). The
`obs`-specific addition is **scoped per-event field predicates** —
`my_app::ObsRequestCompleted[route=admin]=trace`. Existing
EnvFilter strings parse unchanged; the extension is additive.
Validate against the EnvFilter test vectors vendored under
`vendors/tracing/tracing-subscriber/src/filter/env/`. Reject only
syntactically invalid inputs.

### D6-4 — `obs-sdk` default features include `otel` after P0-6
Once P0-6 lands a real OTLP transport, flip
`crates/obs-sdk/Cargo.toml` `default = ["dev", "panic-hook"]` →
`default = ["dev", "panic-hook", "otel"]`. A user opting for
compile-time slimness can pass `default-features = false` and pick
their sinks. Update spec 61 § 2.11 in the same commit.

### D6-5 — `audit.fsync_mode` defaults to `per_batch`, not
`per_record`
Performance matters more than the marginal durability gain. The
worker drains AUDIT records in 64-record batches; `per_batch`
fsyncs once per batch (≈ 1 fsync per 64 records vs 1 fsync per 1)
so steady-state throughput is ~ 64 × better while the durability
window is bounded to one batch. Operators who need stricter
durability can flip to `per_record` in `obs.yaml`. Default in
`AuditConfig::default()` and document the trade-off in spec 11
§ 6.4 + spec 70 § 7.

---

## 8. Definition of done

- Every P0 item has an integration test pinning the regression.
- `make soak` passes for 30 min with ObsSinkDropped == 0 under
  Phase 6.2's real OTLP transport.
- `cargo bench --workspace` produces a `baseline.json` and CI fails
  on > 10% regression on any of spec 71 § 4's named benches.
- All trybuild fixtures for L001–L013 exist and pass.
- `obs.yaml` round-trips through the loader, env overlay, watcher,
  and SIGHUP reload, surfacing `ObsConfigReloaded`.
- Bridge: `tracing::info!` inside an `obs::scope!` shows up in
  ClickHouse with the same `trace_id` as sibling `obs::emit!`s.
- `obs-otel/tests/` holds a `MockOtelCollector` round-trip suite
  covering Logs, Metrics, Traces.
- Spec text errata in § 5 are corrected in-place.
