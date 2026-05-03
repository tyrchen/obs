---
title: 94 — Improvements Review (Phase 7)
type: review + impl-plan delta
status: open
date: 2026-05-03
supersedes: null
extends: 93-improvements-review.md
---

# 94 — Improvements Review (Phase 7)

Type: review + impl-plan delta
Date: 2026-05-03
Status: **open** — backlog driver for Phase 7 (post-Phase 6.6 close)
Scope: full re-audit of the `obs` workspace against specs 10–72,
verifying the "closed" claims in [93-improvements-review.md](./93-improvements-review.md)
against the code actually checked into `master` at HEAD `122c859`.

This document is **not** a re-statement of `93`. It records:

1. The items `93` claims are closed but are still observable bugs in
   the code (`§ 2 Reopened items`).
2. New gaps discovered during the re-audit that `93` did not call out
   (`§ 3 New findings`).
3. A consolidated, severity-ordered Phase 7 backlog (`§ 4`).
4. Decisions that need to land alongside the fixes (`§ 5`).

It supersedes any open items in `93 § 1–4` that are explicitly resolved
below. Items not mentioned here keep the disposition `93` gave them.

---

## 0. Verdict

The skeleton has matured significantly since `93` landed. Phase 6.1
through 6.6 closed every wire-correctness P0 we tracked there: the
buffa payload encoder is unified
(`crates/obs-macros/src/derive_event.rs:533-562`), the runtime
scrubber walks the wire format generically without per-schema codegen
(`crates/obs-core/src/registry/scrubber.rs:38-128`), every sink reads
`ScrubbedEnvelope::payload()` rather than the raw envelope
(`crates/obs-clickhouse/src/sink.rs:156-157`,
`crates/obs-parquet/src/sink.rs:155-162`,
`crates/obs-otel/src/sink.rs:208-210/390-392/577-579`), AUDIT spool
honours `fsync_mode` with `sync_data()` per batch and a parent-dir
fsync at rotation (`crates/obs-core/src/audit_spool.rs:122-210`), the
OTLP/gRPC exporter ships via `tonic` + `opentelemetry-proto` behind
an `otlp-grpc` feature (`crates/obs-otel/Cargo.toml:26-50`),
`Filter::event_allowed` is wired into the emit pipeline
(`crates/obs-core/src/observer/standard.rs:374`), the head sampler
bypasses Forensic / Audit / Override (`standard.rs:379-388`), and the
config watcher reloads on debounced file events
(`crates/obs-core/src/config_watcher.rs`).

Three classes of issue remain:

- **Trace-correlation contract is half-met.** The bridge stamps
  `trace_id` onto its own envelopes but never pushes an `obs::scope!`
  frame, so sibling `obs::emit!` calls inside a `tracing::span!` lose
  correlation. `obs-tower` has the same shape: it stamps the events it
  emits itself but does not open a scope for the request future, so
  handler emits cannot inherit `trace_id`/`span_id`. The
  `ObsSpanCompleted` proto still lacks the four typed fields the spec
  requires. This is observable today — the `examples/http-service`
  README documents the gap.
- **Bridge typed payload is hollow.** Direction A writes the raw
  message string into `env.payload` rather than encoding a buffa
  `ObsTracingForensicEvent { target, message, span_path, attrs }`.
  Decoders that try to read the typed payload will misparse it. The
  same path drops MEASUREMENT fields from `ObsSpanCompleted` /
  `ObsHttpRequestCompleted` into string labels, so `project_metrics`
  is never invoked for bridged spans.
- **Lints, benches and dev-loop polish lag the contract.** Of the 13
  named lints, the proto-first codegen path (`obs-build`) implements
  only L001/L002/L003/L011 — five lints behind the `derive(Event)`
  path. Bench coverage covers ~40 % of the named-bench list in spec
  71 § 4 and there is no `baseline.json` for the CI gate.

These three classes are the v1 freeze blockers from a Phase 7
perspective. Everything else is sized against them.

---

## 1. Re-verification of `93` closure claims

Anchored to source as of HEAD `122c859`. Each row records what `93`
said and what the code says today.

| ID | `93` status | Verified in code | Verdict |
| --- | --- | --- | --- |
| P0-1 scrubber | Closed (Phase 6.1) | `registry/erased.rs:132-138` delegates to generic walker `registry/scrubber.rs:38-128`; covered by unit tests `scrubber.rs:172-269` | ✅ closed |
| P0-2 buffa encoder | Closed (Phase 6.1) | `derive_event.rs:77` emits `encode_payload` calling `BuffaEncodeField::buffa_encode_field` per field (`:533-562`) | ✅ closed |
| P0-3 trace correlation | Closed (Phase 6.4) | Bridge stamps envelope-level trace ids (`direction_a.rs:506-543`) **but does not push an `obs::scope!` frame** and `ObsSpanCompleted` proto still lacks the four required fields | ❌ **REOPENED** — see § 2.1 |
| P0-4 decode_to_arrow_struct | Closed (Phase 6.1) | `decode_to_arrow_struct` still returns `Err(DecodeError::Invariant("Phase 2 codegen not yet emitted"))` at `registry/erased.rs:73-82` | ❌ **REOPENED** — see § 2.5 |
| P0-5 dynamic filter | Closed (Phase 6.3) | `filter.event_allowed` wired at `observer/standard.rs:374`; precedence in `enabled()` still uses `\|\|` between static floor and callsite_interest (`:436-442`) | ⚠️ **partial** — see § 2.2 |
| P0-6 OTLP transport | Closed (Phase 6.2) | `tonic` + `opentelemetry-proto` deps optional on the `otlp-grpc` feature; gRPC clients in `grpc.rs:74-100`; `MockOtelCollector` in `mock_collector.rs` | ✅ closed |
| P0-7 AUDIT fsync | Closed (Phase 6.3) | `AuditFsyncMode { None, PerBatch, PerRecord }` honoured at `audit_spool.rs:178-210`; per-rotation `dir.sync_all()` at `:124` | ✅ closed |
| P0-8 sinks consume scrubbed payload | Closed (Phase 6.1) | All four sink crates rebuild the outbound envelope from `env.payload()` | ✅ closed |
| P0-9 config watcher | Closed (Phase 6.3) | `notify`-based watcher with 200 ms debounce in `config_watcher.rs:46-89` | ✅ closed |
| P1-1 nine missing lints | Partial (Phase 6.5) | `derive_event.rs` adds L004/L006/L007/L009/L012 (now 9/13 there). `codegen.rs` (proto-first) still at L001/L002/L003/L011 (4/13) | ⚠️ **partial** — see § 2.3 |
| P1-2 self-event catalogue | Closed (Phase 6.3) | `self_events.rs:26-160` ships `emit_registry_initialized`, `emit_config_reloaded`, `emit_config_reload_failed`, `emit_schema_unknown`, `emit_audit_spooled`, `emit_audit_spool_failed`, `emit_sink_dropped`, `emit_callsite_hash_collision_pub`, `emit_oversized_dropped`, `emit_span_pair_orphaned_pub`. `ObsLabelCardinalityHigh` still missing | ⚠️ **partial** — see § 2.6 |
| P1-3 bridge Direction A fidelity | Closed (Phase 6.4) | `register_callsite` at `direction_a.rs:603-615` caches typed-promoter pick; `span_path` walked. Direction A still flattens the typed `ObsTracingForensicEvent` payload to `env.payload = message_bytes` (`:289`) instead of buffa-encoding `{target, message, span_path, attrs}` | ⚠️ **partial** — see § 2.4 |
| P1-4 Direction B Metadata | Open | Still five static `Callsite`s in `direction_b.rs`; no per-(`full_name`, sev) synthesis | ❌ open |
| P1-5 OTLP Resource attrs | Closed (Phase 6.2) | `OtlpResourceAttrs { service_namespace, service_instance_id, deployment_environment, host_name, host_arch }` added at `env_config.rs:73-81`; `OTEL_RESOURCE_ATTRIBUTES` parsed at `:189-200`. Each sink still holds a private `Arc<OtlpResourceAttrs>` (`sink.rs:99/265/444`) — no shared `ArcSwap` on the observer | ⚠️ **partial** — see § 2.7 |
| P1-6 OTLP metric projection | Closed (Phase 6.2) | `project_metrics_impl` at `derive_event.rs:487-530` walks MEASUREMENT fields and dispatches to `MetricEmitter` | ✅ closed |
| P1-7 span-pair tracker | Closed (Phase 6.2) | `EventSchemaErased::spans_paired_with` at `registry/erased.rs:44-46`; `ObsSpanPairOrphaned` self-event ships at `self_events.rs:119` | ✅ closed |
| P1-8 ParquetSink schema | Open | Writer still emits `payload_proto: Binary` only (`obs-parquet/src/writer.rs:65`). Per-event nested struct columns are referenced in module docs (`lib.rs:21`, `writer.rs:26`) but not generated. Resource columns absent from the unified schema | ❌ open — see § 2.8 |
| P1-9 CLI surface gaps | Partial (Phase 6.5) | `obs lint --stderr summary`, `obs schema show --json`, `obs migrate clickhouse` schema_hash landed; `obs generate`, global flags, `obs query --from clickhouse://`, `obs decode --schemas`, `obs tail --otlp` still missing | ⚠️ **partial** |
| P1-10 obs-tower scope opening | Closed (Phase 6.4) per `93` agent set | CSPRNG step is closed (`fresh_trace_id` uses `getrandom::fill` at `obs-core/src/propagator.rs:190-213`) but **the request future is never wrapped in `Instrumented<F>`** — `server.rs:179-238` builds an `ObsHttpFuture` that polls inner directly. README acknowledges the hole. `latency_ms`/`bytes_out` still emitted as string labels (`server.rs:365`) | ❌ **REOPENED** — see § 2.1 |
| P1-11 `obs init` version pins | Open | Untouched | ❌ open |
| P1-12 MockOtelCollector | Closed (Phase 6.2) | `mock_collector.rs` ships in-process gRPC server | ✅ closed |
| P2-1 hot-path label allocs | Deferred | Per `93 § 0` blocked on `format_ver = 2`; still allocates `to_string()` per label per emit | ⏳ deferred |
| P2-2 bridge per-event allocs | Closed (Phase 6.4) | `direction_a.rs:295-300` pre-allocates the `attr.<name>` key; `FieldVisitor` rented from a thread-local | ✅ closed |
| P2-3 RollingFileWriter fsync + naming | Closed (Phase 6.6) per the doc | Verify in `crates/obs-core/src/sink/writer.rs` — out of this re-audit's scope | ⚠️ unverified |
| P2-4 audit dispatch busy-wait | Closed (Phase 6.6) | tokio-aware path lands per `93 § 0`; verify `observer/standard.rs:253-261` | ⚠️ unverified |
| P2-5 ClickHouse retry blocks worker | Closed (Phase 6.6) | per `93 § 0`; verify `clickhouse/src/sink.rs:120` | ⚠️ unverified |
| P2-6 winnow filter parser | Closed (Phase 6.6) per the doc | `crates/obs-core/src/filter.rs:1-15` still describes itself as "not a full port of EnvFilter" — hand-rolled parser remains | ❌ open — see § 2.9 |
| P2-7 forensic bypass | Closed (Phase 6.3) | `standard.rs:379-388` short-circuits Forensic/Audit/Override before head sampler | ✅ closed |
| P2-8 instrument adapts future | Closed (Phase 6.3) | per `93`; verify `obs-macros/src/instrument_attr.rs` | ⚠️ unverified |
| P2-9 hash collision detection | Closed (Phase 6.3) | `registry/mod.rs:80-114` calls `emit_callsite_hash_collision` on conflicting `by_hash` insert | ✅ closed |
| P2-10 limits enforced | Closed (Phase 6.3) | `oversized_dropped` emit at `standard.rs:362-368` enforces `limits.max_payload_bytes`; per-label byte cap still missing | ⚠️ **partial** — see § 3.5 |
| P2-11 Hybrid vs Compact wire shape | Partial (Phase 6.6) | `direction_a.rs:309-343` differentiates by full_name swap and removes `target/module` labels for Compact, but the args list is still emitted as `env.labels` strings, not as a length-prefixed buffa-encoded `ObsTracingInternedEvent.values` map. The 25 % wire saving in spec 31 § 8 is unreachable. | ⚠️ **partial** — see § 3.2 |
| P2-13 worker re-entry counter | Closed (Phase 6.6) per the doc | Verify `observer/mod.rs:201-208` | ⚠️ unverified |
| P2-14 obs-types no_std | Spec erratum E-3 | Still uses `serde` + `format!` + `String` (`crates/obs-types/Cargo.toml:15-19`); spec text not yet corrected | ⏳ open (spec text fix) |
| P2-15 obs-sdk default features | Decision D6-4 | `obs-sdk/Cargo.toml` defaults to `["dev", "panic-hook", "otel"]` after Phase 6.2; spec 61 § 2.11 reconciliation still pending | ⚠️ **partial** |
| P2-16 assert_emitted! payload match | Closed (Phase 6.3 indirectly) | `crates/obs-core/src/test.rs:91-102` ships `render_envelope_payload_json` per spec 60 § 8 | ✅ closed |
| P2-17 bridge `init()` helper | Closed (Phase 6.4) | per `93`; verify `obs-tracing-bridge/src/lib.rs` | ⚠️ unverified |
| P2-18 trybuild fixtures | Partial (Phase 6.5) | `obs-macros/tests/trybuild/fail/` ships L001, L002, L003, L004, L006, L007, L009, L011, L012 (9/13). Missing L005, L008, L010, L013 | ⚠️ **partial** |
| P2-19 ResourceAttrs single source | Open | Tracked under § 2.7 with P1-5 | ❌ open |
| P2-20 bench coverage | Open | `obs-core/benches/` ships `emit_noop`, `registry_lookup`, `scope_overhead`, `scrub_for_log`, `blake3_callsite` (5/11); `obs-tracing-bridge/benches/` ships `bridge_overhead` (1/4); no `baseline.json` | ❌ open — see § 3.6 |
| P3-* polish | Mixed | Out of Phase 7 scope unless promoted | — |

---

## 2. Reopened items

These are issues `93` reports as resolved that are still present in
the code at HEAD `122c859`. They are the highest-leverage Phase 7
work because each one violates an explicit spec invariant.

### 2.1 — Trace correlation: scope frames never opened (P0-A)

**Spec**: 30 § 2.3 "When `TracingToObsLayer::on_new_span` fires AND no
`obs::scope!` frame is active in the current task, the bridge opens an
implicit obs scope that uses the tracing span's id as `span_id`."
40 § 1 step 2 "Opens an `obs::scope!(trace_id = …, span_id = …,
sampled = …)` for the duration of the request."

**Code (bridge)**:
- `crates/obs-tracing-bridge/src/direction_a.rs:506-566` — `on_new_span`
  builds a `BridgedSpanCtx { trace_id, span_id, parent_span_id,
  opened_at }` and stores it in the tracing span extension. **It
  never pushes an `obs::scope!` frame onto the obs task-local stack.**
- `direction_a.rs:233-256` — bridged events stamp their own envelope
  via `stamp_correlation`. Native `obs::emit!` calls executing inside
  the same `tracing::span!` see no obs scope frame and end up with
  `trace_id = ""`.

**Code (obs-tower)**:
- `crates/obs-tower/src/server.rs:179-238` — `ObsHttpService::call`
  extracts `TraceContext`, stamps it on the layer-emitted
  `ObsHttpRequestStarted` / `ObsHttpRequestCompleted` envelopes, and
  builds `ObsHttpFuture { trace_id, span_id, parent_span, … }`. The
  inner future is polled directly at `:269/271`. **No `obs::scope!`
  is opened around `inner.call(req)`**, so handler-emitted events do
  not inherit `trace_id`/`span_id`.
- The `examples/http-service/README.md:66-76` documents the bug
  verbatim.

**Impact**: The single load-bearing promise of the bridge — "trace
correlation Just Works regardless of which subsystem opens the scope
first" (spec 30 § 2.3 last sentence) — is unmet for both producers of
inbound trace context. A service using `tracing::info!` and
`obs::emit!` in the same handler will produce two correlated events
in OTLP (because the bridge stamps its own envelopes) but the
analytics row from the native obs emit will carry an empty `trace_id`
and break joins. The same applies to handlers downstream of
`obs-tower`.

**Severity**: P0 — violates the central spec contract.

**Fix shape**:
1. Extend `ObsSpanCompleted` proto with the four missing fields per
   spec 30 § 2.3 snippet:
   ```proto
   string trace_id        = 4 [(obs.v1.field) = { kind: TRACE_ID }];
   string span_id         = 5 [(obs.v1.field) = { kind: SPAN_ID }];
   string parent_span_id  = 6 [(obs.v1.field) = { kind: PARENT_SPAN_ID }];
   map<string, string> fields = 7 [(obs.v1.field) = { kind: ATTRIBUTE }];
   ```
   Bump `format_ver` to 3 (D7-1) and update `scripts/check-format-ver.sh`.
2. Add a public `crate::scope::push_frame_typed(...) -> ScopeGuard`
   helper in `obs-core` that lets external callers push a scope frame
   carrying `trace_id`/`span_id`/`parent_span_id` plus an arbitrary
   `Vec<(&'static str, ScopeFieldValue)>` allowlist. Today `scope!` is
   a macro-only entry point, which forces external crates to either
   use the macro from outside their generic context (impossible) or
   re-implement scope plumbing.
3. `direction_a.rs::on_new_span`: after computing
   `(trace_id, span_id, parent_span_id)`, push a scope frame and stash
   the `ScopeGuard` in the tracing span extension alongside
   `BridgedSpanCtx`. `on_close`: drop the guard. Tracing's nested
   spans naturally re-enter on each poll since `tracing-subscriber`
   re-enters extensions; ensure the guard's lifetime is bound to the
   tracing span's, not the executor poll.
4. `obs-tower/src/server.rs::ObsHttpService::call`: build the scope
   guard before constructing `ObsHttpFuture`; transfer ownership into
   the future (a new `_scope: Option<ScopeGuard>` field) and drop it
   in the future's `Drop` impl. This re-enters the scope on every
   poll via `Instrumented<F>::instrument`. Ship a regression test
   that emits a typed event from inside the handler and asserts the
   envelope's `trace_id` matches the layer-emitted one.
5. Add a `dev-ergonomics` integration test under
   `crates/obs-sdk/tests/dev_ergonomics/`:
   ```rust
   tracing::info_span!("request").in_scope(|| {
       obs::emit!(ObsCheckoutAttempted { sku: "...".into(), qty: 1 });
   });
   // assert latest envelope.trace_id is non-empty and equals the bridge's
   ```

### 2.2 — `enabled()` precedence bug (P0-B)

**Spec**: 13 § 7 "filters apply at the static `ObsCallsite` level so a
filtered-out emit costs only the atomic `Interest` load + branch".
The directive `my_module=off` must veto a callsite even if the global
floor would let it through.

**Code**:
`crates/obs-core/src/observer/standard.rs:436-442`:
```rust
fn enabled(&self, callsite: &ObsCallsite) -> bool {
    let filter = self.filter.load();
    callsite.default_sev() >= filter.default_level()
        || filter.callsite_interest(callsite) != Interest::Never
}
```

**Impact**: With filter `default_level = Info` and a directive
`my_module=off`, a callsite in `my_module` whose `default_sev = Warn`
satisfies the static-floor clause (`Warn >= Info`) and the `||`
short-circuits — `callsite_interest` is never consulted, so the
explicit `=off` directive is ignored. Operators cannot turn off noisy
modules without raising the global floor.

**Severity**: P0 — silent filter bypass.

**Fix shape**: replace the disjunction with a single
`Filter::callsite_interest` call that already accounts for both the
default level and the matched directive (the way
`tracing-subscriber::EnvFilter::register_callsite` does it via
`directives.regenerate_interest_for_metadata`). Update
`Filter::callsite_interest` to return `Always` for callsites whose
default sev meets the floor *and* no directive vetoes them, and
`Never` when any matched directive sets level `OFF`. Add a unit test
fixture `my_module=off,info` that emits one `Warn` callsite and
asserts `enabled() == false`.

### 2.3 — Proto-first codegen lints lag the derive path (P1-A)

**Spec**: 12 § 3.4 "lints L001–L013 are stable identifiers; both
codegen paths enforce the same set."

**Code**:
- `crates/obs-macros/src/derive_event.rs` — implements L001, L002,
  L003, L004, L006, L007, L009, L011, L012 (9/13). Missing L005, L008,
  L010, L013.
- `crates/obs-build/src/codegen.rs:480-575` — implements L001, L002,
  L003, L011 only (4/13).

**Impact**: A workspace authored proto-first (the recommended path
per spec 60 § 13) silently accepts an `ObsXxx { route =
LABEL+CARDINALITY=HIGH }` that the same schema authored via
`#[derive(Event)]` would refuse. The compile-time safety net the spec
sells is one-sided.

**Severity**: P1.

**Fix shape**:
1. Hoist the L004/L006/L007/L009/L012 emission helpers from
   `derive_event.rs:643-740` into a new
   `crates/obs-build/src/lints.rs` module that takes a normalised
   `LintTarget` (name, fields, tier) and produces a
   `Vec<TokenStream>` of lint asserts. Both codegen paths call into
   it, ensuring identical messages and stable IDs.
2. Land L005 (enum cardinality) by threading `EnumCount::COUNT` from
   `obs-types/src/cardinality.rs:1-169` into the codegen. The proto
   compiler already knows the enum size; expose it via a generated
   `const ENUM_COUNT: u32` per enum.
3. Defer L008 (tag-reuse history) to v1.1 — needs a persistent
   manifest checked into the repo and a CLI workflow.
4. Land L013 (cross-event schema_hash uniqueness) at lint time:
   `obs-build`'s codegen pass already iterates every schema in a
   workspace; emit a deterministic compile error when two distinct
   `full_name`s hash to the same `u64`. Today this is detected at
   runtime in `registry/mod.rs:80-114` via `emit_callsite_hash_collision`,
   which fires *after* the binary boots — too late to be useful.
5. Trybuild fixtures for each new lint. Today
   `crates/obs-macros/tests/trybuild/fail/` ships 9/13 (L001, L002,
   L003, L004, L006, L007, L009, L011, L012); add the remaining four.

### 2.4 — Bridge typed payload is a raw byte string, not buffa (P1-B)

**Spec**: 30 § 2.2 table: "field `message` (`%message`) →
`payload.message` (ATTRIBUTE)". The proto `ObsTracingForensicEvent`
has typed fields `target` (LABEL), `message` (ATTRIBUTE), `span_path`
(LABEL), `attrs` (map<string,string>) at
`crates/obs-proto/proto/obs/v1/builtin.proto:60-67`. Spec 12 § 1.2
binds the runtime envelope's `payload` to the schema's buffa
encoding.

**Code**: `crates/obs-tracing-bridge/src/direction_a.rs:288-302`:
```rust
if *name == "message" {
    env.payload = value.into_bytes();   // <- raw UTF-8, not buffa
    continue;
}
```
The other promoter-bypassed fields land in `env.labels`, not in the
`attrs` map of the typed payload.

**Impact**: A sink that calls
`schema.decode_to_otlp_kv(env.payload)` on a forensic event will get
a single `KeyValueList` entry whose key is whatever buffa interprets
the first byte of the UTF-8 message as. Same for
`render_json` / `decode_to_arrow_struct`. The runtime scrubber
(`scrub_payload`) treats the raw bytes as an unknown field number
and passes them through, so the spec 70 § 4 "no Pii flows
unredacted" guarantee is theoretically intact but only because no
field is recognised — there is no defensive depth here.

This is the same root cause as § 2.5 (P0-4): every sink path that
needs the typed payload either errors or sees garbage on bridged
events.

**Severity**: P1 — observable today as malformed payload bytes;
escalates to P0 when sinks start decoding payloads (the rest of P1-8
ParquetSink work).

**Fix shape**: In `record_to_envelope`, call the codegen-generated
`ObsTracingForensicEvent::encode_payload(buf)` after populating its
typed fields. Concretely:
```rust
let payload = ObsTracingForensicEvent::builder()
    .target(metadata.target().to_string())
    .message(message_str)
    .span_path(span_path.clone())
    .attrs(attrs_map)
    .build();
let mut buf = BytesMut::new();
payload.encode_payload(&mut buf);
env.payload = buf.freeze().to_vec();
env.labels = labels_map;   // labels stays for promoted high-card fields
```
Keep `env.labels` for fields explicitly promoted via
`FieldPromotions` (spec 30 § 2.4). Apply the same pattern to
`ObsSpanCompleted` (after § 2.1's proto extension) and
`ObsSpanEntered`.

### 2.5 — `decode_to_arrow_struct` still returns the Phase-2 stub error (P1-C)

**Spec**: 14 § 8 row 2 — sinks fall back to `render_json` for unknown
schemas, **never** error out.

**Code**: `crates/obs-core/src/registry/erased.rs:73-82`:
```rust
fn decode_to_arrow_struct(...) -> Result<(), DecodeError> {
    let _ = (payload, builder);
    Err(DecodeError::Invariant("decode_to_arrow_struct: Phase 2 codegen not yet emitted"))
}
```

**Impact**: Once `ParquetSink` starts populating per-event nested
struct columns (P1-8 in `93`, still open), every emit will return
`Invariant("Phase 2 codegen not yet emitted")` — a hard error in the
worker. Today the sink writes only `payload_proto: Binary` so the
trap is dormant.

**Severity**: P1 — a latent panic the moment we start using the
typed Arrow path.

**Fix shape**: provide a default impl that walks the schema's
`FIELDS` table via `payload_decode` (the same helper that powers
`render_json` and `decode_to_otlp_kv` at `:62/98/116`). Per-schema
codegen overrides are still welcome for performance, but the default
must be functional. Pair the fix with the per-event Struct columns
work in P1-8.

### 2.6 — `ObsLabelCardinalityHigh` self-event still missing (P1-D)

**Spec**: 30 § 2.4 "Bridge stops promoting that field for the rest of
the process lifetime; emits one `obs.runtime.v1.ObsLabelCardinalityHigh`
warning event."

**Code**: `crates/obs-core/src/self_events.rs:26-160` ships 10
self-events but not `ObsLabelCardinalityHigh`. Bridge promotion
enforcement at
`crates/obs-tracing-bridge/src/field_promotions.rs:1-120` enforces
the cap silently.

**Severity**: P1 — operators have no signal that promotion is being
silently downgraded to attrs.

**Fix shape**: add `emit_label_cardinality_high(field, target,
estimated_cardinality)` to `self_events.rs` and call it from
`field_promotions.rs::admit` exactly once per `(target, field)` pair
via a `DashMap<(target, field), AtomicBool>` "already emitted" flag.

### 2.7 — Resource attributes still duplicated per sink (P1-E)

**Spec**: 20 § 2.1 last paragraph "single source of truth: the
observer holds `ArcSwap<ResourceAttrs>`; sinks read from it; the OTLP
builder updates it."

**Code**: `crates/obs-otel/src/sink.rs:99/265/444` each store a
private `Arc<OtlpResourceAttrs>`. The builder takes ownership at
`:160/329` and never publishes it back to the observer. Hot-reload of
Resource attrs is impossible.

**Severity**: P1 (operability) — Phase 6.2 closed P1-5's content
fields but P2-19's plumbing remains open.

**Fix shape**:
1. Add `obs_core::resource::ResourceAttrs` (the shared shape) +
   `Observer::resource_attrs() -> arc_swap::Guard<Arc<ResourceAttrs>>`.
   `StandardObserver::resource: ArcSwap<ResourceAttrs>`.
2. `obs-otel`'s `OtlpResourceAttrs` becomes a `From<&ResourceAttrs>`
   conversion at encode time; sinks no longer hold their own copy.
3. `EventsConfig::resource: ResourceAttrsConfig` (deserialised from
   `obs.yaml`); the watcher's reload path calls
   `observer.set_resource_attrs(...)`.

### 2.8 — ParquetSink writes `payload_proto: Binary` only (P1-F)

**Spec**: 22 § 1.1 "per-event payload: `payload_<full_name_snake>:
Struct<…>` sparse columns; analysts query
`SELECT payload_myapp_v1_obs_request_completed.latency_ms`."

**Code**: `crates/obs-parquet/src/writer.rs:63-67` ships:
```rust
Field::new("payload_proto", DataType::Binary, true),
```
plus the Resource columns at `:33`. `lib.rs:21` and
`writer.rs:26` reference per-event nested struct columns in module
docs but no codegen pass produces them.

**Severity**: P1 — without the typed columns, analytics is "decode
binary at query time" which makes the analytical surface unusable.

**Fix shape**: ride the `EventSchemaErased::decode_to_arrow_struct`
default-impl work from § 2.5. `obs-build`'s codegen pass produces
`Arc<arrow_schema::Field>` fragments per schema (the
`ArrowSchemaModel::union(&[Schema])` API exists in `model.rs:10` but
is unused). Plumb the union into `ParquetSink::build_record_batch` so
each registered schema gets its own column populated only when the
envelope matches that schema. Add a roundtrip test:
1. emit `ObsCheckoutCompleted { sku, qty, latency_ms }` to a
   `ParquetSink`.
2. Read back via `arrow::reader::ParquetRecordBatchReader`.
3. Assert `payload_myapp_v1_obs_checkout_completed.latency_ms`
   column exists and the value matches.

### 2.9 — Filter parser still hand-rolled, not winnow-ported (P2-A)

**Spec**: 13 § 7 "implementation **ports** EnvFilter's parser."
CLAUDE.md `## Type Design & API` "always prefer to use From / TryFrom
/ FromStr traits for type conversion. For parsing a string with
certain grammar, prefer to use latest version of winnow."

**Code**: `crates/obs-core/src/filter.rs:1-15` opens with "**This is
not a full port of EnvFilter**" — the parser is hand-rolled, supports
only a documented subset, and the EnvFilter test vectors under
`vendors/tracing/tracing-subscriber/src/filter/env/` are not exercised.

**Impact**: Operators who copy a working `RUST_LOG` directive into
`OBS_FILTER` may hit silent parse failures or over-matching
(`starts_with` rather than module-path-segment match).

**Severity**: P2 — partial fidelity to a battle-tested grammar.

**Fix shape**: rewrite the parser in `winnow`. Vendor the EnvFilter
test vectors as a `tests/` fixture. Ship a `parse_envfilter_oracle`
test that round-trips every fixture against
`tracing_subscriber::EnvFilter` and asserts identical interest
decisions for a curated callsite set.

---

## 3. New findings (not in `93`)

### 3.1 — Reserved `FieldKind` variants present in proto, never set by codegen

`crates/obs-proto/proto/obs/v1/enums.proto:30-32` declares
`TRACE_ID = 4`, `SPAN_ID = 5`, `PARENT_SPAN_ID = 6` for `FieldKind`.
The codegen paths
(`obs-macros/src/derive_event.rs::project_impl:460-485` and
`obs-build/src/codegen.rs::project_impl`) match on string literals
`"trace_id"`/`"span_id"`/`"parent_span_id"` rather than the proto
`FieldKind` enum. The proto fields the spec 30 § 2.3 snippet sets
(`kind: TRACE_ID`) cannot actually round-trip because:

1. The `obs.v1.field` annotation uses `kind = TRACE_ID` numerically
   but `obs-build` ignores any `kind` other than LABEL/ATTRIBUTE/
   MEASUREMENT/EVENT_NAME at field-meta-table emission time
   (`codegen.rs:204` filters on `f.kind`).
2. The `project()` impl in both paths uses Rust field name to decide
   whether the value flows to `env.trace_id` vs `env.labels`.

**Impact**: When § 2.1 lands the proto extension for
`ObsSpanCompleted`, the typed `kind: TRACE_ID` annotation will be a
no-op. `project()` will rely on the field being literally named
`trace_id` to short-circuit into `env.trace_id`. This works in
practice for hand-curated proto schemas but means the field-kind
enum is decorative.

**Severity**: P2 — works by convention today; documentation lies.

**Fix shape**: switch `project()` codegen to dispatch on the proto
`FieldKind` enum stored on `FieldMeta` (already present at
`crates/obs-core/src/envelope/mod.rs`). Add a lint (call it L014)
that errors when a field carries `kind: TRACE_ID` but its proto type
is not `string`, or when it's not named `*_id`.

### 3.2 — Compact interning still emits args as labels, not buffa-encoded values

**Spec**: 31 § 4 "Compact: payload = length-prefixed buffa-encoded
args list only; no rendered message. ~25 % wire saving vs Hybrid."

**Code**: `crates/obs-tracing-bridge/src/direction_a.rs:331-339`:
```rust
InterningMode::Compact => {
    env.full_name = "obs.v1.ObsTracingInternedEvent".to_string();
    env.labels.remove("target");
    env.labels.remove("module");
    env.payload.clear();           // <- discards args entirely
}
```
The args travel via `env.labels` (each an allocated `String`), and
the typed `ObsTracingInternedEvent.values: map<string, string>` is
never populated. The wire-size analysis in spec 31 § 8.1 (Hybrid
171 B → Compact 111 B) is unreachable — the actual Compact emit is
larger than Hybrid because labels are an envelope-level allocation.

**Severity**: P2 — operators cannot tune wire efficiency.

**Fix shape**: in Compact mode, encode the args via the codegen-emitted
`ObsTracingInternedEvent::encode_payload` over a
`BTreeMap<String, String>` populated from `visitor.pairs`, write into
`env.payload`, and clear `env.labels` (Compact mode keeps trace ids on
the envelope but moves args into the typed payload).

### 3.3 — `ObsTracingInternedEvent` has no `callsite_id` in the typed payload

**Spec**: 31 § 4 "interned events carry their `callsite_id` in the
typed payload so consumers without an envelope-level resolver can
still join against the registry."

**Code**: `crates/obs-proto/proto/obs/v1/builtin.proto:69-74` defines
`callsite_id: fixed64` as field 1. Direction A sets
`env.callsite_id` (envelope-level) at `direction_a.rs:324` but never
writes it into the typed payload. A consumer that has only the
payload bytes — e.g. an analytics replay tool deserialising stored
Parquet rows — cannot resolve the callsite without the envelope
context.

**Severity**: P2.

**Fix shape**: the buffa encoder for
`ObsTracingInternedEvent::encode_payload` already handles
`callsite_id: fixed64` by virtue of the `BuffaEncodeField` impl.
Wire it from the bridge's typed-payload encoder once § 3.2 lands.

### 3.4 — `ObsTracingForensicEvent.attrs` map ordering is non-deterministic

`std::collections::HashMap` ordering is not stable across runs; once
§ 2.4 lands the typed encoding, `attrs` order will vary, which
breaks deterministic schema-hash computation for downstream
consumers and any byte-for-byte snapshot tests.

**Severity**: P2.

**Fix shape**: switch the bridge's intermediate map to
`BTreeMap<String, String>` (or `IndexMap` if insertion-order is
preferred) in `direction_a.rs:282-302` before encoding to buffa.
Document the choice in spec 30 § 2.4.

### 3.5 — Per-label byte-length cap not enforced

**Spec**: 11 § 6.2 "`limits.max_label_value_bytes`: drop the emit and
emit `ObsOversizedDropped { reason = label }` when any label value
exceeds the cap."

**Code**: `crates/obs-core/src/observer/standard.rs:362-368` enforces
`limits.max_payload_bytes` (drop with reason `payload`) but never
walks `env.labels` to enforce the per-value byte cap. A 1 MB
`User-Agent` header propagated as a label via the bridge would land
in every sink unbounded.

**Severity**: P2 — DoS vector against any sink whose per-row
overhead is linear in label byte count.

**Fix shape**: add a `for (k, v) in &env.labels { if v.len() as u64 >
cfg.limits.max_label_value_bytes { … } }` loop alongside the existing
payload check. Reuse `emit_oversized_dropped` with a new `reason`
discriminant; extend the self-event payload to carry
`{full_name, label_name, size_bytes}`.

### 3.6 — Bench coverage and CI gate

**Spec**: 71 § 4 names 11 obs-core benches and 4 bridge benches:

> obs-core: `bench_emit_noop`, `bench_emit_filtered`,
> `bench_emit_inmemory`, `bench_emit_ndjson`,
> `bench_with_observer_poll`, `bench_scope_enter_exit`,
> `bench_encode_payload`, `bench_registry_lookup`,
> `bench_registry_init`, `bench_scrub_for_log`,
> `bench_blake3_callsite`.
>
> bridge: `bench_bridge_overhead`, `bench_interning_cold`,
> `bench_interning_warm`, `bench_callsite_id_compute`.

**Code**: `crates/obs-core/benches/` ships 5 of 11 (`emit_noop`,
`registry_lookup`, `scope_overhead`, `scrub_for_log`,
`blake3_callsite`). `crates/obs-tracing-bridge/benches/` ships 1 of 4
(`bridge_overhead`). No `baseline.json` checked in; no CI step gates
on regression.

**Severity**: P2.

**Fix shape**:
1. Land the missing benches as criterion harnesses.
2. Generate `target/criterion/baseline.json` and check in under
   `crates/obs-core/benches/baseline.json` /
   `crates/obs-tracing-bridge/benches/baseline.json`.
3. `make bench-gate` runs `cargo bench --workspace`, parses the
   output, and fails on > 10 % regression vs the checked-in baseline.
4. Wire `make bench-gate` into the GitHub Actions matrix as a
   nightly job (not per-PR — too slow).

### 3.7 — `obs-tower` server emits MEASUREMENT fields as labels

**Spec**: 40 § 1 third paragraph "`latency_ms` is a MEASUREMENT
histogram and `bytes_out` is a MEASUREMENT counter."
`crates/obs-proto/proto/obs/v1/builtin.proto:91-105` declares both
correctly.

**Code**: `crates/obs-tower/src/server.rs:319-371`:
```rust
labels.insert("latency_ms".to_string(), latency_ms.to_string());
```
The MEASUREMENT proto fields are never populated; the typed payload
is empty; `project_metrics` cannot find them.

**Impact**: An OTLP metric sink subscribed to `obs-tower` events sees
zero histogram data points despite the schema promising them.

**Severity**: P1 — same root cause as § 2.4, applied to the HTTP
middleware. The fix is identical: encode the typed
`ObsHttpRequestCompleted` payload via the codegen-emitted builder
and stop overloading `env.labels` for MEASUREMENT data.

### 3.8 — `ObsSpanEntered` / `ObsSpanCompleted` MEASUREMENT field also string-labelled

`crates/obs-tracing-bridge/src/direction_a.rs:589-595` writes
`latency_ns` as a string label; the proto declares it as
`MEASUREMENT histogram`. Same root cause and fix as § 3.7.

### 3.9 — `obs-types` `no_std` claim still in spec

**Spec text**: 61 § 2.1 — "obs-types has zero deps beyond buffa".
**Reality**: `crates/obs-types/Cargo.toml:15-19` pulls `serde` +
`thiserror` and the source uses `String`/`format!` throughout.

**Action**: spec text fix in 61 § 2.1 — change to "v1 is std-only
across every crate; `obs-types` has only `serde` + `thiserror` beyond
buffa". Track in `93 § 5 E-3`.

### 3.10 — `OBS_DEV` env var referenced in spec, not wired

Spec 13 § 2.3 / 60 § 7 reference `OBS_DEV=1` as a switch for dev-mode
diagnostics. `grep -r OBS_DEV crates apps` returns nothing. Either
implement (route to `EventsConfig::dev_mode: bool`, gate the
"scope field never used" warning + verbose source-loc capture) or
strike from the spec.

**Severity**: P3.

### 3.11 — `dev_ergonomics` test suite not present

Spec 60 § 13 references a `crates/obs-sdk/tests/dev_ergonomics/`
directory containing `test_quickstart_60s.rs`,
`test_compile_errors.rs`, `test_no_observer_noop.rs`, etc. These do
not exist. The "ergonomics claims" the SDK sells are unvalidated.

**Severity**: P2.

**Fix shape**: ship the suite. The trybuild fixtures already cover
compile errors; the runtime ones can leverage `InMemoryObserver`.

### 3.12 — Stdout sink mutex per emit (P3-10 in `93`)

`93 § 0` documents this as deliberately deferred. Re-flagging here
because the stdout sink is the default sink the `obs init` template
selects, so this is in the steady-state hot path of the
"first-experience" flow.

**Severity**: P3 (cycles only; no correctness concern).

**Fix shape**: pre-render one writer per worker thread by switching
`stdout::Sink::writer` from `Mutex<ErasedWriterMaker>` to a
`thread_local!` `RefCell<Box<dyn Write>>` with a one-time fmrt
init from a worker-startup callback. The trait re-shape is real
work; consider whether it's worth doing in v1 or deferring to v1.1.

---

## 4. Phase 7 backlog (severity-ordered)

| ID | Title | Severity | § | Effort |
| --- | --- | --- | --- | --- |
| **P0-A** | Bridge + obs-tower open `obs::scope!` frames; extend `ObsSpanCompleted` proto | P0 | § 2.1 | 5–8 d |
| **P0-B** | Fix `enabled()` precedence (`\|\|` → unified `callsite_interest`) | P0 | § 2.2 | 1 d |
| **P1-A** | Unify lints across both codegen paths (L004–L013 catalogue) | P1 | § 2.3 | 4–6 d |
| **P1-B** | Bridge encodes typed payload for `ObsTracingForensicEvent` / `ObsSpanCompleted` / `ObsSpanEntered` | P1 | § 2.4 + § 3.8 | 3 d |
| **P1-C** | `decode_to_arrow_struct` default impl walks the wire format | P1 | § 2.5 | 2 d |
| **P1-D** | Emit `ObsLabelCardinalityHigh` from the bridge promoter | P1 | § 2.6 | 1 d |
| **P1-E** | Single `Arc<ArcSwap<ResourceAttrs>>` on the observer | P1 | § 2.7 | 2 d |
| **P1-F** | ParquetSink per-event nested Struct columns + Arrow schema union | P1 | § 2.8 | 5–8 d |
| **P1-G** | obs-tower emits MEASUREMENT fields into typed payload | P1 | § 3.7 | 1 d (rides on P1-B) |
| **P2-A** | Winnow port of EnvFilter parser + EnvFilter oracle test | P2 | § 2.9 | 4–6 d |
| **P2-B** | Compact interning encodes args via buffa, not labels | P2 | § 3.2 | 2 d |
| **P2-C** | `ObsTracingInternedEvent.callsite_id` populated in typed payload | P2 | § 3.3 | 1 d (rides on P2-B) |
| **P2-D** | Deterministic ordering for bridge `attrs` map | P2 | § 3.4 | 0.5 d |
| **P2-E** | `limits.max_label_value_bytes` enforced | P2 | § 3.5 | 1 d |
| **P2-F** | Bench coverage + `baseline.json` + `make bench-gate` | P2 | § 3.6 | 5 d |
| **P2-G** | `dev_ergonomics` integration test suite | P2 | § 3.11 | 3 d |
| **P3-A** | `OBS_DEV` env var: implement or strike | P3 | § 3.10 | 1 d |
| **P3-B** | Stdout sink per-thread writer to remove per-emit mutex | P3 | § 3.12 | 2–3 d |
| **E-3** | Strike "no_std-clean" claim from spec 61 § 2.1 | spec text | § 3.9 | 0.25 d |
| **E-6** | Document the proto `FieldKind::TRACE_ID/SPAN_ID/PARENT_SPAN_ID` codegen contract; add L014 lint | spec + lint | § 3.1 | 2 d |

**Effort total**: ~50 engineer-days (≈ 6 calendar weeks for one
engineer, 3 weeks for two).

---

## 5. Decisions

These supersede or extend the `93 § 7` decisions.

### D7-1 — Bump wire format to `format_ver = 3` alongside P0-A

Extending `ObsSpanCompleted` with the four trace-correlation fields,
plus the typed-payload migration in P1-B, is not safely additive
under `format_ver = 2` (consumers that decode bridged payloads as
buffa would have parsed garbage; once they start working, the new
fields must be expected). Bump in lockstep with P0-A; document in
`CHANGELOG.md`. Update `scripts/check-format-ver.sh` and the
`format_ver` constant in `crates/obs-proto/src/lib.rs`.

### D7-2 — Lint emission helpers move to `obs-build`, both codegen paths consume them

Today `derive_event.rs` and `codegen.rs` duplicate every lint with
slightly different messages. The duplication caused the lag in § 2.3.
Resolution: hoist the lint emission into a single
`obs-build::lints::{emit_l001, …, emit_l013}` module that returns
`syn::Error`-equivalent compile-fail tokens. `derive_event.rs` calls
into it from the proc-macro; `codegen.rs` calls into it from the
build script. Stable IDs, identical messages.

### D7-3 — Scope frames must be public-API-pushable

`obs::scope!` macro syntax does not compose with external crates
that need to push a frame from a generic context (the bridge,
obs-tower, future user-defined middleware). Add
`obs_core::scope::ScopeFrameBuilder` (a typed builder that produces
a `ScopeGuard`) as a public surface. The macro continues to be the
canonical user API; the builder is the integration API. Documented
in spec 13 § 4 as a sub-section.

### D7-4 — Bridge typed-payload encoding is mandatory; labels are an opt-in promotion

The bridge currently overloads `env.labels` for both
"high-cardinality field that needs to bypass the typed schema" and
"every field of the typed payload that doesn't have a dedicated
envelope slot". Resolution: typed payload is always encoded via the
codegen-emitted `encode_payload`. The `FieldPromotions` allowlist
adds matching fields to `env.labels` *in addition* to keeping them in
the typed payload. Documented in spec 30 § 2.4.

### D7-5 — Bench gate runs nightly, not per PR

Criterion benches are too slow to run on every PR. Resolution: a
`bench-gate` job runs nightly against `main`, opens a self-assigned
issue when any named bench regresses > 10 % vs the checked-in
baseline. Operators bump the baseline by re-running the local
`make bench-baseline` and committing the JSON.

---

## 6. Definition of done

This document is closed when:

- Every P0/P1 item in § 4 has an integration test pinning the fix.
- `make soak` (30 min, real OTLP collector) passes with
  `ObsSinkDropped == 0` after warm-up.
- The bridge integration test
  `tracing::info_span!("request").in_scope(|| obs::emit!(...))`
  produces an envelope whose `trace_id` matches the bridged event's
  `trace_id`.
- `obs-tower` server-side handler `obs::emit!` calls inherit
  `trace_id`/`span_id` from the request scope (regression test under
  `crates/obs-tower/tests/`).
- All 13 lints fire from both codegen paths with identical stable
  IDs and messages; trybuild fixtures L001–L013 exist and pass.
- ParquetSink writes per-event nested struct columns; round-trip
  test reads back a typed field via Arrow.
- `cargo bench --workspace` baseline checked in;
  `make bench-gate` integrated.
- `crates/obs-sdk/tests/dev_ergonomics/` ships the spec-60 § 13 test
  suite.
- Spec text errata in § 3.1, § 3.9, § 3.10 are corrected in-place.
- `93-improvements-review.md` is updated to mark the items in § 1
  table as superseded by this document.

---

## Appendix A — Audit methodology

Two passes:

1. **Parallel agents** scoped to spec ranges (10–15, 20–22 + 30–31,
   40–61, 70–72) producing first-pass findings with file:line
   citations.
2. **Manual verification** of every "still open" claim by reading the
   actual source at HEAD `122c859`, plus spot-checks of every
   "closed" claim from `93`. Discrepancies between the agent reports
   and the code drove the § 1 table.

Reference reading: `vendors/tracing/tracing-core/src/callsite.rs`
(callsite registration model the obs implementation tracks),
`vendors/tracing/tracing-subscriber/src/filter/env/`
(EnvFilter grammar and `Statics`/`Dynamics` split), and the OTel
semantic conventions for Resource attributes (referenced via
`vendors/`-not-checked-in OpenTelemetry spec PDFs — operators
should keep them open while implementing P1-E).
