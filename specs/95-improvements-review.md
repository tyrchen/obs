---
title: 95 — Improvements Review (Phase 7 close + Phase 8 backlog)
type: review + impl-plan delta
status: open
date: 2026-05-03
supersedes: null
extends: 94-improvements-review.md
---

# 95 — Improvements Review (Phase 7 close + Phase 8 backlog)

Type: review + impl-plan delta
Date: 2026-05-03
Status: **open** — backlog driver for Phase 8 (post-Phase-7 close)
Scope: full re-audit of the `obs` workspace at HEAD `f03c8df`,
verifying the P0/P1/P2/P3 closures claimed by commits `ab27856`
through `41bcd4c` against the code on disk, and surfacing the next
wave of gaps before the v1 freeze in [92-rfc-v1.md](./92-rfc-v1.md).

This document is **not** a re-statement of `94`. It records:

1. The Phase-7 closures that landed cleanly (`§ 1`).
2. Phase-7 closures with remaining gaps (`§ 2`).
3. New findings discovered during the re-audit (`§ 3`).
4. A consolidated, severity-ordered Phase 8 backlog (`§ 4`).
5. Decisions that need to land alongside the fixes (`§ 5`).
6. Definition of done (`§ 6`).

Items in `94 § 1–4` not mentioned here keep the disposition `94` gave
them. Items confirmed closed are listed for forensics; items still
load-bearing are restated with current code citations.

---

## 0. Verdict

Phase 7 has landed almost everything `94` flagged. P0-A (scope frame
propagation), P0-B (filter precedence), P1-B/C/D/E/F/G (typed-payload
encoding, default `decode_to_arrow_struct`, label-cardinality
self-event, shared `ResourceAttrs`, ParquetSink per-event nested
columns, `obs-tower` MEASUREMENT routing), P2-A (winnow EnvFilter
port), P2-B/C (Compact interning + `callsite_id` in payload), P2-E
(`max_label_value_bytes`), P3-A (`OBS_DEV` env var), and E-6
(FieldKind contract / L014) all check out against the source. The
runtime is one short hop from the v1 freeze.

Three classes of issue remain — none are P0, but each violates a
distinct spec invariant or breaks a documented dev-erg promise:

- **Lint plumbing didn't follow D7-2.** `48bf5e3` unified the lint
  *messages* but kept the *implementations* duplicated between
  `crates/obs-macros/src/derive_event.rs` and
  `crates/obs-build/src/codegen.rs`. The shared
  `obs-build::lints` module D7-2 mandated does not exist, L013 lives
  only in the proto-first path, L014 enforces field name but not
  proto type in the codegen path, and the trybuild fixtures cover
  only 9 of 14 lints (`L005`, `L008`, `L010`, `L013`, `L014`
  missing). Drift between the two paths will recur the moment a
  fourteenth lint lands.
- **Outbound HTTP and analytics surface still half-met.** The
  `obs-tower` *client* generates a fresh `trace_id` on every
  outbound call, ignoring the active `obs::scope!` frame
  (`crates/obs-tower/src/client.rs:140-145`); `ObsHttpClientCompleted`
  ships labels only and no typed `latency_ms`. The Parquet/ClickHouse
  sinks have no Resource columns (`service_namespace`,
  `deployment_environment`, `host_name`, `host_arch`, the
  `ResourceAttrs::extra` map), so an analytics row sits next to its
  OTLP sibling missing identity the spec says all sinks share.
  OTLP exporters never set the W3C `traceparent` header on the
  outbound RPC, so trace context dies at the process boundary.
- **Examples and dev-erg surface lag the runtime.** All three
  example READMEs document gaps that Phase 7 closed
  (`examples/http-service/README.md:66-76` cites P1-10 as open;
  P0-A closed it). No example demonstrates `#[obs::instrument]`,
  `obs::SpanTrace`, `obs::forensic!`, multi-tenant per-task
  observer, or the tracing-bridge migration story. Examples ship
  no `obs.yaml`. The CLI is missing `obs generate`, every
  spec-50 § 2 global flag (`--root`, `--config`, `--format`,
  `--no-color`, `--quiet`, `-v`/`-vv`), `obs decode --schemas`,
  `obs tail --filter`, and surfaces a *different* lint catalogue
  than codegen. `make bench-gate` is implemented locally but
  never invoked from CI (D7-5 says nightly).

These are the v1 freeze blockers from a Phase 8 perspective.

---

## 1. Re-verification of `94` closure claims

Anchored to source as of HEAD `f03c8df`. Each row records what `94`
said and what the code says today.

| ID | `94` status | Verified in code | Verdict |
| --- | --- | --- | --- |
| P0-A scope frame propagation | open | `crates/obs-core/src/scope/builder.rs` ships `ScopeFrameBuilder` (D7-3); `crates/obs-tracing-bridge/src/direction_a.rs:653-675` `on_enter`/`on_exit` push and pop a context frame; `crates/obs-tower/src/server.rs:227-289` builds a `ScopeFrame` and re-enters it from `ObsHttpFuture::poll`; regression test `crates/obs-tower/tests/scope.rs:69-137`; `ObsSpanCompleted`/`ObsSpanEntered` extended with the four typed fields (`crates/obs-proto/proto/obs/v1/builtin.proto:52-66`); `format_ver` bumped to 3 (`crates/obs-proto/src/lib.rs:64`, lock test `:98`) | ✅ closed |
| P0-B filter precedence | open | `crates/obs-core/src/observer/standard.rs:467-476` delegates to `Filter::callsite_interest`; regression test `test_off_directive_vetoes_callsite_above_floor` in `crates/obs-core/src/filter.rs` | ✅ closed |
| P1-A unify lints (D7-2) | open | Messages aligned but the shared `obs-build::lints` module **does not exist**; lints duplicated at `derive_event.rs:588-761` and `codegen.rs:542-702` | ⚠️ **partial** — see § 2.1 |
| P1-B bridge typed payload | open | `crates/obs-tracing-bridge/src/direction_a.rs:266-333` builds a typed `ObsTracingForensicEvent` and calls `encode_into(&typed, &mut env.payload)` at `:333`; same for `ObsSpanEntered` (`:613-633`) and `ObsSpanCompleted` (`:702-712`) | ✅ closed |
| P1-C `decode_to_arrow_struct` default impl | open | `crates/obs-core/src/registry/erased.rs:76-82` delegates to `payload_decode::decode_to_arrow_struct_default` (`crates/obs-core/src/registry/payload_decode.rs:136-200`) | ✅ closed |
| P1-D `ObsLabelCardinalityHigh` | open | `crates/obs-core/src/self_events.rs:133-147` ships `emit_label_cardinality_high_pub`; `crates/obs-tracing-bridge/src/field_promotions.rs:99-103` calls it once per `(target, field)` via a `DashMap<(String, &'static str), ()>` gate | ✅ closed |
| P1-E single shared `ResourceAttrs` | open | `crates/obs-core/src/observer/standard.rs:88` field, `:155` setter, `:570-572` getter; OTLP sinks read from observer at `crates/obs-otel/src/sink.rs:244-254/438-448/638-648` instead of holding private copies | ✅ closed |
| P1-F ParquetSink per-event nested Struct columns | open | `crates/obs-parquet/src/writer.rs:30-44` emits nested Struct columns; per-row dispatch via `decode_to_arrow_struct` at `:219`; sparse population at `:207-236` | ✅ closed (no roundtrip integration test) |
| P1-G `obs-tower` MEASUREMENT in typed payload | open | `crates/obs-tower/src/server.rs:414-436` encodes `ObsHttpRequestCompleted` with typed `latency_ms` and `bytes_out`; labels mirror only the LABEL fields | ✅ closed (server side only — see § 3.2) |
| P2-A winnow port | open | `crates/obs-core/src/filter.rs:11/335-414` ships a winnow grammar; tests at `:630-693` | ✅ closed (no EnvFilter oracle test — § 3.7) |
| P2-B Compact interning args via buffa | open | `crates/obs-tracing-bridge/src/direction_a.rs:361-391` writes a typed `ObsTracingInternedEvent { callsite_id, values }` and encodes via `encode_into` at `:391` | ✅ closed |
| P2-C `callsite_id` in interned payload | open | `direction_a.rs:373` populates the typed field; `:379-382` confirms intent | ✅ closed |
| P2-D BTreeMap ordering for bridge attrs | open | `direction_a.rs:282/330/383-389` uses `BTreeMap<String, String>` for deterministic insertion before encoding | ✅ closed |
| P2-E `max_label_value_bytes` | open | `crates/obs-core/src/observer/standard.rs:387-399` walks `env.labels` and emits `ObsOversizedDropped { reason = "label" }` | ✅ closed |
| P2-F bench coverage + baseline + gate | open | `crates/obs-core/benches/baseline.json` checked in (24 named benches across both crates); `Makefile:79-87` defines `bench-baseline`/`bench-gate`; `scripts/bench-gate.sh` enforces 10 % regression | ⚠️ **partial** — gate not wired into CI (§ 3.5) |
| P2-G dev_ergonomics integration suite | open | `crates/obs-sdk/tests/dev_ergonomics/` ships all seven spec-60 § 13 files plus eight extras | ✅ closed |
| P3-A `OBS_DEV` env var | open | `crates/obs-core/src/config.rs:62/108-128` exposes `dev_mode` via the `OBS_*` env override layer | ✅ closed |
| P3-B stdout sink per-thread writer | deferred | unchanged | ⏳ deferred |
| E-3 spec text — strike `no_std` claim | spec text | unchanged | ❌ open (still a spec text fix) |
| E-6 FieldKind contract + L014 | open | Codegen path enforces field name (`crates/obs-build/src/codegen.rs:650-679`); derive path follows (`crates/obs-macros/src/derive_event.rs:708-733`); proto type check **not implemented**; no trybuild fixture | ⚠️ **partial** — see § 2.2 |
| P1-4 Direction B per-callsite synthesis | open | Single static callsite still in `crates/obs-tracing-bridge/src/direction_b.rs:8-12`; no per-(`full_name`, sev) synthesis | ❌ open (still as in `94`) |
| P1-9 CLI surface gaps | partial | `obs generate` still missing; spec-50 § 2 global flags absent; `obs decode --schemas`, `obs tail --filter`, `obs version --schema` still missing — § 3.3 | ❌ open |
| P1-11 `obs init` version pins | open | unchanged | ❌ open |
| P2-1 hot-path label allocs | deferred | unchanged | ⏳ deferred |
| P2-15 `obs-sdk` default features | partial | unchanged | ⏳ open (spec-text reconciliation) |

---

## 2. Phase-7 closures with remaining gaps

These items are listed as "closed" by Phase 7 commit messages but
fall short of the `94` § 4 fix shape in observable ways.

### 2.1 — Lint plumbing did not follow D7-2 (P1-AA)

**Spec / decision**: 12 § 3.4 binds both codegen paths to identical
lint IDs and messages; D7-2 (94 § 5) mandates a shared
`obs-build::lints` module that both paths consume so divergence is
structurally impossible.

**Code**:
- `crates/obs-macros/src/derive_event.rs:588-761` — full lint
  emission inline.
- `crates/obs-build/src/codegen.rs:542-702` — full lint emission
  inline, mirrored by hand.
- No `crates/obs-build/src/lints.rs` (or equivalent) module exists;
  the unified surface D7-2 calls for never landed.

**Impact**:
- L013 (cross-event `schema_hash` uniqueness) is implemented in
  `codegen.rs:718-738` but is **absent** from `derive_event.rs`. A
  workspace authored entirely via `#[derive(Event)]` will compile
  two distinct `full_name`s that hash to the same `u64` and only
  detect it at runtime via `crates/obs-core/src/registry/mod.rs`'s
  `emit_callsite_hash_collision` — too late to be useful.
- L014 enforces only the field-name suffix (`*_id`) in both paths
  and never validates that the proto type for a `TRACE_ID` /
  `SPAN_ID` / `PARENT_SPAN_ID` field is `string`. The `94 § 3.1`
  fix shape called out both checks.
- Drift will recur. The next lint added by either path will be
  forgotten in the other.

**Severity**: P1.

**Fix shape**:
1. Land `crates/obs-build/src/lints.rs` exporting one
   `LintInput` (name, fields, tier, default_sev, tag history,
   workspace prefix) and one
   `fn emit_lints(input: &LintInput) -> Result<Vec<TokenStream>, Vec<LintError>>`
   that returns a Vec of compile-fail tokens for each violation.
2. `derive_event.rs` calls into it from the proc-macro after
   building `LintInput` from the parsed `syn::DeriveInput`.
3. `codegen.rs` calls into it from the build script after building
   `LintInput` from `prost-types::FileDescriptorSet`.
4. Move L013 and the missing half of L014 into the shared module
   so both paths get them automatically.
5. Add trybuild fixtures `L005`, `L008` (when implemented),
   `L010`, `L013`, `L014` under
   `crates/obs-macros/tests/trybuild/fail/`. Each `.stderr`
   snapshot pins the message wording.

### 2.2 — L014 proto-type validation missing (P2-AA)

**Spec**: 94 § 3.1 fix shape: "add a lint (call it L014) that
errors when a field carries `kind: TRACE_ID` but its proto type
is not `string`, or when it's not named `*_id`".

**Code**: `crates/obs-build/src/codegen.rs:650-679` checks the
field name only; the proto type (`prost_types::FieldDescriptorProto::r#type`)
is read for the FieldKind dispatch but never asserted to be
`TYPE_STRING`. `crates/obs-macros/src/derive_event.rs:708-733`
similarly checks only the Rust field name.

**Impact**: A field declared `bytes trace_id = 6 [(obs.v1.field) = { kind: TRACE_ID }]`
compiles, then panics at first `project()` call when the codegen
tries to read `&field.value` as `&str`. The L014 spec promise of
"misuse caught at compile time" is half-met.

**Severity**: P2 (latent panic; current schemas all use `string`).

**Fix shape**: in the shared module from § 2.1, also assert
`f.r#type() == FieldType::String` for any TRACE/SPAN/PARENT_SPAN_ID
field; emit message
`error[L014]: field 'trace_id' has kind TRACE_ID but proto type is bytes; expected string`.

### 2.3 — `obs-cli lint` catalogue diverges from codegen (P2-AB)

**Code**: `apps/obs-cli/src/cmd/lint.rs:86-100` runs L001, L002,
L003, L004, L005, L010, L011, L013. **Missing**: L006, L007, L009,
L012, L014. Operators running `obs lint` against a workspace see a
strictly smaller catalogue than `cargo build` would refuse — the
inverse of the dev-erg promise that "the CLI lint surface mirrors
the build's".

**Severity**: P2.

**Fix shape**: route the CLI's per-schema check through the same
shared lint module from § 2.1. Today `cmd/lint.rs` has a parallel
implementation; it should call into `obs_build::lints::emit_lints`
and render the resulting `LintError`s as CLI text/JSON.

---

## 3. New findings (not in `93`/`94`)

### 3.1 — `obs-tower` client ignores active scope (P1-AC)

**Spec**: 40 § 1 — outbound client middleware reads the active
`obs::scope!`'s `trace_id`/`span_id` so a chained downstream call
within a request preserves trace continuity (the standard
distributed-tracing contract).

**Code**: `crates/obs-tower/src/client.rs:140-145`:
```rust
let ctx = TraceContext {
    trace_id: fresh_trace_id(),    // <- always a new id
    span_id:  fresh_span_id(),
    parent_span_id: String::new(),
    sampled: true,
};
```
The active `obs::scope!` frame (now exposed via `crate::scope`'s
public surface from D7-3) is never consulted. There is no fallback
chain "use scope frame if present, else generate fresh".

**Impact**: A handler that calls
`client.get("https://upstream/...").send()` from inside a request
scope sends a `traceparent` whose `trace_id` is unrelated to the
request's. Span correlation breaks at every process hop. This is
exactly the bug `obs-tower` exists to prevent.

**Severity**: P1 — silent correctness loss for distributed traces.

**Fix shape**:
```rust
let active = obs_core::scope::active_correlation();   // (trace_id, span_id) | None
let ctx = match active {
    Some((trace_id, span_id)) => TraceContext {
        trace_id,
        span_id: fresh_span_id(),     // new span for the outbound call
        parent_span_id: span_id,      // parent = inbound caller's span
        sampled: scope_sampled().unwrap_or(true),
    },
    None => TraceContext {
        trace_id: fresh_trace_id(),
        span_id:  fresh_span_id(),
        parent_span_id: String::new(),
        sampled: true,
    },
};
```
Add `obs_core::scope::active_correlation() -> Option<(String, String)>`
as a sibling to `ScopeFrameBuilder` (D7-3 surface) so external
callers can read what the scope macro stores. Add a regression
test under `crates/obs-tower/tests/client.rs` that opens a scope,
makes a wrapped client call against a `wiremock` server, and
asserts the outbound `traceparent` matches the scope's `trace_id`.

### 3.2 — `ObsHttpClientCompleted` ships labels only, no typed payload (P1-AD)

**Code**: `crates/obs-tower/src/client.rs:228-235` emits with
`latency_ms` and `bytes_out` as `env.labels` strings. The typed
`ObsHttpClientCompleted` payload (a sibling of
`ObsHttpRequestCompleted`, declared in
`crates/obs-proto/proto/obs/v1/builtin.proto`) is never encoded.

**Impact**: same root cause and impact as `94 § 3.7` (which Phase 7
closed for the *server* side only). `project_metrics` for the
client schema sees nothing; OTLP metric sinks emit zero histogram
data points for outbound HTTP latency.

**Severity**: P1 (same severity as the server-side P1-G that
already closed).

**Fix shape**: copy the
`crates/obs-tower/src/server.rs:414-436` pattern to the client
emit path; encode `ObsHttpClientCompleted` via the codegen
builder + `encode_into`. Drop `latency_ms` / `bytes_out` from
`env.labels` (decision D7-4 — labels are opt-in promotion, not a
fallback).

### 3.3 — Parquet / ClickHouse sinks have no Resource columns (P1-AE)

**Spec**: 22 § 1.1 — analytics rows must carry the same identity
as OTLP exports: `service`, `instance`, `version`,
`service_namespace`, `deployment_environment`, `host_name`,
`host_arch`, plus the `ResourceAttrs::extra` map.

**Code**:
- `crates/obs-parquet/src/writer.rs:55-91` ships only `service`,
  `instance`, `version`. `attrs` is built but always empty
  (`writer.rs:198-202`).
- `crates/obs-clickhouse/src/ddl.rs:32-54` mirrors the gap:
  `service`, `instance`, `version`, `labels` map only.
- `crates/obs-core/src/observer/standard.rs:570-572` exposes
  `Observer::resource_attrs()` (P1-E landed). The analytics sinks
  never read it.

**Impact**: With OTLP correctly exporting full Resource and
analytics rows missing it, an analyst joining OTLP with Parquet
on `service=…` cannot disambiguate environments or hosts. The
single-table promise of spec 22 § 1.1 is violated for every join
that needs operational context.

**Severity**: P1 (downgrades the analytics surface in production).

**Fix shape**:
1. Extend the Parquet schema in
   `crates/obs-parquet/src/writer.rs::unified_schema_with_registry`
   with seven columns: `service_namespace` (Utf8), `environment`
   (Utf8), `host_name` (Utf8), `host_arch` (Utf8), `attrs`
   (Map<Utf8, Utf8>). All nullable.
2. Populate from `observer().resource_attrs()` once per batch
   (cheap — one `ArcSwap::load_full`); per-row dispatch reads from
   the snapshot.
3. Mirror in `crates/obs-clickhouse/src/ddl.rs` so `obs migrate
   clickhouse` emits matching DDL.
4. Update `crates/obs-clickhouse/src/sink.rs` insert path to bind
   the new columns.
5. Roundtrip integration test under
   `crates/obs-parquet/tests/`: install observer with non-empty
   `ResourceAttrs`, emit, read back via
   `arrow::reader::ParquetRecordBatchReader`, assert
   `service_namespace` etc. round-trip.

### 3.4 — OTLP exporters never set `traceparent` on outbound RPCs (P1-AF)

**Spec**: 20 § 2.6 — OTLP transport sets the W3C `traceparent`
header on outbound batches when an active scope is present, so
the receiving collector can re-correlate the export call with the
caller.

**Code**: `crates/obs-otel/src/grpc.rs::send_*` builds the tonic
request from the encoded protobuf body but never reads the active
scope or attaches a `traceparent` metadata entry. The HTTP/JSON
exporter (if any) has the same gap.

**Impact**: Trace context dies at every OTLP export boundary. A
trace that crosses *services* via `traceparent` (handled correctly
by `obs-tower`) does **not** include the span the OTLP exporter
itself runs within, so collector-side telemetry on the exporter
is uncorrelated.

**Severity**: P1.

**Fix shape**: at `send_logs`/`send_metrics`/`send_traces`, build
a `tonic::metadata::MetadataMap` entry for `traceparent` from the
active scope (use `obs_core::scope::active_correlation()` from §
3.1's fix). When no scope is active, omit the header. Add a unit
test using `MockOtelCollector` that asserts the captured request's
metadata contains `traceparent` matching the scope's value.

### 3.5 — `make bench-gate` not wired into CI (P2-AC)

**Spec / decision**: D7-5 — bench gate runs nightly, gates on
> 10 % regression vs the checked-in baseline.

**Code**: `Makefile:79-87` defines `bench-gate`;
`scripts/bench-gate.sh` works locally; `crates/obs-core/benches/baseline.json`
is checked in. **`.github/workflows/build.yml:54` only runs
`cargo clippy --all-targets --all-features --tests --benches`** —
this *compiles* benches but never *runs* them. There is no
`bench.yml` (or `nightly.yml`) workflow.

**Impact**: Regressions land silently. The `94 § 6` "definition of
done" item "`cargo bench --workspace` baseline checked in;
`make bench-gate` integrated" is not actually integrated.

**Severity**: P2.

**Fix shape**: ship `.github/workflows/bench-nightly.yml` that
runs on a `schedule: cron: '0 7 * * *'` (after the soak finishes,
before the EU workday), executes `make bench-gate`, and opens an
issue (via `peter-evans/create-issue-from-file`) when any named
bench regresses > 10 %. Operators bump the baseline by re-running
`make bench-baseline` locally and committing the JSON.

### 3.6 — `emit_noop` is 2× over the spec-71 budget (P2-AD)

**Code**: `crates/obs-core/benches/baseline.json` records
`emit_noop = 104.4 ns`. Spec 71 § 3 budget is "noop emit ≤ 50 ns".

The implementation matches what spec 11 § 2.1 prescribes (one
TLS check + one `OBSERVER_GLOBAL.load_full`), so the budget itself
may be optimistic for current hardware — or the `Arc::clone` in
`load_full` is the bottleneck and the hot path should switch to
`load()` (returning a `Guard`) when no override is installed.

**Severity**: P2 (operability — claimed budget cannot be met).

**Fix shape** (one of):
1. Profile with `cargo flamegraph` to confirm the hottest frame
   is the Arc clone; if so, refactor `observer()` to return a
   `Guard<Arc<dyn Observer>>` on the no-override fast path so the
   refcount bump is amortised across the macro expansion.
2. If profiling shows the cost is unavoidable (e.g. the
   `OVERRIDE_COUNT` load + branch is itself ≥ 50 ns on the
   benched hardware), revise spec 71 § 3 to `≤ 100 ns` and call
   it out as a hardware-budget revision.

### 3.7 — winnow filter parser has no EnvFilter oracle test (P2-AE)

**Spec**: 13 § 7 promises *parity* with EnvFilter so operators
can copy a `RUST_LOG` directive into `OBS_FILTER`. P2-A's fix
shape called for "round-trip every fixture against
`tracing_subscriber::EnvFilter` and assert identical interest
decisions for a curated callsite set".

**Code**: `crates/obs-core/src/filter.rs:630-693` ships *unit*
tests for the winnow grammar (well-formed inputs, malformed
inputs, the documented subset). It does **not** import
`tracing_subscriber::EnvFilter` and does not vendor the tracing
test vectors at `vendors/tracing/tracing-subscriber/src/filter/env/`.
Operators have no guarantee that their `RUST_LOG` directive
behaves identically under `OBS_FILTER`.

**Severity**: P2.

**Fix shape**: vendor the EnvFilter test vectors as
`crates/obs-core/tests/envfilter_oracle.rs`; for each fixture,
parse with both `tracing_subscriber::EnvFilter` and
`obs_core::Filter`, then assert identical `Interest` for a
curated callsite set covering: bare level, target-only, target +
level, target + field clause, multiple directives, level OFF,
level + nested module, regex special chars, whitespace, quoted
values.

### 3.8 — Property tests not implemented despite spec-72 § 5 claims (P2-AF)

**Code**: `proptest` is a workspace dep
(`Cargo.toml:88`). `crates/obs-tracing-bridge/Cargo.toml`
imports it but no `proptest!` invocations exist anywhere under
`crates/`. Spec 72 § 5 claims property tests for envelope
round-trip, schema-determinism, trace-id correlation, and
scrubber correctness.

**Severity**: P2.

**Fix shape**: ship five property tests to start —
1. **Envelope round-trip** in `crates/obs-proto/tests/`:
   `proptest::strategy::any::<ObsEnvelopeArbitrary>` →
   `encode_into(&env, &mut buf)` → `ObsEnvelope::decode(&buf)`
   → assert equality.
2. **Schema-hash determinism** in `crates/obs-build/tests/`: same
   `(full_name, tier, default_sev, FIELDS[])` always hashes to
   the same `u64`.
3. **Scrubber correctness** in `crates/obs-core/tests/`:
   `proptest` over arbitrary buffa wire bytes + arbitrary
   Classification mask; assert no SECRET-classified field's bytes
   survive `scrub_for_log`; assert no PII-classified string
   survives unredacted; assert unrelated bytes pass through
   unchanged.
4. **Filter equivalence** (rides on § 3.7): parser → callsite
   interest decision should be a deterministic function of
   `(filter_str, callsite_id)`.
5. **Callsite ID non-zero invariant** in
   `crates/obs-core/tests/`: any `CallsiteAttributes` →
   `callsite_id(...) != 0`.

### 3.9 — Scrubber has no fuzz harness (P2-AG)

**Spec / project policy**: CLAUDE.md `## Safety & Security` —
"add a fuzz harness" for any unsafe contract. The scrubber walks
unbounded buffa wire bytes and is the redaction boundary
between user payloads and durable storage; even though it is
safe Rust, the malformed-input surface area is exactly the place
fuzzing earns its keep.

**Code**: no `fuzz/` directory; no `cargo-fuzz` integration; no
`afl` target. The scrubber's defensive `no_progress` check
(`crates/obs-core/src/registry/scrubber.rs:124`) is the only
guard against malformed bytes causing infinite loops.

**Severity**: P2.

**Fix shape**: add `crates/obs-core/fuzz/` with a `cargo-fuzz`
target `fuzz_targets/scrub_payload.rs` that runs
`scrub_for_log(&schema_arbitrary, &bytes_arbitrary)` and asserts
(a) no panic, (b) bounded execution time (e.g. < 10 ms for
1 MiB input), (c) every output byte is either zero or copied
from the input. Wire into `make fuzz-quick` (10 s / target) for
local runs; longer dedicated runs gated on a `nightly-fuzz`
workflow.

### 3.10 — No per-string length caps in bridge / `obs-tower` (P2-AH)

**Spec / project policy**: CLAUDE.md `## Input Validation` —
length limits on every string from external input, enforced in
bytes; the `User-Agent` ballooning attack is called out by name.

**Code**:
- `crates/obs-tower/src/server.rs:182-183` extracts route via
  `req.uri().path().to_string()` — no cap.
- `crates/obs-tower/src/server.rs::default_route_extractor` and
  any user-supplied route extractor return arbitrary `String` —
  no cap.
- `crates/obs-tracing-bridge/src/direction_a.rs::FieldVisitor`
  reads field values into the typed payload without per-field
  length caps. A 10 MiB `User-Agent` flowing through
  `tracing::info!(user_agent = %ua, …)` lands in the envelope's
  `attrs` map.

`max_payload_bytes` (§ 11 § 6.2) catches the *aggregate* but the
per-field DoS pre-exhausts memory before the aggregate check
runs (the typed payload is built before `max_payload_bytes` is
checked).

**Severity**: P2 (DoS vector against any service exposing
`obs-tower` or the bridge to untrusted callers).

**Fix shape**:
1. Add `EventsConfig::limits.max_external_string_bytes`
   (default 256, per CLAUDE.md guidance).
2. `obs-tower` route extractor wrapper: cap output at
   `max_external_string_bytes`; truncate with
   `…<truncated:N>` suffix and emit one
   `ObsLabelOversized { field, original_size, capped_size }`
   self-event per `(field, route)` pair (deduped).
3. Bridge `FieldVisitor::record_str`: cap per field with the
   same policy.
4. Add a unit test under `crates/obs-tower/tests/dos.rs`
   exercising a 1 MiB `User-Agent` and asserting the envelope
   value is capped.

### 3.11 — Examples ship no `obs.yaml`, no advanced patterns (P2-AI)

**Spec**: 60 § 2 quickstart implies `obs.yaml` is part of the
"60-second" experience. § 4.3 patterns A–M cover ten authoring
patterns; only A (request handler) is reflected in
`examples/http-service`.

**Code**:
- `examples/http-service/`, `examples/batch-pipeline/`,
  `examples/worker-pool/` ship no `obs.yaml`. New users have no
  reference to copy.
- No example demonstrates: `#[obs::instrument]` (60 § 4.3.D),
  conditional severity escalation (4.3.E),
  `obs::forensic!` (4.3.F), `obs::SpanTrace` (13 § 9),
  multi-tenant per-task observer (60 § 4.3.L), tracing-bridge
  migration (60 § 4.3.J), `tokio-console` coexistence (4.3.M).
- The `crates/obs-sdk/tests/dev_ergonomics/` suite covers all
  these but tests are not user-facing examples.

**Severity**: P2 (dev-erg gap; not a runtime correctness issue).

**Fix shape**:
1. Ship `examples/http-service/obs.yaml` with realistic config:
   filter directive, sampling rates per event, OTLP endpoint
   placeholder, ResourceAttrs.
2. Ship `examples/batch-pipeline/obs.yaml` and
   `examples/worker-pool/obs.yaml` with sink-flavoured presets.
3. Add `examples/tracing-migration/` — start with a
   `tracing::info!`-only baseline; show the bridge wiring; show
   one schema-ification pass.
4. Add `examples/multi-tenant/` — three tenants, separate OTLP
   endpoints, `with_per_request_observer`, the registry pattern
   from spec 60 § 4.3.L.
5. Add `examples/forensic-and-spantrace/` — exercise
   `obs::forensic!` with budget enforcement; capture a
   `SpanTrace` into an error type.

### 3.12 — Example READMEs document already-closed bugs (P2-AJ)

**Code**: `examples/http-service/README.md:66-76` documents the
P1-10 (94 P0-A) bug as still open:
> "Handler-emitted ... envelopes do **not** carry the request's
> `trace_id` / `span_id` yet because `obs-tower` does not open
> an `obs::scope!` around the request future (spec 93 P1-10)."

P0-A landed in commit `ab27856` and is verified by
`crates/obs-tower/tests/scope.rs`. The README is stale.
`examples/batch-pipeline/README.md` and
`examples/worker-pool/README.md` likely have the same stale
disclaimers about the now-closed `payload_proto: Binary` schema
(P1-F).

**Severity**: P2 (misleading documentation).

**Fix shape**: read each README, remove the "Known limitation"
sections that cite resolved spec items, and add a "What this
demonstrates" section that maps the example to the spec
patterns it actually exercises (per § 3.11).

### 3.13 — Direction B still uses one static callsite (P1-AG, restated from `94`)

**Code**: `crates/obs-tracing-bridge/src/direction_b.rs:8-12`
declares one static `Callsite`. There is no per-(`full_name`,
`severity`) `Box::leak`'d Metadata synthesis as spec 30 § 3.3
prescribes.

**Impact**: Every `obs::emit!` re-routed back to a tracing
subscriber (under `SpanEmissionMode::OnEvent`) is reported with
the same target/level/name. Subscribers cannot filter by event
type; `tracing-subscriber::EnvFilter` directives like
`myapp.v1.ObsRequestCompleted=trace` simply do not work for
re-routed events.

**Severity**: P1 (open from `94`; restated because it is the
last bridge-fidelity gap).

**Fix shape**: Direction B `register_typed_back(matcher)` should
synthesise Metadata at first sight per (`full_name`, `severity`)
and cache via a `DashMap<MetadataKey, &'static Metadata>` (the
same pattern the bridge already uses for Direction A —
`crates/obs-tracing-bridge/src/direction_a.rs`'s metadata cache).
Box-leak the per-key Metadata at insertion. Add a regression
test that installs the bridge with two distinct schemas, emits
both, and asserts the receiving subscriber observes two distinct
`tracing::Metadata`.

### 3.14 — CLI surface gaps still open (P2-AK)

Restated from `94 § P1-9` for the Phase 8 backlog. As of HEAD:

- **`obs generate`** — missing entirely (spec 50 § 3.2).
- **Global flags** — `--root`, `--config`, `--format`,
  `--no-color`, `--quiet`, `-v`/`-vv` not in
  `apps/obs-cli/src/main.rs::Cli` (spec 50 § 2).
- **`obs decode --schemas`** — missing (spec 50 § 3.8).
- **`obs tail --filter`** — missing (spec 50 § 3.10).
- **`obs version --schema`** — missing (spec 50 § 3.14).
- **`obs audit` tracing-bridge query** — `cmd/audit.rs` does
  not show "tracing-bridge events emitted last 7 days" (spec 50
  § 3.7).
- **Tests** — no smoke tests for `diff`, `decode`, `tail`,
  `audit`, `doctor`.

**Severity**: P2.

**Fix shape**: split into separate Phase-8 tickets per command;
each lands its smoke test.

### 3.15 — Label storage is `HashMap`, not `SmallVec<…Cow…>` (P3-AA)

**Spec**: 71 § 1.1 prescribes
`SmallVec<[(&'static str, Cow<'static, str>); 8]>`.
**Code**: `crates/obs-proto/src/pb/obs.v1.envelope.rs:31` — the
generated envelope uses `buffa`'s `HashMap<&'a str, &'a str>`.
The `Cow` borrowed-path optimisation for `&'static str` label
values described in spec 11 § 5 is therefore unreachable; every
label value allocates.

This is the underlying bottleneck for `94 § P2-1` ("hot-path
label allocs", deferred per `93 § 0`). Both items resolve
together.

**Severity**: P3 (cycles only; deferred for v1.1 per the prior
review unless the bench gate of § 3.5 surfaces it as a freeze
blocker).

**Fix shape**: deferred to v1.1; design depends on whether
buffa's runtime supports `Cow`-shaped label storage. Track via
`buffa` issue.

### 3.16 — `tracing-log` not wired (P3-AB)

**Spec**: 61 § 2 lists `tracing-log` as a workspace dep
(`Cargo.toml:60`). The bridge does not consume it; `log::info!`
calls bypass both `tracing` and `obs`.

**Severity**: P3 (legacy dep; `log`-only crates rare in modern
Rust services).

**Fix shape**: at `obs::tracing_bridge::init()`, call
`tracing_log::LogTracer::init()` so `log::*` macros produce
`tracing::Event`s that the bridge then promotes to obs envelopes.
Document under spec 30 as a one-line opt-out for binaries that
already install `LogTracer` themselves.

---

## 4. Phase 8 backlog (severity-ordered)

| ID | Title | Severity | § | Effort |
| --- | --- | --- | --- | --- |
| **P1-AA** | Shared `obs-build::lints` module; move L013 + L014 in; add missing trybuild fixtures | P1 | § 2.1 | 3–5 d |
| **P1-AC** | `obs-tower` client reads active scope; outbound traceparent matches | P1 | § 3.1 | 1 d |
| **P1-AD** | `ObsHttpClientCompleted` typed-payload encoding | P1 | § 3.2 | 0.5 d (rides on P1-AC) |
| **P1-AE** | Parquet + ClickHouse Resource columns + roundtrip test | P1 | § 3.3 | 2–3 d |
| **P1-AF** | OTLP exporters set `traceparent` on outbound RPCs | P1 | § 3.4 | 1 d (rides on P1-AC's `active_correlation()` helper) |
| **P1-AG** | Direction B per-(`full_name`, sev) Metadata synthesis | P1 | § 3.13 | 2 d |
| **P2-AA** | L014 proto-type validation in shared lint module | P2 | § 2.2 | 0.5 d (rides on P1-AA) |
| **P2-AB** | `obs-cli lint` consumes shared lint module; catalogue parity | P2 | § 2.3 | 1 d (rides on P1-AA) |
| **P2-AC** | `bench-gate` nightly GitHub Action | P2 | § 3.5 | 0.5 d |
| **P2-AD** | `emit_noop` budget — profile + fix or revise spec 71 § 3 | P2 | § 3.6 | 1–2 d |
| **P2-AE** | EnvFilter oracle test (parity with `tracing_subscriber::EnvFilter`) | P2 | § 3.7 | 2 d |
| **P2-AF** | Property tests (envelope round-trip, schema hash, scrubber, filter, callsite-id) | P2 | § 3.8 | 3 d |
| **P2-AG** | `cargo-fuzz` harness for the scrubber | P2 | § 3.9 | 1 d |
| **P2-AH** | Per-string length caps in bridge + `obs-tower` (`max_external_string_bytes`) | P2 | § 3.10 | 1 d |
| **P2-AI** | Example expansion: `obs.yaml` per example + 3 new examples | P2 | § 3.11 | 4–6 d |
| **P2-AJ** | Example README hygiene (remove stale "Known limitation" sections) | P2 | § 3.12 | 0.5 d |
| **P2-AK** | CLI surface: `obs generate`, global flags, missing flags, smoke tests | P2 | § 3.14 | 5–7 d |
| **P3-AA** | Label storage `SmallVec<…Cow…>` (rides on buffa runtime) | P3 | § 3.15 | deferred to v1.1 |
| **P3-AB** | Wire `tracing-log` from `tracing_bridge::init()` | P3 | § 3.16 | 0.5 d |
| **E-3** | Strike `no_std` claim from spec 61 § 2.1 (still open) | spec text | `94 § 3.9` | 0.25 d |

**Effort total**: ~30 engineer-days (≈ 4 calendar weeks for one
engineer, 2 weeks for two).

P1 items are the v1 freeze blockers from a Phase 8 perspective.
P2 items are nice-to-have-before-RFC; deferable post-freeze if
schedule pressure is high.

---

## 5. Decisions

These supersede or extend `94 § 5`.

### D8-1 — Lint emission lives in one module, both paths consume it

D7-2 was correct intent but landed only as message synchronisation,
which left the implementations free to drift. Phase 8 fixes this
structurally: `crates/obs-build/src/lints.rs` becomes the single
source of truth; `derive_event.rs` and `codegen.rs` build a
`LintInput` and call `emit_lints(&input)`. `obs-cli`'s `cmd/lint.rs`
takes the same `LintInput` shape and renders results for terminal
output. The L013 + L014 gaps and the CLI catalogue divergence
all resolve when D8-1 lands.

### D8-2 — `obs_core::scope::active_correlation()` is the integration API

D7-3 added `ScopeFrameBuilder` for *pushing* frames; Phase 8
needs the symmetrical *reading* surface for the outbound HTTP
client (§ 3.1), the OTLP exporter (§ 3.4), and any future
middleware that needs to inherit the active trace context from
generic code. Add
`pub fn active_correlation() -> Option<(String, String)>` and
`pub fn active_sampled() -> Option<bool>` on
`obs_core::scope`. Document under spec 13 § 4 alongside
`ScopeFrameBuilder`.

### D8-3 — Resource attrs are batch-level, not envelope-level

The Parquet/ClickHouse columns from § 3.3 are populated from
`Observer::resource_attrs()` once per *batch*, not per envelope.
Per-envelope reads would burn one `ArcSwap::load_full` per row.
Sinks read once at batch start, snapshot the `Arc<ResourceAttrs>`,
and dispatch every row in the batch from that snapshot. If the
snapshot becomes stale mid-batch (config reload), the next
batch picks up the new value — this is the same eventual-
consistency contract the OTLP sinks already follow.

### D8-4 — Bench gate is nightly + non-blocking on PR

D7-5 said nightly; § 3.5 found it isn't wired anywhere. Phase 8
wires it as a `schedule:` workflow only; PRs do not block on
bench results. A regression opens an issue (assigned to the
on-call) rather than failing the merge — bench noise on shared
runners is too high to gate merges, but a daily issue ages out
quickly.

### D8-5 — Examples are first-class spec artifacts

`94 § 6` definition-of-done required the dev_ergonomics suite to
ship; that landed but tests are not user-facing. Phase 8 adds
five more examples (§ 3.11) and treats their READMEs as part of
the v1 surface — a stale README ages worse than a missing one.
Each example's README has a `## What this demonstrates` section
mapping to spec patterns and an `## Out of scope` section listing
patterns the example deliberately omits, so users don't extrapolate.

---

## 6. Definition of done

This document is closed when:

- Every P1 item in § 4 has an integration test pinning the fix.
- `obs-build::lints` is the single lint module; both codegen
  paths and `obs-cli` consume it; trybuild fixtures exist for
  L001–L014 (skip L008 if still deferred to v1.1).
- `obs-tower` outbound `traceparent` matches the active scope's
  `trace_id`; regression test under `crates/obs-tower/tests/`.
- OTLP exporter sets `traceparent` on the outbound RPC;
  `MockOtelCollector` test asserts the captured metadata.
- ParquetSink + ClickHouseSink emit Resource columns; round-trip
  test reads back via Arrow.
- `Direction B` synthesises one Metadata per
  (`full_name`, severity); regression test asserts subscribers
  observe distinct Metadata.
- `bench-nightly.yml` workflow is scheduled and opening issues
  on regressions > 10 %.
- Property test suite ships (envelope round-trip, schema-hash,
  scrubber, filter, callsite-id).
- `cargo-fuzz` target for the scrubber runs as `make fuzz-quick`
  (10 s) and a longer nightly fuzz job.
- Per-field byte caps land in bridge + `obs-tower`; a 1 MiB
  `User-Agent` regression test passes (truncated, not OOM).
- Examples ship `obs.yaml`; three new examples
  (`tracing-migration`, `multi-tenant`,
  `forensic-and-spantrace`) land; every example README is current.
- CLI ships `obs generate`, the spec-50 § 2 global flags,
  `obs decode --schemas`, `obs tail --filter`, `obs version
  --schema`; smoke tests exist for `diff`, `decode`, `tail`,
  `audit`, `doctor`.
- `94-improvements-review.md` items closed by Phase 8 are
  marked superseded by this document.

---

## Appendix A — Audit methodology

Two passes against HEAD `f03c8df`:

1. **Parallel agent verification** of each `94` closure claim.
   Each agent took a spec range and a list of closure IDs, then
   reported file:line confirmations or counter-examples. Agents
   ran in parallel on six axes: P0-A/B (scope + filter); lint
   catalogue parity; typed-payload encoding + ParquetSink;
   ResourceAttrs + label cap + bench gate + winnow; OTLP/analytics
   sinks; `obs-tower` + bridge corners. Two more covered security
   (scrubber, redactor, `forbid(unsafe_code)`) and perf/testing
   (benches, MockOtelCollector, trybuild, soak).
2. **Manual cross-check** of the agents' "still open" claims,
   re-grepping the actual source on disk and reading the
   surrounding code so the § 3 findings carry exact line numbers
   and accurate fix shapes.

Reference reading: `vendors/tracing/tracing-core/src/dispatcher.rs`
(re-entry guard, scoped dispatcher pattern that obs's CAN_ENTER
mirrors), `vendors/tracing/tracing-subscriber/src/filter/env/`
(EnvFilter grammar test vectors that the § 3.7 oracle test should
import), and the OTel semantic conventions for Resource attributes
(needed for § 3.3's column naming).
