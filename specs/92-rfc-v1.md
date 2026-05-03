# Public RFC — `obs` v1.0

Status: draft for the public-comment window · Owner: obs-core ·
Last updated: 2026-05-02 · Window: 4 weeks · Consumes: every spec in
this directory · Implements: [91-impl-plan.md § 5.8](./91-impl-plan.md#8-phase-5--hardening-soak-rfc-weeks-23-26).

This RFC is the externally-visible summary published at the start of
the v1.0 comment window. Reviewers don't need to read every spec
file — this document captures the load-bearing decisions, the API
surface as it stands at the freeze, the migration story, and the
invariants we are willing to lock for the v1.x line.

> **Reviewer brief.** If you only have 30 minutes: skim § 1 (charter),
> § 4 (API surface at the freeze), § 5 (locked invariants). Then jump
> into the spec(s) for whichever subsystem you want to comment on.

---

## 1. Charter

`obs` is a Rust SDK for **wide structured events**. One emission
becomes a log line, a metric point, a trace span, and an analytics
row in a unified columnar table — schema-first, with compile-time
cardinality / classification safety, three-tier observer resolution
for multi-tenant servers, and a bidirectional `tracing` bridge so no
existing crate has to rewrite to ship.

Source-of-truth design: [specs/00-prd.md](./00-prd.md). Build order
and milestone exit criteria: [specs/90-roadmap.md](./90-roadmap.md)
and [specs/91-impl-plan.md](./91-impl-plan.md).

### 1.1 Non-goals (v1)

Out of scope so reviewers don't need to ask:

- Cross-language SDKs (Go, Python, TypeScript) — Rust-only in v1.
- Cluster-wide sampling agreement.
- An HTTP schema-registry service.
- A GUI for `obs schema show` / `obs diff`.
- `obs query` against Iceberg.

These live under spec [90 § M-future](./90-roadmap.md) and are
revisited for v1.1+ based on adoption signal.

---

## 2. Why this and not `tracing` + crates

`tracing` is excellent. `obs` augments it where production teams
have run into walls:

| Pain point | What `obs` ships |
| --- | --- |
| HIGH-cardinality string sneaks into a Prom label and explodes the index | Compile-time L001/L002 lints reject `cardinality = high` on any field marked `LABEL`; codegen surface refuses to compile. Spec 12. |
| One emit ≠ log + metric + span | The same `EventSchema` produces all four signals; sinks consume `ScrubbedEnvelope` and project the right surface (LogRecord, DataPoint, Span, Arrow row). Spec 14 / 20 / 22. |
| Multi-tenant emits leak across tenants | Three-tier observer resolution (per-task → per-thread → global); a `Future::with_observer(...)` adapter routes a tenant's emits to that tenant's OTLP endpoint without a Mutex. Spec 11 § 3. |
| Sampled-out events are exactly the ones you wanted | `obs::scope!` frames carry a tail-on-error ring buffer; an ERROR-or-higher emit flushes the buffered TRACE/DEBUG events so post-mortem context is intact. Spec 13 § 6. |
| AUDIT events drop under load | Bounded-blocking AUDIT tier with a binary length-prefixed disk spool (CRC-checked, fsync'd, drained on observer init). Spec 11 § 6.4. |
| `tracing` migrants need a rewrite | `TracingToObsLayer` (Direction A) + `ObsToTracingSink` (Direction B) bridge bidirectionally; ship without rewriting any caller. Spec 30. |

---

## 3. Scope summary

| Subsystem | Spec | M-stage | Status |
| --- | --- | --- | --- |
| Vocabulary enums | [10](./10-data-model.md) | M0 | locked |
| Wire envelope | [10](./10-data-model.md) / [`obs/v1/envelope.proto`](../crates/obs-proto/proto/obs/v1/envelope.proto) | M0 | locked at `format_ver = 1` |
| Runtime core (Observer / sinks / config / AUDIT) | [11](./11-runtime-core.md) | M0–M2 | locked |
| Schema authoring + codegen | [12](./12-schema-and-codegen.md) | M0–M1 | locked |
| Emit / scope / filter / sampler | [13](./13-emit-scope-and-filter.md) | M1–M2 | locked |
| Schema registry | [14](./14-schema-registry.md) | M0 | locked |
| `obs.yaml` config | [15](./15-config.md) | M0 | locked |
| OTLP + sinks | [20](./20-otel-and-sinks.md) | M2 | locked |
| Analytics storage (Parquet + ClickHouse) | [22](./22-analytics-storage.md) | M3 | locked |
| `tracing` bridge | [30](./30-tracing-bridge.md) | M2 + M3 | locked |
| Callsite interning | [31](./31-callsite-interning.md) | M3 | locked, default `Off` for v1 |
| HTTP middleware | [40](./40-http-middleware.md) | M3 | locked |
| CLI | [50](./50-cli.md) | M1–M3 | locked |
| Dev ergonomics | [60](./60-dev-ergonomics.md) | M1+ | locked |
| Crates + features | [61](./61-crates-and-features.md) | spans | locked |
| Security + classification | [70](./70-security-and-classification.md) | M0+ | locked |
| Performance budgets | [71](./71-performance-budgets.md) | gates | locked |
| Testing strategy | [72](./72-testing-strategy.md) | spans | locked |
| Glossary + key decisions | [80](./80-glossary.md) / [99](./99-key-decisions.md) | spans | locked |

---

## 4. The v1 surface, at a glance

End-user code (everyday case):

```rust
use obs_sdk::{Event, Severity, StandardObserver, install_observer};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsRequestCompleted {
    #[obs(label, cardinality = "low")]
    route: String,
    #[obs(label, cardinality = "low")]
    status: String,
    latency_ms: u32,
}

fn main() -> anyhow::Result<()> {
    install_observer(StandardObserver::dev()?);
    ObsRequestCompleted::builder()
        .route("list_users")
        .status("ok")
        .latency_ms(42)
        .emit();
    Ok(())
}
```

Façade re-exports (`obs_sdk::*`): `Event`, `Emit`, `EventSchema`,
`EventsConfig`, `Filter`, `Instrument`, `Instrumented`, `MetricEmitter`,
`Observer`, `StandardObserver`, `Sink`, `StdoutSink`, `NdjsonFileSink`,
`InMemoryObserver`, `InMemorySink`, `MakeWriter`, `RollingFileWriter`,
`NonBlockingWriter`, `WorkerGuard`, `WithObserver`, `install_observer`,
`install_panic_hook`, `observer`, `with_observer_task`,
`with_observer_thread_local`, `with_test_observer`.

Macros: `obs_sdk::{emit!, scope!, context!, forensic!, instrument,
test, include_schemas}`.

CLI (`obs <cmd>`): `init`, `validate`, `lint`, `schema show`,
`decode`, `tail`, `query`, `doctor`, `diff`, `audit`, `migrate`,
`version`, `completions`.

---

## 5. Invariants locked for v1.x

These freezes are what reviewers most need to push back on if the
shape feels wrong. Each is enforced by tests + CI:

1. **Envelope wire shape.** `obs/v1/envelope.proto` field layout is
   locked. `ENVELOPE_FORMAT_VER == 1` for the entire v1.x line. Any
   PR that touches `envelope.proto` must bump `ENVELOPE_FORMAT_VER`
   in [`crates/obs-proto/src/lib.rs`](../crates/obs-proto/src/lib.rs);
   the [`format-ver-guard.yml`](../.github/workflows/format-ver-guard.yml)
   workflow fails any PR that doesn't.

2. **`obs-types` enum vocabulary.** Adding variants is non-breaking;
   reordering or removing variants requires a major bump and a
   migration guide entry. Spec 90 § 3.3.

3. **Three-tier observer resolution.** Per-task → per-thread →
   global. The resolver signature does not change in v1.x because
   that would require every emit-site recompile. Spec 11 § 3 / 99 D-3.

4. **`Sink::deliver(ScrubbedEnvelope<'_>)`** is the only delivery
   contract; sinks never see an unscrubbed envelope. Spec 14 § 5.

5. **AUDIT tier semantics.** Bounded blocking on emit (`100 ms`
   default) → disk spool → recovery on next observer init.
   Operators choose `panic` / `abort` / `warn_only` on spool-write
   failure via `audit.on_failure`. Spec 11 § 6.4.

6. **Pipeline order on emit-thread.** `enabled_static` →
   `Observer::enabled` → `project + auto-fill` → `head sampler` →
   `tail buffer` → mpsc. The scrubber runs on the worker thread,
   never on emit. Spec 11 § 4.1.

7. **Bridge default.** `TracingToObsLayer` maps tracing events to
   `ObsTracingForensicEvent` by default; auto-typed promotion is
   opt-in via `register_typed::<E>(matcher, promote)`. Spec 30 § 2.5.

8. **Interning default.** `InterningMode::Off` for v1.0; `Hybrid` /
   `Compact` are opt-in. Default flip is reserved for v1.1+. Spec 31.

9. **Performance gates.** `bench_emit_noop` ≤ 50 ns; full emit
   pipeline budgets in [`71-performance-budgets.md § 4`](./71-performance-budgets.md);
   10 % regression fails CI.

10. **Always shippable.** Per CLAUDE.md, every commit on `master`
    leaves `cargo build && cargo test --workspace --all-features &&
    cargo clippy -- -D warnings && cargo +nightly fmt --check &&
    cargo deny check` green. Phase 5 added `make lint-strict` (clippy
    pedantic with curated allows) and `make soak` (30-second
    50 k-events/sec ObsSinkDropped == 0 assertion) on top.

---

## 6. Open questions for reviewers

Items where reviewer feedback would shape v1.0 stamping:

1. **Default interning mode.** Should v1.0 ship with
   `InterningMode::Off` (current plan) or `Hybrid`? Off is the
   safer default for a new project; Hybrid pays one BLAKE3 hash per
   first-sight callsite for a roughly 20 % wire-size reduction on
   high-cardinality tracing-bridge traffic. Spec 31.

2. **`obs.yaml` SIGHUP reload.** Current plan reloads on SIGHUP +
   `notify`-driven file-watch. Some operators have asked for an
   HTTP `/reload` endpoint. Defer to v1.1 if no one objects.

3. **Bridge Direction B `OnScope` mode and `OtlpTraceSink`
   coexistence.** Spec 30 § 3.5 emits an `ObsConfigInconsistent`
   warning when both are wired (you'd get duplicate spans). Should
   we instead refuse to build the observer? Defaulting to a warning
   is what we have; a hard error is safer but breaks people who
   intentionally route through both for migration.

4. **`obs init` package layout.** The current scaffold creates a
   single crate with a `proto/` (or `src/events.rs`) plus an
   `obs.yaml`. Larger workspaces may want a `crates/<svc>-events`
   sub-crate. Reviewer poll: should `obs init --workspace` exist?

5. **`obs.yaml` strict mode.** Today unknown keys fail (`#[serde(deny_unknown_fields)]`).
   Some users have asked for `--allow-unknown` for forward-compat
   during in-place upgrades. Defer if no objection.

6. **Forensic budget default.** L010 currently caps `obs::forensic!`
   at 5 callsites per crate. Real services range from 0 to ~50; the
   number should probably be 10 or even 20. Reviewer poll.

---

## 7. Migration story

Existing `tracing` users have a no-rewrite migration path:
[`docs/migration-from-tracing.md`](../docs/migration-from-tracing.md).
The bridge keeps every `tracing::info!()` flowing as
`ObsTracingForensicEvent` while you migrate emit-sites to typed
schemas at your own pace. Bridges in both directions are tested as
1000-iteration `--release` stress tests (spec 30 § 6).

---

## 8. Maintenance commitments

- **Semver discipline.** `obs-*` crates honour semver from v1.0:
  patch = bug fixes; minor = additive (e.g. new sink, new built-in
  event); major = wire-shape or `Observer` trait changes.
- **Rust MSRV.** Pinned at the current stable minus one. The
  workspace `rust-toolchain.toml` is the source of truth.
- **Dependency surface.** `cargo deny check` runs on every PR;
  license set + advisory ignores documented inline in `deny.toml`.
- **Performance regressions.** `cargo bench` runs against
  `benches/baseline.json`; > 10 % regression on any tracked path
  fails CI (spec 71 § 5).
- **24-h soak before each release.** `make soak-24h` exercises the
  full emit pipeline at 50 k events/sec into a `NonBlockingWriter`-
  backed NDJSON sink; the run asserts `ObsSinkDropped == 0` after a
  60 s warm-up window.

---

## 9. How to comment

- File an issue at `https://github.com/TODO/issues` with the prefix
  `[rfc/v1]` followed by the spec number you're commenting on.
- Push back on any of the **§ 5 invariants** if you think the shape
  is wrong — those are what we're locking for the entire v1.x line.
- Suggest renames freely; the API surface is still moveable until
  the comment window closes.

The window closes 4 weeks after the RFC publish date (spec 91 § 5.8).
At close, every accepted change is folded back into the relevant
spec, this RFC is updated with a "what changed" appendix, and v1.0
is stamped.
