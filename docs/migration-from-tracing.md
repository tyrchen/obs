# Migration Guide — `tracing` → `obs`

Status: stable for v1.0 · Last updated: 2026-05-02 · Audience:
crates currently emitting through `tracing` / `tracing-subscriber` who
want the typed-events / sampling / OTel-mapping story `obs` provides.

This document is the **5-page migration brief** referenced by spec
[90 § M4](../specs/90-roadmap.md#m4--hardening--soak--rfc-week-23-26).
It complements but does not replace [`60-dev-ergonomics.md`](../specs/60-dev-ergonomics.md)
(the dev-erg surface) or [`30-tracing-bridge.md`](../specs/30-tracing-bridge.md)
(the bridge mechanics).

---

## 1. Why migrate

`tracing` gives you span trees, structured fields, and an
`EnvFilter`-driven dispatcher. `obs` gives you all of that plus:

- **Schema-first authoring.** Every event is a typed Rust struct or a
  `.proto` message. The compiler enforces label cardinality
  (`HIGH`-cardinality fields cannot become OTel attributes), payload
  size caps, classification (`PII` vs `INTERNAL`), and the Obs-prefix
  convention. See spec [12](../specs/12-schema-and-codegen.md).
- **Tier semantics** — every event is `LOG`, `METRIC`, `TRACE`, or
  `AUDIT`. Tiers drive sink routing (`OtlpMetricSink` only sees
  `METRIC`-tier events) and the AUDIT tier ships with bounded
  blocking + disk-spool recovery (spec 11 § 6.4).
- **Sampling that survives propagation.** Inbound W3C
  `traceparent.sampled` is honoured before the local sampler decides;
  per-event-name overrides live in `obs.yaml` (spec 13 § 6).
- **Tail-on-error ring buffer.** Every `obs::scope!` frame buffers
  the last 64 `TRACE`/`DEBUG` events; on `ERROR` they all flush.
  No more "we sampled out the only event that mattered."
- **Multi-tenant out of the box.** A per-task observer (set via
  `Future::with_observer`) routes one tenant's events to one OTLP
  endpoint without leaking into another's. Spec 11 § 3.1.

You keep the bridge in either direction during the transition —
nothing forces a big-bang rewrite.

---

## 2. Concept mapping

| `tracing` | `obs` | Notes |
| --- | --- | --- |
| `tracing::info!(name = "x", k = v)` event | `obs::emit!(Severity::Info, MyEvent { k: v })` | The macro form is for ad-hoc; production code uses the typed builder: `MyEvent::builder().k(v).emit()`. |
| `tracing::span!` | `obs::scope!(MyScope { … })` | A scope frame carries auto-fill state and a tail-on-error buffer; spans correspond to TRACE-tier scope frames. |
| `tracing::Span::current()` field map | `obs::context!(k = v)` adds a label to the active scope | Auto-fill puts label values onto every emit inside the scope without re-typing them. |
| `tracing::instrument` | `#[obs::instrument]` | Single-event default emits one `ObsFnExecuted` on return; opt-in `enter = true` for the two-event Started/Completed pair. |
| `EnvFilter::new("info,sqlx=warn")` | `obs::Filter::parse("info,sqlx=warn")` | Same grammar (Statics + Dynamics split). Field-value clauses match envelope `labels`. Spec 13 § 7. |
| `tracing_subscriber::fmt::Layer` | `StdoutSink::new(FormatterStyle::Full)` | Four formatter styles: `Full`, `Compact`, `Pretty`, `Json`. |
| `tracing_appender::rolling::RollingFileAppender` | `RollingFileWriter` (size or time-based) wrapped in `NonBlockingWriter` | Same composition as `tracing_appender::non_blocking`. |
| `tracing_opentelemetry::layer()` | `OtlpLogSink` + `OtlpMetricSink` + `OtlpTraceSink` | Each tier has its own sink so the OTLP attribute set is bounded by the schema's LABEL fields. |
| `tracing_log::LogTracer` | `TracingToObsLayer` (Direction A of the bridge) | Captures every `tracing::info!()`-shaped emit as an `ObsTracingForensicEvent`. |
| `tracing::Subscriber` | `Observer` trait + sinks | Three-tier resolution; see spec 11 § 3. |

---

## 3. Drop-in: keep your `tracing` callers

The fastest migration path is **no caller changes**. Install the
bridge once and every `tracing::info!()` becomes an obs event:

```rust
use obs_sdk::{StandardObserver, install_observer, install_panic_hook};
use obs_tracing_bridge::TracingToObsLayer;
use tracing_subscriber::layer::SubscriberExt;

fn main() -> anyhow::Result<()> {
    // 1. Build + install the obs observer.
    let observer = StandardObserver::dev()?;
    install_observer(observer);
    install_panic_hook();

    // 2. Install TracingToObsLayer as the global tracing subscriber.
    let subscriber = tracing_subscriber::registry().with(TracingToObsLayer::new());
    tracing::subscriber::set_global_default(subscriber)?;

    // 3. Existing tracing! emits now route to your obs sinks.
    tracing::info!(target: "myapp", route = "list_users", "request done");
    Ok(())
}
```

What this does:

- `tracing::info!()` is intercepted by `TracingToObsLayer::on_event`.
- The bridge looks up the tracing callsite in the per-process
  `ObsCallsiteRegistry`. First-sight inserts an entry (spec 31 § 3.2);
  subsequent emits hit the cache.
- The event is mapped to `ObsTracingForensicEvent` by default — a
  built-in schema that carries the `target`, `level`, `message`, and
  `fields` map. No schema you authored.
- Your sinks see it as any other `ScrubbedEnvelope`. `OtlpLogSink`
  ships it. `NdjsonFileSink` writes the JSON. `obs query --from
  file.ndjson` filters it.

You stay on this path **forever** for code you don't control
(`sqlx`, `tower-http`, etc.). For code you do control, move to typed
events as you have time.

---

## 4. Authoring the replacement events

Two modes; pick whichever fits your codebase.

### 4.1 Rust-first (`#[derive(Event)]`)

```rust
use obs_sdk::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsRequestCompleted {
    #[obs(label, cardinality = "low")]
    pub route: String,
    #[obs(label, cardinality = "low")]
    pub status: String,
    pub latency_ms: u32,
}

// Replaces: tracing::info!(route = %route, status = %status, ...)
ObsRequestCompleted::builder()
    .route("list_users")
    .status("ok")
    .latency_ms(42)
    .emit();
```

The macro emits the `EventSchema` impl, the `linkme`-collected
registry entry, the typed builder, and the L001/L002/L003/L011
const-eval lints (cardinality, classification, prefix). Spec 12.

### 4.2 Proto-first (`obs-build`)

```proto
// proto/myapp/v1/events.proto
syntax = "proto3";
package myapp.v1;
import "obs/v1/options.proto";

message ObsRequestCompleted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };
  string route = 1 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
  string status = 2 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
  uint32 latency_ms = 3;
}
```

`build.rs` invokes `obs_build::Config::new().compile()`; the rest of
the user-facing API is the same as the Rust-first path. Pick proto
when you have a polyglot service or you want a wire-format anchor
that lives outside Rust.

### 4.3 What to migrate first

A pragmatic ordering:

1. **Hot-path / business-event emits** that drive metrics or alerts
   — `request_completed`, `payment_processed`, `cache_hit`. These
   benefit most from typed schemas (compiler-checked cardinality).
2. **Errors with structured context** — typed schemas let you ship
   `Severity::Error` events with payload-shaped fields, and the
   tail-on-error ring buffer flushes preceding TRACE events.
3. **Span-shaped operations** — wrap with `obs::scope!` so auto-fill
   threads tenant / request labels through every inner emit.
4. **Stay forensic forever**: third-party crate logs, dev-only
   debug, ad-hoc sites. The bridge keeps these flowing as
   `ObsTracingForensicEvent`s — no rewrite required.

---

## 5. Filter / sampling translation

`tracing-subscriber`'s `EnvFilter` parser became `obs::Filter`; the
grammar is identical. The big behaviour differences:

| Mechanism | `tracing` | `obs` |
| --- | --- | --- |
| Inbound W3C `traceparent.sampled` | Honoured by `tracing-opentelemetry` if you wire it | Honoured by the head sampler **before** the local rate; opt-out via `obs.yaml` `sampling.honour_traceparent_sampled = false`. |
| Per-callsite filter cache | `tracing::Interest` per callsite | Same shape (`Interest::{Never,Sometimes,Always}`) plus a `generation` so config reload invalidates. Spec 11 § 2. |
| Tail-on-error | Not built-in | Every `obs::scope!` frame holds a 64-event ring buffer; an `ERROR`-or-higher emit inside the frame flushes the ring. Spec 13 § 6. |
| Per-event sample rate | Not built-in | `sampling.per_event` map in `obs.yaml`. Useful for noisy events (`ObsCacheLookup` — sample 1%). |
| Hot reload | `EnvFilter` reload requires plumbing | `StandardObserver::reload_config` swaps the filter and bumps the generation; every callsite re-probes. SIGHUP path lands in Phase 3. |

A typical translation:

```bash
# tracing
RUST_LOG="info,sqlx=warn,myapp::cache=trace"

# obs
OBS_FILTER="info,sqlx=warn,myapp::cache=trace"
# or in obs.yaml:
# filter: "info,sqlx=warn,myapp::cache=trace"
# sampling.per_event:
#   myapp.v1.ObsCacheLookup: 0.01
```

---

## 6. OTLP export

The biggest win for migrators that already export to OTLP:

```rust
use obs_otel::otlp_trio_from_env;

let observer = StandardObserver::builder()
    .service("myapp", env!("CARGO_PKG_VERSION"))
    .sink_for(Tier::Log, otlp_trio_from_env()?.logs)
    .sink_for(Tier::Metric, otlp_trio_from_env()?.metrics)
    .sink_for(Tier::Trace, otlp_trio_from_env()?.traces)
    .build()?;
```

`otlp_trio_from_env()` reads the standard OTel env vars
(`OTEL_EXPORTER_OTLP_ENDPOINT`, etc.). The three sinks each map their
tier into the right OTel signal — `OtlpLogSink` to `LogRecord`,
`OtlpMetricSink` to `Metric` (with bounded enum-LABEL attribute
sets), `OtlpTraceSink` to `Span` (three-pattern Started/Completed
mapping per spec 20 § 2.5).

You don't lose `tracing`'s span context — both directions of the
bridge propagate `trace_id`/`span_id` via the envelope.

---

## 7. Tests

`tracing`'s `tracing-test` crate captures emits per-test. `obs`
ships an equivalent that's parallel-safe:

```rust
#[obs::test]
async fn login_emits_event() -> anyhow::Result<()> {
    login("alice").await?;
    assert_emitted!(MyApp::v1::ObsLoginSucceeded { user = "alice" });
    Ok(())
}
```

`#[obs::test]` installs an `InMemoryObserver` per-thread (sync) or
per-task (async) for the duration of the test, so you can run
`cargo test` parallel without `serial_test`. Spec 72 § 3.

---

## 8. Common gotchas

- **`tracing::Span::current()` ≠ `obs::scope!`.** `tracing` spans
  build a tree implicitly via thread-local context. `obs::scope!`
  is explicit — if you don't open a scope, `obs::context!` has no
  receiver. Read [`13 § 2`](../specs/13-emit-scope-and-filter.md#2-scopes).
- **Per-thread observer + `tokio::spawn` migration.**
  `with_test_observer(...)` only sets the per-thread slot; if your
  code spawns a task, the new task sees a different thread and
  loses the override. Use `Future::with_observer` or
  `with_observer_task` for async. Spec 11 § 3.1.
- **Forensic budget (L010).** `obs lint --strict` walks every
  callsite and fails if your crate exceeds the per-crate forensic
  budget (default 5). The bridge's `ObsTracingForensicEvent`s
  do **not** count against this — the budget targets `obs::forensic!`
  in your own code.
- **AUDIT tier on first install.** `StandardObserver::build` calls
  `recover_audit_spool` which drains any leftover `*.audit.bin`
  files. If your prior process crashed mid-write, you'll see an
  `ObsAuditSpoolRecovered` self-event with the count. This is
  the system working — review the spool dir for unprocessed
  records before clearing.

---

## 9. Cheat sheet

```text
tracing::info!(target: "x", k = v, "msg")
  →  TracingToObsLayer (no rewrite)
  →  TypedSchema::builder().k(v).emit()  (when you migrate)

tracing_subscriber::EnvFilter::new(spec)
  →  obs::Filter::parse(spec)

#[tracing::instrument]
  →  #[obs::instrument]                    // single-event default

tracing_appender::rolling::Builder
  →  RollingFileWriter::builder() + NonBlockingWriter::new(_, cap)

tracing_opentelemetry::layer
  →  OtlpLogSink / OtlpMetricSink / OtlpTraceSink

tracing-test
  →  #[obs::test] + assert_emitted!
```

---

## 10. Where to ask

- Bug / feature requests: `https://github.com/TODO/issues`.
- Design discussion: `./specs/` (file an issue against the
  closest-numbered spec).
- Performance regressions: include `cargo bench --bench
  emit_hot_path` deltas vs `benches/baseline.json` (10% threshold).
