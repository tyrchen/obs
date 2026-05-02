# Design — Emit, Scope, Instrument, Filter

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [11-runtime-core.md](./11-runtime-core.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md)

This spec defines the user-facing macro/builder surface — `obs::emit!`,
`obs::scope!`, `obs::Instrumented<F>`, `#[obs::instrument]`, the
`obs::Filter` DSL, and `obs::forensic!`. It also clarifies the
relationship between `obs::scope!` and `tracing::Span` (they are *not*
equivalent — see § 4).

> v3 changes: split out from the v2 monolithic architecture spec;
> added `obs::Instrumented<F>` future adapter so scopes can cross
> spawned-task boundaries; clarified `obs::scope!` is a
> field-allowlist + tail buffer, not a span analogue; changed
> `#[obs::instrument]` default to emit one event (not two); split off
> filter precedence rules; documented `emit_at` no longer clamps
> upward only; introduced `obs::SpanTrace` for error-with-context
> capture.

## 1. Two emit forms: builder is canonical, macro is shorthand

`obs` ships two equivalent emit forms. **The builder form is canonical
and is what the docs, scaffolding, and AI prompts default to.** The
macro is a *shorthand* for terse cases.

```rust
// PRIMARY — chained typed builder, preferred for clarity and editor support:
ObsRequestCompleted::builder()
    .route(Route::ListUsers)
    .status(Status::Ok)
    .latency_ms(48)
    .emit();                         // .emit_at(Severity::Warn) to escalate or demote

// SHORTHAND — struct-literal macro, useful for tiny events:
obs::emit!(ObsHelloEmitted { who: Audience::World });
obs::emit!(Severity::Warn, ObsUpstreamFailed { route, error_kind });
```

Why the builder is canonical:

- **`rust-analyzer` chain-completion** lights up immediately after
  `::builder().` — no need to look up field names.
- **Required-field errors are pinpointed** at `.emit()` (typed-builder
  marker types refuse to compile when a required setter is missing).
- **Reads top-down** — the eye scans setters in order; the literal
  form forces a struct shape that mixes ordering with semantics.
- **Trivially refactorable** — adding or removing one field is a
  one-line change; struct literals require finding the exact braces.
- **Composable** — a helper can `fn into_builder(self) -> Builder<...>`
  so common defaults are reusable.

The macro form remains for cases where a struct literal genuinely
reads better — typically events with one or two fields, or terse
escalation: `obs::emit!(Severity::Warn, ObsThing { x });`.

The `Severity` keyword form (`obs::emit!(WARN, …)`) is supported via
re-exported severity idents `obs::TRACE`, `obs::DEBUG`, `obs::INFO`,
`obs::WARN`, `obs::ERROR`, `obs::FATAL` — `obs::emit!(WARN, …)` and
`obs::emit!(Severity::Warn, …)` are the same call.

### 1.1 `emit_at(sev)` — both directions

Earlier drafts clamped `emit_at` upward only: a schema with
`default_sev = Info` could be escalated to `Warn` but not demoted to
`Debug`. That clamp is removed. Legitimate cases for demotion exist
("during graceful shutdown a normally-INFO event should be DEBUG"),
and the asymmetry confused users. The runtime accepts whatever
severity the call site passes; the schema's `default_sev` is just the
default when `emit()` is used.

The OTLP severity mapping ([20-otel-and-sinks.md § 2.2](./20-otel-and-sinks.md#22-severity--otlp-severitynumber))
applies to whatever value reaches the envelope.

## 2. The `obs::scope!` macro

```rust
let _scope = obs::scope!(
    trace_id  = req.id.clone(),
    tenant_id = tenant.clone(),
);
```

`obs::scope!` does **two** orthogonal things, both bound to the same
RAII guard:

1. **Trace correlation**: any field on the active scope frame whose
   name is `FIELD_KIND_TRACE_ID` / `SPAN_ID` / `PARENT_SPAN_ID` on the
   emitted event auto-fills the corresponding envelope id.
2. **Field broadcasting**: the named fields are an *allowlist* — any
   matching field on the emitted event whose value is the type's
   default sentinel is auto-filled from the scope.

The frame additionally holds a 64-deep ring buffer for tail-on-error
sampling (see [11-runtime-core.md § 4](./11-runtime-core.md#4-per-tier-workers-and-sinks)
for the worker model and [§ 6 below](#6-sampling) for sampling).

Effects on entry:

1. Push a `ScopeFrame { fields, tail_buffer: VecDeque::with_capacity(64), seen_error: false }`
   onto a `tokio::task_local!` stack (or thread-local for sync code).
2. Every subsequent `obs::emit!` first inspects the stack: if a field
   on the event schema is empty *and* a frame above declares a value
   for that field name, the value is auto-filled.
3. The frame's `tail_buffer` records every emitted envelope at TRACE
   or DEBUG until either:
   - an event with `sev >= ERROR` is emitted → buffer is flushed
     (sampling_reason = `tail_on_error`), `seen_error = true`, or
   - the scope guard is dropped → buffer is discarded.
4. When the scope guard is dropped, the frame is popped. **No
   `on_request_end()` call to forget.** This is a deliberate fix for
   the leak class found in scope-by-string designs.

`obs::scope!` is an **explicit allowlist**: only the fields named in
the macro propagate; nothing else from the surrounding context leaks
into events.

### 2.1 Auto-fill semantics for default-vs-explicit values

Auto-fill rule: a scope-declared field overrides an event field only
when the event field's value is the type's default sentinel
(`String::new()` for strings, `0` for numerics with `#[obs(trace_id)]`
or `#[obs(label)]` annotation marked `default-fillable`).

There is one sharp edge: an explicit empty string passed at the call
site is indistinguishable from the default. The codegen mitigates
this by typing `default-fillable` `String` fields as `Option<String>`
in the generated `ArgsBuilder` (so `.trace_id("")` → `Some("")` ≠ the
default `None`), and the auto-fill path only fires when the option is
`None`. The user never sees `Option`; the typed-builder setter takes
`impl Into<String>` as before.

### 2.2 `obs::context!` — broadcasting only, no tail buffer

Some users want field broadcasting without the per-scope tail buffer
(it costs ~64 envelope-sized slots per active scope). For that case
the SDK ships `obs::context!`:

```rust
let _ctx = obs::context!(tenant_id = tenant.clone());
```

`obs::context!` is `obs::scope!` minus the tail buffer. Use this for
deeply nested helpers that just want to broadcast a constant (rather
than re-thread it manually). Use `obs::scope!` at request boundaries
where tail-on-error matters.

### 2.3 Validation of declared fields

A proc-macro cannot scan the entire binary at expansion time, so we
cannot prove at compile time that every named field is actually
consumed by some `EventSchema`. We do this:

- **At observer init**, the SDK builds a global `BTreeSet<&'static str>`
  of field names declared as LABEL or TRACE_ID across every
  `EventSchema` registered in the binary (the codegen emits a
  `register_schema` call per type, collected by `inventory`).
- **In dev mode** (`OBS_DEV=1` or debug builds), the first emit inside
  a scope frame whose declared fields contain a name absent from that
  set issues a one-time `tracing::warn!` (or stderr line) naming the
  field. In release builds the check is skipped.

This is best-effort runtime validation, not compile-time enforcement.
Documented as such — the original spec language was misleading.

## 3. `obs::Instrumented<F>` — async scope adapter

`tokio::task_local!` does not propagate across `tokio::spawn`. So a
naive

```rust
let _scope = obs::scope!(trace_id = req.id);
tokio::spawn(async move { /* loses scope here */ });
```

orphans the spawned task. `obs::Instrumented<F>` is the fix, mirroring
`tracing-futures::Instrument`:

```rust
use obs::Instrument;            // trait, brings .instrument() into scope

tokio::spawn(
    async move {
        ObsBackgroundStarted::builder().emit();   // sees parent scope
    }
    .instrument(obs::scope!(trace_id = req.id.clone(), tenant_id = tenant.clone())),
);
```

`Instrumented<F>`'s `Future::poll` enters the scope before delegating
to the inner future and exits the scope on return; the scope guard
lives inside the `Instrumented` value, so cancellation drops it
correctly.

The scope can also be detached and re-applied later:

```rust
let scope = obs::scope!(trace_id = req.id).into_inner();   // unwrap the guard
let fut   = some_future().instrument_with(scope);
```

`Instrumented<F>` is in `obs-sdk`, gated by no feature; library crates
get it free.

## 4. `obs::scope!` is **not** a `tracing::Span`

Reader confusion is common, so this section is explicit.

| `tracing::Span` | `obs::scope!` |
| --- | --- |
| Has start time + multiple `enter`/`exit` cycles | RAII guard; no enter/exit, no duration |
| `Span::record(field, value)` after creation | No post-hoc field recording on the scope itself |
| Multiple subscribers can attach independent state via `extensions` | Per-task task-local; one scope, one stack |
| Span has its own `Id` produced by the subscriber | Scope has no id; the trace/span ids come from the `trace_id`/`span_id` *fields* |
| Spans nest into a tree per subscriber | Frames stack per task |

If you want **span semantics with duration**, the canonical recipes
are:

1. **Started/Completed event pair** — emit `ObsRequestStarted` at the
   front edge and `ObsRequestCompleted` at the back; latency is a
   field on `Completed`. Read [60-dev-ergonomics.md § 4](./60-dev-ergonomics.md#4-authoring-patterns).
2. **`#[obs::instrument]`** — wraps a function body in a scope and
   emits a single completion event with `latency_ns`. § 5.

Bridging spans from `tracing::Span` (e.g. `tower-http`'s spans) into
`obs` produces `obs.v1.ObsSpanCompleted` envelopes via the bridge.
That is *the* canonical span representation for non-typed sources.
See [30-tracing-bridge.md § 2.3](./30-tracing-bridge.md#23-span-mapping).

## 5. The `#[obs::instrument]` attribute

```rust
#[obs::instrument(
    fields(route, tenant_id),
    skip(raw_body),
)]
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    // ...
}
```

Default expansion (one event, on exit):

```rust
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    let _scope    = obs::scope!(route = req.route(), tenant_id = req.tenant());
    let __started = std::time::Instant::now();
    let __res     = async move { /* original body */ }.await;
    obs::emit!(ObsFnExecuted {
        fn_name:    "handle_list_users",
        latency_ns: __started.elapsed().as_nanos() as u64,
    });
    __res
}
```

Earlier drafts emitted *two* events (`ObsFnEntered` + `ObsFnExited`),
which doubled traffic on hot paths for marginal value. The new
default is one event (`ObsFnExecuted`); explicit `enter = true`
re-enables the entered event:

```rust
#[obs::instrument(enter = true, fields(route))]   // emits both ObsFnEntered + ObsFnExecuted
```

`ObsFnExecuted` is shipped in `obs-proto/builtin.proto`; LOG-tier,
INFO-default-sev, fields `fn_name` (LABEL, MEDIUM) and `latency_ns`
(MEASUREMENT histogram). `ObsFnEntered` is also shipped for the
opt-in case.

The proc-macro respects the `Instrumented<F>` adapter: an `async fn`
expansion uses `.instrument()` so the scope crosses `await` points
correctly without leak.

## 6. Sampling

Two-stage sampling, both configured live via `ArcSwap<EventsConfig>`:

- **Head sampling**: per `(event_full_name, severity)` rate. Fast path,
  one `f64` comparison.
- **Tail-on-error**: a per-scope ring buffer (capacity 64) holds recent
  events; if any subsequent event in the same `obs::scope!` is
  `>= ERROR`, the buffer is flushed. Otherwise the buffer is dropped
  when the scope guard is dropped.

The tail buffer lives in a `tokio::task_local!` storage; entering an
`obs::scope!` macro pushes a new buffer onto the per-task stack. The
scope guard's `Drop` impl is what triggers either flush or discard —
**there is no "request_end()" call to forget**.

```rust
pub async fn handle_request(req: Request) -> Response {
    let _scope = obs::scope!(trace_id = req.id.clone());

    ObsRequestStarted::builder().route(route_of(&req)).emit();
    let r = process(req).await;          // may emit ObsUpstreamFailed (ERROR)
    ObsRequestCompleted::builder()
        .route(route_of(&req))
        .status(if r.is_ok() { Status::Ok } else { Status::ServerError })
        .latency_ms(r.elapsed_ms())
        .emit();

    r
    // _scope dropped (including on async cancel — see [11-runtime-core.md § 8.1](./11-runtime-core.md#81-async-cancellation)):
    // if any ERROR seen, flush full buffer; else discard.
}
```

## 7. The `obs::Filter` DSL

`obs::Filter` mirrors `tracing-subscriber::EnvFilter`:

```
OBS_FILTER="info,myapp::auth=debug,myapp.v1.ObsRequestCompleted=trace"
```

Filters apply at the static `ObsCallsite` level so a filtered-out
emit costs only the atomic `Interest` load + branch (see
[11-runtime-core.md § 2](./11-runtime-core.md#2-the-obscallsite-and-atomic-interest-cache)).

### 7.1 Field-value directives

The DSL accepts the tracing-equivalent `target[field=value]` syntax:

```
OBS_FILTER="info,myapp.v1.ObsRequestCompleted[route=admin]=trace"
```

The match is against label values on the envelope; the runtime checks
labels in the post-projection step before deciding to drop, so this
costs one `BTreeMap::get` per filter directive. Only fields that are
LABEL-class are matchable (matching on ATTRIBUTE values would require
decoding the typed payload, which the filter pipeline avoids).

### 7.2 Filter precedence

The full system has three filter layers; precedence is documented
once here:

```
┌─ tracing::EnvFilter (RUST_LOG) ─┐
│  gates which tracing events     │  ← only relevant if obs-tracing-bridge is installed
│  reach Layer::on_event          │
└─────────────────────────────────┘
              │
              ▼
┌─ obs::Filter (OBS_FILTER /  ────┐
│  obs.yaml `filter:`)            │  ← gates native obs::emit! and bridged events
│  evaluated against ObsCallsite  │
└─────────────────────────────────┘
              │
              ▼
┌─ Sink-side filters (per-sink) ──┐
│  e.g. severity floor on stdout  │  ← optional; documented per sink
└─────────────────────────────────┘
```

`OBS_FILTER` overrides `obs.yaml.filter` for the lifetime of the
process; both reference the same DSL grammar.

### 7.3 Cache invalidation on reload

When `EventsConfig` reloads (SIGHUP, file watcher, programmatic
`reload()`), `Observer::reload_filter()` is called. The implementation
bumps `Observer::generation`, which makes every `ObsCallsite::enabled`
re-query observer interest on the next emit. Stale per-callsite
caches are flushed transparently — see
[11-runtime-core.md § 3.2](./11-runtime-core.md#32-filter-cache-invalidation-on-reload).

## 8. The forensic escape hatch

```rust
obs::forensic!(
    site = "billing::reconcile",
    message = "unexpected ledger state",
    {
        "ledger_id" => ledger_id,
        "expected"  => expected_balance,
        "actual"    => actual_balance,
    }
);
```

This emits `obs.v1.ObsForensicEvent` with `site`, `message`, and a
`map<string, string>` payload. It is rate-limited per-site, per-process,
governed by `[package.metadata.obs] forensic_max = N` and validated by
`obs lint`. Forensic events are **always** flushed regardless of
sampling, so emergency data is never lost.

The intent is that forensic usage trends to zero over time as schemas
mature. The CLI surfaces "how many forensic events emitted last week
per site" as a work-driving signal for engineering teams.

Rate-limit details are in [11-runtime-core.md § 6.3](./11-runtime-core.md#63-forensic-rate-limit).

## 9. `obs::SpanTrace` — error capture with scope context

Tracing-error provides `SpanTrace` for capturing the active span chain
into an error type for backtrace-style debugging. `obs::SpanTrace` is
the analogue:

```rust
use obs::SpanTrace;

#[derive(thiserror::Error, Debug)]
pub enum BillingError {
    #[error("ledger drift")]
    Drift {
        #[source] inner: anyhow::Error,
        scope:           SpanTrace,
    },
}

fn reconcile() -> Result<(), BillingError> {
    do_work().map_err(|e| BillingError::Drift {
        inner: e,
        scope: SpanTrace::capture(),     // walks the obs::scope! stack
    })
}
```

`SpanTrace::capture()` walks the active task's `obs::scope!` stack and
records `(name, fields)` for each frame. `Display` prints them with
the closest frame first. Cheap (no allocation if the stack is empty);
linear in stack depth otherwise.

`SpanTrace` integrates with `anyhow::Error` via a `Display` impl that
includes the trace; users can embed it in error types directly with
`thiserror`.

## 10. Build dependencies

| Depends on | Provides |
| --- | --- |
| [10-data-model.md](./10-data-model.md) | Foundation types |
| [11-runtime-core.md](./11-runtime-core.md) | `Observer`, `ObsCallsite`, scope task-local |
| [12-schema-and-codegen.md](./12-schema-and-codegen.md) | typed builders, `EventSchema` |

This spec ships in `obs-macros` (proc-macros) and `obs-core` (runtime
support for scope frames, filters, forensic limiter). See
[61-crates-and-features.md § 2.3 + § 2.4](./61-crates-and-features.md).
