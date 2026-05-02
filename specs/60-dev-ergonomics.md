# Design — Developer Ergonomics

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md), [61-crates-and-features.md](./61-crates-and-features.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md), [72-testing-strategy.md](./72-testing-strategy.md)

> v3 changes: cross-references retargeted to the post-split spec
> structure; clarified that `obs::scope!` is *not* a tracing-`Span`
> analogue (see [80-glossary.md](./80-glossary.md) and
> [13-emit-scope-and-filter.md § 4](./13-emit-scope-and-filter.md#4-obsscope-is-not-a-tracingspan));
> parallel-test fix references the per-thread observer override slot
> in [11-runtime-core.md § 3](./11-runtime-core.md#3-the-observer-trait)
> and the `#[obs::test]` attribute now uses
> `obs::with_test_observer` so cargo's default parallelism is safe;
> removed the misleading "scope ≈ Span::enter()" line in the v2 draft.

The PRD's bar is "match `tracing` ergonomics with stronger guarantees".
This document is the contract for what that actually feels like to use.
Every example here must compile against the API surface in
[61-crates-and-features.md](./61-crates-and-features.md) — if it doesn't, the spec is
wrong, not the example.

## 1. North star

A user should be able to:

1. **Add the dep, scaffold a schema, emit an event in 60 seconds.**
2. **Read a call site in 1 second** and know which event it is, what
   its semantics are, and where to look it up.
3. **Get a clear compile error** when they violate a guarantee
   (cardinality, classification, naming), with a fix suggestion.
4. **Trust that local dev "just works"** with no observer setup
   (events go nowhere, like `tracing` without a subscriber).
5. **Migrate from `tracing`** without a flag day — both can coexist
   in the same binary.
6. **Test events with `assert_eq!` ergonomics** — no mock framework.

The non-negotiable: a typed emit call site should be no longer than
the equivalent `tracing::info!` call site for the same set of fields.
The `Obs*` type prefix and field annotations are extra characters at
the *schema* file, not at every call site.

### 1.1 The two emit forms (canonical: builder)

`obs` ships two equivalent emit forms. **The builder form is canonical
and is what the docs, scaffolding, and AI prompts default to.** The
macro is a *shorthand* for terse cases.

```rust
// PRIMARY — chained typed builder, preferred for clarity and editor support:
ObsRequestCompleted::builder()
    .route(Route::ListUsers)
    .status(Status::Ok)
    .latency_ms(48)
    .emit();                         // .emit_at(Severity::Warn) to escalate

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

## 2. Quickstart (60 seconds)

```bash
$ cargo new myapp --bin && cd myapp
$ obs init --mode rust .
   added obs-sdk = "0.1" to dependencies
   created src/events.rs with example ObsHelloEmitted
   created obs.yaml with default observer config
   updated src/main.rs to install observer

$ cargo run
2026-05-02T14:23:11.123Z INFO  myapp.v1.ObsHelloEmitted who=world
```

`src/events.rs` after `obs init --mode rust`:

```rust
use obs::Event;

#[derive(Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsHelloEmitted {
    #[obs(label, cardinality = "low")]
    pub who: Audience,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, obs::EnumLabel)]
pub enum Audience { World, Universe }
```

`src/main.rs`:

```rust
mod events;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    obs_sdk::install_observer(
        obs_sdk::StandardObserver::dev()  // stdout sink, dev-friendly
    );

    events::ObsHelloEmitted::builder()
        .who(events::Audience::World)
        .emit();

    obs_sdk::observer().shutdown().await;
    Ok(())
}
```

That is the entire setup. From here, swap `dev()` for the OTLP wiring
in [61-crates-and-features.md § 3](./61-crates-and-features.md#3-end-to-end-usage-example)
when ready for production.

## 3. Mental model

> **One event = one log record + N metric data points + (optionally) one
> span + one analytics row, all from one `.emit()` call.**

The user's job is to define the *shape* of the event once. The runtime
handles the fan-out.

| Mental hook | What it maps to in the runtime |
| --- | --- |
| The event *type* (`ObsRequestCompleted`) | A protobuf message + Rust struct + `EventSchema` impl |
| Each field with `LABEL` | A bounded dimension on the metric, an attribute on the OTLP log/span, a low-cardinality column in `obs_events` |
| Each field with `MEASUREMENT` | A metric data point (counter/gauge/histogram) emitted on each `.emit()` |
| Each field with `ATTRIBUTE` | A column in the analytics row; not on metrics |
| Each field with `TRACE_ID` | The `trace_id` on the envelope (auto-filled from `obs::scope!`) |
| `obs::scope!` | A RAII guard that holds a field allowlist + tail-on-error buffer. **Not a tracing-`Span` analogue** — see [80-glossary.md](./80-glossary.md). |
| `obs::forensic!` | The escape hatch for "I haven't typed this yet"; budgeted, surfaced in audits |

## 4. Authoring patterns

### 4.1 Proto-first vs Rust-first

| Use proto-first when… | Use rust-first when… |
| --- | --- |
| Schema is shared across crates / services / future languages | Schema is single-crate, single-binary |
| Team has a proto registry / lint tooling | Want to stay in Rust toolchain only |
| Want the `.proto` to be the canonical artifact | Want to colocate event definitions with code that emits them |

Both modes generate the *same* `EventSchema` impls and the same
builder. The only user-visible difference is where the schema text
lives.

### 4.2 Naming

- Event message name: **`Obs<Concept>` + past tense** for things that
  happened (`ObsRequestCompleted`, `ObsUserSignedUp`,
  `ObsCheckoutAbandoned`).
- Event message name: **`Obs<Concept>Started`** for the front edge of
  a long-running operation; matching `Completed` event for the back
  edge.
- Field name: `snake_case`, descriptive, no `_ms`/`_ns` if the unit
  is implied by the metric annotation (but include if standalone).
- Enum variants: `PascalCase`, no prefix.

### 4.3 The ten most-used patterns

#### A. Request handler (HTTP)

```rust
async fn handle(req: Request) -> Response {
    let _scope = obs::scope!(trace_id = req.id.clone(),
                              tenant_id = req.tenant.clone());

    ObsRequestStarted::builder()
        .route(req.route())
        .emit();

    let started = std::time::Instant::now();
    let resp    = serve(req).await;
    let ms      = started.elapsed().as_millis() as u64;

    ObsRequestCompleted::builder()
        .route(resp.route())
        .status(resp.status_class())
        .latency_ms(ms)
        .bytes_out(resp.bytes())
        .emit();

    resp
    // _scope drops; if any ERROR was emitted in `serve`, the tail
    // buffer flushes; otherwise discarded.
}
```

#### B. Background batch job

```rust
async fn nightly_recompute() {
    let _scope = obs::scope!(job = "nightly_recompute".to_string());

    ObsJobStarted::builder()
        .job_kind(JobKind::NightlyRecompute)
        .emit();

    let stats = run_recompute().await;

    ObsJobCompleted::builder()
        .job_kind(JobKind::NightlyRecompute)
        .rows_processed(stats.rows)
        .elapsed_ms(stats.elapsed.as_millis() as u64)
        .emit();
}
```

#### C. Library crate (no observer assumed)

```rust
pub fn parse_widget(input: &[u8]) -> Result<Widget, Error> {
    let started = std::time::Instant::now();
    let result  = parse_inner(input);

    ObsWidgetParsed::builder()
        .ok(result.is_ok())
        .bytes_in(input.len() as u64)
        .elapsed_ns(started.elapsed().as_nanos() as u64)
        .emit();

    result
}
```

`parse_widget` works whether or not the binary installed an observer.
No observer = events are discarded after one atomic load.

#### D. Spanning function with attribute macro

```rust
#[obs::instrument(fields(route, tenant_id), skip(raw_body))]
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    // body emits events; trace_id, route, tenant_id auto-flow
}
```

#### E. Conditional severity escalation

```rust
let sev = if elapsed_ms > 5000 { Severity::Warn } else { Severity::Info };
ObsRequestCompleted::builder()
    .route(route)
    .status(status)
    .latency_ms(elapsed_ms)
    .emit_at(sev);
```

#### F. Forensic escape (rare; audited)

```rust
// Crate's metadata.obs.forensic_max governs the budget.
obs::forensic!(
    site = "billing::reconcile",
    message = "ledger drift",
    {
        "ledger_id" => ledger_id,
        "delta_cents" => delta.to_string(),
    }
);
```

#### G. Test assertion

```rust
#[tokio::test]
async fn signup_emits_event() {
    let (obs, handle) = obs::InMemoryObserver::new();
    obs::install_observer(obs);

    signup_flow().await;

    let events = handle.drain();
    assert!(events.iter().any(|e|
        e.full_name == "myapp.v1.ObsUserSignedUp"
        && e.labels.get("channel") == Some(&"web".into())
    ));
}
```

#### H. Streaming sink under load

```rust
// Configured in obs.yaml; no code change.
sinks:
  - type: parquet
    base_dir: s3://obs-events/myapp/
    layout: single
    roll: { max_bytes: 268435456, max_age_secs: 60 }
```

#### I. CLI inspection during dev

```bash
$ obs tail --file ./events.ndjson | jq 'select(.sev=="ERROR")'
$ obs query --from ./events.ndjson --since 5m --event myapp.v1.ObsUpstreamFailed
$ obs schema show myapp.v1.ObsRequestCompleted
```

#### J. Migrating from tracing in place

```rust
fn main() {
    // Both tracing and obs work; tracing events become forensic obs events.
    obs::install_observer(StandardObserver::dev());
    obs::install_panic_hook();                         // FATAL-on-panic
    tracing_subscriber::registry()
        .with(obs::tracing_bridge::TracingToObsLayer::new())
        .init();

    tracing::info!(user_id = 42, "still works");      // → ObsTracingForensicEvent
    ObsUserSignedIn::builder().user_id(42).emit();     // → ObsUserSignedIn
}
```

#### K. HTTP middleware (axum / tower)

```rust
use axum::Router;
use obs_tower::ObsHttpLayer;

let app = Router::new()
    .route("/api/users",   get(list_users))
    .route("/api/users/:id", get(get_user))
    .layer(
        ObsHttpLayer::server()
            .with_route_extractor(|req| req.uri().path().to_string())
    );
// Inside list_users / get_user, obs::scope! is already open with
// trace_id (from inbound traceparent or freshly generated) and route.
// ObsHttpRequestStarted/Completed are emitted automatically.
```

Outbound calls inject `traceparent` automatically when wrapped:

```rust
let client = reqwest::Client::builder()
    .build()?
    .with_layer(obs_tower::ObsHttpClientLayer::new());
let resp = client.get("https://upstream/...").send().await?;
// traceparent + tracestate added; ObsHttpClientCompleted emitted.
```

#### L. Multi-tenant observer per request

Per-tenant observers wired through the HTTP layer. Each tenant's
events go to that tenant's sinks (separate OTLP endpoint, separate
Parquet bucket); a global default catches everything else.

```rust
// Built once per tenant; cached in a registry.
fn observer_for(tenant_id: &str) -> Arc<dyn Observer> {
    StandardObserver::builder()
        .service("my-api", env!("CARGO_PKG_VERSION"))
        .resource_attr("tenant_id", tenant_id)
        .sink_for(Tier::Log,
                  OtlpLogSink::builder()
                      .endpoint(format!("https://otlp.{tenant_id}.example.com"))
                      .build()?)
        .build()
        .map(Arc::new)
        .expect("tenant observer build")
}

let registry = Arc::new(TenantObserverRegistry::new(observer_for));

let app = axum::Router::new()
    .route("/api/...", get(handle))
    .layer(ObsHttpLayer::server()
        .with_per_request_observer({
            let registry = registry.clone();
            move |req| req.headers()
                .get("x-tenant-id")
                .and_then(|h| h.to_str().ok())
                .and_then(|t| registry.get(t))
        }));
```

Inside `handle`, every `obs::emit!` lands in the tenant-specific
sinks. Background tasks spawned during the request must explicitly
carry the override:

```rust
async fn handle(req: Request) -> Response {
    let scope = obs::scope!(trace_id = req.id);
    tokio::spawn(
        background_audit(req.id)
            .with_observer(obs::observer())   // capture & forward current tier
            .instrument(scope.clone()),
    );
    serve(req).instrument(scope).await
}
```

See [11-runtime-core.md § 3.1](./11-runtime-core.md#31-the-three-tiers-and-what-each-is-for)
for the resolution rules and propagation matrix.

#### M. Coexisting with `tokio-console`

```rust
// Both work in the same binary; they target different things.
console_subscriber::init();              // tokio runtime introspection
obs::install_observer(StandardObserver::dev());

// Application events go through obs; runtime/task events via tokio-console.
```

## 5. IDE & autocomplete

The codegen is intentionally optimised for `rust-analyzer`:

- **`ObsXxx::builder()`** opens a typed-builder with one method per
  required field. RA shows missing-field errors inline as the user
  types `.build()` (or `.emit()`).
- **Builder method docs** carry through from the proto comments via
  `obs-build` — `///` doc-comments on `.proto` fields are emitted
  onto the corresponding builder setter.
- **Hover on `ObsXxx`** shows: tier, default severity, schema hash,
  source file path. (Generated `///` on the type.)
- **Goto-definition** on a generated type lands in `OUT_DIR/obs/...`,
  which is fine for inspection. Goto-implementation on `EventSchema`
  also works.
- **Generated builder is concrete**, not behind a `dyn Trait`, so RA
  can chain-complete.

## 6. Compile error quality

The runtime intentionally produces compile errors that name the
offending field, file, and rule. Examples:

### L001 — Cardinality on LABEL

```
error[L001]: field `user_id` is LABEL but cardinality `High` is not label-compatible
   --> src/events.rs:14:5
    |
 14 |     #[obs(label, cardinality = "high")]
    |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    = note: LABEL fields must be Low or Medium cardinality.
            High and Unbounded are illegal because they would explode
            the metric attribute set.
    = help: change to `#[obs(attribute, cardinality = "high")]`
            (an ATTRIBUTE is logged but never becomes a metric dim)
```

### L002 — PII on LABEL

```
error[L002]: field `email` is LABEL with classification PII
   --> proto/myapp/v1/events.proto:18:3
    |
 18 |   string email = 4 [(obs.v1.field) = { kind: LABEL, classification: PII }];
    |                                                ^^^^^             ^^^
    = note: PII fields cannot be LABEL because labels become metric
            attributes that are kept indefinitely and leak into vendor backends.
    = help: change kind to ATTRIBUTE so the value is logged + analytics-only,
            and the redactor can scrub it on the durable path.
```

### L011 — Missing `Obs` prefix

```
error[L011]: event type `RequestDone` does not start with `Obs`
   --> proto/myapp/v1/events.proto:42:1
    |
 42 | message RequestDone { ... }
    |         ^^^^^^^^^^^
    = help: rename to `ObsRequestDone`. The Obs prefix gives every event
            type a unique visual identity at call sites.
```

### Generated-builder: missing required field

```
error: the trait bound `…BuilderState<((), ((), ()))>: BuildableTo<…Args>` is not satisfied
   --> src/handler.rs:31:9
    |
 31 |         .emit();
    |          ^^^^ required field `latency_ms` was not set
```

(Provided by `typed-builder`; we improve the message via a custom
`#[builder(crate_module_path = ..., type_name_format = ...)]` config.)

## 7. Local dev experience

- **No observer = no panic.** A `obs::emit!` in a binary that never
  called `install_observer` is a noop. Cost: one atomic load.
- **`OBS_DEV=1`** flips the default observer (`StandardObserver::dev()`)
  to render events as one-line text on stdout. Format matches `obs tail`.
- **Hot reload of `obs.yaml`.** Sending `SIGHUP` to the process (or
  modifying the file when `reload_on_sighup()` is set) reloads
  sampling, rate limits, classification, and filter without restart.
- **`OBS_FILTER`** environment variable applies an `EnvFilter`-style
  allowlist before the observer dispatches. Same DSL as
  `tracing-subscriber::EnvFilter`:
  ```
  OBS_FILTER="info,myapp::auth=debug,myapp.v1.ObsRequestCompleted=trace"
  ```
- **`obs tail --stdin`** lets a developer pipe `cargo run` output
  through the pretty-printer if running with the NDJSON sink:
  ```bash
  $ cargo run | obs tail --stdin
  ```

## 8. Test ergonomics

- `InMemoryObserver` returns a handle with `drain()`, `wait_for(predicate, timeout)`,
  and `count(filter)` helpers — no third-party mock framework.
- `obs::test::assert_emitted!(handle, MyEvent { route: ..., .. })` —
  pattern-match assertion macro that ignores untouched fields.
- A `#[obs::test]` attribute installs an `InMemoryObserver` *on the
  current thread only* (via `obs::with_test_observer`, see
  [11-runtime-core.md § 3](./11-runtime-core.md#3-the-observer-trait))
  for the duration of the test and removes it after. This means:
  - cargo's default parallel test runner is safe; tests do not leak
    observers across each other,
  - no `serial_test` annotation is required,
  - library code called from inside the test still sees the test
    observer because the per-thread slot wins over the global.

The full testing strategy — including trybuild fixtures for compile
errors, the mock OTLP collector, property tests, and the dev-erg
suite layout — lives in [72-testing-strategy.md](./72-testing-strategy.md).

```rust
// `#[obs::test]` accepts `Result<(), E>` returns so error paths use `?`
// rather than `.unwrap()` (project policy: no `unwrap`/`expect` —
// CLAUDE.md § Error Handling).
#[obs::test]
async fn billing_emits_charge_event() -> anyhow::Result<()> {
    charge_card("4242…").await?;

    obs::test::assert_emitted!(ObsChargeAttempted {
        outcome: ChargeOutcome::Approved,
        ..
    });
    Ok(())
}
```

## 9. Production parity

- The same code that ran with `StandardObserver::dev()` in dev runs
  with `StandardObserver::builder()...` in production. There are no
  `#[cfg(debug_assertions)]` branches inside the SDK that change
  semantics — the observer is the single point of variation.
- The `OBS_DEV` env var, when present, switches the renderer but does
  not change which events fire. Useful for `kubectl exec` debugging
  on a production pod.
- Every internal SDK metric is itself an `obs.runtime.v1.Obs*` event,
  so it lands in the same dashboards as user events. This means
  "the SDK is broken" is observable through the SDK, which is
  unavoidably circular but at least the observer's own emissions go
  through a separate channel that always has at least the
  `StdoutSink` available.

## 10. Migration from `tracing`

Strategy in three steps:

1. **Bridge.** Add `obs-tracing-bridge` to `tracing_subscriber::registry()`.
   Every existing `tracing::info!(...)` becomes an
   `ObsTracingForensicEvent`. No code changes.
2. **Audit.** Run `obs audit` weekly to see which targets emit the
   most tracing-bridge events. Those are the candidates to type up
   into proper schemas.
3. **Schema-ify in batches.** For each high-volume target:
   - Define the `ObsXxx` schema (proto or rust).
   - Replace the `tracing::info!` calls in that module.
   - The bridge volume drops; the audit dashboard makes progress
     visible.

There is **no flag day**. A binary can run with both for as long as
it takes; the only cost is the one extra event type
(`ObsTracingForensicEvent`) flowing through the unified table.

## 11. Anti-patterns and the SDK's prevention

| Anti-pattern | How the SDK prevents it |
| --- | --- |
| Logging a JSON-serialised request body into `tracing::info!` | No `String`-message API on `obs::emit!`; the only inputs are typed event structs |
| Using `user_id` as a metric label | L001 + L002 lint catch it at compile time |
| Adding a SECRET token to a long-retained tier | L003 lint catches it; the scrub dispatcher strips it at the boundary even if L003 is silenced |
| Drift between `service`/`route` naming across crates | `obs lint` (workspace-wide) flags LABEL fields with the same name but different cardinality/type/classification across schemas (L013); enforced in CI |
| Forgetting to flush before exit | `observer().shutdown().await` is documented in scaffold; failing to call it leaves events in-flight (warned in dev mode) |
| Reaching for `forensic!` to avoid writing a schema | `forensic_max` budget + audit report turn this into a visible team-level signal |
| Multiple subscribers disagreeing on event shape | Single global observer; sinks are children, cannot mutate the envelope |

## 12. AI-assisted authoring

This SDK is designed *for* AI authoring as well as human authoring.
Specific affordances:

- **Schema is the prompt.** An agent reading `proto/myapp/v1/events.proto`
  has the entire contract in one file. Builder method names match
  field names exactly; there is no naming layer between schema and
  call site.
- **Compile errors are addressable.** Each lint emits a stable error
  ID (`L001` … `L013`) and a `help:` line with a fix. Agents can
  pattern-match on the ID and apply the fix without re-reading the
  whole file.
- **Codegen is deterministic.** Same input proto → byte-identical
  output every time. An agent that has cached "the builder for
  `ObsXxx` is `ObsXxx::builder().route(...).status(...)...`" can
  rely on it across runs.
- **`obs schema show` is the lookup.** When an agent doesn't recall
  whether `tenant_id` exists on a given event, one CLI call gives
  the full surface area in a stable text format.
- **Naming convention is a syntactic check.** `Obs*` prefix is a
  trivial regex an agent applies to every event type it generates.
  No semantic ambiguity about whether a struct is an event type.
- **`obs init --mode rust`** scaffolds an idiomatic minimum and gives
  the agent a starting point with the conventions baked in.

## 13. The dev-erg test suite

A dedicated test suite in `crates/obs-sdk/tests/dev_ergonomics/`
backs every claim in this document:

- `test_quickstart_60s.rs` — verifies `obs init` + `cargo run` works
  end-to-end against a fixture crate.
- `test_compile_errors.rs` — `trybuild` test cases for each lint
  ID, asserting the *exact* error message format documented above.
- `test_no_observer_noop.rs` — emit on a fresh process; verify
  no panic, no allocation beyond one atomic load.
- `test_hot_reload.rs` — write `obs.yaml`, send `SIGHUP`, verify
  the new sampling rate applies to the next emit.
- `test_in_memory_observer.rs` — verify `assert_emitted!` matches
  on partial fields and timeouts on `wait_for`.
- `test_tracing_bridge.rs` — install bridge; emit
  `tracing::info!`; assert the result is an
  `ObsTracingForensicEvent`.

CI runs this suite on every PR; failure here is treated as severely
as a clippy regression.
