# obs — User Guide

> **Audience:** application engineers and SREs adopting `obs` to instrument
> a Rust service. **Goal:** install the SDK, author your first event,
> wire it into logs / metrics / traces / analytics, and operate it in
> production.

Other guides:
[Developer Guide](./dev-guide.md) ·
[Migration from `tracing`](./migration-from-tracing.md) ·
[中文用户指南](./user-guide.zh-CN.md) ·
[Specs index](../specs/index.md)

---

## Table of contents

1. [What `obs` is, in one paragraph](#1-what-obs-is-in-one-paragraph)
2. [Install](#2-install)
3. [60-second quickstart](#3-60-second-quickstart)
4. [Mental model](#4-mental-model)
5. [Authoring events](#5-authoring-events)
6. [Emitting events](#6-emitting-events)
7. [Scopes, context, and trace correlation](#7-scopes-context-and-trace-correlation)
8. [Filtering and sampling](#8-filtering-and-sampling)
9. [Sinks](#9-sinks)
10. [HTTP middleware (`obs-tower`)](#10-http-middleware-obs-tower)
11. [Bridging `tracing`](#11-bridging-tracing)
12. [Configuration (`obs.yaml`)](#12-configuration-obsyaml)
13. [Multi-tenant / per-task observers](#13-multi-tenant--per-task-observers)
14. [The CLI](#14-the-cli)
15. [Testing your code](#15-testing-your-code)
16. [Operations](#16-operations)
17. [FAQ](#17-faq)

---

## 1. What `obs` is, in one paragraph

`obs` is a schema-first, wide-events SDK for Rust services. You author
**one typed event** per logical operation; one `.emit()` call lands as a
log record, a set of metric data points, optionally a trace span, and
an analytics row in a single sparse Parquet/ClickHouse table — all from
the same definition. The SDK enforces label cardinality, classification
(PII / SECRET), and naming conventions at compile time, so the kinds of
mistakes that page on-call (label explosion, secret in logs, drifting
field names across services) become build errors.

If you have read the PRD, this guide is its operational dual. If you
have not, [`specs/00-prd.md`](../specs/00-prd.md) explains *why*.

---

## 2. Install

### 2.1 As a library (in your service)

Add the façade and the build helper in `Cargo.toml`:

```toml
[dependencies]
obs-sdk = { version = "0.1", features = ["otel", "parquet"] }

[build-dependencies]
obs-build = "0.1"      # only needed for proto-first authoring

[package.metadata.obs]
schema-source = "proto"     # or "rust" (Rust-first); never both
proto-root    = "proto"     # only for proto-first
forensic_max  = 5           # per-crate budget for `obs::forensic!`
```

Default features on `obs-sdk`: `dev` (StdoutSink), `otel` (OTLP gRPC + HTTP),
`panic-hook` (FATAL-on-panic). Opt in via features for `parquet`,
`clickhouse`, `tracing-bridge`, `tower`, `test`. To strip everything
except the core API, add `default-features = false`.

### 2.2 As a CLI (one binary, optional)

```bash
cargo install --path apps/obs-cli           # from a workspace checkout
# or, once published:
cargo install obs-cli
obs --version                               # sanity check
```

You **do not** need the CLI to use the SDK — services link `obs-sdk`
directly. The CLI is for authoring (`init`, `validate`, `lint`, `diff`,
`schema show`), inspection (`tail`, `query`, `decode`), and back-end
plumbing (`migrate`).

---

## 3. 60-second quickstart

```bash
cargo new myapi --bin && cd myapi
obs init --mode rust .          # scaffold src/events.rs + obs.yaml + main.rs
cargo run
# → 1730000000.000000000 INFO  myapi.v1.ObsHelloEmitted who=world
```

`obs init` writes three files:

- `src/events.rs` — example `ObsHelloEmitted` with one `LABEL` field.
- `obs.yaml` — runtime config (filter, sampling, limits, sinks).
- `src/main.rs` — installs `StandardObserver::dev()`.

That is the entire setup. `obs init --mode proto .` does the same but
with a `.proto` schema and a `build.rs` instead of `#[derive(Event)]`.

---

## 4. Mental model

> **One event = one log record + N metric data points + (optionally) one
> span + one analytics row, all from one `.emit()` call.**

| Concept | What it is |
| --- | --- |
| **Event** | A typed Rust struct (or `.proto` message) that fully describes one logical thing that happened. Names start with `Obs` (e.g. `ObsRequestCompleted`). |
| **Tier** | Routing hint: `LOG`, `METRIC`, `TRACE`, or `AUDIT`. Drives which sink chain receives the envelope. AUDIT is durable (never silent-dropped). |
| **Severity** | OTel-aligned `Trace..Fatal`. Schema declares `default_sev`; `.emit_at(Severity::Warn)` overrides up *or* down. |
| **Field role** | Each field has a `FieldKind` — `LABEL`, `ATTRIBUTE`, `MEASUREMENT`, `TRACE_ID`, `SPAN_ID`, `PARENT_SPAN_ID`, `TIMESTAMP_NS`, `DURATION_NS`, `FORENSIC`. |
| **Cardinality** | `LOW < 10 · MEDIUM < 10k · HIGH < 1M · UNBOUNDED`. LABEL fields **must** be Low or Medium — high-cardinality on a label is a compile error. |
| **Classification** | `INTERNAL` / `PII` / `SECRET`. Drives runtime scrubbing. |
| **Observer** | The single dispatcher that owns sinks and the per-tier worker pool. Three-tier resolution: per-task → per-thread → global. |
| **Sink** | A typed consumer (`StdoutSink`, `OtlpLogSink`, `ParquetSink`, …) bound to a tier. Sinks see a `ScrubbedEnvelope<'_>`, never raw payloads. |
| **Scope** | A RAII guard that holds a label allowlist and a 64-event tail-on-error ring buffer. **Not** a `tracing::Span` analogue. |
| **Schema registry** | Process-wide catalogue assembled at startup via `linkme`; sinks use it to decode payloads. |

The user's job is to define the *shape* of the event once. The runtime
handles the fan-out.

---

## 5. Authoring events

### 5.1 Rust-first (`#[derive(Event)]`)

```rust
use obs_sdk::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsRequestCompleted {
    #[obs(label, cardinality = "medium")]
    pub route: Route,

    #[obs(label, cardinality = "low")]
    pub status_class: StatusClass,

    #[obs(attribute, cardinality = "high", classification = "pii")]
    pub user_id: UserId,

    #[obs(measurement, metric(histogram, unit = "ms",
        bounds = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 5_000]))]
    pub latency_ms: u64,

    #[obs(trace_id)]
    pub trace_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, obs_sdk::EnumLabel)]
pub enum StatusClass { TwoXx, ThreeXx, FourXx, FiveXx }
```

The macro emits the `EventSchema` impl, the `linkme`-collected registry
entry, the typed builder, and compile-time lint assertions. See
[spec 12 § 4](../specs/12-schema-and-codegen.md) for the complete
attribute grammar.

### 5.2 Proto-first (`obs-build`)

```proto
// proto/myapi/v1/events.proto
syntax = "proto3";
package myapi.v1;
import "obs/v1/options.proto";

message ObsRequestCompleted {
  option (obs.v1.event) = { tier: TIER_LOG, default_sev: SEVERITY_INFO };

  Route        route        = 1 [(obs.v1.field) = { kind: LABEL, cardinality: MEDIUM }];
  StatusClass  status_class = 2 [(obs.v1.field) = { kind: LABEL, cardinality: LOW }];
  string       user_id      = 3 [(obs.v1.field) = { kind: ATTRIBUTE,
                                                    cardinality: HIGH,
                                                    classification: PII }];
  uint64       latency_ms   = 4 [(obs.v1.field) = { kind: MEASUREMENT,
                                                    metric: { kind: HISTOGRAM, unit: "ms",
                                                              bounds: [1,5,10,25,50,100,250,500,1000,5000] } }];
  string       trace_id     = 5 [(obs.v1.field) = { kind: TRACE_ID }];
}
```

```rust
// build.rs
fn main() -> anyhow::Result<()> {
    obs_build::Config::new()
        .files(&["proto/myapi/v1/events.proto"])
        .include("proto")
        .include_obs_options()
        .out_dir(std::env::var("OUT_DIR")?)
        .compile()?;
    Ok(())
}

// src/lib.rs
obs_sdk::include_schemas!("myapi.v1");
```

The two modes generate **byte-identical** `EventSchema` impls. Pick proto
when you have multiple services or you want a wire-format anchor that
lives outside Rust; pick Rust-first when one binary owns the schema.
Mixing modes inside a single crate is a build error.

### 5.3 Naming conventions

- **Event message name:** `Obs<Concept>` + past tense for completed
  things (`ObsRequestCompleted`, `ObsUserSignedUp`,
  `ObsCheckoutAbandoned`). For long-running operations, pair
  `Obs<Concept>Started` and `Obs<Concept>Completed` — the OTel sink
  collapses the pair into a single span.
- **Field names:** `snake_case`, descriptive, include the unit when
  standalone (`latency_ms`, `bytes_out`).
- **Enum variants:** `PascalCase`, no prefix.
- **Workspace-wide override:** the `Obs` prefix is enforced by lint L011.
  To use a different prefix workspace-wide, set
  `[workspace.metadata.obs] event_prefix = "Evt"` in your top-level
  `Cargo.toml` (the SDK's built-in `obs.runtime.v1.*` events keep
  their prefix regardless).

### 5.4 Compile-time lints (L001–L013)

| ID | What it catches | Why it matters |
| --- | --- | --- |
| **L001** | `LABEL` field with `cardinality = High`/`Unbounded`. | Labels become metric attributes; high cardinality blows up TSDB indexes. |
| **L002** | `PII` classification on a `LABEL` field. | PII labels leak into every vendor backend with no expiry. |
| **L003** | `SECRET` classification on a `LOG`-tier or `AUDIT`-tier event. | These tiers are durable; secrets must never land there. |
| **L004** | `MEASUREMENT` field without a `MetricSpec`. | Without metric metadata, the metric sink cannot project the value. |
| **L005** | Enum used as `LABEL` exceeds the cardinality cap. | Enum variant explosion is detected at compile time, not at the dashboard. |
| **L006** | `AUDIT` event with no PII/SECRET fields. | Suspicious — AUDIT exists for sensitive things; warns. |
| **L007** | Field name not `snake_case`. | Cross-service consistency. |
| **L008** | Reusing a previously deleted proto tag. | Would corrupt historical NULL semantics in Parquet/ClickHouse. |
| **L009** | Empty event (no fields). | Almost always a bug. |
| **L010** | `obs::forensic!` budget exceeded for the crate. | Forensic is the escape hatch; budget keeps the team honest. |
| **L011** | Event name does not start with `Obs` (or workspace prefix). | Visual identity at every call site. |
| **L012** | Field name shadows an envelope name (`ts_ns`, `service`, …). | Would silently overwrite envelope columns at sink time. |
| **L013** | Same `LABEL` name across events with conflicting type/cardinality/classification. | Labels with the same name across events must be the same dimension. |

Each lint produces an actionable error pointing at the file, line, and a
fix suggestion. See
[spec 60 § 6](../specs/60-dev-ergonomics.md#6-compile-error-quality)
for example messages.

---

## 6. Emitting events

`obs` ships **two equivalent emit forms**. The builder is canonical
(docs and AI prompts default to it); the macro is shorthand.

### 6.1 Builder (canonical)

```rust
ObsRequestCompleted::builder()
    .route(Route::ListUsers)
    .status_class(StatusClass::TwoXx)
    .user_id(uid)
    .latency_ms(elapsed.as_millis() as u64)
    .emit();
```

- `rust-analyzer` chain-completion lights up immediately after
  `::builder().`.
- Required-field errors are pinpointed at `.emit()` (typed-builder
  marker types refuse to compile when a required setter is missing).
- `.emit_at(Severity::Warn)` overrides severity in either direction.

### 6.2 Macro (shorthand)

```rust
obs_sdk::emit!(ObsHelloEmitted { who: Audience::World });
obs_sdk::emit!(WARN, ObsUpstreamFailed { route, error_kind });
```

Use the macro when the struct literal genuinely reads better — typically
events with one or two fields, or terse severity escalation. Bare
severity idents (`TRACE`/`DEBUG`/`INFO`/`WARN`/`ERROR`/`FATAL`) are
re-exported from `obs_sdk` for ergonomic use.

### 6.3 What happens after `.emit()`

1. **Static `ObsCallsite::enabled` check** — single atomic load. If the
   filter said "never", returns immediately (~25 ns).
2. **`Observer::enabled`** — only called when the cache says
   "sometimes" (i.e., generation might have changed).
3. **`EventSchema::project`** — fills envelope labels and auto-fills
   missing setters from the active scope frame.
4. **Head sampler** — rate or per-event override.
5. **Tail-on-error buffer** — push into the active scope's 64-deep ring.
6. **`mpsc::try_send`** to the per-tier worker. **Hot path returns here**
   (~1 µs end-to-end on a 2024 laptop, with `StandardObserver` and
   no-op sinks).
7. (Worker thread) scrubber → `ScrubbedEnvelope<'_>` → `Sink::deliver`.

The emit thread never blocks for sinks except on `AUDIT` (see § 16.3).

---

## 7. Scopes, context, and trace correlation

```rust
async fn handle(req: Request) -> Response {
    let _scope = obs_sdk::scope!(
        trace_id  = req.id.clone(),
        tenant_id = req.tenant.clone(),
    );

    ObsRequestStarted::builder()
        .route(req.route())
        .emit();      // trace_id auto-fills from the scope

    let started = std::time::Instant::now();
    let resp    = serve(req).await;

    ObsRequestCompleted::builder()
        .route(resp.route())
        .status_class(resp.status_class())
        .latency_ms(started.elapsed().as_millis() as u64)
        .emit();      // trace_id + tenant_id auto-filled

    resp
    // _scope drops here. If serve() emitted ERROR, the tail buffer
    // flushes — every previously-sampled-out TRACE/DEBUG event under
    // this scope now ships. If not, they are discarded.
}
```

### 7.1 `scope!` vs `context!`

- **`obs_sdk::scope!`** — binds an allowlist of fields **and** a
  64-event tail-on-error ring buffer. Use at request boundaries.
- **`obs_sdk::context!`** — same field allowlist, **no tail buffer**.
  Use in deeply nested helpers where you don't want to start a new
  ring buffer.

### 7.2 `scope!` is **not** `tracing::Span`

- No start/end times. No enter/exit cycles.
- No `Span::record` after the fact.
- For "function entered" / "function returned" with duration, use
  `#[obs::instrument]` or a Started/Completed event pair.

### 7.3 `tokio::spawn` orphans scope by default

```rust
tokio::spawn(
    background_audit(req_id)
        .instrument(scope_clone)              // carry scope across .await
        .with_observer(obs_sdk::observer()),  // carry observer too
);
```

Both `instrument(...)` and `.with_observer(...)` populate orthogonal
slots on the same `Instrumented<F>` wrapper. Without them, the spawned
task sees the *global* observer with *no* scope.

### 7.4 `#[obs::instrument]`

```rust
#[obs::instrument(fields(route, tenant_id), skip(raw_body))]
async fn handle_list_users(req: Request, raw_body: Bytes) -> Response {
    // body emits events; trace_id, route, tenant_id auto-flow
}
```

Default emits **one** `ObsFnExecuted` event with `latency_ns` on return.
Add `enter = true` to opt back into the legacy two-event
(`ObsFnEntered` + `ObsFnExecuted`) pattern.

---

## 8. Filtering and sampling

### 8.1 The filter DSL

`obs::Filter` ports `tracing-subscriber::EnvFilter` grammar **verbatim**
so `RUST_LOG`-style strings work unchanged:

```bash
OBS_FILTER="info,myapi::auth=debug,myapi.v1.ObsRequestCompleted=trace"
OBS_FILTER='info,myapi.v1.ObsRequestCompleted[route=admin]=trace'
```

`[field=value]` clauses match against envelope **`labels`** only.
Filter clauses on `ATTRIBUTE` fields silently match nothing — `obs lint`
warns when you write one.

### 8.2 Sampling order

For each emit, in order:

1. **Inbound W3C `traceparent.sampled`** — if the request carried a
   propagated decision, honour it (always emit if set, always drop if
   cleared). Opt out via `obs.yaml` `sampling.honour_traceparent_sampled = false`.
2. **Head sampler** — per `(full_name, severity)` rate. Single `f64`
   compare, deterministic seed for reproducibility.
3. **Tail-on-error** — per-scope ring buffer flushed when *any* event
   in the same scope hits ERROR or FATAL.

### 8.3 Per-event overrides

```yaml
# obs.yaml
filter: "info,myapi::cache=debug"
sampling:
  default_rate: 1.0
  per_event:
    "myapi.v1.ObsCacheLookup": 0.01      # sample 1% — high volume
    "myapi.v1.ObsHealthcheck": 0.001
  always_log_at_or_above: warn          # bypass for important things
```

### 8.4 Hot reload

Edit `obs.yaml` and:

- send `SIGHUP` (Unix), or
- enable `reload_on_sighup()` / `notify` watcher when building the
  observer (recommended cross-platform).

`StandardObserver::reload_config` swaps the filter atomically and bumps
each callsite's generation; the next emit re-probes its `Interest`
state. Parse failure keeps the old config and emits
`ObsConfigReloadFailed`.

What can hot-reload (✅), what can't (❌):

| Setting | Hot reload? |
| --- | --- |
| `filter`, `sampling.*`, `limits.*`, `audit.on_failure` | ✅ |
| Sink endpoints (some) | ⚠ partial |
| `audit.channel_capacity`, queue sizes, `service.*`, parquet/CH URLs | ❌ restart |

---

## 9. Sinks

A sink is a typed consumer of `ScrubbedEnvelope<'_>`. Bind sinks to
tiers when you build the observer:

```rust
use obs_sdk::*;

let observer = StandardObserver::builder()
    .service("myapi", env!("CARGO_PKG_VERSION"))
    .instance(hostname::get()?.to_string_lossy().into_owned())
    .sink_for(Tier::Log,    NdjsonFileSink::new("./events.ndjson")?)
    .sink_for(Tier::Metric, otel::OtlpMetricSink::from_env()?)
    .sink_for(Tier::Trace,  otel::OtlpTraceSink::from_env()?)
    .sink_for(Tier::Audit,  ParquetSink::builder()
        .base_dir("/var/log/audit-parquet")
        .build()?)
    .config_from_yaml_path("./obs.yaml")?
    .reload_on_sighup()
    .build()?;

install_observer(observer);
```

### 9.1 Built-in sinks

| Sink | Purpose | Crate |
| --- | --- | --- |
| `StdoutSink` | Pretty / Compact / Full / JSON formatter to stdout | `obs-core` |
| `NdjsonFileSink` | NDJSON to a file (with optional rolling) | `obs-core` |
| `RollingFileWriter` | Daily / hourly / size-based file rotation | `obs-core` |
| `NonBlockingWriter` | Bounded mpsc + background thread; drop on overflow | `obs-core` |
| `OtlpLogSink` / `OtlpMetricSink` / `OtlpTraceSink` | OTLP gRPC + HTTP | `obs-otel` |
| `ParquetSink` | Single sparse `obs_events` table to local FS / S3 / GCS / Azure | `obs-parquet` |
| `ClickHouseSink` | Native batched INSERT into one `obs_events` table | `obs-clickhouse` |
| `InMemoryObserver`'s sink | Test-only; ring buffer with `drain()` / `wait_for(...)` | `obs-core` (test feature) |

### 9.2 Stdout formatter styles

```rust
StdoutSink::builder()
    .formatter(FormatterStyle::Json)        // Full | Compact | Pretty | Json
    .make_writer(LevelSplitWriter::new(StdoutWriter, StderrWriter))  // INFO→stdout, WARN+→stderr
    .build()?;
```

### 9.3 OTLP

```rust
// 12-factor: read OTEL_EXPORTER_OTLP_ENDPOINT etc.
let (logs, metrics, traces) = obs_otel::otlp_trio_from_env()?;

let observer = StandardObserver::builder()
    .service("myapi", env!("CARGO_PKG_VERSION"))
    .sink_for(Tier::Log,    logs)
    .sink_for(Tier::Metric, metrics)
    .sink_for(Tier::Trace,  traces)
    .build()?;
```

What the OTLP sinks do:

- **`OtlpLogSink`** — 1:1 `LogRecord` mapping. `event.name = full_name`,
  `obs.schema_hash` attribute, raw payload bytes, severity bucket-floor
  to `SeverityNumber`.
- **`OtlpMetricSink`** — one data point per `MEASUREMENT` field;
  instrument name `<full_name>.<field>`. Counter / Gauge / Histogram
  according to the schema's `MetricSpec`.
- **`OtlpTraceSink`** — `*Started`/`*Completed` event pairs collapse
  into a single span; `*Started` becomes a span event. Single events
  with a duration field become full spans; one-shot events become
  zero-duration spans.

Service identity (`service`/`instance`/`version` + any
`resource_attr(...)` calls) goes on the OTel `Resource` once. Per-event
attributes never duplicate it.

### 9.4 Analytics: Parquet + ClickHouse

```rust
// Parquet — single sparse table, S3-compatible
ParquetSink::builder()
    .base_dir("s3://obs-events/myapi/")
    .layout(ParquetLayout::Single)              // default
    .roll_max_bytes(256 * 1024 * 1024)
    .roll_max_age(Duration::from_secs(300))
    .compression(ParquetCompression::Zstd)
    .partition_by(&["service", "date"])
    .build()?;

// ClickHouse — same schema shape, native MergeTree
ClickHouseSink::builder()
    .url("http://clickhouse:8123")
    .database("obs")
    .table("obs_events")
    .batch_size(8192)
    .build()?;
```

Both write into a single sparse `obs_events` table with one nullable
struct column per event type. Cross-event joins are one query; new
events append a column with NULLs in old rows. Get the DDL for
ClickHouse out of the CLI:

```bash
obs migrate clickhouse --schemas $(pwd)/proto > obs_events.sql
```

`auto_migrate` defaults to **false**; production runs the DDL through
your normal CI/CD path.

---

## 10. HTTP middleware (`obs-tower`)

```rust
use axum::{Router, routing::get};
use obs_tower::server::ObsHttpLayer;

let app = Router::new()
    .route("/api/users",    get(list_users))
    .route("/api/users/:id", get(get_user))
    .layer(
        ObsHttpLayer::server()
            .with_route_extractor(|req| req.uri().path().to_string())
    );
```

What this does on every request:

- Extracts `traceparent`/`tracestate` from headers (W3C). Falls back to
  a freshly generated `ObsTraceCtx` if absent.
- Opens a `scope!(trace_id, span_id, sampled)` for the duration of the
  request. Inside `list_users` / `get_user`, every `.emit()` inherits
  the trace context.
- Emits `ObsHttpRequestStarted` (off by default; opt in with
  `with_emit_started(true)`) at entry.
- Emits `ObsHttpRequestCompleted` at exit with `route`, `method`,
  `status_class` (LABEL: `2xx|3xx|4xx|5xx|err`), `latency_ms` (histogram),
  `bytes_out` (counter).

Outbound calls inject `traceparent` automatically when wrapped:

```rust
use obs_tower::client::ObsHttpClientLayer;

let svc = tower::ServiceBuilder::new()
    .layer(ObsHttpClientLayer::new())
    .service(hyper_util::client::legacy::Client::builder(
        hyper_util::rt::TokioExecutor::new()).build_http());

let resp = svc.call(http::Request::get("https://upstream/...").body(())?).await?;
// traceparent + tracestate added; ObsHttpClientCompleted emitted.
```

---

## 11. Bridging `tracing`

You almost never have to change a `tracing::info!()` call to start
benefiting from `obs`. Install the bridge once:

```rust
use obs_sdk::{StandardObserver, install_observer, install_panic_hook};
use obs_tracing_bridge::TracingToObsLayer;
use tracing_subscriber::{layer::SubscriberExt, Registry};

let observer = StandardObserver::dev()?;
install_observer(observer);
install_panic_hook();

tracing_subscriber::registry()
    .with(TracingToObsLayer::new())
    .init();

// Existing tracing! emits now route through obs sinks too.
tracing::info!(target: "myapi", route = "list_users", "request done");
```

The bridge maps each tracing event to a built-in
`ObsTracingForensicEvent` (carries `target`, `level`, `message`, and a
field map). For high-volume targets, register a typed promoter so the
bridge produces a typed `Obs*` event instead — see
[spec 30 § 2.5](../specs/30-tracing-bridge.md) and
[`docs/migration-from-tracing.md`](./migration-from-tracing.md).

A reverse bridge (`ObsToTracingSink`) lets `obs::emit!` events show up
in `cargo run` stdout via a `tracing-subscriber::fmt` host — useful for
incremental adoption in an existing tracing-only stack.

---

## 12. Configuration (`obs.yaml`)

```yaml
# obs.yaml — runtime config (compile-time settings live in Cargo.toml)

filter: "info,myapi::auth=debug,myapi.v1.ObsRequestCompleted=trace"

sampling:
  default_rate: 1.0
  per_event:
    "myapi.v1.ObsHealthcheck": 0.001
    "myapi.v1.ObsCacheLookup": 0.01
  always_log_at_or_above: warn
  honour_traceparent_sampled: true
  tail_buffer_capacity: 64

limits:
  max_payload_bytes: 262144         # 256 KiB; range 1 KiB .. 16 MiB
  max_label_value_bytes: 1024

audit:
  channel_capacity: 1024
  block_ms_max: 100
  spool_after_ms: 250
  spool_dir: "/var/lib/myapi/audit-spool"
  spool_max_bytes: 1073741824       # 1 GiB
  spool_max_age: "7d"
  on_failure: panic                  # panic | abort | warn_only

queues:
  log_capacity: 8192
  metric_capacity: 8192
  trace_capacity: 8192

sinks:
  otlp:
    endpoint: "https://otel-collector.example.com:4317"
    protocol: grpc
    headers:
      authorization: "Bearer ${OBS_OTLP_TOKEN}"

service:
  name: "myapi"
  version: "1.4.2"
```

### 12.1 Conventions

- Every struct uses `serde(deny_unknown_fields)` — typos fail loud with
  line numbers.
- Durations parse with `humantime`: `"30s"`, `"5m"`, `"7d"`. Bare
  numbers and fractional units (`"1.5h"`) are rejected.
- `${VAR}` env interpolation is supported in strings; an unset variable
  is a config-load error (no silent fallback).
- Secrets in config are wrapped in `secrecy::SecretString`; round-trip
  serialisation prints `<redacted>` to prevent accidental leakage.

### 12.2 Env-var overrides

Every field is reachable via `OBS_*` with `__` separator:

```bash
OBS_SAMPLING__DEFAULT_RATE=0.1
OBS_SINKS__OTLP__ENDPOINT="https://otel.staging.local:4317"
OBS_AUDIT__SPOOL_DIR=/mnt/spool
```

Map fields (e.g. `sampling.per_event`) are not env-overridable.

### 12.3 `obs validate`

```bash
obs validate ./obs.yaml
```

Pre-deploy: catches typos, bad rates, illegal sink combinations, missing
env vars.

---

## 13. Multi-tenant / per-task observers

The Observer trait has **three-tier resolution**:

```
per-task (tokio task-local) → per-thread → global
```

For multi-tenant servers, build one observer per tenant and wire a
selector into `obs-tower`:

```rust
fn observer_for(tenant_id: &str) -> Arc<dyn Observer> {
    StandardObserver::builder()
        .service("myapi", env!("CARGO_PKG_VERSION"))
        .resource_attr("tenant_id", tenant_id)
        .sink_for(Tier::Log,
            obs_otel::OtlpLogSink::builder()
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

Inside `handle`, every `.emit()` lands in the tenant-specific sinks.

### 13.1 Async gotchas

- **`tokio::spawn` does NOT propagate observer or scope.** Wrap the
  future:

  ```rust
  tokio::spawn(
      background_work(req_id)
          .instrument(scope_clone)
          .with_observer(obs_sdk::observer()),
  );
  ```

- **`with_observer_thread_local` is sync-only.** Holding the guard
  across `.await` is wrong — a different task may resume on the same
  thread and inherit the observer. The function is named verbosely to
  make the misuse visible at the call site. Use `Future::with_observer`
  for async.

- **Per-task observer drop is synchronous.** Keep tenant observers in
  a long-lived registry; don't construct a fresh one per request.

---

## 14. The CLI

```
$ obs --help
Authoring         init validate lint generate doctor
Schema governance schema show / lint / diff / audit
Data inspection   decode tail query
Backends          migrate clickhouse / parquet
Meta              version completions
```

### 14.1 Authoring

```bash
obs init --mode rust .                        # scaffold a Rust-first crate
obs init --mode proto .                       # scaffold a proto-first crate
obs validate proto/myapi/v1/events.proto \
            --include proto                   # check the .proto round-trips
obs lint --strict --root .                    # all L001..L013, fail on any
obs generate --root .                         # one-shot codegen for inspection
obs doctor --root .                           # diagnose deps/config/schema-source
```

### 14.2 Schema governance

```bash
obs schema show myapi.v1.ObsRequestCompleted \
   --schemas ./proto                          # full field table + sink projection
obs diff origin/main HEAD                     # exit 2 on breaking changes
obs audit --root .                            # workspace-wide forensic + AUDIT report
```

### 14.3 Data inspection

```bash
obs tail --file ./events.ndjson | jq 'select(.sev=="ERROR")'
obs tail --stdin                              # pipe from cargo run
obs query --from ./events.ndjson \
          --since 5m \
          --event myapi.v1.ObsRequestCompleted \
          --label route=list_users
obs decode batch.bin > events.ndjson          # binary ObsBatch → NDJSON
obs decode --audit-spool /var/lib/myapi/audit-spool
```

### 14.4 Backends

```bash
obs migrate clickhouse --schemas ./proto > obs_events.sql
obs migrate parquet    --schemas ./proto > obs_events.arrow.json
```

### 14.5 Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Tool error (invocation, IO, parsing) |
| `2` | Breaking schema diff (`obs diff`) |

CI integration:

```yaml
- run: obs lint --strict --root .
- run: obs diff origin/main HEAD               # blocks on exit 2
- run: obs migrate clickhouse --diff origin/main..HEAD --out preview.sql
```

---

## 15. Testing your code

```toml
# Cargo.toml
[dev-dependencies]
obs-sdk = { version = "0.1", features = ["test"] }
```

### 15.1 `#[obs::test]`

```rust
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

`#[obs::test]` installs an `InMemoryObserver` per-thread (sync) or
per-task (async) for the duration of the test. Cargo's default parallel
test runner stays safe — no `serial_test` annotation required.

`assert_emitted!` is a partial-match macro; `..` ignores fields you
don't care about.

### 15.2 `InMemoryObserver` directly

```rust
let (observer, handle) = obs_sdk::InMemoryObserver::new();
obs_sdk::install_observer(observer);

signup_flow().await;

let events = handle.drain();
assert!(events.iter().any(|e|
    e.full_name == "myapi.v1.ObsUserSignedUp"
    && e.labels.get("channel") == Some(&"web".into())
));
```

`handle` exposes `drain()`, `wait_for(predicate, timeout)`, and
`count(filter)`.

### 15.3 Mock OTLP collector

```rust
let (mock, addr) = obs_otel::test::MockOtelCollector::start().await?;
let sink = OtlpLogSink::builder().endpoint(format!("http://{addr}")).build()?;
obs_sdk::install_observer(StandardObserver::builder().sink_for(Tier::Log, sink).build()?);

ObsRequestCompleted::builder().route(Route::ListUsers).status(Status::Ok).emit();
obs_sdk::observer().flush().await;

let logs = mock.received_logs().await;
assert_eq!(logs[0].attributes.get("event.name"),
           Some("myapi.v1.ObsRequestCompleted"));
```

### 15.4 Test gotchas

- `tokio::spawn` does **not** inherit the per-task observer the test
  installed — use `.with_observer(obs_sdk::observer())` on the spawned
  future.
- Keep the `InMemoryObserver`'s capacity larger than the events your
  test produces (default 1024); overflow drops silently.

---

## 16. Operations

### 16.1 Self-events

The SDK emits its own observability through the same observer. Watch
for these `obs.runtime.v1.*` events in your dashboards:

| Event | Meaning |
| --- | --- |
| `ObsSinkDropped {tier, reason}` | Per-tier mpsc full or sink overflow. Reasons: `channel_full`, `retry_queue_full`, `writer_overflow`. |
| `ObsConfigReloaded` / `ObsConfigReloadFailed` | Hot reload outcome. |
| `ObsAuditSpooled` / `ObsAuditSpoolFailed` / `ObsAuditSpoolRecovered` | AUDIT durability path. |
| `ObsPanicked` | Panic hook flushed and re-panicked. |
| `ObsForensicBudgetExceeded` | A crate exceeded its forensic budget. |
| `ObsLabelCardinalityHigh` | A LABEL field's distinct value count crossed its declared cap. |
| `ObsOversizedDropped` | A payload exceeded `limits.max_payload_bytes`. |
| `ObsSchemaUnknown` | Foreign-producer envelope hit the registry's lookup miss path. |

### 16.2 Hot-path performance

Per-emit budgets on a 2024-class laptop (criterion gates fail on >10 %
regression):

| Path | Budget |
| --- | --- |
| Noop emit (no observer) | ≤ 110 ns |
| Filtered-out emit | ≤ 25 ns |
| Observer resolution (no override) | ≤ 15 ns |
| Per-thread / per-task override | ≤ 30 ns |
| `Future::with_observer().poll` | ≤ 30 ns / poll |
| Full emit, sinks no-op | ≤ 1 µs P50 |
| NDJSON sink | ≤ 1.5 µs P50 |
| Scope enter + exit | ≤ 100 ns |

### 16.3 The AUDIT tier

AUDIT is the only tier that can block the emit thread, and the only one
that never silent-drops:

1. `try_send` to the AUDIT mpsc (cap `audit.channel_capacity`).
2. If full, **block up to `audit.block_ms_max`** (100 ms default).
3. If still full, **spool to disk**: binary length-prefixed buffa with
   per-record CRC32C in `audit.spool_dir`.
4. On observer init, the spool is drained (FIFO). Recovery emits
   `ObsAuditSpoolRecovered` with the count.
5. If the spool is unwritable, `audit.on_failure` fires (`panic` /
   `abort` / `warn_only`).

Run `obs decode --audit-spool /var/lib/myapi/audit-spool` to inspect
records that didn't drain on a previous run.

### 16.4 Graceful shutdown

```rust
async fn run() -> anyhow::Result<()> {
    // ... your service ...
    obs_sdk::observer().shutdown().await;   // flushes per-tier workers
    Ok(())
}
```

`StandardObserver::Drop` calls `shutdown_blocking` with a 250 ms cap,
so even forgotten shutdowns leave little in flight — but for clean
data, always `await observer().shutdown()`.

### 16.5 Panic safety

```rust
obs_sdk::install_panic_hook();
```

On panic: emits `ObsPanicked` (FATAL) carrying the message and
location, calls `shutdown_blocking`, then chains the previously
installed hook so your existing crash reporter still runs.

---

## 17. FAQ

**Q. Do I need an observer for `obs::emit!` to do anything safe?**
No. With no observer installed, an emit is a noop costing one atomic
load. This makes library crates safe to instrument unconditionally.

**Q. Can I keep using `tracing::info!` and `obs::emit!` in the same binary?**
Yes — that's the canonical migration story (`obs-tracing-bridge`).
There is no flag day. See [migration-from-tracing.md](./migration-from-tracing.md).

**Q. Does `obs` work without `tokio`?**
Sinks and the observer are tokio-based. Std-only library crates can
emit events safely (the noop path uses no tokio). Production binaries
need tokio.

**Q. Why the `Obs` prefix?**
Visual identity at every call site, plus mechanical greppability
(`grep -r 'Obs[A-Z]' src/` finds every event call). Override
workspace-wide via `[workspace.metadata.obs] event_prefix`.

**Q. How do I emit a one-off event without authoring a schema?**
`obs::forensic!(site, message, { "k" => "v" })`. It's budgeted per crate
(`forensic_max` in `Cargo.toml`) and audited. Treat it as a TODO marker,
not a steady state.

**Q. Why not just one giant proto file?**
Encouraged: per-bounded-context `.proto` files (e.g. `proto/billing/v1/`,
`proto/auth/v1/`). The CLI's `--schemas` flag walks directories.

**Q. Where do schemas live at runtime?**
Each schema registers itself into a `linkme::distributed_slice` at
link time. `StandardObserver::build()` collects them into a
`SchemaRegistry`. If aggressive LTO strips the slice, the build will
return an error suggesting you call `obs::include_schemas!`.

**Q. What's the difference between LABEL, ATTRIBUTE, MEASUREMENT?**
- **LABEL** — bounded dimension; becomes a metric attribute, an OTLP
  log/span attribute, a dictionary-encoded analytics column. Cardinality
  enforced.
- **ATTRIBUTE** — high-cardinality value; logged + analytics, **never** a
  metric dim. Compatible with PII when redacted.
- **MEASUREMENT** — numeric value that becomes a metric data point
  (counter / gauge / histogram).

**Q. How do I redact PII in bridged tracing events?**
The bridge runs a name-pattern redactor by default
(`password|secret|token|api[_-]?key|authorization|cookie|ssn|credit[_-]?card|bearer`).
Plug a custom `Redactor` for your domain — see spec 70 § 6.

---

Next: [Developer Guide](./dev-guide.md) — internals, sink contract,
schema registry, performance work, contributing.
