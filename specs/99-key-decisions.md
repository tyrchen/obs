# Key Design Decisions

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02

The decisions below are the load-bearing ones — each is the kind a
future contributor is most likely to question and try to undo. Each
includes the alternative we considered and why we rejected it. The
list is consolidated from earlier per-spec "Key Design Decisions"
sections so a reviewer can read it linearly without context-switching
across files.

Linked from [10-data-model.md](./10-data-model.md),
[11-runtime-core.md](./11-runtime-core.md),
[13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md),
[20-otel-and-sinks.md](./20-otel-and-sinks.md),
[22-analytics-storage.md](./22-analytics-storage.md),
[30-tracing-bridge.md](./30-tracing-bridge.md), and
[31-callsite-interning.md](./31-callsite-interning.md).

## Wire format & schema

### D1 — Wire format is `buffa`, not `prost`

Custom proto options first-class via `buffa-reflect`'s
`DescriptorPool`; zero-copy `*View<'a>`; no `protoc` on PATH for
hermetic CI; `preserve_unknown_fields` by default. Production-tested
elsewhere in the same author's projects.
[10-data-model.md](./10-data-model.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md).

### D2 — Schema hash is a build-time `u64`, not runtime, not 32 bytes

BLAKE3 the descriptor at build time, truncate to a `u64` const.
Runtime hashing per-emit would be wasteful and forfeits CI-time
schema-version verification. 64 bits is sized for accidental-collision
avoidance at realistic schema counts (≤ 10⁴ per workspace) — *not* a
tamper-detection primitive (buffa payloads are not signed). Saves
24 bytes per envelope vs the 32-byte natural BLAKE3 output and
matches the `callsite_id` shape so the runtime treats both id
namespaces uniformly.

### D3 — `Obs*` prefix on event type names

Convention enforced by lint L011, not the type system. Visual
distinction at call sites + greppability + codegen pattern matching.
Defaults to warning; error under `--strict`.

### D4 — Rust-first and proto-first authoring share one EventSchema impl

Single source of truth per crate (`schema-source = "rust" | "proto"`).
Mixing is a build-time error. The proc-macro emits the same
`EventSchema` impl as the proto codegen so downstream code is mode-
agnostic.

## Storage

### D5 — Single sparse table, not table-per-event

One wide table with sparse per-event struct columns. The `WHERE
trace_id = X` read pattern is one query. Schema evolution is
additive. File counts in object storage are `O(hours)`, not
`O(events × hours)`. The original wide-events shape (Honeycomb,
Snowplow, Segment converge here). Per-event tables remain opt-in
for very-high-volume single-event workloads.

### D6 — `service`/`instance`/`version` go on the OTel Resource, not on every record

OTel's correct shape; per-record duplication wastes wire bytes and
confuses queries. `ResourceAttrs` on the runtime exposes the same
identity to non-OTLP sinks (Parquet, ClickHouse) so analytical rows
aren't missing identity that OTLP carries.

## Runtime topology

### D7 — Global Observer, not contextual `Subscriber`

OTel's contextual propagation is powerful but adds friction to
every library. A global observer (with `ArcSwap` for
swap-on-test-init) makes a library emission cost one TLS check + one
atomic load. **A per-thread override slot** rescues parallel-test
ergonomics without forcing `serial_test`. Cross-process trace
propagation is via OTel propagators at HTTP/gRPC boundaries —
orthogonal to the in-process observer.

### D8 — Per-tier mpsc workers, not a shared queue

Failure isolation (a hung Parquet writer cannot stall metric
emission), per-tier channel sizing, deterministic shutdown ordering.
Cost: 4 tokio tasks. Negligible.

### D9 — AUDIT tier is allowed to block on the emit thread

LOG/METRIC/TRACE drop on backpressure (silent + metric increment).
AUDIT does not — silent drop on AUDIT is a compliance failure. The
runtime spins on `try_send` for ≤ 100 ms, then spools to disk. Never
silent-dropped. This is the one place the emit path can block; it's
deliberate.

### D10 — No panic on emit, ever

`debug_assert!` is acceptable; `panic!` is not. An observability bug
that takes down the host process is a worse incident than a missing
event. The clippy lints
`-W clippy::unwrap_used -W clippy::expect_used -W clippy::panic
-W clippy::indexing_slicing` are deny-on-warn for emit-path modules.

### D11 — Atomic `Interest` cache on the static callsite (tracing parity)

Earlier drafts cached filter decisions in a `DashMap<&'static
ObsCallsite, FilterDecision>` — a heap lookup on every emit. The
v3 design moves the cached `Interest` (`AtomicU8`) plus a
`generation: AtomicU32` onto the `ObsCallsite` static itself. Hot
path is one atomic load + branch when the cache hits, identical to
tracing's mechanic. Filter reload bumps `Observer::generation`,
which transparently invalidates every callsite cache.

### D12 — Labels projected at emit time, not at sink time

`EventSchema::project` writes labels into the envelope's `labels` map
once. Cheap sinks (metric counter, OTel attribute writer, audit
filter) iterate `labels` without touching the typed payload. Pays
the projection cost once for many sinks rather than re-extracting
per sink.

### D13 — Service identity is read via `ArcSwap`, not stored in atomics

`service`/`instance`/`version` change at most once per process
lifetime. Storing them per-emit is waste; static atomics make
mid-test reset awkward. ArcSwap splits the difference: cheap reads,
simple replacement, no atomics fighting.

### D14 — `Pin<Box<dyn Future>>` for async trait methods (object-safety exception)

`Observer` and `Sink` are used as `dyn Trait` for `ArcSwap` and the
sink router. Native `async fn` in traits is not object-safe with
arbitrary returned futures. CLAUDE.md § Async & Concurrency calls
this out as an explicit exception. Equivalent to `async-trait` but
without the proc-macro on the SDK's hot path.

## Ergonomics

### D15 — Builder is canonical, macro is shorthand

Chained typed builder is what the docs, scaffolding, and AI prompts
default to. `obs::emit!` exists as sugar for terse one- or
two-field events and severity escalation. Both expand to the same
callsite-gated dispatch. RA chain-completion + pinpoint
required-field errors + refactor-friendliness drive the choice.

### D16 — `obs::scope!` is a field allowlist + tail buffer, *not* a span

A span has start/end times, multiple enter/exit cycles, post-hoc
`Span::record(field, value)` updates. `obs::scope!` has none of
that. Span semantics with duration are emitted via a
`Started`/`Completed` event pair or `#[obs::instrument]`. The
documentation makes this explicit so users don't expect tracing-span
behaviour. [13-emit-scope-and-filter.md § 4](./13-emit-scope-and-filter.md#4-obsscope-is-not-a-tracingspan).

### D17 — Field inheritance is an explicit allowlist, not implicit propagation

`tracing` allows any subscriber-attached field to flow into nested
events implicitly. We require fields to be named in `obs::scope!(...)`
explicitly. This avoids:

- A high-cardinality field accidentally inherited into a metric
  attribute set.
- A PII field flowing into a label without explicit declaration.

Validation is best-effort runtime (init-time `inventory` walk + dev-
mode first-emit warning); a proc-macro can't see the whole binary.

### D18 — Trace correlation auto-fills via `obs::scope!`, schema field is the contract

User code calls `obs::scope!(trace_id = req.id)`. The codegen for
`EventSchema::project` checks the active scope's field map for any
field annotated `FIELD_KIND_TRACE_ID` on the schema and auto-fills it
if the call site left it default. Fixes the regression in earlier
wide-event SDKs that required passing `request_id` at every emit.

### D19 — `obs::Instrumented<F>` adapter required for spawned tasks

`tokio::task_local!` does not propagate to `tokio::spawn`. The
explicit adapter mirrors `tracing-futures::Instrument` and makes the
"spawn loses scope" footgun a documented opt-in instead of a silent
correctness bug. [13-emit-scope-and-filter.md § 3](./13-emit-scope-and-filter.md#3-obsinstrumentedf--async-scope-adapter).

### D20 — `emit_at(sev)` accepts both directions; no upward-only clamp

Earlier drafts clamped to escalation only. Demotion is sometimes
legitimate (graceful-shutdown DEBUG of a normally-INFO event), and
the asymmetry confused readers. The schema's `default_sev` is just
the default when `emit()` is used; `emit_at` accepts whatever the
call passes.

### D21 — `#[obs::instrument]` emits one event by default

The earlier two-event default (`ObsFnEntered` + `ObsFnExited`)
doubled traffic on hot paths for marginal value. The new default is
one `ObsFnExecuted` with `latency_ns`; opt in to `ObsFnEntered` via
`enter = true`.

## Sampling & operations

### D22 — Tail buffer scoped to `obs::scope!` Drop, not `request_id` string

Scope-keyed buffers in a `DashMap<String, RequestTrack>` (the
obvious implementation) leak when a handler short-circuits without
calling `on_request_end`. We tie the buffer's lifetime to the RAII
guard `obs::scope!` returns; cleanup happens in `Drop` regardless of
control-flow. Same pattern as `tracing::Span::enter`.

### D23 — Forensic events are budgeted, not banned

Sometimes there is no schema yet. `obs::forensic!` emits an
`ObsForensicEvent` carrying free-form data. Each crate has a
`forensic_max` budget in `Cargo.toml`; the CLI lints excess. The
trend over time should be toward zero forensic calls, but we
provide the escape hatch rather than force an "emergency PR for a
new schema" workflow during incidents.

### D24 — Panic hook is opt-in but officially documented

`obs::install_panic_hook()` is opt-in (calling `install_observer`
does not implicitly install it) because some users have a richer
panic hook already (e.g. Sentry). When opted in, it captures one
`ObsPanicked` and `shutdown_blocking()` before chaining the previous
hook — so even `panic = "abort"` profiles get the event flushed.

## Bridge

### D25 — Bridge `Layer` is the default; `Subscriber` is the escape hatch

Layer composes with `tracing-subscriber::registry()`, so existing
`EnvFilter`, `fmt::layer()`, `tracing-opentelemetry`, and
`console-subscriber` continue to work. Subscriber form is a one-liner
for binaries that don't want any registry composition.

### D26 — Forensic-by-default; auto-typing is opt-in

Every bridged event lands in `ObsTracingForensicEvent` until the
user explicitly registers a typed promoter. Useful out-of-the-box
without forcing a schema for every third-party log line.

### D27 — Loop break uses thread-local guards plus a reserved target

Two-guard scheme works because bridge work is synchronous within a
single dispatch. The `obs.bridge` reserved target is defence-in-depth
for the case where a future change might introduce an `await`
between guard set and clear.

### D28 — Span correlation is task-local, not envelope-stamped

Trace context flows through the same `tokio::task_local!` that
`obs::scope!` and Direction A both write to. One source of truth per
task, regardless of which subsystem opened the context. Avoids the
failure mode where two subsystems disagree on `trace_id`.

### D29 — Bridge `Metadata` is leaked, never freed

`tracing` requires `&'static`. Per-process leakage is bounded by the
number of distinct event schemas (typically ≤ 10³). Acceptable cost
for not needing arena allocators or unsafe lifetime extension.

## Callsite interning

### D30 — BLAKE3 hash, not linker addresses

`defmt`'s address-as-id trick depends on a single ELF binary; we are
distributed and need stable ids across processes. BLAKE3 over the
canonical callsite tuple gives deterministic ids for free.

### D31 — `callsite_id == 0` is reserved; the hasher perturbs to avoid it

The truncated BLAKE3 has a 1-in-2⁶⁴ chance of producing zero. The
truncation post-step replaces `0` with `1` (or rotates to bytes
8..16) so the reserved value never collides with a real id. Cheap
fix for a vanishingly rare case.

### D32 — `Off` is the v1 default for interning; opt-in by config

Interning changes the consumer contract. Shipping it on by default
in v1 would force every downstream consumer (Parquet readers,
ClickHouse importers, CLI users) to learn about the registry on day
one. Off is safe; users opt in when ready. Default flip is a v1.1
question.

### D33 — Three modes (Off / Hybrid / Compact), not a binary

`Hybrid` keeps the rendered message in the payload so `tail -f` and
ad-hoc consumers still work without the registry. `Compact` is the
final form for highest-volume deployments. The split lets operators
trade readability for bytes consciously.

### D34 — Re-emit cadence, not "once-only"

A produce-once-consume-many model is fragile to consumer churn. We
re-emit registrations periodically (10 min / 10k events default) so
late or restarted consumers catch up within minutes.

### D35 — Callsite id is on the envelope, not a label

LABEL fields are metric dimensions; a high-cardinality `callsite_id`
would explode the metric attribute set. `fixed64 callsite_id = 15`
on the envelope keeps it queryable in OLAP and skippable for metric
sinks.

### D36 — `ObsCallsiteRegistered` is itself a normal `obs::emit!`

It rides the same observer pipeline, gets the same retention, can be
filtered by `OBS_FILTER`, etc. We do NOT introduce a side-channel —
too easy to drift out of sync with data events.

### D37 — Direction B reconstitutes full `Metadata` on the way out

If we emitted degraded `target = "obs.bridge"` events to tracing,
existing tooling that targets `"sqlx::query"` would break. The sink
looks up the original metadata and reconstitutes a faithful tracing
event so downstream layers see no difference between "never interned"
and "round-tripped through interning".

## Open / deferred decisions

| Decision | Status | Notes |
| --- | --- | --- |
| Default sampling policy out of the box | leaning tail-on-error by default | configurable off; locked at M2 |
| Thin tracing-subscriber-shaped facade for users who want `obs` data plane but tracing macros | leaning yes, behind a feature flag | not the recommended path |
| In-tree DuckDB sink | deferred | Parquet + external DuckDB covers v1 |
| Cross-language SDKs (Go / Python / TypeScript) | deferred to post-1.0 | gated on demand |
| Cluster-wide sampling agreement | deferred | per-process head + tail-on-error in v1 |
| Schema registry HTTP service | deferred | considered when > 5 services share schemas |
| `SpanEmissionMode::OnScope` + `OtlpTraceSink` double-OTel-span | deferred to v1.1 | document recommends OnScope only in dev |
| Cross-process callsite registry sharing (Unix socket) | deferred to v1.1 | per-process today |
| `u64` id collision under > 1 M ids | open | birthday-bound < 2⁻⁴⁴; widen to 96/128 via feature flag if a workspace exceeds 10⁵ |
| `Obs*` prefix lint default level | open | error under `--strict`; warning otherwise |

## Build dependencies

This document is reference-only; it has no build dependencies and is
not on the implementation critical path. It exists so a reviewer can
audit *why* in one read.
