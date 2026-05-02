# Design — Testing Strategy

Status: draft v1 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [11-runtime-core.md](./11-runtime-core.md), [60-dev-ergonomics.md](./60-dev-ergonomics.md)

This spec consolidates the testing approach across the workspace. It
covers the test pyramid (unit / integration / property / bench), the
`InMemoryObserver` test harness, the `#[obs::test]` attribute, the
parallel-test ergonomics (the per-thread observer override slot), the
trybuild compile-error fixtures, and the canonical mock OTLP collector.

## 1. Test pyramid by crate

| Crate | Unit | Integration | Property | Bench | trybuild |
| --- | --- | --- | --- | --- | --- |
| `obs-types` | enums, const fns | — | — | — | — |
| `obs-proto` | encode/decode round-trip + view | — | proto round-trip | — | — |
| `obs-macros` | parse + emit | — | — | — | bad inputs (one .rs per lint) |
| `obs-build` | parser, codegen | end-to-end with fixture proto | — | codegen wall time | — |
| `obs-core` | observer (3-tier resolution + re-entry guard), sink, sampler, scope, filter, **schema registry** (hash/name lookup, miss fallback), **`ScrubbedEnvelope`** (lifetime, scrubber failure), AUDIT spool format + recovery | InMemoryObserver, multi-tenant per-task observer | env. round-trip, registry determinism | emit hot path, registry lookup, scrubber, observer resolution | borrow-checker fixtures: ScrubbedEnvelope cannot escape lifetime |
| `obs-otel` | mappers | mock OTel collector | — | encode timings | — |
| `obs-parquet` | unified-schema gen | round-trip via `arrow` reader | — | batch write | — |
| `obs-clickhouse` | DDL gen | docker-compose CH | — | insert throughput | — |
| `obs-cli` | per-subcommand | trycmd against fixtures | — | — | — |
| `obs-tracing-bridge` | Layer/Sink/matcher/redactor | full bridge suite (loop, span correlation, PII, auto-typed, interning) | event/envelope round-trip | forward + reverse overhead | — |
| `obs-tower` | layer factory | axum/hyper end-to-end | — | per-request overhead | — |
| `obs-sdk` | feature gating | dev-ergonomics suite (60-dev-ergonomics.md § 13) | — | — | error-message regression |

## 2. The `InMemoryObserver` harness

`InMemoryObserver` is the canonical in-process test sink.

```rust
let (observer, handle) = InMemoryObserver::new();
obs::install_observer(observer);

ObsLoggedIn::builder().method(AuthMethod::Password).user_id("u-1").emit();

let events = handle.drain();
assert_eq!(events.len(), 1);
assert_eq!(events[0].full_name, "myapp.v1.ObsLoggedIn");
```

`handle` provides:

- `drain() -> Vec<ObsEnvelope>` — take everything emitted so far.
- `wait_for(predicate, Duration) -> Result<ObsEnvelope, Timeout>` —
  block until any event matches; useful for async tests.
- `count(filter) -> usize` — non-destructive count.

The observer holds a bounded ring buffer (default capacity 1024)
internally so a runaway test doesn't OOM. Tests that legitimately
emit > 1024 events configure the capacity:

```rust
InMemoryObserver::builder().capacity(8192).build()
```

## 3. The `#[obs::test]` attribute and parallel-test ergonomics

`cargo test` runs tests in parallel within the same process. A
naive `obs::install_observer(InMemoryObserver::new())` in `setup`
races against other tests: test #2's events end up in test #1's
buffer or vice versa.

The `#[obs::test]` attribute uses the **per-thread** observer
override slot from [11-runtime-core.md § 3](./11-runtime-core.md#3-the-observer-trait)
for sync/single-thread tests, and the **per-task** override (via
`Future::with_observer`) for async tests. Both keep parallelism
intact:

```rust
// async test expands to (per-task tier):
#[tokio::test]
async fn foo() -> anyhow::Result<()> {
    let observer = Arc::new(InMemoryObserver::new());
    let handle   = observer.handle();
    async move {
        // ... user body ...
    }
    .with_observer(observer.clone())
    .await
}

// sync test expands to (per-thread tier):
#[test]
fn foo() -> anyhow::Result<()> {
    let (observer, handle) = InMemoryObserver::new();
    obs::with_test_observer(observer, || {
        // ... user body ...
    })
}
```

Effect: only this task (async) or this thread (sync) sees the test
observer. Parallel tests do not interfere — async tests are
correct even when tokio migrates them across worker threads,
because the per-task slot follows the task. Library code called
from inside the test still sees `obs::observer()` returning the
test observer because the per-task slot wins over the per-thread
slot wins over the global. No `serial_test` mandate.

#### `tokio::spawn` from inside a test does NOT inherit by default

Per the propagation rules in [11 § 3.1](./11-runtime-core.md#31-the-three-tiers-and-what-each-is-for),
`tokio::spawn` creates a new task whose `OBSERVER_TASK` slot is
empty — events emitted from the spawned task fall through to the
**global** observer, not the test observer. This trips up tests
like:

```rust
#[obs::test]
async fn t() -> anyhow::Result<()> {
    tokio::spawn(async {
        ObsBackgroundDone::builder().emit();   // → global, not test handle
    }).await?;
    obs::test::assert_emitted!(ObsBackgroundDone { .. });   // FAILS
    Ok(())
}
```

To capture spawned-child events in the test buffer, carry the
observer forward explicitly:

```rust
#[obs::test]
async fn t() -> anyhow::Result<()> {
    let o = obs::observer();   // captures the test observer
    tokio::spawn(async move {
        ObsBackgroundDone::builder().emit();
    }.with_observer(o)).await?;
    obs::test::assert_emitted!(ObsBackgroundDone { .. });   // OK
    Ok(())
}
```

This is documented in `test_multi_tenant_observer.rs` as the
positive pattern, and a trybuild-style "you forgot to forward the
observer" hint is on the M3 polish list.

The attribute supports `Result<T, E>` returns so tests use `?` per
CLAUDE.md's no-`unwrap` policy:

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

`assert_emitted!` is a partial-match macro: any field marked `..`
is ignored.

## 4. Compile-error fixtures (trybuild)

Each lint defined in [12-schema-and-codegen.md § 3.4](./12-schema-and-codegen.md)
has a corresponding trybuild fixture under
`crates/obs-macros/tests/trybuild/`:

```
trybuild/
├── L001_label_high_card.rs              # expected error: cardinality
├── L001_label_high_card.stderr
├── L002_pii_label.rs                    # expected error: pii on label
├── L002_pii_label.stderr
├── L003_secret_log_tier.rs
├── L003_secret_log_tier.stderr
├── L004_measurement_no_metric.rs
├── L004_measurement_no_metric.stderr
├── L005_enum_overflow.rs
├── L005_enum_overflow.stderr
├── L011_missing_obs_prefix.rs
├── L011_missing_obs_prefix.stderr
├── L012_envelope_shadow.rs
├── L012_envelope_shadow.stderr
└── ok_*.rs                              # positive cases that must compile
```

The `.stderr` snapshots pin the *exact* error message including the
`help:` lines. CI runs `cargo test -p obs-macros -- trybuild` and
fails the job on any drift. Updates require explicit human approval
(`TRYBUILD=overwrite cargo test`).

This is what makes the dev-ergonomics promises in
[60-dev-ergonomics.md § 6](./60-dev-ergonomics.md#6-compile-error-quality)
load-bearing rather than aspirational.

## 5. Property tests (proptest)

Where invariants are easy to state in terms of inputs:

| Crate | Property |
| --- | --- |
| `obs-proto` | `encode(decode(x)) == x` for arbitrary envelope shapes |
| `obs-core` | `emit(scope_field=X) → envelope.trace_id == X` for arbitrary X |
| `obs-build` | codegen output is deterministic byte-for-byte across runs |
| `obs-tracing-bridge` | round-trip property: tracing event → obs envelope → tracing event preserves target/level/string-fields |

Property test seeds are committed to the repo when a regression is
caught, so the failing input replays in CI.

## 6. Mock OTLP collector

`obs-otel`'s integration tests bring up an in-process `tonic` server
that implements the OTLP gRPC services and records what it sees:

```rust
let (mock, addr) = obs_otel::test::MockOtelCollector::start().await?;
let sink = OtlpLogSink::builder().endpoint(format!("http://{addr}")).build()?;

obs::install_observer(StandardObserver::builder().sink_for(Tier::Log, sink).build()?);
ObsRequestCompleted::builder().route(Route::ListUsers).status(Status::Ok).emit();
obs::observer().flush().await;

let logs = mock.received_logs().await;
assert_eq!(logs.len(), 1);
assert_eq!(logs[0].attributes.get("event.name"), Some("myapp.v1.ObsRequestCompleted"));
```

The mock asserts on the OTLP wire shape, not the SDK's internal
state — gives us protocol-conformance regression coverage.

## 7. Dev-ergonomics test suite

Backs every claim in [60-dev-ergonomics.md](./60-dev-ergonomics.md):

```
crates/obs-sdk/tests/dev_ergonomics/
├── test_quickstart_60s.rs       # `obs init` + cargo run end-to-end
├── test_compile_errors.rs       # delegates to obs-macros trybuild
├── test_no_observer_noop.rs     # verifies no panic, ≤ one atomic load
├── test_in_memory_observer.rs   # assert_emitted! patterns + wait_for timeout
├── test_hot_reload.rs           # SIGHUP changes sampling rate
├── test_tracing_bridge.rs       # bridge lifts tracing::info! into ObsTracingForensicEvent
├── test_parallel_tests.rs       # runs 32 #[obs::test]s concurrently; verifies no cross-contamination
├── test_panic_hook.rs           # ObsPanicked emitted before process tears down
├── test_multi_tenant_observer.rs # per-task observer override via Future::with_observer
│                                  # (11 § 3.1); spawns N concurrent tasks each with
│                                  # its own observer; asserts events route correctly
│                                  # even when tokio migrates tasks across threads
├── test_registry_init.rs        # observer init walks EVENT_SCHEMAS; asserts every
│                                  # ObsXxx in the test fixture appears in by_name and
│                                  # by_hash; asserts ObsRegistryInitialized fired
└── test_scrubbed_envelope.rs    # SECRET stripped, PII redacted, original env.payload
                                   # untouched; ScrubError causes drop-with-metric,
                                   # not leak through to a sink
```

CI runs this suite on every PR; failure is treated as severely as a
clippy regression.

## 8. Bench gating

Benches and their CI gates live in [71-performance-budgets.md § 4 / § 5](./71-performance-budgets.md#4-bench-harness).

## 9. Build dependencies

| Depends on | Provides |
| --- | --- |
| [11-runtime-core.md](./11-runtime-core.md) | `with_test_observer`, per-thread override slot |
| [60-dev-ergonomics.md](./60-dev-ergonomics.md) | dev-erg promises this suite backs |
| [12-schema-and-codegen.md](./12-schema-and-codegen.md) | lint IDs and messages pinned by trybuild |

Test ergonomics ship in `obs-sdk`'s `test` feature flag (cfg-only,
zero release cost). See [61-crates-and-features.md § 2.11](./61-crates-and-features.md#211-obs-sdk).
