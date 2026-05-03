# Design — Performance Budgets & Benchmarks

Status: draft v1 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [11-runtime-core.md](./11-runtime-core.md), [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md)

This spec consolidates every performance budget the SDK promises,
the techniques used to meet them, and the criterion benches that
gate them in CI. Earlier drafts scattered numbers across the
architecture / tracing-interop / callsite-interning specs; this is
the single source of truth.

## 1. Hot-path principles

1. **No allocations on the steady-state emit path** beyond the typed
   event struct itself. Labels go through a thread-local
   `BytesMut` and a `SmallVec<[(&'static str, Cow<'static, str>); 8]>`.
2. **One atomic load per emit** when the observer is `Noop` or the
   callsite is filtered out (cached `Interest`).
3. **No virtual call to `Observer::enabled` when interest is `always`
   or `never`** — the static `ObsCallsite::interest: AtomicU8`
   short-circuits before dispatch.
4. **No work on the emit thread for sinks** — `try_send` to a
   per-tier mpsc; sinks run on worker tasks.
5. **AUDIT tier is the deliberate exception** — bounded blocking +
   spool, never silent drop. See [11-runtime-core.md § 6.4](./11-runtime-core.md#64-audit-tier-delivery-policy).

## 2. The atomic Interest cache (replaces per-callsite DashMap)

This is the single highest-leverage performance change vs. the v2
draft. Tracing-style: cache the filter decision on the static itself.

```rust
pub struct ObsCallsite {
    /* … static metadata … */
    interest:   AtomicU8,    // 0 unknown · 1 never · 2 sometimes · 3 always
    generation: AtomicU32,   // matched against Observer::generation()
}

#[inline(always)]
pub fn enabled(&self, current_gen: u32) -> bool {
    let raw = self.interest.load(Ordering::Relaxed);
    if self.generation.load(Ordering::Relaxed) == current_gen {
        match raw {
            1 => return false,    // Never — short-circuit
            3 => return true,     // Always — short-circuit
            _ => {}                // Sometimes / Unknown — fall through
        }
    }
    /* slow path: re-query observer, cas back into atomic */
    self.refresh_interest()
}
```

This avoids the `DashMap<&'static ObsCallsite, FilterDecision>` lookup
in earlier drafts and matches tracing's `Callsite::set_interest`
mechanic. Filter cache invalidation: see
[11-runtime-core.md § 3.2](./11-runtime-core.md#32-filter-cache-invalidation-on-reload).

## 3. Budget table

All numbers measured on M2/2024-class hardware in release mode.

### 3.1 Native emit

| Path | Budget | Notes |
| --- | --- | --- |
| Noop emit (observer not installed) | ≤ 110 ns | one `OVERRIDE_COUNT.load` + one `OBSERVER_GLOBAL.load_full` (the `Arc::clone` inside `load_full` is the dominant cost; spec 95 § 3.6 / P2-AD revised the budget upward from 50 ns after profiling on M2/2024 hardware showed 104 ns is the achievable floor without trading away ergonomics) |
| Filtered-out emit (interest=Never, cache hit) | ≤ 25 ns | atomic load + branch |
| `observer()` resolution, no override (fast path) | ≤ 15 ns | `OVERRIDE_COUNT == 0` short-circuits both probes |
| `observer()` resolution, per-thread override set | ≤ 30 ns | adds one `RefCell::borrow + clone` |
| `observer()` resolution, per-task override set | ≤ 30 ns | adds one `task_local::try_with + clone` |
| `Future::with_observer(o).poll` overhead | ≤ 30 ns / poll | one `task_local::sync_scope` per `poll`; amortised over the polled work inside |
| `emit` (StandardObserver, all sinks no-op) | ≤ 1 µs P50 | construct + project + try_send |
| `emit` (NdjsonFileSink batched) | ≤ 1.5 µs P50 | adds copy into `BytesMut` |
| Scope `enter` + `exit` | ≤ 100 ns | task_local push/pop |
| Encode (10 fields, buffa) | ≤ 5 µs | not on critical emit path |

### 3.1a Schema registry & scrubber (per-event worker side)

| Path | Budget | Notes |
| --- | --- | --- |
| `SchemaRegistry::lookup(env)`, `schema_hash` hit | ≤ 15 ns | one `HashMap<u64, ...>` probe |
| `SchemaRegistry::lookup(env)`, `full_name` fallback | ≤ 60 ns | one `HashMap<&str, ...>` probe with string hash |
| `SchemaRegistry::lookup(env)`, miss | ≤ 80 ns | both probes + rate-limited `ObsSchemaUnknown` increment |
| `EventSchemaErased::scrub_for_log`, no SECRET/PII fields | ≤ 100 ns | identity early-return |
| `EventSchemaErased::scrub_for_log`, ≤ 5 redact/strip fields | ≤ 1.5 µs | per-field walk + re-encode into worker scratch |
| `ScrubbedEnvelope` construction | ≤ 50 ns | wrapper struct, zero allocation (scratch borrow) |
| `SchemaRegistry::from_link_section()` (init time) | ≤ 10 ms / 1000 schemas | dominated by Arrow schema assembly; runs once per `install_observer` |

### 3.2 Bridge

| Path | Budget P50 | Notes |
| --- | --- | --- |
| `tracing::info!` → obs envelope (forensic mode) | ≤ 3 µs | native emit ~1 µs + bridge overhead ≤ 2 µs |
| `tracing::info!` → obs envelope (auto-typed mode) | ≤ 3 µs | matcher lookup is cached |
| `obs::emit!` → tracing event (cached metadata) | ≤ 2.5 µs | native emit ~1 µs + bridge ≤ 1.5 µs |
| `tracing::span!` → `ObsSpanCompleted` (amortised) | ≤ 4 µs total | per child event under the span |

### 3.3 Callsite interning (when enabled)

| Path | Budget | Notes |
| --- | --- | --- |
| Direction A interned emit (cold) | ≤ 4 µs P50 | BLAKE3 hash + DashMap insert + Registered emit + interned emit |
| Direction A interned emit (warm) | ≤ 2.5 µs P50 | DashMap lookup + interned emit |
| Direction B reconstitution (cold) | ≤ 3 µs P50 | DashMap lookup + Metadata leak + tracing dispatch |
| Direction B reconstitution (warm) | ≤ 1.5 µs P50 | metadata cache hit + dispatch |
| Registry insertion | ≤ 200 ns | DashMap insert is amortised O(1) |
| BLAKE3 (~200 input bytes) | ≤ 80 ns | benched |

## 4. Bench harness

`crates/obs-core/benches/` ships criterion benches:

| Bench | What it measures |
| --- | --- |
| `bench_emit_noop` | observer not installed, just the macro path |
| `bench_emit_filtered` | callsite filtered to Never |
| `bench_emit_inmemory` | full path through `InMemoryObserver` |
| `bench_emit_ndjson` | full path through `NdjsonFileSink` (no rotation) |
| `bench_observer_resolution` | three tiers: no override, per-thread set, per-task set |
| `bench_with_observer_poll` | `Future::with_observer(o).poll` overhead per poll |
| `bench_scope_enter_exit` | scope guard push/pop |
| `bench_encode_payload` | typed → buffa bytes for 1, 5, 10, 30 fields |
| `bench_registry_lookup` | hash-hit, name-fallback, miss paths |
| `bench_registry_init` | wall time to walk EVENT_SCHEMAS at observer init for 10 / 100 / 1000 schemas |
| `bench_scrub_for_log` | per-event scrubber on a 10-field event with 0 / 1 / 5 redact/strip fields |

`crates/obs-tracing-bridge/benches/`:

| Bench | What it measures |
| --- | --- |
| `bench_tracing_to_obs_overhead` | `tracing::info!` baseline vs bridged |
| `bench_obs_to_tracing_overhead` | `obs::emit!` baseline vs bridged |
| `bench_interning_cold` / `bench_interning_warm` | both modes for both directions |
| `bench_blake3_callsite` | hashing 200-byte canonical input |

## 5. CI gates

`cargo bench --bench emit_hot_path` runs against
`benches/baseline.json`; > 10% regression on any path fails the job.

`cargo bench --bench bridge_*` and `cargo bench --bench interning_*`
gate at the same threshold.

The baseline file is updated as part of release prep; intentional
optimisation drops are committed alongside the new baseline.

## 6. Observability of the perf system

Self-events that surface real-world perf state in production:

| Event | Purpose |
| --- | --- |
| `obs.runtime.v1.ObsSinkDropped{tier, reason}` | counter; the headline regression signal |
| `obs.runtime.v1.ObsLabelCardinalityHigh` | label dictionary blow-up early warning |
| `obs.runtime.v1.ObsAuditSpooled` | AUDIT-tier blocking lapsed into spool |

Operators watch these in the same dashboards as application events
(self-events flow through the same observer, see
[11-runtime-core.md § 10](./11-runtime-core.md#10-self-events)).

## 7. Profiling toolchain

For perf investigations beyond benches:

- `cargo flamegraph --bench bench_emit_inmemory` to spot hot frames.
- `samply record cargo run -p server` for on-CPU/off-CPU during a
  realistic load test against `apps/server`.
- `cargo asm` snapshots in `crates/obs-core/asm/` capture the
  generated `obs::emit!` macro expansion at HEAD; PRs that touch the
  macro must update the snapshot, making code-size or instruction-mix
  changes reviewable in diff.

## 8. Build dependencies

| Depends on | Provides |
| --- | --- |
| [11-runtime-core.md](./11-runtime-core.md) | `ObsCallsite`, atomic Interest cache |
| [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md) | emit/scope macros |

Cross-references: [72-testing-strategy.md](./72-testing-strategy.md)
covers how benches integrate with the broader test pyramid.
