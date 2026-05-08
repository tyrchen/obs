# obs — schema-first wide-events SDK for Rust

Status: pre-1.0 (Phase 5 hardening, [spec 91](./specs/91-impl-plan.md))

> 中文版本：[README.zh-CN.md](./README.zh-CN.md)

`obs` is a Rust SDK for **wide structured events**: emits, sampling,
filtering, scope auto-fill, OTLP / Parquet / ClickHouse sinks, and a
two-way bridge to the `tracing` crate — all driven by schemas you
author either as Rust structs (`#[derive(Event)]`) or as `.proto`
messages (`obs-build`).

The full design lives under [`./specs/`](./specs); start with
[00-prd.md](./specs/00-prd.md) for the charter and
[91-impl-plan.md](./specs/91-impl-plan.md) for the dependency-ordered
build. **For day-to-day usage, jump to the
[User Guide](./docs/user-guide.md).** For internals and contributing,
see the [Developer Guide](./docs/dev-guide.md).

## 60-second tour

```bash
# 1. Install the CLI (≤ 30 s on a 2024-class laptop with cargo's
#    registry cache cold; ~10 s warm).
cargo install --git https://github.com/TODO obs-cli

# 2. Scaffold a fresh schema crate.
obs init demo
cd demo

# 3. Build + run. The scaffold installs a StandardObserver with a
#    StdoutSink(Full), so the first emit prints to stdout.
cargo run
# → 1730000000.000000000 info demo.v1.ObsHelloEmitted {who=world}
```

The exact command surface is documented in
[`specs/50-cli.md`](./specs/50-cli.md).

## What you get

| Concern | What `obs` ships |
| --- | --- |
| **Schema authoring** | `#[derive(Event)]` (Rust-first) or `obs-build` (proto-first). Both produce byte-identical generated artefacts (spec 12 § 1.2). |
| **Hot-path emit** | Atomic-`Interest` cache on every callsite; ~25 ns when filtered out, ~110 ns noop, ~1 µs when delivered. Spec 11 § 2 / 71 § 4. |
| **Three-tier observer resolution** | Per-task → per-thread → global. Multi-tenant servers install a per-task observer; tests install per-thread; production wires the global. Spec 11 § 3. |
| **Sinks** | `StdoutSink`, `NdjsonFileSink` (with `RollingFileWriter` + `NonBlockingWriter`), `OtlpLogSink` / `OtlpMetricSink` / `OtlpTraceSink`, `ParquetSink`, `ClickHouseSink`. Spec 20 / 22. |
| **Sampling + filter** | `obs::Filter` (EnvFilter-shaped), head sampler, tail-on-error ring buffer per `obs::scope!` frame. Spec 13. |
| **Tracing bridge** | `TracingToObsLayer` (every `tracing::info!` becomes a typed `ObsTracingForensicEvent`) and `ObsToTracingSink` (replay events through `tracing_log` so existing `tracing-subscriber` consumers keep working). Spec 30. |
| **AUDIT tier** | Bounded blocking + on-disk spool with CRC-checked recovery on observer init. Spec 11 § 6.4. |
| **HTTP middleware** | `obs-tower` — `ObsHttpLayer` for axum/tower stacks, with multi-tenant observer dispatch via header. Spec 40. |
| **CLI** | `obs init`, `obs validate`, `obs lint`, `obs schema show`, `obs decode`, `obs tail`, `obs query`, `obs diff`, `obs audit`, `obs migrate`. Spec 50. |

## Workspace layout

```
crates/
  obs-types          # 7 vocabulary enums (Tier, Severity, FieldKind, …)
  obs-proto          # envelope.proto / builtin.proto + buffa codegen
  obs-core           # Observer, sinks, registry, sampling, scope, filter
  obs-macros         # #[derive(Event)], obs::emit!, #[obs::test], …
  obs-build          # build.rs codegen for proto-first authoring
  obs-kit            # façade re-export
  obs-otel           # OTLP log / metric / trace sinks
  obs-parquet        # Parquet sink (Single-table layout)
  obs-clickhouse     # ClickHouse sink
  obs-tower          # HTTP server + client middleware
  obs-tracing-bridge # bidirectional tracing ↔ obs bridge
apps/
  obs-cli            # the `obs` developer CLI
  server             # demo: hello-world emit
  server-proto       # demo: proto-first authoring path
  soak               # 50k-events/sec soak harness (spec 90 § M4)
examples/            # four runnable example services (todomvc, interop pair, sinks-showcase)
specs/               # design specs (read 00-prd.md → 99-key-decisions.md)
docs/                # guides, migration, research notes
```

## Build, test, lint

```bash
# Always-shippable targets per CLAUDE.md / spec 90 § 0.
cargo build
cargo test --workspace --all-features
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Phase-5 hardening targets (spec 90 § M4 / impl-plan 5.x).
make lint-strict       # cargo clippy -W clippy::pedantic with curated allows
make audit             # cargo deny check advisories
make deny              # cargo deny check (advisories + bans + licenses + sources)
make soak              # 30 s, 50 k events/sec, ObsSinkDropped == 0 assertion
make soak-24h          # full 24-h soak (run before stamping v1.0)
make check-format-ver  # spec 90 § 3.3 envelope wire-shape lock
```

## Documentation

### Guides (start here)

| Guide | Audience | English | 中文 |
| --- | --- | --- | --- |
| **User Guide** | App engineers + SREs adopting obs in a service. Install, schema authoring, emit/scope/filter, sinks, OTLP, multi-tenant, CLI, ops. | [user-guide.md](./docs/user-guide.md) | [user-guide.zh-CN.md](./docs/user-guide.zh-CN.md) |
| **Developer Guide** | Contributors and sink/bridge implementers. Architecture, observer model, callsite cache, sink contract, registry, codegen, perf, testing, contributing. | [dev-guide.md](./docs/dev-guide.md) | [dev-guide.zh-CN.md](./docs/dev-guide.zh-CN.md) |
| **Migration from `tracing`** | Crates currently on `tracing-subscriber` who want typed events / sampling / OTel mapping. | [migration-from-tracing.md](./docs/migration-from-tracing.md) | — |

### Authoritative references

| Where | What |
| --- | --- |
| [`./specs`](./specs) | Authoritative design — read in numeric order or follow `index.md`. |
| [`./examples`](./examples) | Four runnable example services covering todomvc, two interop modes, and a sinks-fan-out showcase. |
| [`./docs/research/`](./docs/research/) | Phase-0 spike memos. |
| `cargo doc --workspace --no-deps --open` | Generated rustdoc for every public item. |

## License

MIT — see [LICENSE.md](./LICENSE.md).
