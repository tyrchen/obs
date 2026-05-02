# Specs Index

A schema-first, wide-event observability stack for Rust services. The
goal is **`tracing`-class ergonomics** with **compile-time schema,
cardinality, and classification safety**, **first-class OpenTelemetry
integration**, and **first-class analytics**, so a single emission
point doubles as a log line, a metric data point, a trace span, and
an analytics row in a unified columnar table.

Stack: `buffa` for wire types, `buffa-reflect` for schema introspection,
the `Obs*` event-name convention, single sparse `obs_events` analytics
table, and a Rust-only toolchain in v1.

The specs are numbered to reflect the **build order**. Reading them
top-to-bottom matches the milestone progression in [90-roadmap.md](./90-roadmap.md):
M0 covers 10–13, M1 closes 12 and adds 60/72, M2 covers 20 and parts
of 30/71, M3 closes everything else.

## Documents

### Vision

| File | Type | Purpose |
| --- | --- | --- |
| [00-prd.md](./00-prd.md) | PRD | Vision, users, success metrics, non-goals |

### Foundation (M0)

| File | Type | Purpose |
| --- | --- | --- |
| [10-data-model.md](./10-data-model.md) | Design | Wide events, envelope, schema_hash, the `Obs*` naming convention |
| [11-runtime-core.md](./11-runtime-core.md) | Design | Observer + per-thread test override, `ObsCallsite` with atomic `Interest`, sinks, workers, config + reload, AUDIT delivery (binary spool + recovery), pipeline order, panic hook, threading |
| [12-schema-and-codegen.md](./12-schema-and-codegen.md) | Design | `.proto` annotations, `#[derive(Event)]`, lints L001–L013, `EventSchema`, generated builder, the `MetricEmitter` / `BuildableTo` / `FieldCapture` / `SpanCtx` / `EnumCount` trait surface, single-table Arrow fragments |
| [13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md) | Design | `obs::emit!`, `obs::scope!`, `obs::context!`, `obs::Instrumented<F>`, `#[obs::instrument]` (single-event default), `obs::Filter` DSL (ports EnvFilter grammar) + precedence, W3C `traceparent.sampled` propagation, `obs::forensic!`, `obs::SpanTrace` |
| [14-schema-registry.md](./14-schema-registry.md) | Design | `EventSchemaErased` object-safe trait, `linkme`-based link-time schema registration, `ScrubbedEnvelope` worker→sink handoff, sink decode contract |

### Sinks & analytics (M2 → M3)

| File | Type | Purpose |
| --- | --- | --- |
| [20-otel-and-sinks.md](./20-otel-and-sinks.md) | Design | OTLP mapping (Logs / Metrics / Traces, Resource, severity, propagation); built-in sinks, `MakeWriter` abstraction, time + size rolling, formatter styles |
| [22-analytics-storage.md](./22-analytics-storage.md) | Design | Single sparse `obs_events` table; `ParquetSink`; `ClickHouseSink`; Iceberg/Delta positioning |

### Bridge & interning (M2 → M3)

| File | Type | Purpose |
| --- | --- | --- |
| [30-tracing-bridge.md](./30-tracing-bridge.md) | Design | Bidirectional `tracing` ↔ `obs` bridge: `Layer` + `Subscriber`, auto-typing, span correlation, loop break, recommended init |
| [31-callsite-interning.md](./31-callsite-interning.md) | Design | `defmt`-style callsite interning for the bridged tracing path: BLAKE3 ids (with `0`-reserved fix), registry, `Off`/`Hybrid`/`Compact` modes, wire-size analysis |

### Ecosystem (M3)

| File | Type | Purpose |
| --- | --- | --- |
| [40-http-middleware.md](./40-http-middleware.md) | Design | `obs-tower`: HTTP `tower::Layer` for W3C trace context propagation + typed HTTP request events |
| [50-cli.md](./50-cli.md) | Design | `obs` CLI: lint, validate, codegen, query, diff, callsites, migrate (Rust-only in v1) |

### Cross-cutting

| File | Type | Purpose |
| --- | --- | --- |
| [60-dev-ergonomics.md](./60-dev-ergonomics.md) | Design | North star, quickstart, mental model, errors, AI authoring, the **emit-form rationale** |
| [61-crates-and-features.md](./61-crates-and-features.md) | Design | Workspace layout, dependency graph, feature flags |
| [70-security-and-classification.md](./70-security-and-classification.md) | Design | Threat model, `Classification` lints, payload scrubber, bridge PII redactor, `secrecy::Secret*` integration, AUDIT-tier semantics |
| [71-performance-budgets.md](./71-performance-budgets.md) | Design | All P50/P99 budgets, the atomic-`Interest` cache pattern, criterion bench harness, CI gates |
| [72-testing-strategy.md](./72-testing-strategy.md) | Design | Test pyramid, `InMemoryObserver`, `#[obs::test]` parallel-safe attribute, trybuild compile-error fixtures, mock OTLP collector, dev-erg suite |

### Reference

| File | Type | Purpose |
| --- | --- | --- |
| [80-glossary.md](./80-glossary.md) | Reference | Terminology disambiguation (envelope vs event, scope vs span, sink vs layer, …) |
| [90-roadmap.md](./90-roadmap.md) | Roadmap | Phased delivery M0 → M3 with exit criteria, build-order graph, perf gates, definition of done |
| [99-key-decisions.md](./99-key-decisions.md) | Reference | Consolidated load-bearing design decisions (D1–D37) |

## Reading order

For first-time readers:

1. **[00-prd.md](./00-prd.md)** — what we are building and for whom.
2. **[80-glossary.md](./80-glossary.md)** — vocabulary, especially
   the *scope vs span* distinction.
3. **[10-data-model.md](./10-data-model.md)** — the wire shape every
   downstream system sees.
4. **[60-dev-ergonomics.md](./60-dev-ergonomics.md)** — the contract
   for what using the SDK feels like; concrete code examples.
5. **[11-runtime-core.md](./11-runtime-core.md)** — the engine.
6. **[13-emit-scope-and-filter.md](./13-emit-scope-and-filter.md)** —
   the macros and scope semantics.
7. **[12-schema-and-codegen.md](./12-schema-and-codegen.md)** — how
   a `.proto` becomes typed Rust + lints.
8. **[14-schema-registry.md](./14-schema-registry.md)** — the
   object-safe schema registry that lets sinks decode payloads.
9. **[20-otel-and-sinks.md](./20-otel-and-sinks.md)** — sinks and
   OpenTelemetry mapping.
10. **[22-analytics-storage.md](./22-analytics-storage.md)** — the
    analytical view.
11. **[30-tracing-bridge.md](./30-tracing-bridge.md)** and
    **[31-callsite-interning.md](./31-callsite-interning.md)** —
    interop and the wire-size optimisation.
12. **[40-http-middleware.md](./40-http-middleware.md)**,
    **[50-cli.md](./50-cli.md)** — ecosystem.
13. **[70-security-and-classification.md](./70-security-and-classification.md)**,
    **[71-performance-budgets.md](./71-performance-budgets.md)**,
    **[72-testing-strategy.md](./72-testing-strategy.md)** —
    cross-cutting contracts.
14. **[99-key-decisions.md](./99-key-decisions.md)** — *why* it is
    shaped this way (read after #1–#13 for max signal).
15. **[61-crates-and-features.md](./61-crates-and-features.md)** —
    workspace shape, what to put where.
16. **[90-roadmap.md](./90-roadmap.md)** — milestone exit criteria.

## Build-order graph (summary)

```
00-prd
  │
  ▼
10-data-model
  │
  ▼
11-runtime-core ────┐
  │                 │
  ▼                 ▼
12-schema-and-     13-emit-scope-
codegen            and-filter
  │                 │
  └─────┬───────────┘
        ▼
14-schema-registry      ◄── sink decode contract; required by 20/22/30
        │
        ▼
20-otel-and-sinks ──► 22-analytics-storage
        │
        ▼
30-tracing-bridge ──► 31-callsite-interning
        │
        ▼
40-http-middleware    50-cli
```

`60-dev-ergonomics`, `61-crates-and-features`,
`70-security-and-classification`, `71-performance-budgets`,
`72-testing-strategy`, `80-glossary`, `99-key-decisions` are
cross-cutting and read alongside the build-order specs.

## Naming conventions (binding)

- Public Rust namespace: `obs::*`
- Public proto namespace: `obs.v1`
- CLI binary: `obs`
- **Event message names: `Obs<EventName>`** (enforced by lint L011;
  see [10-data-model.md § 7](./10-data-model.md#7-naming-convention-obs-event-types))
- Envelope field names use short forms: `ts_ns`, `sev`, `service`,
  `instance`, `version`, `format_ver` — see
  [10-data-model.md § 6](./10-data-model.md#6-envelope)

Where these documents refer to "the runtime", they mean the collection
of crates listed in [61-crates-and-features.md](./61-crates-and-features.md).
