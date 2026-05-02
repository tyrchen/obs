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

## Documents

| File | Type | Purpose |
| --- | --- | --- |
| [wide-events-prd.md](./wide-events-prd.md) | PRD | Vision, users, success metrics, non-goals |
| [architecture-design.md](./architecture-design.md) | Design | Runtime data model, observer, sinks, OTel mapping (incl. SeverityNumber + Resource), trace context propagation, production concerns, defaults table, **Key Design Decisions** |
| [schema-codegen-design.md](./schema-codegen-design.md) | Design | `.proto` annotations and build-time codegen pipeline (buffa + buffa-reflect) |
| [crates-design.md](./crates-design.md) | Design | Workspace layout, per-crate public API (incl. `obs-tower`, `obs-tracing-bridge` bidirectional) |
| [cli-design.md](./cli-design.md) | Design | `obs` CLI: lint, validate, codegen, query, diff (Rust-only in v1) |
| [dev-ergonomics-design.md](./dev-ergonomics-design.md) | Design | North star, quickstart, mental model, errors, testing, migration, AI authoring; **emit-form rationale** |
| [tracing-interop-design.md](./tracing-interop-design.md) | Design | Bidirectional `tracing` ↔ `obs` bridge: Layer + Subscriber, auto-typing, span correlation, loop break, recommended init |
| [callsite-interning-design.md](./callsite-interning-design.md) | Design | `defmt`-style callsite interning for the bridged tracing path: BLAKE3 ids, registry, `Off`/`Hybrid`/`Compact` modes, wire-size analysis |
| [impl-plan.md](./impl-plan.md) | Impl Plan | Phased delivery M0 → M3 with exit criteria |

## Reading order

1. **PRD** — what we are building and for whom
2. **Architecture** — the data model and runtime; **read § 10 Key
   Design Decisions** for the load-bearing choices
3. **Schema codegen** — how a `.proto` file becomes typed Rust + lints
4. **Crates** — how the surface area is split into shippable units
5. **Dev ergonomics** — the contract for what using the SDK feels like
6. **CLI** — the developer-facing tooling
7. **Impl plan** — how we cut milestones

## Naming conventions (binding)

- Public Rust namespace: `obs::*`
- Public proto namespace: `obs.v1`
- CLI binary: `obs`
- **Event message names: `Obs<EventName>`** (enforced by lint L011;
  see [architecture-design.md § 1.5](./architecture-design.md#15-naming-convention-obs-event-types))
- Envelope field names use short forms: `ts_ns`, `sev`, `service`,
  `instance`, `version`, `format_ver` — see
  [architecture-design.md § 1.4](./architecture-design.md#14-envelope)

Where this document refers to "the runtime", it means the collection
of crates listed in [crates-design.md](./crates-design.md).
