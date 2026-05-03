# obs-example-tracing-migration

Migrate a tracing-only service to obs typed events without breaking
existing `tracing::*` log lines. Spec 60 § 4.3.J / spec 95 § 3.11 /
P2-AI.

## Run

```bash
cargo run -p obs-example-tracing-migration
```

## What this demonstrates

- **§ 4.3.J — bridge install**: `obs_tracing_bridge::init(None)`
  forwards every `tracing::*` call into the obs runtime, so the
  existing log surface keeps working while typed schemas land
  incrementally.
- **§ 1.2 — typed promotion**: `#[derive(obs::Event)]` lets one hot
  call site graduate from a free-form `tracing::info!` to a typed
  `ObsCheckoutAttempted` schema. The rest of the codebase keeps
  emitting via `tracing::*` and lands in the same observer.

## Out of scope

- Inbound HTTP middleware → `examples/http-service/`
- Multi-tenant per-task observer → `examples/multi-tenant/`
