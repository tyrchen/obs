# obs-example-multi-tenant

Per-task observer routing — emit pipelines that carry tenant identity
all the way down to the sink. Spec 60 § 4.3.L / spec 95 § 3.11 / P2-AI.

## Run

```bash
cargo run -p obs-example-multi-tenant
```

## What this demonstrates

- **§ 4.3.L — per-task observer**: `obs_core::with_observer_task`
  pins an observer to a tokio task; emits inside the task route to
  the override, emits outside route to the global default.
- **Single binary, multiple sinks**: each tenant gets its own
  in-memory observer; in production these would be
  `StandardObserver`s wired to per-tenant OTLP endpoints.

## Out of scope

- Inbound HTTP request scope → `examples/http-service/`
- Tracing-bridge migration → `examples/tracing-migration/`
