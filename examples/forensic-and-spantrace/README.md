# obs-example-forensic-and-spantrace

Demonstrates two of the dev-erg patterns from
`60-developer-experience.md` § 4.3 that the other examples don't cover:

1. **`obs::forensic!`** — emergency emit with per-callsite rate
   limiting (spec 11 § 6.3 / spec 13 § 8). Use it to capture a
   one-off snapshot when a normal structured event would be too
   rigid.
2. **`obs::SpanTrace`** — snapshot of the active scope ancestry
   (spec 13 § 9). Render into an error type so the rendered chain
   reaches user-visible logs.

## Run

```bash
cargo run -p obs-example-forensic-and-spantrace
```

## What this demonstrates

- **§ 4.3.F — `obs::forensic!`**: stable site-id + `{ "k" => v }`
  attribute syntax; per-callsite governor cap.
- **§ 4.3.F — budget enforcement**: when the per-callsite budget is
  exceeded, the runtime emits one
  `obs.runtime.v1.ObsForensicBudgetExceeded` self-event and silently
  drops subsequent emits until the limiter window expires.
- **§ 13 § 9 — `SpanTrace`**: capture from inside nested scopes; the
  rendered trace mirrors the active ancestry innermost-first.

## Out of scope

- Inbound HTTP middleware → `examples/http-service/`
- Multi-tier sinks → `examples/batch-pipeline/`
- Per-task observer routing → `examples/multi-tenant/` (planned)
