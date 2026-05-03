# obs-example-todomvc

A TodoMVC HTTP backend (no UI) wired end-to-end with the obs SDK using
the **proto-first** authoring path. This is the example to read first
if you want to see what an app developer's day-to-day workflow looks
like — author proto, lint it with `obs`, run the binary, watch events
flow into a daily NDJSON sink, then slice them with `obs query`.

> All commands below assume you run them from the repo root
> (`/Users/tchen/projects/mycode/rust/obs`). Replace with your own
> checkout path. `obs` must be on `$PATH` (run `cargo install --path
> apps/obs-cli` once if not).

## What `obs init` produces

```bash
obs init --mode proto --package todomvc.v1 examples/todomvc
```

The scaffold drops:

```
examples/todomvc/
├── Cargo.toml          # obs-sdk + obs-build deps
├── obs.yaml            # observer runtime config
├── build.rs            # invokes obs_build::Config
├── proto/
│   └── todomvc/v1/
│       └── events.proto    # one `ObsHelloEmitted` placeholder
└── src/
    └── main.rs         # obs_sdk::include_schemas!() + emit demo
```

For this example we replaced the placeholder with the six events that
back the TodoMVC backend (see `proto/todomvc/v1/events.proto`):
`ObsTodoCreated`, `ObsTodoUpdated`, `ObsTodoCompleted`,
`ObsTodoDeleted`, `ObsTodoListQueried` (TIER_METRIC, with counter +
histogram measurements), and `ObsHttpRequestProcessed`.

## 1. Validate the schema

```bash
obs validate examples/todomvc/proto/todomvc/v1/events.proto \
  --include examples/todomvc/proto
```

Expected:

```
OK · 6 annotated event(s)
  - todomvc.v1.ObsHttpRequestProcessed (tier=Log, sev=Info, fields=4)
  - todomvc.v1.ObsTodoCompleted (tier=Log, sev=Info, fields=3)
  - todomvc.v1.ObsTodoCreated (tier=Log, sev=Info, fields=3)
  - todomvc.v1.ObsTodoDeleted (tier=Log, sev=Info, fields=3)
  - todomvc.v1.ObsTodoListQueried (tier=Metric, sev=Info, fields=4)
  - todomvc.v1.ObsTodoUpdated (tier=Log, sev=Info, fields=3)
```

## 2. Lint

```bash
obs lint --schemas $(pwd)/examples/todomvc/proto
```

`--schemas` requires an absolute path. Expected:

```
0 error(s) · 0 warning(s) · 6 event(s) scanned
```

## 3. Inspect one schema

```bash
obs schema show todomvc.v1.ObsTodoCompleted \
  --schemas $(pwd)/examples/todomvc/proto
```

Expected (truncated):

```
Event:        todomvc.v1.ObsTodoCompleted
Tier:         Log
Default sev:  Info
Schema hash:  0xa2cb2d845dc31ee6

Fields:
  #   NAME           KIND           CARD    CLASS
  1   todo_id        Label Low Internal
  2   list           Label Low Internal
  3   latency_ms_since_creation Attribute Unspecified Internal
```

## 4. Doctor

```bash
obs doctor --root examples/todomvc
```

Expected (all green ✔):

```
✔ obs-sdk in [dependencies]
✔ obs-build in [build-dependencies]
✔ build.rs invokes obs_build::Config::compile()
✔ schema-source = proto; proto/ exists with 1 .proto files
ℹ obs.yaml not found; observer will run with defaults

4 OK · 0 ERROR · 1 INFO
```

## 5. Run the server

```bash
cargo run -p obs-example-todomvc -- --port 8090
```

You'll see:

```
obs-example-todomvc listening on http://127.0.0.1:8090
ndjson sink: ./obs-out/todomvc.ndjson
```

The pretty stdout sink prints any TRACE/AUDIT-tier event; LOG and
METRIC events flow straight into `./obs-out/todomvc-YYYY-MM-DD.ndjson`
(daily rolling — see `RollingPolicy::Daily`).

## 6. Exercise every route

```bash
# Create
curl -s -X POST http://127.0.0.1:8090/todos \
  -H 'content-type: application/json' \
  -d '{"title":"buy milk","list":"groceries"}'
# → {"id":"todo-1", ...}

# List (filter = all | active | completed)
curl -s 'http://127.0.0.1:8090/todos?filter=active'
# → {"items":[{"id":"todo-1", ...}]}

# Update title
curl -s -X PATCH http://127.0.0.1:8090/todos/todo-1 \
  -H 'content-type: application/json' \
  -d '{"title":"buy oat milk"}'

# Mark completed (emits ObsTodoCompleted with dwell time)
curl -s -X PATCH http://127.0.0.1:8090/todos/todo-1 \
  -H 'content-type: application/json' \
  -d '{"completed":true}'

# Delete
curl -i -X DELETE http://127.0.0.1:8090/todos/todo-1
# → HTTP/1.1 204 No Content

# Health
curl -s http://127.0.0.1:8090/healthz   # → ok
```

## 7. Tail the NDJSON live

`RollingFileWriter` writes `<prefix>-YYYY-MM-DD.ndjson` (UTC). Pick
the file for today:

```bash
obs tail --file examples/todomvc/obs-out/todomvc-$(date -u +%Y-%m-%d).ndjson
```

## 8. Query

```bash
obs query \
  --from examples/todomvc/obs-out/todomvc-$(date -u +%Y-%m-%d).ndjson \
  --event todomvc.v1.ObsTodoCompleted \
  --since 5m
```

You can also stack `--label list=groceries` and `--severity warn` to
narrow further. The output is the same NDJSON shape on stdout, so it
pipes cleanly into `jq` / `obs tail --stdin` / your warehouse loader.

## Per-tenant routing

If your service is multi-tenant and each tenant should ship to its
own sink (per-tenant OTLP endpoint, per-tenant Parquet partition,
etc.), wrap the per-request work in `with_observer_task`:

```rust
use obs_core::{Observer, observer::with_observer_task};
use std::sync::Arc;

// Resolve the tenant's observer however your tenancy model dictates —
// from a header, a JWT claim, a path prefix, etc.
async fn handle_request(tenant_id: &str, /* ... */) {
    let tenant_observer: Arc<dyn Observer> = lookup_tenant_observer(tenant_id);
    with_observer_task(tenant_observer, async {
        // Every `.emit()` from inside this future lands on the
        // tenant's observer. Anything spawned outside still uses the
        // global default.
        ObsTodoCreated::builder().todo_id(...).emit();
        store.create(/* ... */).await;
    }).await;
}
```

The global observer (installed via `install_observer(...)` at startup)
remains the fallback for background tasks and anything outside a
tenant scope. `with_observer_task_sync` is the non-async equivalent.
