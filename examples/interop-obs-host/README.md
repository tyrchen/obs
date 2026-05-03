# obs-example-interop-obs-host

> I'm building a new service with **obs typed events**. My deps
> (`hyper`, `reqwest`, `tower`, …) emit via `tracing::*`. I want a
> single observability pipeline so those bridged emits land in the
> same observer as my typed events.

This example shows the **obs-first** side of bidirectional interop:
the app's primary authoring style is obs (proto-first builders), and
the `obs-tracing-bridge` is installed as a Direction-A subscriber so
every existing `tracing::info!`/`warn!` in the dependency graph
funnels into the same observer.

## File tree

```
examples/interop-obs-host/
├── Cargo.toml          # obs-sdk + obs-tracing-bridge deps
├── build.rs            # invokes obs_build::Config
├── proto/
│   └── orders/v1/
│       └── events.proto    # ObsOrderPlaced, ObsOrderShipped
└── src/
    ├── main.rs         # observer + bridge install + demo flow
    └── schema.rs       # obs_sdk::include_schemas!("orders.v1")
```

This is the post-edit version of `obs init --mode proto --package
orders.v1 examples/interop-obs-host` — the scaffold's placeholder
event was replaced with the two domain events above.

## 1. Validate

```bash
obs validate examples/interop-obs-host/proto/orders/v1/events.proto \
  --include examples/interop-obs-host/proto
```

Expected:

```
OK · 2 annotated event(s)
  - orders.v1.ObsOrderPlaced (tier=Log, sev=Info, fields=4)
  - orders.v1.ObsOrderShipped (tier=Log, sev=Info, fields=3)
```

## 2. Lint

```bash
obs lint --schemas $(pwd)/examples/interop-obs-host/proto
```

Expected: `0 error(s) · 0 warning(s) · 2 event(s) scanned`.

## 3. Doctor

```bash
obs doctor --root examples/interop-obs-host
```

Expected: `4 OK · 0 ERROR · 1 INFO`.

## 4. Run with the bridge ON (default)

```bash
cargo run -p obs-example-interop-obs-host
```

Annotated stdout:

```
tracing→obs bridge installed
─── orders.v1.ObsOrderPlaced …             ← typed obs emit
─── obs.v1.ObsTracingForensicEvent … hyper::client::pool …  ← BRIDGED tracing::info!
─── obs.v1.ObsTracingForensicEvent … hyper::client::pool …  ← BRIDGED tracing::warn!
─── obs.v1.ObsTracingForensicEvent … reqwest::connect  …    ← BRIDGED tracing::info!
─── orders.v1.ObsOrderShipped …            ← typed obs emit
```

Note the `request_id = demo-001` label appears on **every** event —
typed and bridged alike — because both sides observe the same
`obs::scope!` frame. The `ObsCallsiteRegistered` lines are the bridge
announcing tracing call-sites it has interned for downstream
filtering.

## 5. Run with the bridge OFF

```bash
cargo run -p obs-example-interop-obs-host -- --bridge false
```

Output collapses to **only** the typed emits:

```
tracing→obs bridge DISABLED — 3rd-party emits will be dropped
─── orders.v1.ObsOrderPlaced …
─── orders.v1.ObsOrderShipped …
```

The three `tracing::*` calls in `main.rs` still execute, but with no
subscriber installed they go straight to the bit bucket — exactly the
"silent dependency telemetry" failure mode the bridge is designed to
fix.
