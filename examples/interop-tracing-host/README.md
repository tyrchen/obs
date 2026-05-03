# obs-example-interop-tracing-host

> I have an existing app that uses `tracing-subscriber`. I'm adopting
> an internal library that emits **obs typed events**. I don't want to
> switch my whole observability stack — I just want the obs events to
> land in my existing tracing pipeline alongside everything else.

This example shows the **tracing-host** side of bidirectional interop:
the host owns `tracing_subscriber::fmt()` and is otherwise unmodified;
a Direction-B `ObsToTracingSink` is plugged into the obs observer so
typed `payments.v1.*` emits become `tracing::Event`s that the host's
fmt subscriber renders alongside its own `tracing::*` calls.

## File tree

```
examples/interop-tracing-host/
├── Cargo.toml          # obs-sdk + obs-tracing-bridge + tracing-subscriber
├── build.rs            # invokes obs_build::Config
├── proto/
│   └── payments/v1/
│       └── events.proto    # ObsPaymentAuthorized, ObsPaymentDeclined
└── src/
    ├── main.rs         # tracing-subscriber + obs observer install + demo
    └── schema.rs       # obs_sdk::include_schemas!("payments.v1")
```

This is the post-edit version of `obs init --mode proto --package
payments.v1 examples/interop-tracing-host`.

## 1. Validate

```bash
obs validate examples/interop-tracing-host/proto/payments/v1/events.proto \
  --include examples/interop-tracing-host/proto
```

Expected:

```
OK · 2 annotated event(s)
  - payments.v1.ObsPaymentAuthorized (tier=Log, sev=Info, fields=4)
  - payments.v1.ObsPaymentDeclined (tier=Log, sev=Warn, fields=3)
```

## 2. Lint

```bash
obs lint --schemas $(pwd)/examples/interop-tracing-host/proto
```

Expected: `0 error(s) · 0 warning(s) · 2 event(s) scanned`.

## 3. Doctor

```bash
obs doctor --root examples/interop-tracing-host
```

Expected: `4 OK · 0 ERROR · 1 INFO`.

## 4. Run

```bash
cargo run -p obs-example-interop-tracing-host
```

Every line below is rendered by `tracing_subscriber::fmt` — the host's
pre-existing setup. The `host::startup`/`host::policy` lines come from
ordinary `tracing::info!`/`warn!` calls; the `payments.v1.*` lines are
typed obs emits that travelled through `ObsToTracingSink` and reached
the same fmt subscriber:

```
INFO host::startup: service ready
WARN host::policy: rate-limit exceeded… merchant="merch-acme"
INFO payments.v1.ObsPaymentAuthorized: obs.full_name="payments.v1.ObsPaymentAuthorized" obs.labels="{\"card_brand\":\"visa\",\"merchant_id\":\"merch-acme\",\"payment_id\":\"pay-7001\"}"
WARN payments.v1.ObsPaymentDeclined:  obs.full_name="payments.v1.ObsPaymentDeclined"  obs.labels="{\"decline_reason\":\"rate_limited\",\"merchant_id\":\"merch-acme\",\"payment_id\":\"pay-7002\"}"
```

Note the **WARN** severity on `ObsPaymentDeclined` came for free — the
proto declares `default_sev: SEVERITY_WARN`, so callers just write
`.emit()` and the bridge maps it onto `tracing::Level::WARN`.

The dynamic-target mode (default in `ObsToTracingSink::new()`) gives
each `(event, severity)` pair its own tracing target like
`payments.v1.ObsPaymentAuthorized`, so directives like
`payments.v1.ObsPaymentAuthorized=trace` work in `RUST_LOG`.

## 5. Filter through tracing — uniformly

The same `EnvFilter` directives govern host emits AND bridged obs
emits. Crank up the obs side independently:

```bash
RUST_LOG=info,obs=trace cargo run -p obs-example-interop-tracing-host
```

Or silence one obs event while keeping host logs:

```bash
RUST_LOG=info,payments.v1.ObsPaymentDeclined=off \
  cargo run -p obs-example-interop-tracing-host
```

This is the whole point of the tracing-host story: one filter, one
output, zero new operational concepts.
