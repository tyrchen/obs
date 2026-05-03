# obs SDK examples

Three runnable apps showing how to integrate the obs SDK across the
three observability surfaces. Each example is a workspace member —
`cargo run -p <name>` from the repo root works without further setup.

| Example | Surface | Sinks exercised | What it shows |
| --- | --- | --- | --- |
| [`http-service`](./http-service/) | tracing | `StdoutSink`, `OtlpLogSink` (gRPC, opt-in) | axum HTTP service, `obs-tower::ObsHttpLayer::server()` for W3C `traceparent` propagation, typed events with severity escalation on 4xx |
| [`batch-pipeline`](./batch-pipeline/) | analytics | `StdoutSink`, `ParquetSink` | synthetic ETL emitting LOG + METRIC tier events; produces partitioned `obs_events-*.parquet` files for downstream OLAP |
| [`worker-pool`](./worker-pool/) | metrics | `StdoutSink`, `OtlpMetricSink` (gRPC, opt-in) | worker-pool simulator emitting MEASUREMENT-flagged fields; works with `StdoutDebugExporter` out of the box and a real OTLP collector when `OTEL_EXPORTER_OTLP_ENDPOINT` is set |

## Run

```bash
# tracing — start the service then probe it from another terminal
cargo run -p obs-example-http-service

# analytics — produces ./obs-out/parquet/...
cargo run -p obs-example-batch-pipeline -- --batches 10 --rows 5000

# metrics — workers + tasks
cargo run -p obs-example-worker-pool -- --workers 4 --tasks 200
```

Each example's README explains its inputs, expected output, and any
known gaps tracked in [`specs/93-improvements-review.md`](../specs/93-improvements-review.md).

## Why these three?

Together they cover the spec 93 § 6.1–6.3 surface and exercise every
sink / observer path that landed in Phase 6:

- The buffa payload encoder + runtime scrubber (P0-1 / P0-2) — every
  emit goes through `EventSchema::encode_payload` and the
  `scrub_for_log` default impl.
- The OTLP/gRPC exporter (P0-6) — http-service and worker-pool both
  wire `GrpcOtlpExporter` when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
- The Parquet sink (P1-8 follow-up) — batch-pipeline produces real
  Parquet files with the spec 22 sparse `obs_events` schema.
- The async drain path (`Observer::shutdown().await`) — every
  example explicitly drains so users see the correct shutdown
  pattern.

Each example also surfaces a known limitation in its README so the
gap between "what works today" and "what spec 93 will close" is
honest.
