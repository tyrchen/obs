# obs-example-worker-pool

A worker-pool simulator that demonstrates the **metrics-focused** obs
SDK surface end-to-end:

- METRIC-tier `ObsWorkerTaskCompleted` with two MEASUREMENT fields
  (`latency_ms`, `queue_depth`) and two LABEL fields (`worker_id`,
  `task_kind`). Once the codegen `project_metrics` impl lands (spec
  93 P1-6) these flow as OTLP `Sum` / `Histogram` data points; today
  they are stored as the typed payload with the MEASUREMENT field
  role flag preserved.
- LOG-tier `ObsWorkerStarted` / `ObsWorkerStopped` so OTel /
  ClickHouse have one row per worker lifecycle, with
  `tasks_processed` for the worker-level summary.
- `OtlpMetricSink` wired through the new `obs_otel::GrpcOtlpExporter`
  when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, else falls back to
  `StdoutDebugExporter` so the example always produces visible
  output.

## Run

```bash
# stdout-only (no collector required):
cargo run -p obs-example-worker-pool

# bigger run:
cargo run -p obs-example-worker-pool -- --workers 8 --tasks 1000

# with a real OTLP/gRPC collector:
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
  cargo run -p obs-example-worker-pool -- --workers 8 --tasks 1000
```

## What to look for

- One `ObsWorkerStarted` per worker.
- N `ObsWorkerTaskCompleted` envelopes total, where N == `--tasks`.
  Each carries `worker_id`, `task_kind`, `latency_ms`,
  `queue_depth`. The MEASUREMENT-flagged fields are what the
  forthcoming OTLP metrics projection will turn into
  `Sum(monotonic=true)` / `Histogram` data points.
- One `ObsWorkerStopped` per worker with `tasks_processed`.
- When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the exporter ships an
  `ExportMetricsServiceRequest` per batched window to the collector.
  The current projection emits one counter point per envelope (the
  Phase-1 placeholder); the typed measurement fields will replace
  this once spec 93 P1-6 lands.

## Known limitations (spec 93 follow-ups)

- **P1-6**: codegen does not yet emit a typed `project_metrics` impl;
  the metric sink falls back to a `<full_name>.count = 1` counter per
  envelope. The latency_ms / queue_depth fields are *captured* in the
  payload, just not yet projected as OTLP data points.
- **P1-5**: full OTel Resource attribute set (`service.namespace`,
  `service.instance.id`, `deployment.environment`, `host.*`) is not
  yet populated.
