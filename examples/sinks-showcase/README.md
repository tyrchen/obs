# obs-example-sinks-showcase

Three sinks. One emit. Three consumers.

Each call site (`ObsShowcase*::builder()...emit()`) hands the same typed event to a registry that fans it out by **tier** to the sink shape that fits the consumer:

- **Console** (`StdoutSink::Pretty`) — fallback for everything not explicitly routed; gives operators eyeball-friendly output while the binary runs.
- **OTLP** (`OtlpLogSink` + `OtlpMetricSink`) — when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. LOG + METRIC stream to your collector for live dashboards / alerts.
- **Parquet** (`ParquetSink`) — partitioned by `service` and `date` for OLAP. Always retains AUDIT (the row of truth); also catches LOG + METRIC when no OTLP endpoint is configured so you never lose data.

## Architecture

```
                   .emit()
                      │
                      ▼
            ┌─────────────────────┐
            │  obs registry / tier router  │
            └─────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
   console (fallback)  parquet (LOG+METRIC+AUDIT)  otlp (when configured: LOG+METRIC)
```

When OTLP is enabled, `sink_for(Tier::Log, ...)` and `sink_for(Tier::Metric, ...)` rebind those tiers from Parquet to OTLP — Parquet keeps **only AUDIT**. The SDK does not currently expose a tier-level fan-out sink (only `TeeWriter` for byte-level writers), so this trade-off is documented rather than hidden behind a custom multi-sink.

## Scaffold

```
obs init --mode proto --package showcase.v1 examples/sinks-showcase
```

produces (already present in this directory):

- [`Cargo.toml`](./Cargo.toml) — workspace dep wiring + `obs-build` build dep.
- [`build.rs`](./build.rs) — calls `obs_build::Config::compile()` over the proto file.
- [`proto/showcase/v1/events.proto`](./proto/showcase/v1/events.proto) — three annotated messages.
- [`src/main.rs`](./src/main.rs) + [`src/sinks.rs`](./src/sinks.rs) — observer wiring + the emit loop.

## Validate the schema

```
obs validate examples/sinks-showcase/proto/showcase/v1/events.proto \
  --include examples/sinks-showcase/proto
# → OK · 3 annotated event(s)
```

## Lint the schema

```
obs lint --schemas $(pwd)/examples/sinks-showcase/proto
# → 0 error(s) · 0 warning(s) · 3 event(s) scanned
```

## Doctor the crate setup

```
obs doctor --root examples/sinks-showcase
# → 4 OK · 0 ERROR · 1 INFO  (obs.yaml not present; defaults are fine for the demo)
```

## Run — Parquet only

```
cargo run -p obs-example-sinks-showcase -- --requests 50 --out ./obs-out
```

Stdout (the fallback sink) shows the pretty-formatted record only when no tier-routed sink consumes it; with all three tiers bound to Parquet, only the boot/shutdown chatter appears on the terminal. The Parquet files land here:

```
ls -lh obs-out/parquet/service=obs-example-sinks-showcase/date=*/obs_events-*.parquet
```

## Run — OTLP enabled

```
docker run --rm -p 4317:4317 otel/opentelemetry-collector
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
  cargo run -p obs-example-sinks-showcase -- --requests 50
```

The example logs `OTLP enabled: LOG + METRIC route to OTLP; AUDIT stays in Parquet (analytics row of truth)`. LOG + METRIC events flow to the collector; the AUDIT trail still writes Parquet so analytics + compliance never depend on a live collector.

## Inspect Parquet

The Parquet files follow the unified-schema layout described in [spec 22](../../specs/22-unified-schema.md). When you are ready to query them from an OLAP engine:

```
obs migrate --root examples/sinks-showcase --target arrow > schema.json
obs migrate --root examples/sinks-showcase --target ddl   > ddl.sql
```

`obs query --from parquet://./obs-out/parquet/` will land in a follow-up release.
