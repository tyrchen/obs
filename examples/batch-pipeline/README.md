# obs-example-batch-pipeline

An ETL-style pipeline that demonstrates the **analytics-focused** obs
SDK surface end-to-end:

- LOG-tier `ObsBatchProcessed` events (one per batch, with
  `pipeline` / `outcome` labels and `rows` / `bytes_in` attributes).
- METRIC-tier `ObsBatchMeasured` events with measurement fields
  (`latency_ms`, `bytes_in`) so a downstream OLAP query can sum or
  histogram the per-batch numbers.
- `ParquetSink` writing under `./obs-out/parquet/` partitioned by
  `service` and `date`. Files land as `obs_events-{batch_id}.parquet`
  with the spec 22 § 1.1 sparse single-table schema.
- `StdoutSink` fallback so the pipeline's progress shows up in the
  console while it runs.

## Run

```bash
# default: 10 batches × 5,000 rows each
cargo run -p obs-example-batch-pipeline

# bigger run:
cargo run -p obs-example-batch-pipeline -- --batches 50 --rows 50000

# write to a different output dir:
cargo run -p obs-example-batch-pipeline -- --out /tmp/obs-pipeline
```

## Inspect the produced files

```bash
ls -lh ./obs-out/parquet/service=*/date=*/
```

Each Parquet file is a sparse `obs_events` table with the envelope
columns plus `payload_proto` (the buffa-encoded typed payload). Use
any Parquet reader to inspect — Apache Arrow, DuckDB, ClickHouse:

```bash
# DuckDB:
duckdb -c "SELECT full_name, COUNT(*) FROM './obs-out/parquet/**/*.parquet' GROUP BY 1"

# typical output:
# obs_example_batch_pipeline.v1.ObsBatchProcessed  10
# obs_example_batch_pipeline.v1.ObsBatchMeasured   10
```

Once the spec 93 P1-9 follow-up lands, `obs query --from
parquet://./obs-out/parquet/` will provide the same view through the
SDK CLI.

## What to look for

In the stdout output:

- `outcome=ok` for most batches at INFO severity.
- `outcome=retried` every 7th batch escalated to WARN (so a tail-on-
  error sampler / alert pipeline picks it up).
- Both the LOG `ObsBatchProcessed` and the METRIC `ObsBatchMeasured`
  envelopes land in the same Parquet table — the analytics consumer
  can `WHERE tier = 'metric'` to extract just the measurements.

## What this demonstrates

Mapping back to the patterns in `60-developer-experience.md`:

- **§ 4.3.G — analytics sink**: `ParquetSink` writes the unified
  `obs_events` table sparse-NULL'd per `payload_<full_name>` Struct
  column (spec 22 § 1.1).
- **§ 4.3.E — conditional severity**: 1-in-7 batch escalates to WARN.
- **Resource columns** (spec 95 § 3.3 / P1-AE): rows carry
  `service_namespace` / `environment` / `host_name` / `host_arch`
  populated from the observer's `ResourceAttrs`.

## Out of scope

- Realtime trace export → `examples/http-service/`
- Multi-tenant per-task observer → `examples/multi-tenant/`
- `obs::forensic!` budget + SpanTrace → `examples/forensic-and-spantrace/`
