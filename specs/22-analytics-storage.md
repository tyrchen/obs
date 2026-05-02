# Design — Analytics Storage Model

Status: draft v3 · Owner: obs-core · Last updated: 2026-05-02 · Depends on: [10-data-model.md](./10-data-model.md), [11-runtime-core.md](./11-runtime-core.md), [12-schema-and-codegen.md](./12-schema-and-codegen.md), [20-otel-and-sinks.md](./20-otel-and-sinks.md)

This spec defines the analytical storage shape — one wide table per
service that holds every event type as a sparse struct column — and
the two analytical sinks that target it: `ParquetSink` and
`ClickHouseSink`. Per-event tables remain available as opt-in.

> v3 changes: split out from the v2 monolithic architecture spec;
> `ResourceAttrs` are now also written into rows (previously OTLP-only
> identity); spelling out file path conventions; pulled in the
> Iceberg/Delta positioning text.

## 1. Single sparse table

The default analytical store is **one sparse columnar table** that
contains every event type, not one table per type.

### 1.1 Table shape

| Column group | Columns | Notes |
| --- | --- | --- |
| Envelope | `ts_ns`, `full_name`, `schema_hash`, `tier`, `sev`, `trace_id`, `span_id`, `parent_span_id`, `service`, `instance`, `version`, `sampling_reason`, `callsite_id` | One row per event |
| Resource | `service_namespace`, `deployment_environment`, `host_name`, `host_arch`, `attrs: map<string, string>` | Read from `ResourceAttrs` so non-OTLP rows have the same identity OTLP carries |
| Common labels | `labels: map<string, string>` | All label fields, key by name |
| Per-event payload | `payload_<full_name_snake>: Struct<…>` | Sparse: only populated when `full_name` matches |
| Raw fallback | `payload_proto: bytes` | Original buffa-encoded bytes for unknown schemas |

Example (DDL elided):

```
ts_ns                | full_name                        | sev   | labels                                       | payload_myapp_v1_obs_request_completed                       | payload_myapp_v1_obs_user_signed_up
─────────────────────┼──────────────────────────────────┼───────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────┼─────────────────────────────────────
1746150225123456000  | myapp.v1.ObsRequestCompleted     | INFO  | {route:list_users, status:ok, tenant:acme}   | {user_id: u-42, latency_ms: 48, bytes_out: 2048}             | NULL
1746150225789012000  | myapp.v1.ObsUserSignedUp         | INFO  | {channel:web, country:US}                    | NULL                                                         | {user_id: u-99, plan: pro}
```

Per-event payload columns are nullable structs; sparse columnar
storage (Parquet, ClickHouse) compresses the unused ones to ~1 byte
per row.

### 1.2 Why one table, not one-table-per-event

| Concern | Per-event tables | Single sparse table |
| --- | --- | --- |
| Cross-event time joins (`SELECT * WHERE trace_id = X`) | union N tables | one query |
| Adding a new event type | new table + new ingest pipeline + new schema migration | append a struct column; old rows have NULL |
| Schema evolution | per-table migration on every add | additive; existing rows unchanged |
| File count in object storage | `O(events × hours)` | `O(hours)` |
| Catalog clutter | tens to hundreds of tables | one table per service |
| Query "give me everything in the last 5 min" | union all tables | trivial |
| Compression of label keys | repeated per file | dictionary-encoded once |
| Operational analogue | Mixpanel/Amplitude with named tables | Honeycomb / Snowplow Atomic / Segment unified events table |

The single-table model is the original wide-events shape and is what
Honeycomb, Snowplow, and Segment converge on. It optimises for the
read patterns wide-events were designed to enable: ad-hoc OLAP across
the entire event stream.

Per-event tables remain available as an opt-in for very-high-volume
single-event-type workloads where partition pruning by `full_name` is
not enough. This is configured per-sink, not per-event:

```rust
ParquetSink::builder()
    .layout(ParquetLayout::Single)               // default
    // .layout(ParquetLayout::TablePerEvent)     // opt-in for high-volume splits
    .build()?
```

### 1.3 Analytics is a view, not a tier

Analytics is not a separate signal type — it is what falls out of the
single-table model. Every wide event is implicitly an "analytics event"
because:

- `full_name` is the event name (Mixpanel `event` column equivalent)
- `labels` are the dimensions
- numeric `MEASUREMENT` fields are the metrics
- `trace_id` is the session/journey key
- `ts_ns` is the timeline

A funnel query is `SELECT full_name, count(*) FROM obs_events WHERE
trace_id IN (…) GROUP BY full_name`; a cohort query is `WHERE
full_name = 'myapp.v1.ObsUserSignedUp' AND labels['country'] = 'US'`;
a retention query joins the table to itself on `labels['user_id']`.
There is no analytics-tier sink — `ParquetSink` and `ClickHouseSink`
*are* the analytics sinks.

## 2. `ParquetSink`

Writes batches as Parquet files using a **single Arrow schema** that
contains all event types as sparse struct columns.

```rust
pub struct ParquetSink { /* … */ }

impl ParquetSink {
    pub fn builder() -> ParquetSinkBuilder;
}

pub enum ParquetLayout {
    /// Single sparse table; all events written to obs_events.parquet
    /// with per-event-type struct columns. Default.
    Single,
    /// One file per event type. Opt-in for very-high-volume splits.
    TablePerEvent,
}

pub struct ParquetSinkBuilder {
    pub fn base_dir(self, dir: impl Into<PathBuf>) -> Self;
    pub fn layout(self, l: ParquetLayout) -> Self;          // default Single
    pub fn roll_max_bytes(self, n: u64) -> Self;
    pub fn roll_max_age(self, d: Duration) -> Self;
    pub fn compression(self, c: ParquetCompression) -> Self;
    pub fn partition_by(self, fields: &[&str]) -> Self;     // e.g. ["service", "date"]
    pub fn build(self) -> Result<ParquetSink>;
}
```

File path on disk:

```
base_dir/service=my-api/date=2026-05-02/hour=14/obs_events-{batch_id}.parquet
```

Schema discovery is automatic — the sink reads the `EventSchema`
registry populated at observer init and combines all per-event Arrow
field fragments into one table schema. The Arrow fragments are
generated from `.proto` annotations by `obs-build`; see
[12-schema-and-codegen.md § 3.6](./12-schema-and-codegen.md#36-the-single-table-arrow-schema).

### 2.0a Crash and partial-write semantics

Parquet places its footer at the end of the file. A process killed
mid-batch leaves a footer-less `.parquet` file that **most readers
refuse to open** (DuckDB, Trino, polars all error). We treat this
explicitly:

- **Atomic rename**: every batch is written to
  `obs_events-{batch_id}.parquet.tmp`. On clean close, the file is
  renamed to `obs_events-{batch_id}.parquet` (POSIX atomic on the
  same filesystem; `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING`
  on Windows). Readers only ever see fully-written files.
- **Crash recovery**: at observer init, the `ParquetSink` scans
  `base_dir` for any `*.parquet.tmp` and **deletes them**. They
  are partial by definition; their data is lost. The runtime emits
  one `obs.runtime.v1.ObsAnalyticsPartialDropped{file, bytes}`
  per file removed.
- **Durability bound**: the data loss window equals the un-flushed
  in-memory batch (default 256 MiB or 5 min). Operators who need
  tighter bounds set `roll_max_bytes` and `roll_max_age` lower; the
  cost is more files in object storage.
- **Object stores**: S3 / GCS / Azure Blob Storage are
  natively-atomic on write (the upload is single-shot or multipart-
  with-commit). The `*.tmp` rename pattern collapses to "do not
  PUT until the buffer is closed" via `opendal::Operator::write`'s
  `commit_on_close` semantics. No cleanup pass needed.

### 2.1 Iceberg / Delta Lake position

`obs-parquet` emits plain Parquet files into a directory layout
designed to be a valid Iceberg/Delta-compatible warehouse table when
combined with downstream catalog metadata. We do not write Iceberg
manifests directly in v1; users wire `nessie`, `polaris`, or AWS Glue
on top. The Arrow schema is stable (additive only; see
[12-schema-and-codegen.md § 5](./12-schema-and-codegen.md#5-schema-evolution--versioning)),
so an external catalog is safe.

## 3. `ClickHouseSink`

Native ClickHouse insertion into a single `obs_events` table per
service.

```rust
pub struct ClickHouseSink { /* … */ }

impl ClickHouseSink {
    pub fn builder() -> ClickHouseSinkBuilder;
}

pub struct ClickHouseSinkBuilder {
    pub fn url(self, url: impl Into<String>) -> Self;
    pub fn database(self, db: impl Into<String>) -> Self;
    pub fn table(self, name: impl Into<String>) -> Self;     // default "obs_events"
    pub fn auto_migrate(self, on: bool) -> Self;             // default false (CI step instead)
    pub fn batch_size(self, n: usize) -> Self;
    pub fn build(self) -> Result<ClickHouseSink>;
}
```

The CLI `obs migrate clickhouse` (see [50-cli.md](./50-cli.md))
emits the DDL for the schemas at build time so production DBs are
migrated by an explicit step, not at runtime.

DDL strategy (one table):

```sql
CREATE TABLE obs_events (
    ts_ns                            DateTime64(9),
    full_name                        LowCardinality(String),
    schema_hash                      UInt64,
    sev                              LowCardinality(String),
    trace_id                         String,
    span_id                          String,
    parent_span_id                   String,
    callsite_id                      UInt64,
    service                          LowCardinality(String),
    instance                         LowCardinality(String),
    version                          LowCardinality(String),
    service_namespace                LowCardinality(String),
    deployment_environment           LowCardinality(String),
    host_name                        LowCardinality(String),
    host_arch                        LowCardinality(String),
    sampling_reason                  LowCardinality(String),
    labels                           Map(LowCardinality(String), String),
    attrs                            Map(LowCardinality(String), String),
    payload_myapp_v1_obs_request_completed  Nested( route LowCardinality(String),
                                                    status LowCardinality(String),
                                                    /* ... */ ),
    payload_myapp_v1_obs_user_signed_up     Nested( /* ... */ ),
    payload_proto                    String CODEC(ZSTD)
)
ENGINE = MergeTree
PARTITION BY toDate(ts_ns)
ORDER BY (ts_ns, full_name, trace_id);
```

Cardinality dictionary degrades if labels admit very many distinct
values at runtime. Operators monitor
`obs.runtime.v1.ObsLabelCardinalityHigh` — see
[11-runtime-core.md § 6.7](./11-runtime-core.md#67-clickhouse-runtime-cardinality).

### 3.1 ClickHouse durability semantics

ClickHouse accepts batched `INSERT` over its native protocol; each
batch is committed atomically server-side. The `ClickHouseSink`:

- **Buffers in memory** up to `batch_size` (default 8192) or
  `flush_interval` (default 1 s), whichever first.
- On flush, sends one INSERT; on success the batch is durable
  server-side.
- On failure (network or 5xx), retries with exponential backoff
  (1 s → 30 s, 5 attempts). After max attempts, increments
  `obs.runtime.v1.ObsSinkFailed{sink=clickhouse}` (catalogued in
  [11-runtime-core.md § 10](./11-runtime-core.md#10-self-events))
  and drops the batch.
- **Crash bound**: in-memory batches not yet flushed at the time
  of process kill are lost. The bound equals `batch_size` envelopes
  or `flush_interval` worth, whichever the user configured.

Operators who need stronger guarantees pair `ClickHouseSink` with
`ParquetSink` to the same `obs_events` table shape and reconcile
later — the codegen Arrow schema is identical so this is mechanical.
ClickHouse's `MergeTree` engine deduplicates on `(ts_ns, full_name,
trace_id)` if the same row is replayed.

## 4. Schema evolution rules — analytics view

`obs schema diff` (CLI, see [50-cli.md § 3.6](./50-cli.md#36-obs-diff))
enforces the ruleset across the analytical schema too. The
data-model spec ([10-data-model.md § 6](./10-data-model.md#6-envelope))
guarantees the envelope is stable; the per-event Arrow fragments are
additive-only (new event types append a struct column; old rows have
NULL).

Field-level rule changes that affect analytics:

| Change | Effect on analytics layout |
| --- | --- |
| Add new field on existing event | Append column to that event's struct; old rows have NULL |
| Reuse a deleted field tag | Banned — would corrupt historical NULL semantics |
| Change field type | Banned — Parquet/ClickHouse cannot reinterpret |
| Promote classification (PII) | Existing rows untouched; redactor strips on new writes |

Each `obs migrate clickhouse` run produces both the initial DDL and
the diff against a baseline ref; CI gates on the diff's
breaking-change count.

## 5. Build dependencies

| Depends on | Provides |
| --- | --- |
| [10-data-model.md](./10-data-model.md) | Envelope columns |
| [11-runtime-core.md](./11-runtime-core.md) | `Sink` trait, `ResourceAttrs` |
| [12-schema-and-codegen.md](./12-schema-and-codegen.md) | Per-event Arrow fragments, payload struct columns |
| [20-otel-and-sinks.md](./20-otel-and-sinks.md) | `MakeWriter`, sink router |

Sinks ship in `obs-parquet` and `obs-clickhouse`. Both are opt-in
features on `obs-sdk`. See [61-crates-and-features.md § 2.7 / § 2.8](./61-crates-and-features.md).
