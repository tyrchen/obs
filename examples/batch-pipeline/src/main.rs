//! Example: a synthetic ETL pipeline that emits typed obs events to a
//! `ParquetSink` for downstream analytics.
//!
//! Demonstrates:
//! - LOG-tier `ObsBatchProcessed` events (one per batch, fire-and-forget).
//! - METRIC-tier `ObsBatchMeasured` events with measurement fields (`rows`, `bytes_in`,
//!   `latency_ms`) — the analytics row's payload_proto preserves every typed field.
//! - `ParquetSink` writing under `<out_dir>/parquet/`, partitioned by `service` and `date` (and
//!   optionally `hour`).
//! - Stdout fallback so you can watch the pipeline make progress without grepping the Parquet
//!   files.
//!
//! Run:
//!   cargo run -p obs-example-batch-pipeline -- --batches 10 --rows 5000
//!
//! Inspect the produced files:
//!   ls -lh ./obs-out/parquet/service=*/date=*/
//!
//! Once `obs query --from parquet://...` lands (spec 93 P1-9 follow-up)
//! you'll be able to do:
//!   obs query --from parquet://./obs-out/parquet/ --type \
//!     'obs_example_batch_pipeline.v1.ObsBatchMeasured'

#![allow(missing_docs)]

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use clap::Parser;
use obs_parquet::{ParquetLayout, ParquetSink};
use obs_sdk::{
    Event, FormatterStyle, Severity, Sink, StandardObserver, StdoutSink, Tier, install_observer,
    observer as resolve_observer,
};
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(name = "obs-example-batch-pipeline", version)]
struct Cli {
    /// Number of batches to process.
    #[arg(long, default_value_t = 10)]
    batches: u32,
    /// Rows per batch.
    #[arg(long, default_value_t = 5_000)]
    rows: u32,
    /// Output directory (relative to CWD).
    #[arg(long, default_value = "./obs-out")]
    out: PathBuf,
}

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsBatchProcessed {
    /// Pipeline name (low cardinality — ~1 per deployment).
    #[obs(label, cardinality = "low")]
    pipeline: String,
    /// Outcome label so analytics can group by failure mode.
    #[obs(label, cardinality = "low")]
    outcome: String,
    /// Total rows ingested in this batch.
    #[obs(attribute)]
    rows: u32,
    /// Bytes ingested.
    #[obs(attribute)]
    bytes_in: u64,
}

#[derive(Debug, Default, Event)]
#[event(tier = "metric", default_sev = "info")]
struct ObsBatchMeasured {
    /// Pipeline name as a label so metric series are per-pipeline.
    #[obs(label, cardinality = "low")]
    pipeline: String,
    /// Latency for the batch.
    #[obs(measurement)]
    latency_ms: u64,
    /// Bytes ingested as a measurement so analytics can sum / avg.
    #[obs(measurement)]
    bytes_in: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let parquet_dir = cli.out.join("parquet");
    install_observer_with_sinks(&parquet_dir)?;

    eprintln!(
        "starting pipeline: {batches} batches × {rows} rows → {out}",
        batches = cli.batches,
        rows = cli.rows,
        out = parquet_dir.display(),
    );

    for i in 0..cli.batches {
        let started = std::time::Instant::now();
        let bytes_in = process_batch(cli.rows).await;
        let latency_ms: u64 = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let outcome = if i.is_multiple_of(7) {
            // Inject a simulated retry path every 7 batches so analytics
            // sees more than one outcome value.
            "retried"
        } else {
            "ok"
        };
        ObsBatchProcessed::builder()
            .pipeline("orders-etl".to_string())
            .outcome(outcome.to_string())
            .rows(cli.rows)
            .bytes_in(bytes_in)
            .emit_at(if outcome == "retried" {
                Severity::Warn
            } else {
                Severity::Info
            });
        ObsBatchMeasured::builder()
            .pipeline("orders-etl".to_string())
            .latency_ms(latency_ms)
            .bytes_in(bytes_in)
            .emit();
        eprintln!("  batch {i:>3} → outcome={outcome} latency_ms={latency_ms} bytes_in={bytes_in}");
    }

    // Drain the per-tier workers and force the Parquet sink to flush
    // its in-memory batch — without this, ~16 envelopes (LOG + METRIC
    // per batch) sit in the partition buffer until the 256 MiB roll
    // threshold or 5-min age threshold kicks in. Spec 11 § 6.4.
    //
    // We MUST use the async `shutdown()` not `shutdown_blocking()`
    // because we're already inside `#[tokio::main]` — `block_on`-from-
    // inside-runtime panics. (Reported in spec 93 as a P2 ergonomic
    // gap: shutdown_blocking should detect the in-runtime case.)
    eprintln!("flushing observer...");
    let _ = tokio::time::timeout(Duration::from_secs(5), resolve_observer().shutdown()).await;
    sleep(Duration::from_millis(200)).await;
    eprintln!(
        "done. inspect: ls -lh {}/service=*/date=*/",
        parquet_dir.display()
    );
    Ok(())
}

async fn process_batch(rows: u32) -> u64 {
    // Pretend we're reading from upstream; bytes_in scales with rows.
    let per_row_bytes: u64 = 64;
    sleep(Duration::from_millis(20)).await;
    u64::from(rows) * per_row_bytes
}

fn install_observer_with_sinks(parquet_dir: &Path) -> Result<()> {
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Compact));
    let parquet: Arc<dyn Sink> = Arc::new(
        ParquetSink::builder()
            .base_dir(parquet_dir.to_path_buf())
            .layout(ParquetLayout::Single)
            .partition_by(&["service", "date"])
            .default_service("obs-example-batch-pipeline")
            .build()?,
    );

    let observer = StandardObserver::builder()
        .service("obs-example-batch-pipeline", env!("CARGO_PKG_VERSION"))
        .instance("local")
        // METRIC + LOG → Parquet (analytics row); fallback stdout for
        // human visibility while the pipeline runs.
        .sink_for(Tier::Metric, Arc::clone(&parquet))
        .sink_for(Tier::Log, Arc::clone(&parquet))
        .sink_fallback(stdout)
        .build()?;
    install_observer(observer);
    Ok(())
}
