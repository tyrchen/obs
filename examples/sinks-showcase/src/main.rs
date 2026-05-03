//! Example: a single binary that fans the same `.emit()` calls out to
//! three destinations:
//!
//! 1. **Console** — `StdoutSink(FormatterStyle::Pretty)` as the fallback sink so operators always
//!    see what's flowing.
//! 2. **OTLP** — `OtlpLogSink` (LOG) + `OtlpMetricSink` (METRIC) when `OTEL_EXPORTER_OTLP_ENDPOINT`
//!    (or `--otlp-endpoint`) is set. Quietly skipped otherwise.
//! 3. **Parquet** — `ParquetSink` partitioned by `service` and `date`. Always retains AUDIT (the
//!    row of truth) and also catches LOG + METRIC when no OTLP collector is configured.
//!
//! The point of the example: same typed event types and the same
//! `.emit()` call site, three sink shapes, each tuned to its consumer.
//!
//! Run, Parquet only:
//!   cargo run -p obs-example-sinks-showcase -- --requests 50 --out ./obs-out
//!
//! Run, OTLP enabled:
//!   OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317 \
//!     cargo run -p obs-example-sinks-showcase -- --requests 50

#![allow(missing_docs)]

mod schema;
mod sinks;

use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clap::Parser;
use obs_sdk::observer as resolve_observer;
use tokio::time::sleep;

use crate::{schema::showcase, sinks::install_observer_with_sinks};

#[derive(Debug, Parser)]
#[command(name = "obs-example-sinks-showcase", version)]
struct Cli {
    /// Number of fake requests to generate.
    #[arg(long, default_value_t = 50)]
    requests: u32,
    /// Parquet base directory (relative to CWD).
    #[arg(long, default_value = "./obs-out")]
    out: PathBuf,
    /// Optional OTLP endpoint override; falls back to
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` env var. Skipped when neither is set.
    #[arg(long)]
    otlp_endpoint: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let parquet_dir = cli.out.join("parquet");
    let otlp_url = cli
        .otlp_endpoint
        .clone()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .filter(|s| !s.is_empty());

    install_observer_with_sinks(&parquet_dir, otlp_url)?;

    eprintln!(
        "starting showcase: {requests} requests → parquet={out}",
        requests = cli.requests,
        out = parquet_dir.display(),
    );

    let endpoints = ["/orders", "/checkout", "/profile", "/search"];
    let methods = ["GET", "POST", "PATCH"];
    let clients = ["web", "ios", "android"];

    for i in 0..cli.requests {
        let idx = i as usize;
        let endpoint = endpoints.get(idx % endpoints.len()).copied().unwrap_or("/");
        let method = methods.get(idx % methods.len()).copied().unwrap_or("GET");
        let client = clients.get(idx % clients.len()).copied().unwrap_or("web");
        let status: u32 = if i % 17 == 0 { 503 } else { 200 };
        let latency_ms: u64 = 5 + u64::from(i % 9) * 7;
        let bytes_out: u64 = 256 + u64::from(i % 11) * 64;

        showcase::v1::ObsShowcaseRequest::builder()
            .endpoint(endpoint.to_string())
            .method(method.to_string())
            .client(client.to_string())
            .status_code(status)
            .emit();
        showcase::v1::ObsShowcaseLatency::builder()
            .endpoint(endpoint.to_string())
            .latency_ms(latency_ms)
            .bytes_out(bytes_out)
            .emit();

        // Sample AUDIT events — emit one every 5th request so analytics
        // sees the revenue-affecting trail without dominating volume.
        if i % 5 == 0 {
            let amount_micros: u64 = 1_000_000 + u64::from(i) * 250_000;
            showcase::v1::ObsShowcaseRevenue::builder()
                .currency("USD".to_string())
                .amount_micros(amount_micros)
                .customer_id(format!("cust-{:04}", i % 1000))
                .emit();
        }

        sleep(Duration::from_millis(5)).await;
    }

    eprintln!("flushing observer...");
    let _ = tokio::time::timeout(Duration::from_secs(5), resolve_observer().shutdown()).await;
    eprintln!(
        "done — inspect: ls -lh {}/service=*/date=*/",
        parquet_dir.display()
    );
    Ok(())
}
