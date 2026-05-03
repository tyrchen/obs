//! Sink wiring for the showcase: console + Parquet always; OTLP when
//! `OTEL_EXPORTER_OTLP_ENDPOINT` (or `--otlp-endpoint`) is set.
//!
//! Tier routing chosen so each consumer sees the shape that fits it:
//!
//! - Console (`StdoutSink::Pretty`) — fallback sink. Catches anything not explicitly routed and
//!   gives the operator human-readable output while the binary runs.
//! - Parquet — receives LOG / METRIC / AUDIT when no OTLP endpoint is configured, AUDIT only when
//!   OTLP is on. Partitioned by `service` and `date` so downstream OLAP can prune by day.
//! - OTLP (LOG + METRIC) — when an endpoint is configured. AUDIT stays in Parquet so the analytics
//!   row of truth is never lost to a flaky collector.
//!
//! `sink_for(Tier, ...)` overrides the prior sink for that tier; the
//! example documents that trade-off in the README rather than
//! introducing a custom multi-sink wrapper (the SDK does not currently
//! expose a tier-level fan-out sink — only `TeeWriter` for byte-level
//! sinks). Spec 11 § 6.

use std::sync::Arc;

use anyhow::{Context, Result};
use obs_otel::{
    GrpcOtlpExporter, OtlpEndpoint, OtlpLogSink, OtlpMetricSink, OtlpProtocol, OtlpResourceAttrs,
};
use obs_parquet::{ParquetLayout, ParquetSink};
use obs_sdk::{FormatterStyle, Sink, StandardObserver, StdoutSink, Tier, install_observer};

/// Service identity used by every sink in the showcase.
pub(crate) const SERVICE: &str = "obs-example-sinks-showcase";

/// Build + install the observer with the three-sink fan-out.
pub(crate) fn install_observer_with_sinks(
    parquet_dir: &std::path::Path,
    otlp_url: Option<String>,
) -> Result<()> {
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));
    let parquet: Arc<dyn Sink> = Arc::new(
        ParquetSink::builder()
            .base_dir(parquet_dir.to_path_buf())
            .layout(ParquetLayout::Single)
            .partition_by(&["service", "date"])
            .default_service(SERVICE)
            .build()
            .context("build ParquetSink")?,
    );

    let mut builder = StandardObserver::builder()
        .service(SERVICE, env!("CARGO_PKG_VERSION"))
        .instance(hostname())
        .sink_for(Tier::Log, Arc::clone(&parquet))
        .sink_for(Tier::Metric, Arc::clone(&parquet))
        .sink_for(Tier::Audit, Arc::clone(&parquet))
        .sink_fallback(stdout);

    if let Some(url) = otlp_url {
        let endpoint = OtlpEndpoint {
            url,
            protocol: OtlpProtocol::Grpc,
            headers: Default::default(),
            compression: String::new(),
            timeout_ms: 5_000,
        };
        let exporter =
            Arc::new(GrpcOtlpExporter::connect(&endpoint).context("connect OTLP gRPC exporter")?);
        let resource = OtlpResourceAttrs {
            service_name: SERVICE.to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            ..Default::default()
        };
        let log_sink: Arc<dyn Sink> = Arc::new(
            OtlpLogSink::builder()
                .exporter(Arc::clone(&exporter) as Arc<dyn obs_otel::OtlpExporter>)
                .resource(resource.clone())
                .endpoint(endpoint.clone())
                .build()
                .context("build OtlpLogSink")?,
        );
        let metric_sink: Arc<dyn Sink> = Arc::new(
            OtlpMetricSink::builder()
                .exporter(exporter as Arc<dyn obs_otel::OtlpExporter>)
                .resource(resource)
                .endpoint(endpoint)
                .build()
                .context("build OtlpMetricSink")?,
        );
        builder = builder
            .sink_for(Tier::Log, log_sink)
            .sink_for(Tier::Metric, metric_sink);
        eprintln!(
            "OTLP enabled: LOG + METRIC route to OTLP; AUDIT stays in Parquet (analytics row of \
             truth)"
        );
    } else {
        eprintln!(
            "OTLP not configured: LOG + METRIC + AUDIT all route to Parquet; stdout is the \
             visibility fallback"
        );
    }

    let observer = builder.build().context("build StandardObserver")?;
    install_observer(observer);
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}
