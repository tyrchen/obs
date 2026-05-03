#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

//! OpenTelemetry Protocol (OTLP) sinks for the obs SDK.
//!
//! Phase-3 surface (impl-plan task 3.8): the **mapping** from
//! `ObsEnvelope` to OTLP types is implemented + tested. The transport
//! is pluggable through the [`OtlpExporter`] trait so users can wire a
//! `tonic::Channel`, an HTTP/JSON exporter, or any custom sink. The
//! built-in [`StdoutDebugExporter`] writes JSON to stdout — useful for
//! `apps/server` end-to-end demos and for the sink's unit tests.
//!
//! Spec 20 §§ 2.3, 2.4, 2.5 (OTLP mapping) + § 4 (transport).

mod backpressure;
mod batch;
mod env_config;
#[cfg(feature = "grpc")]
mod grpc;
pub mod logs;
mod mapping;
pub mod metrics;
mod sink;
pub mod traces;

pub use env_config::{OtlpEndpoint, OtlpProtocol, OtlpResourceAttrs, otlp_trio_from_env};
#[cfg(feature = "grpc")]
pub use grpc::GrpcOtlpExporter;
pub use mapping::{LogRecord, MetricPoint, ResourceMessage, SpanRecord};
pub use sink::{
    OtlpExporter, OtlpLogSink, OtlpLogSinkBuilder, OtlpMetricSink, OtlpMetricSinkBuilder,
    OtlpRetry, OtlpTraceSink, OtlpTraceSinkBuilder, StdoutDebugExporter,
};

/// Tests-only mock OTLP collector. Spec 72 § 6 / spec 93 P1-12.
#[cfg(feature = "grpc")]
pub mod test {
    pub use crate::mock_collector::*;
}

#[cfg(feature = "grpc")]
mod mock_collector;

/// Errors returned by `OtlpExporter::export`.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OtlpError {
    /// Underlying transport (HTTP / gRPC / file) failed.
    #[error("transport failure: {0}")]
    Transport(String),
    /// Configuration was incomplete or invalid.
    #[error("configuration error: {0}")]
    Config(String),
    /// Backpressure: the retry queue is full.
    #[error("retry queue full")]
    RetryQueueFull,
}
