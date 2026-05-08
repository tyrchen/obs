//! `obs-prom` — Prometheus scrape exporter for obs.
//!
//! Sink-driven (per the Phase 2 design decision D6): the registry is
//! fed by a `Sink` implementation that looks up each envelope's schema
//! in the obs `SchemaRegistry` and dispatches MEASUREMENT fields
//! through `MetricEmitter::record_*`. The same annotations that
//! produce OTLP metrics drive the scrape output — there is no
//! separate instrumentation surface.
//!
//! The crate deliberately does *not* embed an HTTP server. Consumers
//! mount `render()` on whatever routing framework they already have
//! (axum, hyper, pingora, tonic). This matches `prometheus-client`'s
//! stance and keeps the dependency graph narrow.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use obs_core::Sink;
//! use obs_prom::{PromConfig, PromRegistry, RenderFormat};
//!
//! let prom = Arc::new(PromRegistry::new(PromConfig::default()));
//! let prom_sink: Arc<dyn Sink> = prom.sink();
//! // Mount `prom_sink` on Tier::Metric via FanOutSink or InitBuilder.
//! let _ = (prom, prom_sink, RenderFormat::PrometheusText);
//! ```

#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod accept;
mod config;
mod error;
mod registry;
mod render;
pub mod self_events;
mod series;
mod sink;

pub use accept::format_from_accept;
pub use config::{PromConfig, RenderFormat};
pub use error::PromError;
pub use registry::PromRegistry;
pub use sink::PromSink;
