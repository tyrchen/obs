//! Configuration types for [`PromRegistry`](crate::PromRegistry).

use std::time::Duration;

/// Runtime configuration for a [`PromRegistry`](crate::PromRegistry).
#[derive(Debug, Clone)]
pub struct PromConfig {
    /// Maximum `(metric_name, label_set)` tuples retained. A series
    /// update past the cap is dropped and one `ObsPromSeriesCapExceeded`
    /// self-event fires per cap breach.
    pub max_series: u32,
    /// Evict series idle longer than this on each `render()` call.
    /// `None` disables eviction — series live until process restart
    /// (matches `prometheus-client`'s default).
    pub idle_ttl: Option<Duration>,
    /// Histogram bucket bounds used when a schema does not declare any.
    /// Defaults to Prometheus' classic buckets.
    pub default_histogram_bounds: Vec<f64>,
}

impl Default for PromConfig {
    fn default() -> Self {
        Self {
            max_series: 10_000,
            idle_ttl: None,
            default_histogram_bounds: vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        }
    }
}

/// Content type the registry should emit.
///
/// - [`RenderFormat::PrometheusText`] — classic `text/plain; version=0.0.4` text format used by
///   every Prometheus scraper.
/// - [`RenderFormat::OpenMetricsText`] — OpenMetrics 1.0 text format (CNCF standard, accepted by
///   modern scrapers via the `application/openmetrics-text` Accept header).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RenderFormat {
    /// Prometheus 0.0.4 text.
    #[default]
    PrometheusText,
    /// OpenMetrics 1.0 text.
    OpenMetricsText,
}

impl RenderFormat {
    /// Standard Prometheus/OpenMetrics content type for an HTTP
    /// `Content-Type` header.
    #[must_use]
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::PrometheusText => "text/plain; version=0.0.4; charset=utf-8",
            Self::OpenMetricsText => "application/openmetrics-text; version=1.0.0; charset=utf-8",
        }
    }
}
