//! HTTP `Accept` header parsing helper.

use crate::config::RenderFormat;

/// Pick a [`RenderFormat`] from the `Accept` header value of a scrape
/// request.
///
/// Modern Prometheus scrapers advertise OpenMetrics when they want
/// it; every other scraper gets the 0.0.4 text format. The check is
/// substring-based to match the Prometheus content-negotiation spec,
/// which pins `application/openmetrics-text` on the OpenMetrics
/// branch.
#[must_use]
pub fn format_from_accept(header: &str) -> RenderFormat {
    if header.contains("application/openmetrics-text") {
        RenderFormat::OpenMetricsText
    } else {
        RenderFormat::PrometheusText
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_openmetrics_header_picks_openmetrics() {
        let fmt =
            format_from_accept("application/openmetrics-text;version=1.0.0;q=0.8,text/plain;q=0.5");
        assert_eq!(fmt, RenderFormat::OpenMetricsText);
    }

    #[test]
    fn test_missing_header_picks_prometheus_text() {
        assert_eq!(format_from_accept(""), RenderFormat::PrometheusText);
    }

    #[test]
    fn test_text_only_picks_prometheus_text() {
        assert_eq!(
            format_from_accept("text/plain;q=0.9,*/*;q=0.1"),
            RenderFormat::PrometheusText
        );
    }
}
