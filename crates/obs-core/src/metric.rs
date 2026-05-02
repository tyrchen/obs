//! `MetricEmitter` — visitor implemented by metric sinks. Spec 12 § 3.6.
//!
//! Generated `EventSchema::project_metrics` calls one method per
//! `MEASUREMENT` field on the event. The trait is `&mut self` so
//! implementations can hold transient state (e.g. an attribute set
//! being assembled across calls).

/// Implemented by metric sinks (OTLP metrics, Prometheus exporters).
/// Generated `EventSchema::project_metrics` calls one method per
/// `MEASUREMENT` field. Spec 12 § 3.6.
pub trait MetricEmitter {
    /// Record a counter increment.
    fn record_counter(&mut self, instrument: &'static str, value: u64, unit: Option<&'static str>);
    /// Record a u64 gauge value.
    fn record_gauge_u64(
        &mut self,
        instrument: &'static str,
        value: u64,
        unit: Option<&'static str>,
    );
    /// Record an f64 gauge value.
    fn record_gauge_f64(
        &mut self,
        instrument: &'static str,
        value: f64,
        unit: Option<&'static str>,
    );
    /// Record a histogram observation against the supplied bounds.
    fn record_histogram(
        &mut self,
        instrument: &'static str,
        value: f64,
        unit: Option<&'static str>,
        bounds: &'static [f64],
    );
    /// Attribute set carried into every `record_*` on the same event.
    fn with_attributes(&mut self, attrs: &[(&'static str, &str)]);
}

/// `MetricEmitter` that drops every record. Used as the default in
/// non-metric sinks.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetricEmitter;

impl MetricEmitter for NoopMetricEmitter {
    fn record_counter(&mut self, _: &'static str, _: u64, _: Option<&'static str>) {}
    fn record_gauge_u64(&mut self, _: &'static str, _: u64, _: Option<&'static str>) {}
    fn record_gauge_f64(&mut self, _: &'static str, _: f64, _: Option<&'static str>) {}
    fn record_histogram(
        &mut self,
        _: &'static str,
        _: f64,
        _: Option<&'static str>,
        _: &'static [f64],
    ) {
    }
    fn with_attributes(&mut self, _: &[(&'static str, &str)]) {}
}
