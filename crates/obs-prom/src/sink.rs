//! [`PromSink`] — `Sink` implementation that feeds a [`PromRegistry`](crate::PromRegistry).

use std::sync::Arc;

use obs_core::{MetricEmitter, ScrubbedEnvelope, Sink};

use crate::{
    registry::Inner,
    series::{SeriesKey, SeriesKind, SeriesState},
};

/// Sink wrapper — mount on `Tier::Metric` via
/// [`obs_core::FanOutSink`] alongside the OTLP metric sink.
#[derive(Debug)]
pub struct PromSink {
    inner: Arc<Inner>,
}

impl PromSink {
    pub(crate) fn new(inner: Arc<Inner>) -> Arc<Self> {
        Arc::new(Self { inner })
    }
}

impl Sink for PromSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Collect labels from the envelope once.
        let envelope = env.envelope();
        let mut labels: Vec<(String, String)> = envelope
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        labels.sort();

        let metric_prefix = envelope.full_name.clone();

        let Some(schema) = env.schema() else {
            // No schema → single "emissions" counter keyed *only* by
            // the event name, not the free-form label set. Labels on
            // unregistered events frequently include high-cardinality
            // values (request ids, user ids) that would burn the
            // series cap in seconds. The OTLP metric sink mirrors
            // this by emitting `<full_name>.count = 1`; keep the
            // Prom fallback symmetric but drop labels so the series
            // count is bounded by the schema catalogue size, not the
            // request volume.
            let key = SeriesKey {
                name: counter_metric_name(&format!("{metric_prefix}.count")),
                labels: Vec::new(),
            };
            if let Some(state) = self.inner.get_or_insert(key, SeriesKind::Counter, None) {
                state.record_counter(1);
            }
            return;
        };

        let mut emitter = Emitter {
            inner: Arc::clone(&self.inner),
            prefix: metric_prefix,
            labels,
            extra_attrs: Vec::new(),
        };
        // `project_metrics` returns Err only on a truncated payload.
        // Silently skip — the payload is caller-scrubbed and a future
        // field addition shouldn't break a running scrape handler.
        let _ = schema.project_metrics(env.payload(), &mut emitter);
    }
}

struct Emitter {
    inner: Arc<Inner>,
    prefix: String,
    labels: Vec<(String, String)>,
    extra_attrs: Vec<(String, String)>,
}

impl Emitter {
    fn compose_key(&self, instrument: &'static str, kind: SeriesKind) -> SeriesKey {
        let raw = format!("{}.{}", self.prefix, instrument);
        let name = match kind {
            SeriesKind::Counter => counter_metric_name(&raw),
            _ => sanitize_metric_name(&raw),
        };
        let mut labels = self.labels.clone();
        for (k, v) in &self.extra_attrs {
            labels.push((k.clone(), v.clone()));
        }
        labels.sort();
        // Deduplicate — last-writer-wins on duplicate keys.
        labels.dedup_by(|a, b| a.0 == b.0);
        SeriesKey { name, labels }
    }

    fn insert(
        &self,
        instrument: &'static str,
        kind: SeriesKind,
        unit: Option<&'static str>,
    ) -> Option<Arc<SeriesState>> {
        self.inner
            .get_or_insert(self.compose_key(instrument, kind), kind, unit)
    }
}

impl MetricEmitter for Emitter {
    fn record_counter(&mut self, instrument: &'static str, value: u64, unit: Option<&'static str>) {
        if let Some(state) = self.insert(instrument, SeriesKind::Counter, unit) {
            state.record_counter(value);
        }
    }

    fn record_gauge_u64(
        &mut self,
        instrument: &'static str,
        value: u64,
        unit: Option<&'static str>,
    ) {
        if let Some(state) = self.insert(instrument, SeriesKind::GaugeU64, unit) {
            state.record_gauge_u64(value);
        }
    }

    fn record_gauge_f64(
        &mut self,
        instrument: &'static str,
        value: f64,
        unit: Option<&'static str>,
    ) {
        if let Some(state) = self.insert(instrument, SeriesKind::GaugeF64, unit) {
            state.record_gauge_f64(value);
        }
    }

    fn record_histogram(
        &mut self,
        instrument: &'static str,
        value: f64,
        unit: Option<&'static str>,
        bounds: &'static [f64],
    ) {
        // Per-schema bounds win; fall back to the registry's
        // configured default (leaked once at construction). The
        // `default_bounds()` free function is retained only as a
        // last-resort safety net.
        let buckets: &'static [f64] = if !bounds.is_empty() {
            bounds
        } else if !self.inner.default_bounds.is_empty() {
            self.inner.default_bounds
        } else {
            default_bounds()
        };
        if let Some(state) = self.insert(instrument, SeriesKind::Histogram, unit) {
            state.record_histogram(value, buckets);
        }
    }

    fn with_attributes(&mut self, attrs: &[(&'static str, &str)]) {
        for (k, v) in attrs {
            self.extra_attrs.push(((*k).to_string(), (*v).to_string()));
        }
    }
}

/// Prometheus/OpenMetrics counters require the `_total` suffix on
/// exported sample names. When the schema-declared instrument name
/// already carries an idiomatic terminator (`_total`, `.count`,
/// `.total`), we map it to `_total`; otherwise we append.
fn counter_metric_name(raw: &str) -> String {
    let base = sanitize_metric_name(raw);
    if base.ends_with("_total") {
        return base;
    }
    // Strip common aliases so we don't end up with `_count_total`.
    let stripped: &str = base
        .strip_suffix("_count")
        .or_else(|| base.strip_suffix("_counter"))
        .unwrap_or(&base);
    format!("{stripped}_total")
}

/// Prometheus metric names allow `[a-zA-Z_:][a-zA-Z0-9_:]*`. obs
/// envelopes name things with dots (`myapp.v1.Foo.count`), so swap
/// those for underscores on the way out. Unknown bytes become `_`.
fn sanitize_metric_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for (i, ch) in raw.char_indices() {
        let valid_start = ch.is_ascii_alphabetic() || ch == '_' || ch == ':';
        let valid_rest = valid_start || ch.is_ascii_digit();
        if i == 0 {
            if valid_start {
                out.push(ch);
            } else {
                out.push('_');
            }
        } else if valid_rest {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

fn default_bounds() -> &'static [f64] {
    // Prometheus classic buckets — matches `PromConfig::default()`.
    // Pinned as a `&'static` slice so the `SeriesState::record_histogram`
    // signature (`&'static [f64]`) is satisfied when the envelope
    // schema didn't declare any bounds.
    &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_metric_name_replaces_dots_and_hyphens() {
        assert_eq!(
            sanitize_metric_name("myapp.v1.Foo-bar.count"),
            "myapp_v1_Foo_bar_count"
        );
    }

    #[test]
    fn test_sanitize_metric_name_leading_digit_is_underscored() {
        assert_eq!(sanitize_metric_name("9xyz"), "_xyz");
    }

    #[test]
    fn test_counter_metric_name_appends_total() {
        assert_eq!(counter_metric_name("foo"), "foo_total");
        assert_eq!(counter_metric_name("foo_total"), "foo_total");
        assert_eq!(counter_metric_name("foo.count"), "foo_total");
        assert_eq!(counter_metric_name("foo_counter"), "foo_total");
    }
}
