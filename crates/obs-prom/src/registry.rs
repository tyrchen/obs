//! [`PromRegistry`] — series table + cardinality cap + scrape renderer.

use std::{
    fmt,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use dashmap::DashMap;

use crate::{
    PromError,
    config::{PromConfig, RenderFormat},
    render, self_events,
    series::{SeriesKey, SeriesKind, SeriesState},
    sink::PromSink,
};

/// In-memory series table with cardinality cap + idle eviction.
///
/// Cheap to clone via `Arc`. Construct once at startup, hand the
/// `Arc<PromRegistry>` to both the scrape handler and the sink
/// adapter via [`Self::sink`].
pub struct PromRegistry {
    inner: Arc<Inner>,
}

#[derive(Debug)]
pub(crate) struct Inner {
    pub(crate) config: PromConfig,
    /// `'static` view of `config.default_histogram_bounds`. Leaked
    /// once at [`PromRegistry::new`] so the `record_histogram`
    /// `&'static [f64]` contract is satisfied when a schema does not
    /// declare its own `bounds`. Leaking is safe — the registry's
    /// lifetime is the process, and the Vec is at most a handful of
    /// f64s.
    pub(crate) default_bounds: &'static [f64],
    pub(crate) series: DashMap<SeriesKey, Arc<SeriesState>>,
    pub(crate) dropped: AtomicU64,
    pub(crate) evicted_idle: AtomicU64,
    pub(crate) cap_breach_notified: DashMap<String, ()>,
}

impl fmt::Debug for PromRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromRegistry")
            .field("series", &self.inner.series.len())
            .field(
                "series_dropped_over_cap",
                &self.inner.dropped.load(Ordering::Relaxed),
            )
            .field(
                "evicted_idle",
                &self.inner.evicted_idle.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl PromRegistry {
    /// Construct an empty registry with the supplied config.
    #[must_use]
    pub fn new(config: PromConfig) -> Self {
        // Leak the default bounds so histograms projected from
        // schemas without a `bounds = [...]` annotation still see a
        // `&'static [f64]` — matches `MetricEmitter::record_histogram`'s
        // signature. One-shot, bounded allocation.
        let default_bounds: &'static [f64] =
            Box::leak(config.default_histogram_bounds.clone().into_boxed_slice());
        Self {
            inner: Arc::new(Inner {
                config,
                default_bounds,
                series: DashMap::new(),
                dropped: AtomicU64::new(0),
                evicted_idle: AtomicU64::new(0),
                cap_breach_notified: DashMap::new(),
            }),
        }
    }

    /// Build a [`PromSink`] that feeds this registry from the observer.
    /// Mount it on `Tier::Metric` (typically via
    /// [`obs_core::FanOutSink`]) alongside any OTLP metric sink you
    /// also care about.
    #[must_use]
    pub fn sink(&self) -> Arc<PromSink> {
        PromSink::new(Arc::clone(&self.inner))
    }

    /// Current number of distinct series.
    #[must_use]
    pub fn series_count(&self) -> usize {
        self.inner.series.len()
    }

    /// Count of series drops caused by the cardinality cap.
    #[must_use]
    pub fn series_dropped_over_cap(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    /// Render the registry to `out` in the requested format. For
    /// OpenMetrics, the output is terminated with `# EOF` — a
    /// scraper will refuse any trailing bytes, so do **not**
    /// concatenate additional content after this call. Use
    /// [`Self::render_body`] if you need to merge with other
    /// metric producers (e.g. tok's internal `MetricsRegistry`),
    /// then emit the EOF yourself.
    ///
    /// Applies idle eviction (when configured) before rendering so the
    /// cost amortises into the scrape handler — no background thread
    /// required.
    ///
    /// # Errors
    ///
    /// Returns [`PromError::Io`] when writing to `out` fails.
    pub fn render<W: Write>(&self, out: &mut W, format: RenderFormat) -> Result<(), PromError> {
        self.evict_idle();
        render::render(&self.inner, out, format, true)
    }

    /// Render the registry's series *without* a trailing `# EOF`
    /// marker. Use when you're concatenating this registry's output
    /// with another Prometheus-shaped body and will emit the EOF
    /// yourself afterwards.
    ///
    /// # Errors
    ///
    /// Returns [`PromError::Io`] when writing to `out` fails.
    pub fn render_body<W: Write>(
        &self,
        out: &mut W,
        format: RenderFormat,
    ) -> Result<(), PromError> {
        self.evict_idle();
        render::render(&self.inner, out, format, false)
    }

    /// Convenience wrapper over [`Self::render`] returning a `String`.
    #[must_use]
    pub fn render_to_string(&self, format: RenderFormat) -> String {
        let mut buf = Vec::with_capacity(4096);
        // Writing to `Vec<u8>` is infallible; discard the `Result`.
        let _ = self.render(&mut buf, format);
        String::from_utf8(buf).unwrap_or_default()
    }

    /// Convenience wrapper over [`Self::render_body`] returning a `String`.
    #[must_use]
    pub fn render_body_to_string(&self, format: RenderFormat) -> String {
        let mut buf = Vec::with_capacity(4096);
        let _ = self.render_body(&mut buf, format);
        String::from_utf8(buf).unwrap_or_default()
    }

    /// OpenMetrics `# EOF` terminator that [`Self::render_body`]
    /// omits. Callers concatenating multiple producers emit this
    /// once at the very end of the final body.
    #[must_use]
    pub fn eof_marker(format: RenderFormat) -> &'static str {
        match format {
            RenderFormat::OpenMetricsText => "# EOF\n",
            RenderFormat::PrometheusText => "",
        }
    }

    fn evict_idle(&self) {
        let Some(ttl) = self.inner.config.idle_ttl else {
            return;
        };
        let now = Instant::now();
        let stale: Vec<SeriesKey> = self
            .inner
            .series
            .iter()
            .filter(|entry| entry.value().is_idle(ttl, now))
            .map(|entry| entry.key().clone())
            .collect();
        for key in stale {
            if self.inner.series.remove(&key).is_some() {
                self.inner.evicted_idle.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl Inner {
    /// Lookup-or-insert a series, enforcing the cardinality cap on
    /// insertion. `None` when the cap was exceeded and the caller
    /// should drop this update.
    pub(crate) fn get_or_insert(
        self: &Arc<Self>,
        key: SeriesKey,
        kind: SeriesKind,
        unit: Option<&'static str>,
    ) -> Option<Arc<SeriesState>> {
        if let Some(existing) = self.series.get(&key) {
            return Some(Arc::clone(existing.value()));
        }
        if self.series.len() >= self.config.max_series as usize {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            // One self-event per unique metric name per cap breach —
            // avoids tens of thousands of events when a handler leaks
            // user ids into a label.
            if self
                .cap_breach_notified
                .insert(key.name.clone(), ())
                .is_none()
            {
                let sample = render_label_sample(&key);
                self_events::emit_series_cap_exceeded(&key.name, &sample, self.config.max_series);
            }
            let _ = n;
            return None;
        }
        let state = Arc::new(SeriesState::new(kind, unit));
        self.series.entry(key).or_insert_with(|| Arc::clone(&state));
        Some(state)
    }
}

fn render_label_sample(key: &SeriesKey) -> String {
    if key.labels.is_empty() {
        return "{}".to_string();
    }
    let mut out = String::from("{");
    for (i, (k, v)) in key.labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out.push('}');
    out
}
