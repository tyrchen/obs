//! In-memory series storage.

use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Counter / gauge / histogram kinds exposed through Prometheus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SeriesKind {
    Counter,
    GaugeU64,
    GaugeF64,
    Histogram,
}

impl SeriesKind {
    pub(crate) const fn text_type(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::GaugeU64 | Self::GaugeF64 => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// Fully-qualified series identity: metric name + sorted label pairs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SeriesKey {
    pub name: String,
    /// Sorted (label_name, label_value) pairs so the derived hash is
    /// stable regardless of HashMap iteration order.
    pub labels: Vec<(String, String)>,
}

/// Atomic per-series state.
#[derive(Debug)]
pub(crate) struct SeriesState {
    pub kind: SeriesKind,
    pub unit: Option<&'static str>,
    pub counter: RwLock<u64>,
    pub gauge_u64: RwLock<u64>,
    pub gauge_f64: RwLock<f64>,
    pub histogram: RwLock<HistogramState>,
    pub last_update: RwLock<Instant>,
}

impl SeriesState {
    pub(crate) fn new(kind: SeriesKind, unit: Option<&'static str>) -> Self {
        Self {
            kind,
            unit,
            counter: RwLock::new(0),
            gauge_u64: RwLock::new(0),
            gauge_f64: RwLock::new(0.0),
            histogram: RwLock::new(HistogramState::default()),
            last_update: RwLock::new(Instant::now()),
        }
    }

    pub(crate) fn record_counter(&self, v: u64) {
        {
            let mut c = self.counter.write();
            *c = c.saturating_add(v);
        }
        self.touch();
    }

    pub(crate) fn record_gauge_u64(&self, v: u64) {
        *self.gauge_u64.write() = v;
        self.touch();
    }

    pub(crate) fn record_gauge_f64(&self, v: f64) {
        *self.gauge_f64.write() = v;
        self.touch();
    }

    pub(crate) fn record_histogram(&self, value: f64, bounds: &'static [f64]) {
        let mut h = self.histogram.write();
        h.record(value, bounds);
        drop(h);
        self.touch();
    }

    pub(crate) fn is_idle(&self, ttl: Duration, now: Instant) -> bool {
        now.saturating_duration_since(*self.last_update.read()) > ttl
    }

    fn touch(&self) {
        *self.last_update.write() = Instant::now();
    }
}

/// Classic Prometheus histogram — cumulative bucket counters, sum,
/// total count. Buckets are frozen at the first sample so a series's
/// layout cannot drift mid-stream (Prometheus contract).
#[derive(Debug, Default)]
pub(crate) struct HistogramState {
    pub bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
    pub sum: f64,
    pub count: u64,
}

impl HistogramState {
    fn record(&mut self, value: f64, bounds: &'static [f64]) {
        if self.bounds.is_empty() {
            self.bounds = bounds.to_vec();
            self.bucket_counts = vec![0; self.bounds.len()];
        }
        let upper_bounds = &self.bounds;
        for (i, bound) in upper_bounds.iter().enumerate() {
            if value <= *bound
                && let Some(slot) = self.bucket_counts.get_mut(i)
            {
                *slot = slot.saturating_add(1);
            }
        }
        self.sum += value;
        self.count = self.count.saturating_add(1);
    }
}
