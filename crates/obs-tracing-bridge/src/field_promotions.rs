//! Field promotion allowlist + cardinality enforcement. Spec 30 § 2.4.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use obs_types::{Cardinality, Severity};
use tracing_core::Level;

/// Allowlist of field names that may be promoted from
/// `tracing::Event` fields onto `env.labels`. Each entry carries the
/// declared cardinality cap — once an HLL-style counter exceeds the
/// cap, the promoter falls back to `payload.attrs` and emits one
/// `ObsLabelCardinalityHigh` self-event per `(target, field)` pair.
/// Spec 30 § 2.4 / spec 94 § 2.6.
#[derive(Debug, Default)]
pub struct FieldPromotions {
    entries: DashMap<&'static str, Promotion>,
    /// Per-(target, field) emit-suppression flag. The first overflow
    /// for a given (target, field) emits an `ObsLabelCardinalityHigh`
    /// self-event; subsequent overflows for the same pair are
    /// suppressed so the channel does not amplify.
    emitted_for: DashMap<(String, &'static str), ()>,
}

#[derive(Debug)]
struct Promotion {
    cardinality: Cardinality,
    distinct: AtomicU64,
    overflowed: std::sync::atomic::AtomicBool,
    samples: parking_lot::Mutex<Vec<String>>,
}

impl FieldPromotions {
    /// Empty allowlist.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one promotion.
    #[must_use]
    pub fn promote(self, field: &'static str, cardinality: Cardinality) -> Self {
        self.entries.insert(
            field,
            Promotion {
                cardinality,
                distinct: AtomicU64::new(0),
                overflowed: std::sync::atomic::AtomicBool::new(false),
                samples: parking_lot::Mutex::new(Vec::with_capacity(64)),
            },
        );
        self
    }

    /// Look up a field. Returns `Some(cardinality)` when the field is
    /// allowlisted and below its cap; `None` when not allowlisted or
    /// the cap has been exceeded.
    ///
    /// Equivalent to `admit_with_target(field, value, "")` — used by
    /// callers that don't have a target to qualify the suppression
    /// with.
    #[must_use]
    pub fn admit(&self, field: &str, value: &str) -> Option<Cardinality> {
        self.admit_with_target("", field, value)
    }

    /// Look up a field, emitting `ObsLabelCardinalityHigh` at most
    /// once per `(target, field)` pair when the cap is first crossed.
    /// Spec 94 § 2.6 / P1-D.
    #[must_use]
    pub fn admit_with_target(&self, target: &str, field: &str, value: &str) -> Option<Cardinality> {
        let entry = self.entries.get(field)?;
        if entry.overflowed.load(Ordering::Relaxed) {
            return None;
        }
        let cap = cap_for(entry.cardinality);
        let mut samples = entry.samples.lock();
        if !samples.iter().any(|s| s == value) {
            if samples.len() as u64 >= cap {
                entry.overflowed.store(true, Ordering::Relaxed);
                let distinct = entry.distinct.load(Ordering::Relaxed);
                drop(samples);
                self.emit_overflow_once(target, entry.key(), distinct);
                return None;
            }
            samples.push(value.to_string());
            entry.distinct.fetch_add(1, Ordering::Relaxed);
        }
        Some(entry.cardinality)
    }

    fn emit_overflow_once(&self, target: &str, field: &'static str, estimated_distinct: u64) {
        let key = (target.to_string(), field);
        // Insert returns Some(_) when the key already existed; only
        // the first thread to observe the overflow emits.
        if self.emitted_for.insert(key.clone(), ()).is_some() {
            return;
        }
        obs_core::self_events_public::emit_label_cardinality_high(
            target,
            field,
            estimated_distinct,
        );
    }

    /// Test helper: total distinct values seen for `field`.
    #[must_use]
    pub fn distinct_count(&self, field: &str) -> Option<u64> {
        self.entries
            .get(field)
            .map(|p| p.distinct.load(Ordering::Relaxed))
    }
}

fn cap_for(c: Cardinality) -> u64 {
    match c {
        Cardinality::Low => 32,
        Cardinality::Medium => 1024,
        Cardinality::High | Cardinality::Unbounded => u64::MAX,
        _ => 32,
    }
}

/// Convert a `tracing::Level` to `obs::Severity`. Spec 30 § 2.2.1.
#[must_use]
pub fn level_to_severity(level: Level) -> Severity {
    match level {
        Level::TRACE => Severity::Trace,
        Level::DEBUG => Severity::Debug,
        Level::INFO => Severity::Info,
        Level::WARN => Severity::Warn,
        Level::ERROR => Severity::Error,
    }
}
