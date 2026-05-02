//! Field promotion allowlist + cardinality enforcement. Spec 30 § 2.4.

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use obs_proto::obs::v1::ObsEnvelope;
use obs_types::{Cardinality, Severity};
use tracing_core::Level;

/// Allowlist of field names that may be promoted from
/// `tracing::Event` fields onto `env.labels`. Each entry carries the
/// declared cardinality cap — once an HLL-style counter exceeds the
/// cap, the promoter falls back to `payload.attrs` and emits one
/// `ObsLabelCardinalityHigh` self-event.
#[derive(Debug, Default)]
pub struct FieldPromotions {
    entries: DashMap<&'static str, Promotion>,
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
    #[must_use]
    pub fn admit(&self, field: &str, value: &str) -> Option<Cardinality> {
        let entry = self.entries.get(field)?;
        if entry.overflowed.load(Ordering::Relaxed) {
            return None;
        }
        // Best-effort cardinality probe: the bridge's design names
        // an HLL counter; for v1 we use a bounded `samples` Vec
        // because the workspace deps don't yet pull in `hyperloglog`,
        // and the cap is small enough that an exact distinct count
        // (capped at `cap_for(cardinality)`) is fine.
        let cap = cap_for(entry.cardinality);
        let mut samples = entry.samples.lock();
        if !samples.iter().any(|s| s == value) {
            if samples.len() as u64 >= cap {
                entry.overflowed.store(true, Ordering::Relaxed);
                drop(samples);
                emit_cardinality_warning(field, cap);
                return None;
            }
            samples.push(value.to_string());
            entry.distinct.fetch_add(1, Ordering::Relaxed);
        }
        Some(entry.cardinality)
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

fn emit_cardinality_warning(field: &str, cap: u64) {
    let mut env = ObsEnvelope {
        full_name: "obs.runtime.v1.ObsLabelCardinalityHigh".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_WARN),
        ..Default::default()
    };
    env.labels
        .insert("label_key".to_string(), field.to_string());
    env.labels.insert("cap".to_string(), cap.to_string());
    obs_core::observer().emit_envelope(env);
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
