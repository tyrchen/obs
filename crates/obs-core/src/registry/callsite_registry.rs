//! `ObsCallsiteRegistry` — process-local map from `callsite_id` to the
//! human metadata (target, file, line, level, field names, template).
//!
//! Spec 31. The registry is owned by `StandardObserver`; the bridge
//! (Direction A) inserts on first sight and emits one
//! `obs.runtime.v1.ObsCallsiteRegistered` envelope, and the bridge
//! (Direction B / `ObsToTracingSink`) reads it to reconstitute
//! `tracing::Metadata` for envelopes whose `env.callsite_id != 0`.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use obs_proto::obs::v1::Severity;

/// Callsite source — drives both human display and the BLAKE3 input.
/// Spec 31 § 3.4 enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum CallsiteSource {
    /// Tracing event (Direction A bridge).
    TracingEvent = 1,
    /// Tracing span (Direction A bridge, `ObsSpanCompleted`).
    TracingSpan = 2,
    /// `obs::forensic!` macro.
    Forensic = 3,
    /// `#[obs::instrument]`-emitted `ObsFnEntered`/`ObsFnExited`.
    Instrument = 4,
}

impl CallsiteSource {
    /// Human-readable identifier; matches the CLI render in spec 31 § 5.4.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::TracingEvent => "TRACING_EVENT",
            Self::TracingSpan => "TRACING_SPAN",
            Self::Forensic => "FORENSIC",
            Self::Instrument => "INSTRUMENT",
        }
    }
}

/// Stable record kept per `callsite_id`. Spec 31 § 3.2.
///
/// Records live behind `Arc<CallsiteRecord>` in the registry; we never
/// `Clone` one whole, so omitting `Clone` lets the per-record
/// `AtomicU64` event counter live in-line without indirection.
#[derive(Debug)]
pub struct CallsiteRecord {
    /// 64-bit BLAKE3-derived stable id.
    pub id: u64,
    /// Source vocabulary.
    pub source: CallsiteSource,
    /// Tracing/forensic target (`sqlx::query`, `myapp::auth`, …).
    pub target: String,
    /// Display name (`event src/foo.rs:42`, span name, function name).
    pub name: String,
    /// Module path or empty.
    pub module_path: String,
    /// Source file path or empty.
    pub file: String,
    /// Source line; `None` when unavailable.
    pub line: Option<NonZeroU32>,
    /// Severity / level.
    pub sev: Severity,
    /// Field names in stable order.
    pub field_names: Vec<String>,
    /// Optional rendered template; empty for non-templated paths.
    pub template: String,
    /// Wall-clock ns at registration time (used for re-emit cadence).
    pub registered_ns: u64,
    /// Approximate count of envelopes that referenced this callsite
    /// since the last refresh. Reset by re-emit cadence.
    pub events_since_refresh: AtomicU64,
}

impl CallsiteRecord {
    /// Reset the per-cadence event counter.
    pub fn reset_count(&self) {
        self.events_since_refresh.store(0, Ordering::Relaxed);
    }

    /// Increment the event count and return the new value.
    pub fn observe(&self) -> u64 {
        self.events_since_refresh.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Process-local callsite registry. Spec 31 § 3.2.
///
/// Concurrent access is allowed: the bridge writes once per first-sight
/// callsite and reads once per envelope. `DashMap` matches CLAUDE.md
/// guidance on concurrent maps.
#[derive(Default)]
pub struct ObsCallsiteRegistry {
    by_id: DashMap<u64, Arc<CallsiteRecord>>,
}

impl std::fmt::Debug for ObsCallsiteRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObsCallsiteRegistry")
            .field("len", &self.by_id.len())
            .finish_non_exhaustive()
    }
}

impl ObsCallsiteRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of registered callsites.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// True if no callsites are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Insert `record` if absent. Returns `(record, was_new)` where
    /// `was_new == true` only when this call inserted the record.
    /// Spec 31 § 3.3.
    pub fn insert_if_absent(&self, record: CallsiteRecord) -> (Arc<CallsiteRecord>, bool) {
        let id = record.id;
        if let Some(existing) = self.by_id.get(&id) {
            return (Arc::clone(existing.value()), false);
        }
        let arc = Arc::new(record);
        match self.by_id.entry(id) {
            dashmap::Entry::Occupied(slot) => (Arc::clone(slot.get()), false),
            dashmap::Entry::Vacant(slot) => {
                slot.insert(Arc::clone(&arc));
                (arc, true)
            }
        }
    }

    /// Look up a record by id.
    #[must_use]
    pub fn get(&self, id: u64) -> Option<Arc<CallsiteRecord>> {
        self.by_id.get(&id).map(|r| Arc::clone(r.value()))
    }

    /// Iterate records (snapshot — drops `Ref`s before returning).
    #[must_use]
    pub fn snapshot(&self) -> Vec<Arc<CallsiteRecord>> {
        self.by_id.iter().map(|r| Arc::clone(r.value())).collect()
    }
}

/// Compute a stable 64-bit callsite id from the canonical inputs.
/// Spec 31 § 3.1 — the perturb-to-non-zero path is preserved.
#[must_use]
pub fn callsite_id(
    source: CallsiteSource,
    target: &str,
    file: &str,
    line: Option<u32>,
    level: Severity,
    field_names: &[&str],
    template: &str,
) -> u64 {
    let mut h = blake3::Hasher::new();
    h.update(&[source as u8]);
    h.update(target.as_bytes());
    h.update(file.as_bytes());
    h.update(&line.unwrap_or(0).to_le_bytes());
    h.update(&[severity_byte(level)]);
    for name in field_names {
        h.update(name.as_bytes());
        h.update(b"\x00");
    }
    h.update(template.as_bytes());
    let bytes = h.finalize();
    let raw = bytes.as_bytes();
    let head: [u8; 8] = raw.first_chunk::<8>().copied().unwrap_or([0; 8]);
    let id = u64::from_le_bytes(head);
    if id != 0 { id } else { perturb_to_nonzero(raw) }
}

const fn severity_byte(s: Severity) -> u8 {
    match s {
        Severity::Trace => 1,
        Severity::Debug => 2,
        Severity::Info => 3,
        Severity::Warn => 4,
        Severity::Error => 5,
        Severity::Fatal => 6,
        _ => 0,
    }
}

/// Force a non-zero 64-bit id from a 32-byte BLAKE3 output. Reserved
/// for the `head[0..8] == 0` corner case (probability 2⁻⁶⁴). Spec
/// 31 § 3.1.
#[must_use]
pub fn perturb_to_nonzero(blake_bytes: &[u8]) -> u64 {
    let head2: [u8; 8] = blake_bytes
        .get(8..16)
        .and_then(|s| <[u8; 8]>::try_from(s).ok())
        .unwrap_or([0; 8]);
    u64::from_le_bytes(head2) | 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_callsite_id_should_be_deterministic() {
        let a = callsite_id(
            CallsiteSource::TracingEvent,
            "sqlx::query",
            "src/q.rs",
            Some(42),
            Severity::Info,
            &["rows", "elapsed"],
            "executed query",
        );
        let b = callsite_id(
            CallsiteSource::TracingEvent,
            "sqlx::query",
            "src/q.rs",
            Some(42),
            Severity::Info,
            &["rows", "elapsed"],
            "executed query",
        );
        assert_eq!(a, b);
    }

    #[test]
    fn test_callsite_id_should_never_be_zero_for_real_input() {
        let id = callsite_id(
            CallsiteSource::Forensic,
            "site",
            "",
            None,
            Severity::Info,
            &[],
            "",
        );
        assert_ne!(id, 0);
    }

    #[test]
    fn test_registry_should_dedup_inserts() {
        let reg = ObsCallsiteRegistry::new();
        let rec = CallsiteRecord {
            id: 1,
            source: CallsiteSource::Forensic,
            target: "t".into(),
            name: "n".into(),
            module_path: String::new(),
            file: String::new(),
            line: None,
            sev: Severity::Info,
            field_names: Vec::new(),
            template: String::new(),
            registered_ns: 0,
            events_since_refresh: AtomicU64::new(0),
        };
        let (_a, new1) = reg.insert_if_absent(rec);
        assert!(new1);
        let rec2 = CallsiteRecord {
            id: 1,
            source: CallsiteSource::Forensic,
            target: "t".into(),
            name: "n".into(),
            module_path: String::new(),
            file: String::new(),
            line: None,
            sev: Severity::Info,
            field_names: Vec::new(),
            template: String::new(),
            registered_ns: 0,
            events_since_refresh: AtomicU64::new(0),
        };
        let (_b, new2) = reg.insert_if_absent(rec2);
        assert!(!new2);
        assert_eq!(reg.len(), 1);
    }
}
