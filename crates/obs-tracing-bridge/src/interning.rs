//! Callsite interning — Direction A's first-sight registration plus
//! the per-process counters spec 31 § 3.3 / § 5.1 mandate.

use std::{
    sync::{Arc, atomic::AtomicU64},
    time::{Duration, SystemTime},
};

use obs_core::{CallsiteRecord, CallsiteSource, ObsCallsiteRegistry};
use obs_proto::obs::v1::{ObsEnvelope, Severity};

use crate::prewarm::PrewarmEntry;

/// Default re-emit cadence — spec 31 § 5.2.
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const DEFAULT_REFRESH_EVENT_COUNT: u64 = 10_000;

/// Statistics for the pre-warm pass — useful for tests and operator
/// sanity checks. Spec 31 § 3.3.
#[derive(Debug, Default, Clone, Copy)]
pub struct PrewarmStats {
    /// Number of pre-warm entries that produced a fresh registration.
    pub registered: usize,
    /// Number of pre-warm entries that were already present.
    pub skipped: usize,
}

/// Run the pre-warm pass against `registry`, emitting one
/// `ObsCallsiteRegistered` per fresh entry.
pub fn run_prewarm(registry: &Arc<ObsCallsiteRegistry>, entries: &[PrewarmEntry]) -> PrewarmStats {
    let mut stats = PrewarmStats::default();
    for e in entries {
        let id = obs_core::callsite_id(
            CallsiteSource::TracingEvent,
            e.target,
            e.anchor,
            None,
            e.level,
            e.field_names,
            "",
        );
        let rec = CallsiteRecord {
            id,
            source: CallsiteSource::TracingEvent,
            target: e.target.to_string(),
            name: e.target.to_string(),
            module_path: e.anchor.to_string(),
            file: e.anchor.to_string(),
            line: None,
            sev: e.level,
            field_names: e.field_names.iter().map(|s| (*s).to_string()).collect(),
            template: String::new(),
            registered_ns: now_ns(),
            events_since_refresh: AtomicU64::new(0),
        };
        let (_, was_new) = registry.insert_if_absent(rec);
        if was_new {
            stats.registered += 1;
            emit_registered(id, e.target, e.anchor, e.level, e.field_names);
        } else {
            stats.skipped += 1;
        }
    }
    stats
}

/// Compute or look up the callsite_id for a tracing metadata, register
/// in `registry` if first sight, emit the `ObsCallsiteRegistered`
/// envelope, and return `(id, was_new)`. Spec 31 § 3.3.
pub fn intern_or_lookup(
    registry: &Arc<ObsCallsiteRegistry>,
    target: &str,
    name: &str,
    module_path: &str,
    file: &str,
    line: Option<u32>,
    sev: Severity,
    field_names: &[&str],
    template: &str,
) -> (u64, bool) {
    let id = obs_core::callsite_id(
        CallsiteSource::TracingEvent,
        target,
        file,
        line,
        sev,
        field_names,
        template,
    );
    let rec = CallsiteRecord {
        id,
        source: CallsiteSource::TracingEvent,
        target: target.to_string(),
        name: name.to_string(),
        module_path: module_path.to_string(),
        file: file.to_string(),
        line: line.and_then(std::num::NonZeroU32::new),
        sev,
        field_names: field_names.iter().map(|s| (*s).to_string()).collect(),
        template: template.to_string(),
        registered_ns: now_ns(),
        events_since_refresh: AtomicU64::new(0),
    };
    let (entry, was_new) = registry.insert_if_absent(rec);
    if was_new {
        emit_registered(id, target, file, sev, field_names);
    } else {
        let count = entry.observe();
        let elapsed = now_ns().saturating_sub(entry.registered_ns);
        if count >= DEFAULT_REFRESH_EVENT_COUNT
            || elapsed >= DEFAULT_REFRESH_INTERVAL.as_nanos() as u64
        {
            emit_registered(id, target, file, sev, field_names);
            entry.reset_count();
        }
    }
    (id, was_new)
}

fn emit_registered(
    callsite_id: u64,
    target: &str,
    file: &str,
    sev: Severity,
    field_names: &[&str],
) {
    let mut env = ObsEnvelope {
        full_name: "obs.v1.ObsCallsiteRegistered".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(sev_to_proto(sev)),
        callsite_id,
        sampling_reason: ::buffa::EnumValue::Known(
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_OVERRIDE,
        ),
        ..Default::default()
    };
    env.labels.insert("target".to_string(), target.to_string());
    env.labels.insert("file".to_string(), file.to_string());
    env.labels
        .insert("field_names".to_string(), field_names.join(","));
    obs_core::observer().emit_envelope(env);
}

fn sev_to_proto(s: Severity) -> obs_proto::obs::v1::Severity {
    use obs_proto::obs::v1::Severity as P;
    match s {
        Severity::Trace => P::SEVERITY_TRACE,
        Severity::Debug => P::SEVERITY_DEBUG,
        Severity::Info => P::SEVERITY_INFO,
        Severity::Warn => P::SEVERITY_WARN,
        Severity::Error => P::SEVERITY_ERROR,
        Severity::Fatal => P::SEVERITY_FATAL,
        _ => P::SEVERITY_UNSPECIFIED,
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prewarm::PREWARM_CALLSITES;

    #[test]
    fn test_prewarm_should_register_each_entry_once() {
        let reg = Arc::new(ObsCallsiteRegistry::new());
        let s1 = run_prewarm(&reg, PREWARM_CALLSITES);
        assert_eq!(s1.registered, PREWARM_CALLSITES.len());
        assert_eq!(s1.skipped, 0);
        let s2 = run_prewarm(&reg, PREWARM_CALLSITES);
        assert_eq!(s2.registered, 0);
        assert_eq!(s2.skipped, PREWARM_CALLSITES.len());
    }

    #[test]
    fn test_intern_should_return_stable_id() {
        let reg = Arc::new(ObsCallsiteRegistry::new());
        let (a, was_a) = intern_or_lookup(
            &reg,
            "sqlx::query",
            "evt",
            "sqlx",
            "src/q.rs",
            Some(42),
            Severity::Debug,
            &["rows", "elapsed"],
            "executed query",
        );
        let (b, was_b) = intern_or_lookup(
            &reg,
            "sqlx::query",
            "evt",
            "sqlx",
            "src/q.rs",
            Some(42),
            Severity::Debug,
            &["rows", "elapsed"],
            "executed query",
        );
        assert_eq!(a, b);
        assert!(was_a);
        assert!(!was_b);
    }
}
