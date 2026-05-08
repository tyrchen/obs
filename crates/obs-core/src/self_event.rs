//! Shared primitive for building labels-only self-event envelopes.
//!
//! Sinks, middleware, and tok-side code that emits runtime self-events
//! all end up wanting the same envelope shape: a `full_name`, a `tier`,
//! a `sev`, a wall-clock `ts_ns`, and `sampling_reason =
//! SAMPLING_REASON_RUNTIME` (so downstream consumers can distinguish
//! framework emissions from user emissions). Each of the three
//! self-event modules under `obs-core` / `obs-sink-batch` / `obs-prom`
//! used to hand-roll that envelope plus its own `tier_to_proto` /
//! `sev_to_proto` / `now_ns` helpers; this module collapses those
//! copies into one primitive.
//!
//! After Phase 3b (obs-types retirement), [`Tier`] and [`Severity`]
//! *are* the proto-generated enums — no conversion layer remains.

use buffa::EnumValue;
use obs_proto::obs::v1::{ObsEnvelope, SamplingReason, Severity, Tier};

/// Build a labels-only self-event envelope with `sampling_reason =
/// SAMPLING_REASON_RUNTIME` and the current wall-clock `ts_ns`.
///
/// Typical usage from a sink, middleware adapter, or runtime module:
///
/// ```no_run
/// use obs_core::{observer, self_event, Severity, Tier};
///
/// let mut env = self_event("mylib.v1.WorkerRestart", Tier::Log, Severity::Warn);
/// env.labels.insert("reason".into(), "timeout".into());
/// observer().emit_envelope(env);
/// ```
#[must_use]
pub fn self_event(full_name: &str, tier: Tier, sev: Severity) -> ObsEnvelope {
    ObsEnvelope {
        full_name: full_name.to_string(),
        tier: EnumValue::Known(tier),
        sev: EnumValue::Known(sev),
        sampling_reason: EnumValue::Known(SamplingReason::SAMPLING_REASON_RUNTIME),
        ts_ns: now_ns(),
        ..Default::default()
    }
}

/// Wall-clock timestamp in nanoseconds since the Unix epoch, saturated
/// at `u64::MAX`.
#[must_use]
pub fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use buffa::EnumValue;

    use super::*;

    #[test]
    fn test_should_populate_full_name_tier_sev() {
        let env = self_event("obs.runtime.v1.ObsTest", Tier::Log, Severity::Info);
        assert_eq!(env.full_name, "obs.runtime.v1.ObsTest");
        assert!(matches!(env.tier, EnumValue::Known(Tier::TIER_LOG)));
        assert!(matches!(env.sev, EnumValue::Known(Severity::SEVERITY_INFO)));
    }

    #[test]
    fn test_should_set_sampling_reason_runtime() {
        let env = self_event("obs.runtime.v1.ObsTest", Tier::Metric, Severity::Warn);
        assert!(matches!(
            env.sampling_reason,
            EnumValue::Known(SamplingReason::SAMPLING_REASON_RUNTIME)
        ));
    }

    #[test]
    fn test_should_populate_ts_ns() {
        let before = now_ns();
        let env = self_event("obs.runtime.v1.ObsTest", Tier::Log, Severity::Info);
        let after = now_ns();
        assert!(env.ts_ns >= before);
        assert!(env.ts_ns <= after);
    }
}
