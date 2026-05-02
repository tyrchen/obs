//! Head sampler — per `(full_name, sev)` rate decision.
//!
//! Spec 13 § 6, spec 11 § 4.1 step 4.
//!
//! Decision order, mirroring spec 13 § 6:
//!
//! 1. Inbound `traceparent.sampled` — when set on the active scope frame, the upstream caller
//!    already decided. Returns `SamplingDecision::ParentSet { sampled }` so the caller stamps
//!    `sampling_reason = OVERRIDE` on emit.
//! 2. Severity floor (`always_log_at_or_above`) — bypasses sampling.
//! 3. Per-event rate from config; otherwise the global default rate.
//!
//! The implementation uses `fastrand::f64()`-equivalent behaviour
//! via `rand`-free path — we cheat with a per-thread XorShift to keep
//! the runtime out of the `rand` dependency tree on the hot path.

use std::cell::Cell;

use obs_types::Severity;

use crate::config::SamplingConfig;

/// Outcome of the head-sampler decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SamplingDecision {
    /// The envelope is dropped before reaching the worker.
    Drop,
    /// The envelope is kept under the local rate (or severity floor).
    Keep,
    /// The decision was forced by an upstream `traceparent.sampled`;
    /// the emitter should stamp `SAMPLING_REASON_OVERRIDE`.
    ParentSet {
        /// Whether the parent decided to sample.
        sampled: bool,
    },
}

/// Run the head sampler. `inbound_sampled` is the `traceparent.sampled`
/// bit lifted off the active scope frame; pass `None` when no scope is
/// active or the caller did not propagate W3C trace context.
#[must_use]
pub fn decide(
    cfg: &SamplingConfig,
    full_name: &str,
    severity: Severity,
    inbound_sampled: Option<bool>,
) -> SamplingDecision {
    if cfg.honour_traceparent_sampled
        && let Some(s) = inbound_sampled
    {
        return SamplingDecision::ParentSet { sampled: s };
    }
    if severity >= cfg.always_log_at_or_above {
        return SamplingDecision::Keep;
    }
    let rate = cfg
        .per_event
        .get(full_name)
        .copied()
        .unwrap_or(cfg.default_rate);
    if rate >= 1.0 {
        return SamplingDecision::Keep;
    }
    if rate <= 0.0 {
        return SamplingDecision::Drop;
    }
    if rand_unit_f64() < rate {
        SamplingDecision::Keep
    } else {
        SamplingDecision::Drop
    }
}

thread_local! {
    static SHIFT_STATE: Cell<u64> = Cell::new(seed_state());
}

fn seed_state() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15);
    nanos | 1
}

/// XorShift64-based uniform `[0.0, 1.0)` sample. Cheap and per-thread;
/// suitable for sampling decisions where statistical independence
/// across threads matters more than cryptographic strength.
fn rand_unit_f64() -> f64 {
    SHIFT_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            x = seed_state();
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        cell.set(x);
        // Use the top 53 bits to construct an f64 in [0,1). 53 = mantissa.
        let top = x >> 11;
        (top as f64) / ((1u64 << 53) as f64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SamplingConfig;

    #[test]
    fn test_should_keep_at_or_above_floor() {
        let cfg = SamplingConfig {
            default_rate: 0.0,
            ..Default::default()
        };
        assert_eq!(
            decide(&cfg, "x", Severity::Error, None),
            SamplingDecision::Keep
        );
    }

    #[test]
    fn test_should_drop_below_floor_with_zero_rate() {
        let cfg = SamplingConfig {
            default_rate: 0.0,
            ..Default::default()
        };
        assert_eq!(
            decide(&cfg, "x", Severity::Trace, None),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn test_should_honour_parent_sampled() {
        let cfg = SamplingConfig::default();
        match decide(&cfg, "x", Severity::Trace, Some(true)) {
            SamplingDecision::ParentSet { sampled } => assert!(sampled),
            d => panic!("unexpected decision: {d:?}"),
        }
        match decide(&cfg, "x", Severity::Error, Some(false)) {
            SamplingDecision::ParentSet { sampled } => assert!(!sampled),
            d => panic!("unexpected decision: {d:?}"),
        }
    }

    #[test]
    fn test_should_use_per_event_override() {
        let mut cfg = SamplingConfig {
            default_rate: 0.0,
            ..Default::default()
        };
        cfg.per_event.insert("x".to_string(), 1.0);
        assert_eq!(
            decide(&cfg, "x", Severity::Trace, None),
            SamplingDecision::Keep
        );
    }
}
