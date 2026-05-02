//! `ObsCallsite` and the atomic `Interest` cache.
//!
//! Every emit-site compiles to a unique `static ObsCallsite`. The
//! atomic-`Interest` cache lets a filtered-out emit short-circuit on a
//! single atomic load, with no observer virtual call. See spec 11 § 2.

use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

use obs_types::Severity;

/// Cached interest decision for a callsite. Mirrors `tracing::Interest`.
///
/// - `0` (`Unknown`) — not yet probed; observer must be queried.
/// - `1` (`Never`) — disabled; skip the entire emit branch.
/// - `2` (`Sometimes`) — enabled but still call `Observer::enabled()`
///   per emit (e.g. severity-floor + per-callsite allowlist).
/// - `3` (`Always`) — enabled unconditionally; skip the virtual call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Interest {
    /// Not yet decided — observer will be queried.
    Unknown = 0,
    /// Disabled. The emit branch is skipped after one atomic load.
    Never = 1,
    /// Enabled but `Observer::enabled` must still run.
    Sometimes = 2,
    /// Enabled unconditionally; the virtual call is skipped.
    Always = 3,
}

impl Interest {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Never,
            2 => Self::Sometimes,
            3 => Self::Always,
            _ => Self::Unknown,
        }
    }
}

/// Static metadata for a single emit site. Constructed by codegen via
/// the `const fn` constructor — no heap allocation, no first-emit cost.
///
/// See spec 11 § 2.
#[derive(Debug)]
pub struct ObsCallsite {
    /// Fully qualified event name (`myapp.v1.ObsXxx`).
    full_name: &'static str,
    /// Default severity declared by the schema.
    default_sev: Severity,
    /// `module_path!()` from the emit site.
    module: &'static str,
    /// `file!()` from the emit site.
    file: &'static str,
    /// `line!()` from the emit site.
    line: u32,
    /// Cached interest decision (see [`Interest`]).
    interest: AtomicU8,
    /// Bumped on every config reload so a stale `interest` is treated as
    /// `Unknown` and re-queried (spec 11 § 3.2).
    cached_gen: AtomicU32,
}

impl ObsCallsite {
    /// Construct a callsite. Intended for use by codegen at static init;
    /// the const-fn shape means no heap allocation on first emit.
    #[must_use]
    pub const fn new(
        full_name: &'static str,
        default_sev: Severity,
        module: &'static str,
        file: &'static str,
        line: u32,
    ) -> Self {
        Self {
            full_name,
            default_sev,
            module,
            file,
            line,
            interest: AtomicU8::new(Interest::Unknown as u8),
            cached_gen: AtomicU32::new(0),
        }
    }

    /// Hot-path enabled check.
    ///
    /// Returns `true` if this callsite *might* fire (`Sometimes` /
    /// `Always`); the caller is then expected to invoke
    /// `Observer::enabled` only when the result is `Sometimes`.
    ///
    /// `current_gen` is the observer's `generation()`. On a generation
    /// mismatch the cache is reset to `Unknown` and the caller re-probes.
    #[inline(always)]
    #[must_use]
    pub fn enabled(&self, current_gen: u32) -> EnabledOutcome {
        let cached_gen = self.cached_gen.load(Ordering::Relaxed);
        if cached_gen != current_gen {
            return EnabledOutcome::ReProbe;
        }
        match Interest::from_u8(self.interest.load(Ordering::Relaxed)) {
            Interest::Unknown => EnabledOutcome::ReProbe,
            Interest::Never => EnabledOutcome::Off,
            Interest::Sometimes => EnabledOutcome::SometimesOn,
            Interest::Always => EnabledOutcome::AlwaysOn,
        }
    }

    /// Update the cached interest after probing the observer.
    pub fn cache(&self, interest: Interest, current_gen: u32) {
        self.interest.store(interest as u8, Ordering::Relaxed);
        self.cached_gen.store(current_gen, Ordering::Relaxed);
    }

    /// Force the cache to `Unknown` so the next emit re-probes. Used by
    /// tests; production reload uses [`ObsCallsite::cache`] with the new
    /// generation, which has the same effect.
    pub fn reset_cache(&self) {
        self.interest
            .store(Interest::Unknown as u8, Ordering::Relaxed);
        self.cached_gen.store(0, Ordering::Relaxed);
    }

    /// Fully qualified event name.
    #[must_use]
    pub const fn full_name(&self) -> &'static str {
        self.full_name
    }

    /// Default severity declared by the schema.
    #[must_use]
    pub const fn default_sev(&self) -> Severity {
        self.default_sev
    }

    /// `module_path!()` from the emit site.
    #[must_use]
    pub const fn module(&self) -> &'static str {
        self.module
    }

    /// `file!()` from the emit site.
    #[must_use]
    pub const fn file(&self) -> &'static str {
        self.file
    }

    /// `line!()` from the emit site.
    #[must_use]
    pub const fn line(&self) -> u32 {
        self.line
    }
}

/// Result of the hot-path enabled check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EnabledOutcome {
    /// Cached `Never` — skip the emit branch.
    Off,
    /// Cached `Sometimes` — caller must still call `Observer::enabled`.
    SometimesOn,
    /// Cached `Always` — caller skips `Observer::enabled` and emits.
    AlwaysOn,
    /// Cache is stale or empty — caller must probe and `cache(...)` the
    /// result.
    ReProbe,
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_CALLSITE: ObsCallsite = ObsCallsite::new(
        "test.v1.Probe",
        Severity::Info,
        module_path!(),
        file!(),
        line!(),
    );

    #[test]
    fn test_should_start_in_unknown() {
        TEST_CALLSITE.reset_cache();
        assert_eq!(TEST_CALLSITE.enabled(1), EnabledOutcome::ReProbe);
    }

    #[test]
    fn test_should_short_circuit_on_never() {
        TEST_CALLSITE.cache(Interest::Never, 7);
        assert_eq!(TEST_CALLSITE.enabled(7), EnabledOutcome::Off);
    }

    #[test]
    fn test_should_reprobe_on_generation_mismatch() {
        TEST_CALLSITE.cache(Interest::Always, 7);
        assert_eq!(TEST_CALLSITE.enabled(7), EnabledOutcome::AlwaysOn);
        assert_eq!(TEST_CALLSITE.enabled(8), EnabledOutcome::ReProbe);
    }
}
