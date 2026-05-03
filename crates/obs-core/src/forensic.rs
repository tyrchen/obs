//! Per-callsite rate limiter used by the `obs::forensic!` macro.
//!
//! Spec 11 § 6.3 / spec 13 § 8: forensic emits are an emergency
//! escape hatch; they bypass head sampling but **must** be capped
//! per-callsite so a single buggy caller cannot drown the LOG tier.
//! We use `governor::DefaultDirectRateLimiter` (token bucket; std
//! clock; default 1 emit/sec, burst 5).

use std::{
    num::NonZeroU32,
    sync::{Arc, OnceLock},
};

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};

/// Concrete rate-limiter type used per callsite.
pub type ForensicLimiter =
    RateLimiter<NotKeyed, InMemoryState, DefaultClock, governor::middleware::NoOpMiddleware>;

/// Default quota — 1 emit/sec with a burst of 5. Spec 11 § 6.3 lists
/// "rate-limited per (file, line)" without precise numbers; this
/// matches what observability vendors typically allow before paging.
#[must_use]
pub fn default_forensic_quota() -> Quota {
    // SAFETY: 1 and 5 are both compile-time non-zero literals; the
    // `match` here is purely to avoid `.expect()` per project lint
    // policy (clippy::expect_used).
    let per_sec = match NonZeroU32::new(1) {
        Some(n) => n,
        None => unreachable_zero(),
    };
    let burst = match NonZeroU32::new(5) {
        Some(n) => n,
        None => unreachable_zero(),
    };
    Quota::per_second(per_sec).allow_burst(burst)
}

#[cold]
#[inline(never)]
#[allow(clippy::panic)]
fn unreachable_zero() -> NonZeroU32 {
    panic!("non-zero literal yielded zero (compiler bug)")
}

/// Per-callsite limiter accessor. The macro expansion stores a
/// [`OnceLock<Arc<ForensicLimiter>>`] in a static; the first call
/// constructs the limiter under the default quota.
pub fn ensure_limiter(
    slot: &'static OnceLock<Arc<ForensicLimiter>>,
) -> &'static Arc<ForensicLimiter> {
    slot.get_or_init(|| Arc::new(RateLimiter::direct(default_forensic_quota())))
}

/// Returns `true` if the callsite is currently allowed to emit; the
/// macro silently drops the call when this returns `false`.
pub fn try_acquire_forensic(slot: &'static OnceLock<Arc<ForensicLimiter>>) -> bool {
    let l = ensure_limiter(slot);
    l.check().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_quota_should_allow_burst() {
        static SLOT: OnceLock<Arc<ForensicLimiter>> = OnceLock::new();
        assert!(try_acquire_forensic(&SLOT));
    }

    #[test]
    fn test_per_callsite_isolation() {
        static A: OnceLock<Arc<ForensicLimiter>> = OnceLock::new();
        static B: OnceLock<Arc<ForensicLimiter>> = OnceLock::new();
        // Burst is 5; we should be able to fire up to 5 from each
        // independently before the limiter says no.
        let mut fires_a = 0;
        for _ in 0..5 {
            if try_acquire_forensic(&A) {
                fires_a += 1;
            }
        }
        let mut fires_b = 0;
        for _ in 0..5 {
            if try_acquire_forensic(&B) {
                fires_b += 1;
            }
        }
        assert_eq!(fires_a, 5);
        assert_eq!(fires_b, 5);
    }
}
