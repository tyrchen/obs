//! `test_no_observer_noop` — verifies a fresh process emits without
//! panic and without observable side effects when no observer has
//! been installed and no per-thread/task override is set. Spec 60 § 13
//! + spec 11 § 3 ("no observer = no panic, ≤ one atomic load").

use obs_kit::{Emit, Event};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsNoopProbe {
    #[obs(label, cardinality = "low")]
    pub kind: String,
}

#[test]
fn test_emit_without_observer_should_not_panic() {
    // Other tests in the suite install observers via per-thread slots,
    // so the global remains the default `NoopObserver`. Re-emitting
    // without any per-thread install should hit the noop path.
    ObsNoopProbe { kind: "x".into() }.emit();
    ObsNoopProbe { kind: "x".into() }.emit_at(obs_kit::Severity::Warn);
}
