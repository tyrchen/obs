//! `Emit` blanket trait — provides `.emit()` / `.emit_at(sev)` on
//! every type that implements [`EventSchema`].
//!
//! Spec 61 § 2.4. The macro path emits straight calls to these
//! methods. Hand-rolled tests can call them directly.

use crate::callsite::ObsCallsite;
use crate::envelope::{EventSchema, build_envelope_at};
use crate::observer::{enter_emit_envelope, observer};
use obs_types::Severity;

/// Blanket trait giving every `EventSchema` an `.emit()` /
/// `.emit_at(sev)` shortcut.
pub trait Emit: EventSchema + Sized {
    /// Emit at the schema-declared default severity.
    fn emit(self) {
        emit_inner::<Self>(&self, Self::DEFAULT_SEV)
    }

    /// Emit at the supplied severity (escalate or demote — spec 13 § 1.1).
    fn emit_at(self, sev: Severity) {
        emit_inner::<Self>(&self, sev)
    }
}

impl<E: EventSchema + Sized> Emit for E {}

/// Internal helper used by the blanket `Emit` impl when no caller-side
/// `static __CALLSITE` is available.
///
/// **Hot-path emit sites must NOT route through this function.** The
/// `obs::emit!` macro and the `<Builder>::emit()` setter both inline a
/// `static __CALLSITE: ObsCallsite` so the atomic-Interest cache can
/// short-circuit (spec 11 § 2.1). The blanket helper exists for tests
/// and one-shot constructions where the per-emit callsite cost is
/// dominated by the test's own work.
fn emit_inner<E: EventSchema>(event: &E, sev: Severity) {
    // Stack callsite — no Interest cache benefit, but no allocation
    // either. The hot path goes through `emit_with_callsite` with a
    // `static`.
    let callsite = ObsCallsite::new(
        E::FULL_NAME,
        E::DEFAULT_SEV,
        module_path!(),
        file!(),
        line!(),
    );
    emit_one::<E>(&callsite, event, sev);
}

/// Emit through a caller-supplied **static** callsite. Macro-emitted
/// code calls this so the atomic-Interest cache populates and stays
/// alive across emits. Spec 11 § 2.1.
#[doc(hidden)]
#[inline]
pub fn emit_with_callsite<E: EventSchema>(
    callsite: &'static ObsCallsite,
    event: &E,
    sev: Severity,
) {
    emit_one::<E>(callsite, event, sev)
}

/// Shared implementation: probe the cache, optionally consult the
/// observer, project, dispatch.
#[inline]
fn emit_one<E: EventSchema>(callsite: &ObsCallsite, event: &E, sev: Severity) {
    use crate::callsite::{EnabledOutcome, Interest};
    let o = observer();
    let cur_gen = o.generation();
    let outcome = callsite.enabled(cur_gen);
    let permitted = match outcome {
        EnabledOutcome::AlwaysOn => true,
        EnabledOutcome::Off => false,
        EnabledOutcome::SometimesOn => o.enabled(callsite),
        EnabledOutcome::ReProbe => {
            let allowed = o.enabled(callsite);
            callsite.cache(
                if allowed { Interest::Always } else { Interest::Never },
                cur_gen,
            );
            allowed
        }
    };
    if !permitted {
        return;
    }
    let mut env = build_envelope_at::<E>(callsite, event, sev);
    event.project(&mut env);
    enter_emit_envelope(&o, env);
}

