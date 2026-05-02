//! `Emit` blanket trait — provides `.emit()` / `.emit_at(sev)` on
//! every type that implements [`EventSchema`].
//!
//! Spec 61 § 2.4. The macro path emits straight calls to these
//! methods. Hand-rolled tests can call them directly.

use crate::callsite::ObsCallsite;
use crate::envelope::{EventSchema, build_envelope_at};
use crate::observer::observer;
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

/// Internal helper used by the blanket impl and the macro path.
fn emit_inner<E: EventSchema>(event: &E, sev: Severity) {
    // The callsite construction uses the const-fn constructor so the
    // first emit pays no allocation. In Phase 1 we synthesise a
    // module-local callsite per call (one per `.emit()` site is not
    // achievable from a blanket impl); the `#[derive(Event)]` macro
    // path can later inline a `static __CALLSITE` per call site for
    // the atomic-Interest cache to bite. Spec 11 § 2 + § 5.
    let callsite = ObsCallsite::new(
        E::FULL_NAME,
        E::DEFAULT_SEV,
        module_path!(),
        file!(),
        line!(),
    );
    let mut env = build_envelope_at::<E>(&callsite, event, sev);
    event.project(&mut env);
    let o = observer();
    o.emit_envelope(env);
}

