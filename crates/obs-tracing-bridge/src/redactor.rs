//! Pluggable PII redactor used by `TracingToObsLayer`. Spec 30 § 2.6.

use std::sync::atomic::{AtomicBool, Ordering};

use dashmap::DashMap;
use obs_proto::obs::v1::ObsEnvelope;

/// Redactor outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedactAction {
    /// Pass the value through unchanged.
    Keep,
    /// Replace the value with `[REDACTED:...]`.
    Replaced,
    /// Drop the field entirely.
    Drop,
}

/// Pluggable redactor — runs on every promoted / attribute field
/// before it lands in `payload.attrs` / `env.labels`.
pub trait Redactor: Send + Sync + 'static {
    /// Inspect / mutate `value`. Returning `Replaced` swaps `value`
    /// with the redaction marker; returning `Drop` discards the field.
    fn redact(&self, target: &str, field: &str, value: &mut String) -> RedactAction;
}

/// Default name-pattern redactor: matches case-insensitively against
/// the canonical PII / secret token names. Emits one
/// `obs.runtime.v1.ObsBridgePiiSuspected` per *unique* field name (via
/// `DashMap<String, AtomicBool>`).
#[derive(Debug, Default)]
pub struct DefaultPiiPatternRedactor {
    seen: DashMap<String, AtomicBool>,
}

const PII_PATTERNS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "api-key",
    "authorization",
    "cookie",
    "ssn",
    "credit_card",
    "credit-card",
    "creditcard",
    "bearer",
];

impl DefaultPiiPatternRedactor {
    /// New redactor with the built-in pattern list.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn matches(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        PII_PATTERNS.iter().any(|p| lower.contains(p))
    }

    fn note_first_sight(&self, name: &str) -> bool {
        let entry = self
            .seen
            .entry(name.to_string())
            .or_insert_with(|| AtomicBool::new(false));
        let was = entry.load(Ordering::Relaxed);
        entry.store(true, Ordering::Relaxed);
        !was
    }
}

impl Redactor for DefaultPiiPatternRedactor {
    fn redact(&self, _target: &str, field: &str, value: &mut String) -> RedactAction {
        if !Self::matches(field) {
            return RedactAction::Keep;
        }
        if self.note_first_sight(field) {
            // Emit one ObsBridgePiiSuspected self-event for this name.
            let mut env = ObsEnvelope {
                full_name: "obs.runtime.v1.ObsBridgePiiSuspected".to_string(),
                tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
                sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_WARN),
                ..Default::default()
            };
            env.labels
                .insert("field_name".to_string(), field.to_string());
            obs_core::observer().emit_envelope(env);
        }
        *value = "[REDACTED:bridge_pattern]".to_string();
        RedactAction::Replaced
    }
}
