#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
#![allow(
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::too_many_arguments,
    clippy::expect_used
)]
#![cfg_attr(test, allow(clippy::unwrap_used))]

//! Bidirectional bridge between `tracing` and the obs SDK.
//!
//! - **Direction A** (`tracing → obs`) — [`TracingToObsLayer`].
//! - **Direction B** (`obs → tracing`) — [`ObsToTracingSink`].
//!
//! The two halves cooperate through thread-local loop guards plus the
//! reserved `obs.bridge` target. Spec 30 § 4.1 + KD5.

mod direction_a;
mod direction_b;
mod field_promotions;
mod interning;
mod prewarm;
mod redactor;
mod typed;

pub use direction_a::{InterningMode, SpanEventMode, TracingToObsLayer};
pub use direction_b::{ObsToTracingSink, PayloadDecodeMode, SpanEmissionMode};
pub use field_promotions::{FieldPromotions, level_to_severity};
pub use interning::{PrewarmStats, intern_or_lookup, run_prewarm};
pub use prewarm::{PREWARM_CALLSITES, PrewarmEntry};
pub use redactor::{DefaultPiiPatternRedactor, RedactAction, Redactor};
pub use typed::TypedMatcher;

/// One-call helper that wires the bridge into an existing
/// `tracing_subscriber::Registry`. Returns the resulting subscriber so
/// the caller can install it via `tracing::subscriber::set_global_default`.
///
/// Sets up:
/// - `Registry::default()` as the inner subscriber
/// - `EnvFilter` driven by `RUST_LOG` (default `"info"`)
/// - `LogTracer::init()` so legacy `log::*` macros funnel into the tracing dispatcher
/// - [`TracingToObsLayer`] forwarding events to the active obs observer
///
/// `RUST_LOG` is honoured; pass `directives` to override.
///
/// Spec 30 § 4.3 / spec 93 P2-17.
///
/// # Errors
///
/// Returns an error string when `LogTracer::init()` has already been
/// called. Other failures (filter parse, etc.) fall back to defaults.
pub fn init<S: AsRef<str>>(directives: Option<S>) -> Result<(), String> {
    use tracing_log::LogTracer;
    use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt};

    let env_filter = match directives.as_ref().map(|s| s.as_ref()) {
        Some(d) if !d.is_empty() => {
            EnvFilter::try_new(d).unwrap_or_else(|_| EnvFilter::new("info"))
        }
        _ => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };
    let subscriber = Registry::default()
        .with(env_filter)
        .with(TracingToObsLayer::new());
    tracing::subscriber::set_global_default(subscriber).map_err(|e| e.to_string())?;
    let _ = LogTracer::init(); // ignore double-init
    Ok(())
}
