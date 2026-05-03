#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
#![allow(
    clippy::collapsible_if,
    clippy::collapsible_match,
    clippy::too_many_arguments,
    clippy::expect_used
)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

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
pub use interning::PrewarmStats;
pub use prewarm::{PREWARM_CALLSITES, PrewarmEntry};
pub use redactor::{DefaultPiiPatternRedactor, RedactAction, Redactor};
pub use typed::TypedMatcher;
