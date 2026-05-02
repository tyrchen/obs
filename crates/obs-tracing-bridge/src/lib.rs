#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]

//! Bidirectional bridge between `tracing` and the obs SDK.
//!
//! Phase 3 ships **Direction A** (`tracing → obs`) only; Direction B
//! lands in Phase 4. Spec 30 § 2.

mod direction_a;
mod field_promotions;
mod redactor;

pub use direction_a::{SpanEventMode, TracingToObsLayer};
pub use field_promotions::{FieldPromotions, level_to_severity};
pub use redactor::{DefaultPiiPatternRedactor, RedactAction, Redactor};
