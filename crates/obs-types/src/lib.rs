// Tests routinely use `.unwrap()` for clarity; production code uses `?`.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

//! Foundation enums for the obs SDK.
//!
//! Every other crate in the workspace depends on this one. The seven enums
//! defined here form the vocabulary of the wire envelope ([10-data-model.md
//! § 2-5](../../specs/10-data-model.md)) — `Tier`, `Severity`, `FieldKind`,
//! `Cardinality`, `Classification`, `MetricKind`, `SamplingReason`.
//!
//! All enums:
//! - derive `Copy + Clone + Debug + PartialEq + Eq + Hash`,
//! - implement [`buffa::Enumeration`] so they live on the wire,
//! - serialize/deserialize via `serde` for `obs.yaml` config,
//! - expose `const fn` helpers used by compile-time lints (e.g.
//!   [`Cardinality::is_label_compatible`], [`Cardinality::cap`],
//!   [`Severity::as_str`]).
//!
//! Vocabulary changes here cause an envelope `format_ver` bump (per
//! [10-data-model.md § 6](../../specs/10-data-model.md#6-envelope) and
//! [61-crates-and-features.md § 4](../../specs/61-crates-and-features.md#4-versioning-policy)).
//! That's the intended forcing function.

mod cardinality;
mod classification;
mod field_kind;
mod metric_kind;
mod sampling;
mod severity;
mod tier;

pub use cardinality::Cardinality;
pub use classification::Classification;
pub use field_kind::FieldKind;
pub use metric_kind::MetricKind;
pub use sampling::SamplingReason;
pub use severity::Severity;
pub use tier::Tier;

/// Error returned by `TryFrom<&str>` and `FromStr` parsers when an unknown
/// enum name is encountered. Each enum in this crate uses this type to
/// preserve a uniform error surface across the vocabulary.
#[derive(Debug, thiserror::Error)]
#[error("unknown {kind} variant: {value:?}")]
pub struct UnknownVariant {
    /// The enum name (e.g. `"Severity"`).
    pub kind: &'static str,
    /// The unrecognised input.
    pub value: String,
}
