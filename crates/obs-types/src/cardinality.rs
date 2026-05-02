//! [`Cardinality`] — bound on the number of distinct values a field admits.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Bound on the number of distinct values a field admits at runtime.
///
/// Drives lint L001 (LABEL fields must be Low or Medium) and L005 (enum
/// variant count must fit the cap). See [10-data-model.md § 4](
/// ../../specs/10-data-model.md#4-field-roles).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum Cardinality {
    /// Never appears in a well-formed schema.
    #[default]
    Unspecified = 0,
    /// `< 10` distinct values (status, boolean).
    Low = 1,
    /// `< 10_000` distinct values (route, tenant).
    Medium = 2,
    /// `< 1_000_000` distinct values (user_id) — illegal for LABEL.
    High = 3,
    /// Open / unbounded — illegal for LABEL and MEASUREMENT.
    Unbounded = 4,
}

impl Cardinality {
    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Unbounded => "unbounded",
        }
    }

    /// Numeric cap: the maximum distinct value count permitted at this level.
    /// Returns `u64::MAX` for `Unbounded`. Used by lint L005 (variant count).
    ///
    /// `Unspecified` returns 0 — a schema that did not declare cardinality
    /// should fail the cap check.
    #[must_use]
    pub const fn cap(self) -> u64 {
        match self {
            Self::Unspecified => 0,
            Self::Low => 10,
            Self::Medium => 10_000,
            Self::High => 1_000_000,
            Self::Unbounded => u64::MAX,
        }
    }

    /// True if this cardinality is permitted on a `FieldKind::Label` field.
    /// Labels become metric/span dimensions; only Low and Medium are safe.
    #[must_use]
    pub const fn is_label_compatible(self) -> bool {
        matches!(self, Self::Low | Self::Medium)
    }

    /// True if this cardinality is permitted on a `FieldKind::Measurement`
    /// field. `Unbounded` is illegal — measurement values come from the
    /// numeric type, not from the cardinality.
    #[must_use]
    pub const fn is_measurement_compatible(self) -> bool {
        !matches!(self, Self::Unbounded)
    }
}

impl Enumeration for Cardinality {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Low),
            2 => Some(Self::Medium),
            3 => Some(Self::High),
            4 => Some(Self::Unbounded),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "CARDINALITY_UNSPECIFIED",
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
            Self::Unbounded => "UNBOUNDED",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "CARDINALITY_UNSPECIFIED" => Some(Self::Unspecified),
            "LOW" => Some(Self::Low),
            "MEDIUM" => Some(Self::Medium),
            "HIGH" => Some(Self::High),
            "UNBOUNDED" => Some(Self::Unbounded),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[
            Self::Unspecified,
            Self::Low,
            Self::Medium,
            Self::High,
            Self::Unbounded,
        ]
    }
}

impl FromStr for Cardinality {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "unbounded" => Ok(Self::Unbounded),
            _ => Err(UnknownVariant {
                kind: "Cardinality",
                value: s.to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_round_trip_via_i32() {
        for v in Cardinality::values() {
            assert_eq!(Cardinality::from_i32(v.to_i32()), Some(*v));
        }
    }

    #[test]
    fn test_should_enforce_label_compatibility() {
        assert!(Cardinality::Low.is_label_compatible());
        assert!(Cardinality::Medium.is_label_compatible());
        assert!(!Cardinality::High.is_label_compatible());
        assert!(!Cardinality::Unbounded.is_label_compatible());
    }

    #[test]
    fn test_should_compute_caps() {
        assert_eq!(Cardinality::Low.cap(), 10);
        assert_eq!(Cardinality::Medium.cap(), 10_000);
        assert_eq!(Cardinality::High.cap(), 1_000_000);
        assert_eq!(Cardinality::Unbounded.cap(), u64::MAX);
    }
}
