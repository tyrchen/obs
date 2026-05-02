//! [`Classification`] — security/PII classification of a field.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Security/PII classification of a field.
///
/// Drives lint L002 (PII never on LABEL) and L003 (SECRET never on LOG/AUDIT
/// tier). See [70-security-and-classification.md](
/// ../../specs/70-security-and-classification.md).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum Classification {
    /// Never appears in a well-formed schema.
    #[default]
    Unspecified = 0,
    /// Plain internal field; no special handling.
    Internal = 1,
    /// Personally identifiable information; redactable; never on LABEL.
    Pii = 2,
    /// Stripped before durable write; never on LOG/AUDIT tier.
    Secret = 3,
}

impl Classification {
    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Internal => "internal",
            Self::Pii => "pii",
            Self::Secret => "secret",
        }
    }
}

impl Enumeration for Classification {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Internal),
            2 => Some(Self::Pii),
            3 => Some(Self::Secret),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "CLASSIFICATION_UNSPECIFIED",
            Self::Internal => "INTERNAL",
            Self::Pii => "PII",
            Self::Secret => "SECRET",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "CLASSIFICATION_UNSPECIFIED" => Some(Self::Unspecified),
            "INTERNAL" => Some(Self::Internal),
            "PII" => Some(Self::Pii),
            "SECRET" => Some(Self::Secret),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[Self::Unspecified, Self::Internal, Self::Pii, Self::Secret]
    }
}

impl FromStr for Classification {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "internal" => Ok(Self::Internal),
            "pii" => Ok(Self::Pii),
            "secret" => Ok(Self::Secret),
            _ => Err(UnknownVariant {
                kind: "Classification",
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
        for v in Classification::values() {
            assert_eq!(Classification::from_i32(v.to_i32()), Some(*v));
        }
    }
}
