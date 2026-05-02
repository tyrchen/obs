//! [`Tier`] — primary durable destination for an event.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Primary durable destination for an event.
///
/// Tier is a routing hint — the same envelope may also fan out to
/// metric/trace sinks regardless of tier. `Audit` has stricter delivery
/// semantics (bounded blocking + spool); see [11-runtime-core.md § 6.4](
/// ../../specs/11-runtime-core.md#64-audit-tier-delivery-policy).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum Tier {
    /// `TIER_UNSPECIFIED`; never appears in a well-formed envelope.
    #[default]
    Unspecified = 0,
    /// Durable, queryable; default for most events.
    Log = 1,
    /// Aggregated; payload may be discarded after counter increment.
    Metric = 2,
    /// Spans; envelope `trace_id` / `span_id` are required.
    Trace = 3,
    /// Compliance: separate retention, encryption, immutability.
    Audit = 4,
}

impl Tier {
    /// Stable string label; used by sinks (`labels[\"tier\"]`) and by the
    /// CLI when rendering. Avoid changing — appears in dashboards.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Log => "log",
            Self::Metric => "metric",
            Self::Trace => "trace",
            Self::Audit => "audit",
        }
    }
}

impl Enumeration for Tier {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Log),
            2 => Some(Self::Metric),
            3 => Some(Self::Trace),
            4 => Some(Self::Audit),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "TIER_UNSPECIFIED",
            Self::Log => "TIER_LOG",
            Self::Metric => "TIER_METRIC",
            Self::Trace => "TIER_TRACE",
            Self::Audit => "TIER_AUDIT",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "TIER_UNSPECIFIED" => Some(Self::Unspecified),
            "TIER_LOG" => Some(Self::Log),
            "TIER_METRIC" => Some(Self::Metric),
            "TIER_TRACE" => Some(Self::Trace),
            "TIER_AUDIT" => Some(Self::Audit),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[
            Self::Unspecified,
            Self::Log,
            Self::Metric,
            Self::Trace,
            Self::Audit,
        ]
    }
}

impl FromStr for Tier {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "log" => Ok(Self::Log),
            "metric" => Ok(Self::Metric),
            "trace" => Ok(Self::Trace),
            "audit" => Ok(Self::Audit),
            _ => Err(UnknownVariant {
                kind: "Tier",
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
        for v in Tier::values() {
            assert_eq!(Tier::from_i32(v.to_i32()), Some(*v));
        }
    }

    #[test]
    fn test_should_parse_lowercase() {
        assert_eq!("log".parse::<Tier>().unwrap(), Tier::Log);
        assert_eq!("AUDIT".parse::<Tier>().unwrap(), Tier::Audit);
    }

    #[test]
    fn test_should_reject_unknown() {
        assert!("garbage".parse::<Tier>().is_err());
    }

    #[test]
    fn test_should_round_trip_proto_name() {
        for v in Tier::values() {
            assert_eq!(Tier::from_proto_name(v.proto_name()), Some(*v));
        }
    }
}
