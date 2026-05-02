//! [`SamplingReason`] — provenance recorded on the envelope.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Provenance recorded on the envelope explaining why an event was kept.
///
/// Head-sampler-dropped events never produce an envelope, so they need no
/// enum value. See [10-data-model.md § 5](../../specs/10-data-model.md#5-sampling-provenance).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum SamplingReason {
    /// Never appears in a well-formed envelope.
    #[default]
    Unspecified = 0,
    /// Selected by head-rate roll.
    HeadRate = 1,
    /// Flushed because a sibling event hit `ERROR`/`FATAL`.
    TailError = 2,
    /// `always_log_slower_than_ms` triggered.
    Slow = 3,
    /// Emitted by `obs::forensic!` (always retained).
    Forensic = 4,
    /// AUDIT-tier event (always retained).
    Audit = 5,
    /// SDK self-event (`obs.runtime.v1.*`).
    Runtime = 6,
    /// Per-event `head_rate=1.0` forces always-on.
    Override = 7,
}

impl SamplingReason {
    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::HeadRate => "head_rate",
            Self::TailError => "tail_error",
            Self::Slow => "slow",
            Self::Forensic => "forensic",
            Self::Audit => "audit",
            Self::Runtime => "runtime",
            Self::Override => "override",
        }
    }
}

impl Enumeration for SamplingReason {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::HeadRate),
            2 => Some(Self::TailError),
            3 => Some(Self::Slow),
            4 => Some(Self::Forensic),
            5 => Some(Self::Audit),
            6 => Some(Self::Runtime),
            7 => Some(Self::Override),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "SAMPLING_REASON_UNSPECIFIED",
            Self::HeadRate => "SAMPLING_REASON_HEAD_RATE",
            Self::TailError => "SAMPLING_REASON_TAIL_ERROR",
            Self::Slow => "SAMPLING_REASON_SLOW",
            Self::Forensic => "SAMPLING_REASON_FORENSIC",
            Self::Audit => "SAMPLING_REASON_AUDIT",
            Self::Runtime => "SAMPLING_REASON_RUNTIME",
            Self::Override => "SAMPLING_REASON_OVERRIDE",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "SAMPLING_REASON_UNSPECIFIED" => Some(Self::Unspecified),
            "SAMPLING_REASON_HEAD_RATE" => Some(Self::HeadRate),
            "SAMPLING_REASON_TAIL_ERROR" => Some(Self::TailError),
            "SAMPLING_REASON_SLOW" => Some(Self::Slow),
            "SAMPLING_REASON_FORENSIC" => Some(Self::Forensic),
            "SAMPLING_REASON_AUDIT" => Some(Self::Audit),
            "SAMPLING_REASON_RUNTIME" => Some(Self::Runtime),
            "SAMPLING_REASON_OVERRIDE" => Some(Self::Override),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[
            Self::Unspecified,
            Self::HeadRate,
            Self::TailError,
            Self::Slow,
            Self::Forensic,
            Self::Audit,
            Self::Runtime,
            Self::Override,
        ]
    }
}

impl FromStr for SamplingReason {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "head_rate" => Ok(Self::HeadRate),
            "tail_error" => Ok(Self::TailError),
            "slow" => Ok(Self::Slow),
            "forensic" => Ok(Self::Forensic),
            "audit" => Ok(Self::Audit),
            "runtime" => Ok(Self::Runtime),
            "override" => Ok(Self::Override),
            _ => Err(UnknownVariant {
                kind: "SamplingReason",
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
        for v in SamplingReason::values() {
            assert_eq!(SamplingReason::from_i32(v.to_i32()), Some(*v));
        }
    }
}
