//! [`Severity`] — six levels aligned with OTel `SeverityNumber` buckets.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Six-level severity matching OTel `SeverityNumber` buckets.
///
/// A schema declares a `default_sev`. Call sites may **escalate or demote**
/// through `emit_at(sev)`; the value passed wins. See
/// [10-data-model.md § 3](../../specs/10-data-model.md#3-severity).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum Severity {
    /// `SEVERITY_UNSPECIFIED`; never appears in a well-formed envelope.
    #[default]
    Unspecified = 0,
    /// Most verbose; for fine-grained tracing only.
    Trace = 1,
    /// Diagnostic detail useful when chasing a problem.
    Debug = 2,
    /// Default for normal operational events.
    Info = 3,
    /// Something noteworthy that may require attention.
    Warn = 4,
    /// A failure that affected this operation.
    Error = 5,
    /// A failure severe enough that the process is likely tearing down.
    Fatal = 6,
}

impl Severity {
    /// Stable string label; used by sinks (`labels[\"sev\"]`) and CLI rendering.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
            Self::Fatal => "fatal",
        }
    }

    /// Map to OTLP `SeverityNumber` (1..=24 with 4 buckets per band).
    /// Unspecified maps to 0; fatal maps to 21 (`Fatal`).
    /// See [20-otel-and-sinks.md § 2.2](../../specs/20-otel-and-sinks.md#22-severity--otlp-severitynumber).
    #[must_use]
    pub const fn otlp_number(self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Trace => 1,
            Self::Debug => 5,
            Self::Info => 9,
            Self::Warn => 13,
            Self::Error => 17,
            Self::Fatal => 21,
        }
    }
}

impl Enumeration for Severity {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Trace),
            2 => Some(Self::Debug),
            3 => Some(Self::Info),
            4 => Some(Self::Warn),
            5 => Some(Self::Error),
            6 => Some(Self::Fatal),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "SEVERITY_UNSPECIFIED",
            Self::Trace => "SEVERITY_TRACE",
            Self::Debug => "SEVERITY_DEBUG",
            Self::Info => "SEVERITY_INFO",
            Self::Warn => "SEVERITY_WARN",
            Self::Error => "SEVERITY_ERROR",
            Self::Fatal => "SEVERITY_FATAL",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "SEVERITY_UNSPECIFIED" => Some(Self::Unspecified),
            "SEVERITY_TRACE" => Some(Self::Trace),
            "SEVERITY_DEBUG" => Some(Self::Debug),
            "SEVERITY_INFO" => Some(Self::Info),
            "SEVERITY_WARN" => Some(Self::Warn),
            "SEVERITY_ERROR" => Some(Self::Error),
            "SEVERITY_FATAL" => Some(Self::Fatal),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[
            Self::Unspecified,
            Self::Trace,
            Self::Debug,
            Self::Info,
            Self::Warn,
            Self::Error,
            Self::Fatal,
        ]
    }
}

impl FromStr for Severity {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "trace" => Ok(Self::Trace),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warn),
            "error" | "err" => Ok(Self::Error),
            "fatal" => Ok(Self::Fatal),
            _ => Err(UnknownVariant {
                kind: "Severity",
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
        for v in Severity::values() {
            assert_eq!(Severity::from_i32(v.to_i32()), Some(*v));
        }
    }

    #[test]
    fn test_should_order_correctly() {
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Error < Severity::Fatal);
    }

    #[test]
    fn test_should_map_otlp_numbers() {
        assert_eq!(Severity::Info.otlp_number(), 9);
        assert_eq!(Severity::Fatal.otlp_number(), 21);
    }

    #[test]
    fn test_should_parse_aliases() {
        assert_eq!("warning".parse::<Severity>().unwrap(), Severity::Warn);
        assert_eq!("err".parse::<Severity>().unwrap(), Severity::Error);
    }
}
