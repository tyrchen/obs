//! [`MetricKind`] — metric type for a `MEASUREMENT` field.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Metric type for a `MEASUREMENT` field. See
/// [12-schema-and-codegen.md §
/// 2](../../specs/12-schema-and-codegen.md#2-the-obsv1options-proto-extensions).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum MetricKind {
    /// Never appears in a well-formed schema.
    #[default]
    Unspecified = 0,
    /// Monotonically increasing counter (e.g. bytes_out, request_count).
    Counter = 1,
    /// Last-write-wins gauge (e.g. queue_depth, in_flight).
    Gauge = 2,
    /// Histogram with explicit bucket bounds (e.g. latency_ms).
    Histogram = 3,
}

impl MetricKind {
    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

impl Enumeration for MetricKind {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Counter),
            2 => Some(Self::Gauge),
            3 => Some(Self::Histogram),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "METRIC_KIND_UNSPECIFIED",
            Self::Counter => "METRIC_KIND_COUNTER",
            Self::Gauge => "METRIC_KIND_GAUGE",
            Self::Histogram => "METRIC_KIND_HISTOGRAM",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "METRIC_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "METRIC_KIND_COUNTER" => Some(Self::Counter),
            "METRIC_KIND_GAUGE" => Some(Self::Gauge),
            "METRIC_KIND_HISTOGRAM" => Some(Self::Histogram),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[
            Self::Unspecified,
            Self::Counter,
            Self::Gauge,
            Self::Histogram,
        ]
    }
}

impl FromStr for MetricKind {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "counter" => Ok(Self::Counter),
            "gauge" => Ok(Self::Gauge),
            "histogram" => Ok(Self::Histogram),
            _ => Err(UnknownVariant {
                kind: "MetricKind",
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
        for v in MetricKind::values() {
            assert_eq!(MetricKind::from_i32(v.to_i32()), Some(*v));
        }
    }
}
