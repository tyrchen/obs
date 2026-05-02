//! [`FieldKind`] — role of a field on a wide event.

use std::str::FromStr;

use buffa::Enumeration;
use serde::{Deserialize, Serialize};

use crate::UnknownVariant;

/// Role of a field on a wide event. Drives codegen, OTel mapping, and lints.
///
/// See [10-data-model.md § 4](../../specs/10-data-model.md#4-field-roles).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(i32)]
#[non_exhaustive]
pub enum FieldKind {
    /// Never appears in a well-formed schema.
    #[default]
    Unspecified = 0,
    /// Bounded dimension; safe as metric/span attribute.
    Label = 1,
    /// Free-form; never a metric dimension; in log/span body.
    Attribute = 2,
    /// Numeric; emitted as a metric data point.
    Measurement = 3,
    /// Lifted to envelope `trace_id`.
    TraceId = 4,
    /// Lifted to envelope `span_id`.
    SpanId = 5,
    /// Lifted to envelope `parent_span_id`.
    ParentSpanId = 6,
    /// Overrides envelope `ts_ns`.
    TimestampNs = 7,
    /// Drives span start/end derivation.
    DurationNs = 8,
    /// Opaque blob; never indexed; size-capped.
    Forensic = 9,
}

impl FieldKind {
    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Label => "label",
            Self::Attribute => "attribute",
            Self::Measurement => "measurement",
            Self::TraceId => "trace_id",
            Self::SpanId => "span_id",
            Self::ParentSpanId => "parent_span_id",
            Self::TimestampNs => "timestamp_ns",
            Self::DurationNs => "duration_ns",
            Self::Forensic => "forensic",
        }
    }

    /// True if a value of this kind is lifted from the typed payload to a
    /// dedicated envelope slot (per [10-data-model.md § 6](
    /// ../../specs/10-data-model.md#6-envelope)).
    #[must_use]
    pub const fn is_envelope_lifted(self) -> bool {
        matches!(
            self,
            Self::TraceId | Self::SpanId | Self::ParentSpanId | Self::TimestampNs
        )
    }
}

impl Enumeration for FieldKind {
    fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Label),
            2 => Some(Self::Attribute),
            3 => Some(Self::Measurement),
            4 => Some(Self::TraceId),
            5 => Some(Self::SpanId),
            6 => Some(Self::ParentSpanId),
            7 => Some(Self::TimestampNs),
            8 => Some(Self::DurationNs),
            9 => Some(Self::Forensic),
            _ => None,
        }
    }

    fn to_i32(&self) -> i32 {
        *self as i32
    }

    fn proto_name(&self) -> &'static str {
        match self {
            Self::Unspecified => "FIELD_KIND_UNSPECIFIED",
            Self::Label => "LABEL",
            Self::Attribute => "ATTRIBUTE",
            Self::Measurement => "MEASUREMENT",
            Self::TraceId => "TRACE_ID",
            Self::SpanId => "SPAN_ID",
            Self::ParentSpanId => "PARENT_SPAN_ID",
            Self::TimestampNs => "TIMESTAMP_NS",
            Self::DurationNs => "DURATION_NS",
            Self::Forensic => "FORENSIC",
        }
    }

    fn from_proto_name(name: &str) -> Option<Self> {
        match name {
            "FIELD_KIND_UNSPECIFIED" => Some(Self::Unspecified),
            "LABEL" => Some(Self::Label),
            "ATTRIBUTE" => Some(Self::Attribute),
            "MEASUREMENT" => Some(Self::Measurement),
            "TRACE_ID" => Some(Self::TraceId),
            "SPAN_ID" => Some(Self::SpanId),
            "PARENT_SPAN_ID" => Some(Self::ParentSpanId),
            "TIMESTAMP_NS" => Some(Self::TimestampNs),
            "DURATION_NS" => Some(Self::DurationNs),
            "FORENSIC" => Some(Self::Forensic),
            _ => None,
        }
    }

    fn values() -> &'static [Self] {
        &[
            Self::Unspecified,
            Self::Label,
            Self::Attribute,
            Self::Measurement,
            Self::TraceId,
            Self::SpanId,
            Self::ParentSpanId,
            Self::TimestampNs,
            Self::DurationNs,
            Self::Forensic,
        ]
    }
}

impl FromStr for FieldKind {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "label" => Ok(Self::Label),
            "attribute" => Ok(Self::Attribute),
            "measurement" => Ok(Self::Measurement),
            "trace_id" => Ok(Self::TraceId),
            "span_id" => Ok(Self::SpanId),
            "parent_span_id" => Ok(Self::ParentSpanId),
            "timestamp_ns" => Ok(Self::TimestampNs),
            "duration_ns" => Ok(Self::DurationNs),
            "forensic" => Ok(Self::Forensic),
            _ => Err(UnknownVariant {
                kind: "FieldKind",
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
        for v in FieldKind::values() {
            assert_eq!(FieldKind::from_i32(v.to_i32()), Some(*v));
        }
    }

    #[test]
    fn test_should_identify_envelope_lifted() {
        assert!(FieldKind::TraceId.is_envelope_lifted());
        assert!(FieldKind::SpanId.is_envelope_lifted());
        assert!(!FieldKind::Label.is_envelope_lifted());
        assert!(!FieldKind::Measurement.is_envelope_lifted());
    }
}
