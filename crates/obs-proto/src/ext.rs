//! Ergonomic supplements for proto-generated enums.
//!
//! Phase 3b (obs-migration spec § 4): obs-types is retired. The seven
//! hand-rolled Rust enums collapse into the proto-generated enums in
//! [`crate::obs::v1`]. buffa-codegen doesn't yet emit the full
//! ergonomic surface (`Ord`, `FromStr` with aliases, short-name
//! constants, domain helpers like [`Severity::otlp_number`]), so this
//! module adds them as hand-written supplements.
//!
//! Each enum gains:
//!
//! - **Short-name const aliases** — `Severity::Info`, `Tier::Log`, `Cardinality::High` —
//!   non-wire-breaking re-exports of the canonical SCREAM_CASE variants. Keeps call sites compiling
//!   without the long `SEVERITY_INFO` form every time.
//! - **Ordering** — `PartialOrd + Ord` via discriminant order so `Severity::Info < Severity::Warn`
//!   works.
//! - **Default** — already emitted by buffa-codegen (variant 0).
//! - **Serde** — `Serialize`/`Deserialize` by short lowercased name (`"info"`, `"warn"`) to match
//!   `obs.yaml` config.
//! - **FromStr** — accepts short name + proto name + aliases (`"warning"` → `Warn`, `"err"` →
//!   `Error`). Errors via [`UnknownVariant`].
//! - **Domain methods** — `as_str`, `otlp_number`, `cap`, `is_label_compatible`,
//!   `is_envelope_lifted`, etc.
//!
//! When buffa 0.6 lands with `generate_rich`, this file shrinks to
//! whatever the codegen doesn't cover (likely just serde config
//! naming).

use std::str::FromStr;

use crate::obs::v1::{
    Cardinality, Classification, FieldKind, MetricKind, SamplingReason, Severity, Tier,
};

/// Error returned by `FromStr` parsers when an enum variant isn't
/// recognised. Uniform shape across every enum in this module so
/// callers can match on one type.
#[derive(Debug, thiserror::Error)]
#[error("unknown {kind} variant: {value:?}")]
pub struct UnknownVariant {
    /// Enum name (e.g. `"Severity"`).
    pub kind: &'static str,
    /// Unrecognised input.
    pub value: String,
}

// ============================================================================
// Tier
// ============================================================================

#[allow(non_upper_case_globals)]
impl Tier {
    /// Short-name alias for [`Self::TIER_UNSPECIFIED`].
    pub const Unspecified: Self = Self::TIER_UNSPECIFIED;
    /// Short-name alias for [`Self::TIER_LOG`].
    pub const Log: Self = Self::TIER_LOG;
    /// Short-name alias for [`Self::TIER_METRIC`].
    pub const Metric: Self = Self::TIER_METRIC;
    /// Short-name alias for [`Self::TIER_TRACE`].
    pub const Trace: Self = Self::TIER_TRACE;
    /// Short-name alias for [`Self::TIER_AUDIT`].
    pub const Audit: Self = Self::TIER_AUDIT;

    /// Stable string label; used by sinks (`labels["tier"]`) and the
    /// CLI when rendering.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TIER_LOG => "log",
            Self::TIER_METRIC => "metric",
            Self::TIER_TRACE => "trace",
            Self::TIER_AUDIT => "audit",
            _ => "unspecified",
        }
    }
}

impl Ord for Tier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for Tier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for Tier {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "log" | "tier_log" => Ok(Self::TIER_LOG),
            "metric" | "tier_metric" => Ok(Self::TIER_METRIC),
            "trace" | "tier_trace" => Ok(Self::TIER_TRACE),
            "audit" | "tier_audit" => Ok(Self::TIER_AUDIT),
            "unspecified" | "tier_unspecified" => Ok(Self::TIER_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "Tier",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for Tier {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Tier {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// Severity
// ============================================================================

#[allow(non_upper_case_globals)]
impl Severity {
    /// Short-name alias for [`Self::SEVERITY_UNSPECIFIED`].
    pub const Unspecified: Self = Self::SEVERITY_UNSPECIFIED;
    /// Short-name alias for [`Self::SEVERITY_TRACE`].
    pub const Trace: Self = Self::SEVERITY_TRACE;
    /// Short-name alias for [`Self::SEVERITY_DEBUG`].
    pub const Debug: Self = Self::SEVERITY_DEBUG;
    /// Short-name alias for [`Self::SEVERITY_INFO`].
    pub const Info: Self = Self::SEVERITY_INFO;
    /// Short-name alias for [`Self::SEVERITY_WARN`].
    pub const Warn: Self = Self::SEVERITY_WARN;
    /// Short-name alias for [`Self::SEVERITY_ERROR`].
    pub const Error: Self = Self::SEVERITY_ERROR;
    /// Short-name alias for [`Self::SEVERITY_FATAL`].
    pub const Fatal: Self = Self::SEVERITY_FATAL;

    /// Stable string label used in label values and CLI rendering.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SEVERITY_TRACE => "trace",
            Self::SEVERITY_DEBUG => "debug",
            Self::SEVERITY_INFO => "info",
            Self::SEVERITY_WARN => "warn",
            Self::SEVERITY_ERROR => "error",
            Self::SEVERITY_FATAL => "fatal",
            _ => "unspecified",
        }
    }

    /// Map to OTLP `SeverityNumber` (1..=24 with 4 buckets per band).
    #[must_use]
    pub const fn otlp_number(self) -> i32 {
        match self {
            Self::SEVERITY_TRACE => 1,
            Self::SEVERITY_DEBUG => 5,
            Self::SEVERITY_INFO => 9,
            Self::SEVERITY_WARN => 13,
            Self::SEVERITY_ERROR => 17,
            Self::SEVERITY_FATAL => 21,
            _ => 0,
        }
    }
}

impl Ord for Severity {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for Severity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for Severity {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "trace" | "severity_trace" => Ok(Self::SEVERITY_TRACE),
            "debug" | "severity_debug" => Ok(Self::SEVERITY_DEBUG),
            "info" | "severity_info" => Ok(Self::SEVERITY_INFO),
            "warn" | "warning" | "severity_warn" => Ok(Self::SEVERITY_WARN),
            "error" | "err" | "severity_error" => Ok(Self::SEVERITY_ERROR),
            "fatal" | "severity_fatal" => Ok(Self::SEVERITY_FATAL),
            "unspecified" | "severity_unspecified" => Ok(Self::SEVERITY_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "Severity",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for Severity {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Severity {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// FieldKind
// ============================================================================

#[allow(non_upper_case_globals)]
impl FieldKind {
    /// Short-name alias for `FIELD_KIND_UNSPECIFIED`.
    pub const Unspecified: Self = Self::FIELD_KIND_UNSPECIFIED;
    /// Short-name alias for `LABEL`.
    pub const Label: Self = Self::LABEL;
    /// Short-name alias for `ATTRIBUTE`.
    pub const Attribute: Self = Self::ATTRIBUTE;
    /// Short-name alias for `MEASUREMENT`.
    pub const Measurement: Self = Self::MEASUREMENT;
    /// Short-name alias for `TRACE_ID`.
    pub const TraceId: Self = Self::TRACE_ID;
    /// Short-name alias for `SPAN_ID`.
    pub const SpanId: Self = Self::SPAN_ID;
    /// Short-name alias for `PARENT_SPAN_ID`.
    pub const ParentSpanId: Self = Self::PARENT_SPAN_ID;
    /// Short-name alias for `TIMESTAMP_NS`.
    pub const TimestampNs: Self = Self::TIMESTAMP_NS;
    /// Short-name alias for `DURATION_NS`.
    pub const DurationNs: Self = Self::DURATION_NS;
    /// Short-name alias for `FORENSIC`.
    pub const Forensic: Self = Self::FORENSIC;

    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LABEL => "label",
            Self::ATTRIBUTE => "attribute",
            Self::MEASUREMENT => "measurement",
            Self::TRACE_ID => "trace_id",
            Self::SPAN_ID => "span_id",
            Self::PARENT_SPAN_ID => "parent_span_id",
            Self::TIMESTAMP_NS => "timestamp_ns",
            Self::DURATION_NS => "duration_ns",
            Self::FORENSIC => "forensic",
            _ => "unspecified",
        }
    }

    /// True if a value of this kind is lifted from the typed payload
    /// to a dedicated envelope slot.
    #[must_use]
    pub const fn is_envelope_lifted(self) -> bool {
        matches!(
            self,
            Self::TRACE_ID | Self::SPAN_ID | Self::PARENT_SPAN_ID | Self::TIMESTAMP_NS
        )
    }
}

impl Ord for FieldKind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for FieldKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for FieldKind {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "label" => Ok(Self::LABEL),
            "attribute" => Ok(Self::ATTRIBUTE),
            "measurement" => Ok(Self::MEASUREMENT),
            "trace_id" => Ok(Self::TRACE_ID),
            "span_id" => Ok(Self::SPAN_ID),
            "parent_span_id" => Ok(Self::PARENT_SPAN_ID),
            "timestamp_ns" => Ok(Self::TIMESTAMP_NS),
            "duration_ns" => Ok(Self::DURATION_NS),
            "forensic" => Ok(Self::FORENSIC),
            "unspecified" | "field_kind_unspecified" => Ok(Self::FIELD_KIND_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "FieldKind",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for FieldKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for FieldKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// Cardinality
// ============================================================================

#[allow(non_upper_case_globals)]
impl Cardinality {
    /// Short-name alias for `CARDINALITY_UNSPECIFIED`.
    pub const Unspecified: Self = Self::CARDINALITY_UNSPECIFIED;
    /// Short-name alias for `LOW`.
    pub const Low: Self = Self::LOW;
    /// Short-name alias for `MEDIUM`.
    pub const Medium: Self = Self::MEDIUM;
    /// Short-name alias for `HIGH`.
    pub const High: Self = Self::HIGH;
    /// Short-name alias for `UNBOUNDED`.
    pub const Unbounded: Self = Self::UNBOUNDED;

    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LOW => "low",
            Self::MEDIUM => "medium",
            Self::HIGH => "high",
            Self::UNBOUNDED => "unbounded",
            _ => "unspecified",
        }
    }

    /// Numeric cap: the maximum distinct value count permitted at this
    /// level. Returns [`u64::MAX`] for `Unbounded`.
    #[must_use]
    pub const fn cap(self) -> u64 {
        match self {
            Self::LOW => 10,
            Self::MEDIUM => 10_000,
            Self::HIGH => 1_000_000,
            Self::UNBOUNDED => u64::MAX,
            _ => 0,
        }
    }

    /// True if this cardinality is permitted on a `FieldKind::Label`
    /// field.
    #[must_use]
    pub const fn is_label_compatible(self) -> bool {
        matches!(self, Self::LOW | Self::MEDIUM)
    }

    /// True if this cardinality is permitted on a
    /// `FieldKind::Measurement` field.
    #[must_use]
    pub const fn is_measurement_compatible(self) -> bool {
        !matches!(self, Self::UNBOUNDED)
    }
}

impl Ord for Cardinality {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for Cardinality {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for Cardinality {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "low" => Ok(Self::LOW),
            "medium" => Ok(Self::MEDIUM),
            "high" => Ok(Self::HIGH),
            "unbounded" => Ok(Self::UNBOUNDED),
            "unspecified" | "cardinality_unspecified" => Ok(Self::CARDINALITY_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "Cardinality",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for Cardinality {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Cardinality {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// Classification
// ============================================================================

#[allow(non_upper_case_globals)]
impl Classification {
    /// Short-name alias for `CLASSIFICATION_UNSPECIFIED`.
    pub const Unspecified: Self = Self::CLASSIFICATION_UNSPECIFIED;
    /// Short-name alias for `INTERNAL`.
    pub const Internal: Self = Self::INTERNAL;
    /// Short-name alias for `PII`.
    pub const Pii: Self = Self::PII;
    /// Short-name alias for `SECRET`.
    pub const Secret: Self = Self::SECRET;

    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::INTERNAL => "internal",
            Self::PII => "pii",
            Self::SECRET => "secret",
            _ => "unspecified",
        }
    }
}

impl Ord for Classification {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for Classification {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for Classification {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "internal" => Ok(Self::INTERNAL),
            "pii" => Ok(Self::PII),
            "secret" => Ok(Self::SECRET),
            "unspecified" | "classification_unspecified" => Ok(Self::CLASSIFICATION_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "Classification",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for Classification {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Classification {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// MetricKind
// ============================================================================

#[allow(non_upper_case_globals)]
impl MetricKind {
    /// Short-name alias for `METRIC_KIND_UNSPECIFIED`.
    pub const Unspecified: Self = Self::METRIC_KIND_UNSPECIFIED;
    /// Short-name alias for `METRIC_KIND_COUNTER`.
    pub const Counter: Self = Self::METRIC_KIND_COUNTER;
    /// Short-name alias for `METRIC_KIND_GAUGE`.
    pub const Gauge: Self = Self::METRIC_KIND_GAUGE;
    /// Short-name alias for `METRIC_KIND_HISTOGRAM`.
    pub const Histogram: Self = Self::METRIC_KIND_HISTOGRAM;

    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::METRIC_KIND_COUNTER => "counter",
            Self::METRIC_KIND_GAUGE => "gauge",
            Self::METRIC_KIND_HISTOGRAM => "histogram",
            _ => "unspecified",
        }
    }
}

impl Ord for MetricKind {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for MetricKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for MetricKind {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "counter" | "metric_kind_counter" => Ok(Self::METRIC_KIND_COUNTER),
            "gauge" | "metric_kind_gauge" => Ok(Self::METRIC_KIND_GAUGE),
            "histogram" | "metric_kind_histogram" => Ok(Self::METRIC_KIND_HISTOGRAM),
            "unspecified" | "metric_kind_unspecified" => Ok(Self::METRIC_KIND_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "MetricKind",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for MetricKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for MetricKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ============================================================================
// SamplingReason
// ============================================================================

#[allow(non_upper_case_globals)]
impl SamplingReason {
    /// Short-name alias for `SAMPLING_REASON_UNSPECIFIED`.
    pub const Unspecified: Self = Self::SAMPLING_REASON_UNSPECIFIED;
    /// Short-name alias for `SAMPLING_REASON_HEAD_RATE`.
    pub const HeadRate: Self = Self::SAMPLING_REASON_HEAD_RATE;
    /// Short-name alias for `SAMPLING_REASON_TAIL_ERROR`.
    pub const TailError: Self = Self::SAMPLING_REASON_TAIL_ERROR;
    /// Short-name alias for `SAMPLING_REASON_SLOW`.
    pub const Slow: Self = Self::SAMPLING_REASON_SLOW;
    /// Short-name alias for `SAMPLING_REASON_FORENSIC`.
    pub const Forensic: Self = Self::SAMPLING_REASON_FORENSIC;
    /// Short-name alias for `SAMPLING_REASON_AUDIT`.
    pub const Audit: Self = Self::SAMPLING_REASON_AUDIT;
    /// Short-name alias for `SAMPLING_REASON_RUNTIME`.
    pub const Runtime: Self = Self::SAMPLING_REASON_RUNTIME;
    /// Short-name alias for `SAMPLING_REASON_OVERRIDE`.
    pub const Override: Self = Self::SAMPLING_REASON_OVERRIDE;

    /// Stable string label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SAMPLING_REASON_HEAD_RATE => "head_rate",
            Self::SAMPLING_REASON_TAIL_ERROR => "tail_error",
            Self::SAMPLING_REASON_SLOW => "slow",
            Self::SAMPLING_REASON_FORENSIC => "forensic",
            Self::SAMPLING_REASON_AUDIT => "audit",
            Self::SAMPLING_REASON_RUNTIME => "runtime",
            Self::SAMPLING_REASON_OVERRIDE => "override",
            _ => "unspecified",
        }
    }
}

impl Ord for SamplingReason {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as i32).cmp(&(*other as i32))
    }
}

impl PartialOrd for SamplingReason {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for SamplingReason {
    type Err = UnknownVariant;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "head_rate" => Ok(Self::SAMPLING_REASON_HEAD_RATE),
            "tail_error" => Ok(Self::SAMPLING_REASON_TAIL_ERROR),
            "slow" => Ok(Self::SAMPLING_REASON_SLOW),
            "forensic" => Ok(Self::SAMPLING_REASON_FORENSIC),
            "audit" => Ok(Self::SAMPLING_REASON_AUDIT),
            "runtime" => Ok(Self::SAMPLING_REASON_RUNTIME),
            "override" => Ok(Self::SAMPLING_REASON_OVERRIDE),
            "unspecified" | "sampling_reason_unspecified" => Ok(Self::SAMPLING_REASON_UNSPECIFIED),
            _ => Err(UnknownVariant {
                kind: "SamplingReason",
                value: s.to_string(),
            }),
        }
    }
}

impl serde::Serialize for SamplingReason {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for SamplingReason {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <std::string::String as serde::Deserialize>::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_short_name_aliases() {
        assert_eq!(Severity::Info, Severity::SEVERITY_INFO);
        assert_eq!(Severity::Warn, Severity::SEVERITY_WARN);
    }

    #[test]
    fn test_severity_ord_by_discriminant() {
        assert!(Severity::Info < Severity::Warn);
        assert!(Severity::Error < Severity::Fatal);
    }

    #[test]
    fn test_severity_otlp_number() {
        assert_eq!(Severity::Info.otlp_number(), 9);
        assert_eq!(Severity::Fatal.otlp_number(), 21);
    }

    #[test]
    fn test_severity_from_str_aliases() {
        assert_eq!("warning".parse::<Severity>().unwrap(), Severity::Warn);
        assert_eq!("err".parse::<Severity>().unwrap(), Severity::Error);
        assert_eq!("INFO".parse::<Severity>().unwrap(), Severity::Info);
        assert_eq!("SEVERITY_WARN".parse::<Severity>().unwrap(), Severity::Warn,);
    }

    #[test]
    fn test_tier_as_str_and_from_str() {
        assert_eq!(Tier::Log.as_str(), "log");
        assert_eq!("AUDIT".parse::<Tier>().unwrap(), Tier::Audit);
    }

    #[test]
    fn test_cardinality_caps_and_compat() {
        assert_eq!(Cardinality::Low.cap(), 10);
        assert_eq!(Cardinality::Medium.cap(), 10_000);
        assert_eq!(Cardinality::High.cap(), 1_000_000);
        assert_eq!(Cardinality::Unbounded.cap(), u64::MAX);
        assert!(Cardinality::Low.is_label_compatible());
        assert!(!Cardinality::High.is_label_compatible());
    }

    #[test]
    fn test_field_kind_envelope_lifted() {
        assert!(FieldKind::TraceId.is_envelope_lifted());
        assert!(FieldKind::SpanId.is_envelope_lifted());
        assert!(!FieldKind::Label.is_envelope_lifted());
    }

    #[test]
    fn test_serde_roundtrip_via_short_name() {
        let json = serde_json::to_string(&Severity::Info).unwrap();
        assert_eq!(json, "\"info\"");
        let back: Severity = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Severity::Info);
    }
}
