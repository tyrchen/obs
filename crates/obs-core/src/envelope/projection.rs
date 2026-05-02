//! `EventSchema` — the trait codegen targets when emitting per-event
//! impls. Generic and `Sized`, so it carries associated `const`s and
//! `encode_payload` / `project` methods that do not require object
//! safety. The object-safe complement is
//! [`crate::registry::EventSchemaErased`].

use bytes::BytesMut;
use obs_proto::obs::v1::ObsEnvelope;
use obs_types::{Cardinality, Classification, FieldKind, Severity, Tier};

/// Compile-time field metadata emitted by codegen alongside each
/// `EventSchema` impl. Mirrors the proto-side `obs.v1.FieldMeta`.
#[derive(Debug, Clone, Copy)]
pub struct FieldMeta {
    /// Proto field name (`route`, `latency_ms`, …).
    pub name: &'static str,
    /// Proto field number.
    pub number: u32,
    /// Codegen-classified role.
    pub role: FieldRole,
    /// `LABEL` cardinality cap; `Unspecified` for non-labels.
    pub cardinality: Cardinality,
    /// Classification (drives redaction / SECRET strip).
    pub classification: Classification,
}

impl FieldMeta {
    /// Construct a [`FieldMeta`]. Used by codegen and tests.
    #[must_use]
    pub const fn new(
        name: &'static str,
        number: u32,
        role: FieldRole,
        cardinality: Cardinality,
        classification: Classification,
    ) -> Self {
        Self {
            name,
            number,
            role,
            cardinality,
            classification,
        }
    }
}

/// Codegen-derived classification of a field. Decoupled from the
/// `FieldKind` enum so codegen can stamp this from the descriptor
/// without re-parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FieldRole {
    /// `kind: LABEL`.
    Label,
    /// `kind: ATTRIBUTE`.
    Attribute,
    /// `kind: MEASUREMENT`.
    Measurement,
    /// `kind: TRACE_ID`.
    TraceId,
    /// `kind: SPAN_ID`.
    SpanId,
    /// `kind: PARENT_SPAN_ID`.
    ParentSpanId,
    /// `kind: TIMESTAMP_NS`.
    TimestampNs,
    /// `kind: DURATION_NS`.
    DurationNs,
    /// `kind: FORENSIC`.
    Forensic,
}

impl From<FieldKind> for FieldRole {
    fn from(k: FieldKind) -> Self {
        match k {
            FieldKind::Label => Self::Label,
            FieldKind::Attribute => Self::Attribute,
            FieldKind::Measurement => Self::Measurement,
            FieldKind::TraceId => Self::TraceId,
            FieldKind::SpanId => Self::SpanId,
            FieldKind::ParentSpanId => Self::ParentSpanId,
            FieldKind::TimestampNs => Self::TimestampNs,
            FieldKind::DurationNs => Self::DurationNs,
            FieldKind::Forensic => Self::Forensic,
            // FieldKind is #[non_exhaustive]; defensively map any
            // unrecognised future variant + Unspecified to Attribute.
            _ => Self::Attribute,
        }
    }
}

/// Trait implemented by every `Obs*` event type.
///
/// Codegen emits one impl per `.proto` message or `#[derive(Event)]`
/// struct. Sinks consume the object-safe sibling
/// [`crate::registry::EventSchemaErased`]; this trait is for the
/// monomorphised emit path.
pub trait EventSchema: Send + Sync + Sized + 'static {
    /// Fully qualified event name (`myapp.v1.ObsXxx`).
    const FULL_NAME: &'static str;
    /// Tier the schema declares.
    const TIER: Tier;
    /// Default severity the schema declares.
    const DEFAULT_SEV: Severity;
    /// Per-field metadata table.
    const FIELDS: &'static [FieldMeta];
    /// First 8 bytes of BLAKE3 over the canonical descriptor; baked at
    /// build time. See spec 10 § 6 + spec 12 § 3.5.
    const SCHEMA_HASH: u64;

    /// Encode this event's payload using buffa's encoder into a reused
    /// buffer. The codegen impl forwards to `buffa::Message::encode`.
    fn encode_payload(&self, buf: &mut BytesMut);

    /// Project labels and lift trace/span ids onto the envelope.
    /// Generated; never hand-written. See spec 11 pipeline § 4.1.
    fn project(&self, env: &mut ObsEnvelope);

    /// For `MEASUREMENT`-annotated fields, emit metric data points.
    /// Phase-1 default impl is a no-op so MEASUREMENT-bearing schemas
    /// authored in Phase 1 do not error in metric sinks; Phase 2
    /// codegen overrides this. Spec 12 § 3.2.
    fn project_metrics(&self, sink: &mut dyn crate::metric::MetricEmitter) {
        let _ = sink;
    }
}
