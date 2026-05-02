//! `EventSchemaErased` — object-safe complement to `EventSchema`.

use bytes::BytesMut;
use obs_types::{Severity, Tier};

use crate::{envelope::FieldMeta, metric::MetricEmitter};

/// Sealing supertrait — only `obs-build` codegen and
/// `obs-macros::derive(Event)` may implement [`EventSchemaErased`].
/// External crates go through the codegen so we can add methods to
/// the trait without breaking downstream impls. Spec 14 KD-D49.
pub trait Sealed {}

/// Object-safe view of a single schema. Sinks consume
/// `&'static dyn EventSchemaErased` looked up via the
/// [`crate::registry::SchemaRegistry`].
///
/// **Sealed** via [`Sealed`] supertrait so external crates cannot
/// implement it directly — they must go through `obs-build` codegen
/// or `#[derive(Event)]`. This lets us add methods to the trait
/// later (e.g. a Flatbuffers fast-path) without breaking downstream
/// impls. Spec 14 § 2 + § 11 KD-D49.
#[allow(missing_debug_implementations)]
pub trait EventSchemaErased: Sealed + Send + Sync + 'static {
    /// Stable identity (matches `EventSchema::FULL_NAME`).
    fn full_name(&self) -> &'static str;

    /// First 8 bytes of BLAKE3 over the canonical descriptor; baked
    /// at build time. Matches `EventSchema::SCHEMA_HASH`.
    fn schema_hash(&self) -> u64;

    /// Tier for routing decisions.
    fn tier(&self) -> Tier;

    /// Default severity used when the call site does not override.
    fn default_sev(&self) -> Severity;

    /// Field metadata table; same memory as `EventSchema::FIELDS`.
    fn fields(&self) -> &'static [FieldMeta];

    /// Decode the buffa-encoded payload and emit metric data points
    /// for every `FIELD_KIND_MEASUREMENT` field. Phase-1 default impl
    /// is a no-op so MEASUREMENT-bearing schemas authored in Phase 1
    /// do not error in metric sinks; Phase 2 codegen overrides this.
    /// Spec 14 § 2.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` when the payload cannot be decoded.
    fn project_metrics(
        &self,
        payload: &[u8],
        emitter: &mut dyn MetricEmitter,
    ) -> Result<(), DecodeError> {
        let _ = (payload, emitter);
        Ok(())
    }

    /// Decode the payload into a `StructArray` row whose schema matches
    /// the codegen-emitted Arrow fragment for this event type. Phase-1
    /// default impl is a stub; Phase 2 codegen lands the real
    /// implementation. Spec 14 § 2.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` when the payload cannot be decoded.
    fn decode_to_arrow_struct(
        &self,
        payload: &[u8],
        builder: &mut dyn ArrowStructBuilder,
    ) -> Result<(), DecodeError> {
        let _ = (payload, builder);
        Err(DecodeError::Invariant(
            "decode_to_arrow_struct: Phase 2 codegen not yet emitted",
        ))
    }

    /// Decode the payload into a flat `KeyValueList` body for OTLP
    /// `LogRecord.body`. Phase-1 default impl is a stub. Spec 14 § 2.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` when the payload cannot be decoded.
    fn decode_to_otlp_kv(
        &self,
        payload: &[u8],
        out: &mut Vec<(&'static str, OtlpValue)>,
    ) -> Result<(), DecodeError> {
        let _ = (payload, out);
        Err(DecodeError::Invariant(
            "decode_to_otlp_kv: Phase 2 codegen not yet emitted",
        ))
    }

    /// Render the payload as a JSON object value (no envelope).
    /// Phase-1 default impl yields an empty object; codegen overrides
    /// this with a typed projection in Phase 2 (task 2.1). Spec 14 § 2
    /// / § 4.2.
    ///
    /// # Errors
    ///
    /// Returns `DecodeError` if the payload is truncated or contains
    /// an unrecognised tag in strict mode.
    fn render_json(
        &self,
        payload: &[u8],
        out: &mut serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), DecodeError> {
        let _ = (payload, out);
        Ok(())
    }

    /// Strip / redact classified fields in place. Phase-1 default impl
    /// is a passthrough; codegen overrides this in Phase 2.
    /// Spec 14 § 2 + spec 70 § 4.
    ///
    /// # Errors
    ///
    /// Returns `ScrubError` when re-encoding the payload fails.
    fn scrub_for_log<'a>(
        &self,
        payload: &'a [u8],
        scratch: &'a mut BytesMut,
    ) -> Result<&'a [u8], ScrubError> {
        let _ = scratch;
        Ok(payload)
    }

    /// Returns the codegen-derived OTel attribute set for the per-event
    /// `event.name` plus any per-event constant attributes. Phase-1
    /// default impl returns an empty view. Spec 14 § 2.
    fn otel_attribute_view(&self) -> &'static OtelAttributeView {
        &EMPTY_OTEL_VIEW
    }
}

/// Codegen-emitted Arrow `StructArray` row builder. Phase-1 ships only
/// the trait shape; the real impl lives in `obs-parquet`/`obs-clickhouse`
/// in Phase 4A. Spec 14 § 2.
pub trait ArrowStructBuilder: Send {
    /// Append one row's worth of fields to the underlying builder.
    /// The codegen impl calls `append_*` methods per declared field.
    fn append_null(&mut self);
}

/// OTLP `AnyValue` substitute for the Phase-1 surface. The real OTLP
/// types live in `obs-otel` (Phase 3 task 3.8); we use a small
/// substitute here so `EventSchemaErased::decode_to_otlp_kv` can be
/// declared without a circular `obs-core ↔ obs-otel` dependency.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum OtlpValue {
    /// String body.
    String(String),
    /// 64-bit integer body.
    Int(i64),
    /// Double body.
    Double(f64),
    /// Boolean body.
    Bool(bool),
    /// Raw bytes body.
    Bytes(Vec<u8>),
}

/// View of the OTel attribute set baked into a schema at codegen time.
/// Phase-1 ships an empty struct; codegen populates it in Phase 2.
/// Spec 14 § 2 / spec 20 § 2.3.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct OtelAttributeView {
    /// `event.name` for OTLP (the schema's full name unless overridden).
    pub event_name: &'static str,
    /// Schema-constant attributes attached to every emit.
    pub constant_attrs: &'static [(&'static str, &'static str)],
}

static EMPTY_OTEL_VIEW: OtelAttributeView = OtelAttributeView {
    event_name: "",
    constant_attrs: &[],
};

/// Error returned by `EventSchemaErased::render_json` and the future
/// `decode_to_*` methods (Phase 2). Spec 14 § 2.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// Payload bytes ended mid-record.
    #[error("payload truncated at offset {0}")]
    Truncated(usize),
    /// An unrecognised wire tag and the schema is in strict mode.
    #[error("unknown wire-tag {0}")]
    UnknownTag(u32),
    /// A schema-level invariant was violated.
    #[error("invariant violated: {0}")]
    Invariant(&'static str),
}

/// Error returned by `EventSchemaErased::scrub_for_log`. Spec 14 § 2.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScrubError {
    /// Re-encoding after redaction failed.
    #[error("payload re-encode failed at field {0}")]
    ReencodeFailed(&'static str),
}
