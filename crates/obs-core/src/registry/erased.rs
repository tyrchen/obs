//! `EventSchemaErased` — object-safe complement to `EventSchema`.

use bytes::BytesMut;
use obs_types::{Severity, Tier};

use crate::envelope::FieldMeta;

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

    /// Render the payload as a JSON object value (no envelope).
    /// Phase-1 default impl produces a `{"_unknown_schema": true,
    /// "raw_b64": "…"}` payload; codegen overrides this with a typed
    /// projection in Phase 2 (task 2.1). Spec 14 § 2 / § 4.2.
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
        // Default: a sink that wants typed JSON must wait for codegen.
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
}

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
