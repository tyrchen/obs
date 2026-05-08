//! Generic payload scrubber.
//!
//! `scrub_payload` walks a buffa wire-format payload and re-emits it
//! into `scratch`, redacting any field whose [`FieldMeta::classification`]
//! is `Pii` or `Secret`. The redaction policy:
//!
//! - `LengthDelimited` (string / bytes) fields are rewritten with the marker `"<redacted-{name}>"`
//!   as a UTF-8 string, preserving the wire type so downstream typed decoders continue to read a
//!   string value.
//! - Varint and Fixed-width fields are **dropped** from the output (proto3 default elision); a
//!   redacted numeric tells the operator nothing useful, so we omit it entirely rather than pretend
//!   it was zero.
//! - Fields the schema does not declare (forward-compat / unknown numbers) pass through unchanged
//!   so future producers do not lose data on older consumers.
//!
//! One default impl in [`crate::registry::EventSchemaErased::scrub_for_log`]
//! delegates to this helper — no per-schema codegen needed. Spec 14 §
//! 5 / spec 70 § 4 / spec 93 P0-1.

use buffa::{
    encoding::{Tag, WireType},
    types,
};
use bytes::{Buf, BufMut, BytesMut};
use obs_proto::obs::v1::Classification;

use super::erased::ScrubError;
use crate::envelope::FieldMeta;

/// Redact PII/SECRET fields in `payload` and return a slice of `scratch`
/// with the rewritten bytes. The caller must clear/keep `scratch` as it
/// sees fit; this helper truncates and writes from offset 0.
///
/// # Errors
///
/// Returns [`ScrubError::ReencodeFailed`] when the payload is truncated
/// mid-field. The error name records the field whose decode failed.
pub fn scrub_payload<'a>(
    payload: &'a [u8],
    fields: &'static [FieldMeta],
    scratch: &'a mut BytesMut,
) -> Result<&'a [u8], ScrubError> {
    scratch.clear();
    let mut cursor = payload;
    while cursor.has_remaining() {
        let start = cursor.remaining();
        let tag = Tag::decode(&mut cursor).map_err(|_| ScrubError::ReencodeFailed("tag"))?;
        let number = tag.field_number();
        let wire = tag.wire_type();
        let meta = fields.iter().find(|m| m.number == number);
        let classification = meta
            .map(|m| m.classification)
            .unwrap_or(Classification::Unspecified);
        let is_classified = matches!(classification, Classification::Pii | Classification::Secret);

        match wire {
            WireType::Varint => {
                let v = decode_varint_value(&mut cursor)
                    .map_err(|_| ScrubError::ReencodeFailed("varint"))?;
                if !is_classified {
                    Tag::new(number, WireType::Varint).encode(scratch);
                    encode_raw_varint(v, scratch);
                }
                // Classified varint → drop entirely.
            }
            WireType::Fixed64 => {
                if cursor.remaining() < 8 {
                    return Err(ScrubError::ReencodeFailed("fixed64"));
                }
                let mut bytes = [0u8; 8];
                cursor.copy_to_slice(&mut bytes);
                if !is_classified {
                    Tag::new(number, WireType::Fixed64).encode(scratch);
                    scratch.put_slice(&bytes);
                }
            }
            WireType::Fixed32 => {
                if cursor.remaining() < 4 {
                    return Err(ScrubError::ReencodeFailed("fixed32"));
                }
                let mut bytes = [0u8; 4];
                cursor.copy_to_slice(&mut bytes);
                if !is_classified {
                    Tag::new(number, WireType::Fixed32).encode(scratch);
                    scratch.put_slice(&bytes);
                }
            }
            WireType::LengthDelimited => {
                let len = decode_varint_value(&mut cursor)
                    .map_err(|_| ScrubError::ReencodeFailed("ld_len"))?
                    as usize;
                if cursor.remaining() < len {
                    return Err(ScrubError::ReencodeFailed("ld_payload"));
                }
                if is_classified {
                    let name = meta.map(|m| m.name).unwrap_or("field");
                    let marker = format!("<redacted-{name}>");
                    cursor.advance(len);
                    Tag::new(number, WireType::LengthDelimited).encode(scratch);
                    types::encode_string(&marker, scratch);
                } else {
                    Tag::new(number, WireType::LengthDelimited).encode(scratch);
                    encode_raw_varint(len as u64, scratch);
                    let chunk = cursor
                        .chunk()
                        .get(..len)
                        .ok_or(ScrubError::ReencodeFailed("ld_chunk"))?;
                    scratch.put_slice(chunk);
                    cursor.advance(len);
                }
            }
            WireType::StartGroup | WireType::EndGroup => {
                // Groups are deprecated in proto3 and obs-events do not
                // use them; treat as a hard error rather than silently
                // pass through.
                return Err(ScrubError::ReencodeFailed("group_unsupported"));
            }
            _ => {
                return Err(ScrubError::ReencodeFailed("unknown_wire_type"));
            }
        }
        // Defensive: every iteration must consume at least one byte.
        if cursor.remaining() == start {
            return Err(ScrubError::ReencodeFailed("no_progress"));
        }
    }
    Ok(&scratch[..])
}

fn decode_varint_value(buf: &mut &[u8]) -> Result<u64, ()> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..10 {
        if !buf.has_remaining() {
            return Err(());
        }
        let byte = buf.get_u8();
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
    Err(())
}

fn encode_raw_varint(mut value: u64, buf: &mut BytesMut) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

#[cfg(test)]
mod tests {
    use obs_proto::obs::v1::Cardinality;

    use super::*;
    use crate::envelope::FieldRole;

    fn meta(name: &'static str, number: u32, classification: Classification) -> FieldMeta {
        FieldMeta::new(
            name,
            number,
            FieldRole::Attribute,
            Cardinality::Unspecified,
            classification,
        )
    }

    #[test]
    fn test_should_redact_pii_string_with_marker() {
        // Build a payload: field 1 (string) = "alice@example.com"
        let mut payload = BytesMut::new();
        Tag::new(1, WireType::LengthDelimited).encode(&mut payload);
        types::encode_string("alice@example.com", &mut payload);
        let payload = payload.freeze();

        const FIELDS: &[FieldMeta] = &[FieldMeta::new(
            "email",
            1,
            FieldRole::Attribute,
            Cardinality::Unspecified,
            Classification::Pii,
        )];
        let mut scratch = BytesMut::new();
        let scrubbed = scrub_payload(&payload, FIELDS, &mut scratch).expect("scrub");
        let s = String::from_utf8(scrubbed.to_vec()).expect("utf8");
        assert!(!s.contains("alice@example.com"));
        assert!(s.contains("<redacted-email>"));
    }

    #[test]
    fn test_should_drop_secret_varint() {
        let mut payload = BytesMut::new();
        Tag::new(2, WireType::Varint).encode(&mut payload);
        types::encode_uint64(123_456, &mut payload);
        let payload = payload.freeze();

        const FIELDS: &[FieldMeta] = &[FieldMeta::new(
            "key_id",
            2,
            FieldRole::Attribute,
            Cardinality::Unspecified,
            Classification::Secret,
        )];
        let mut scratch = BytesMut::new();
        let out = scrub_payload(&payload, FIELDS, &mut scratch).expect("scrub");
        // Secret varint → dropped entirely.
        assert!(out.is_empty());
    }

    #[test]
    fn test_should_passthrough_internal_fields() {
        let mut payload = BytesMut::new();
        Tag::new(1, WireType::LengthDelimited).encode(&mut payload);
        types::encode_string("public", &mut payload);
        Tag::new(2, WireType::Varint).encode(&mut payload);
        types::encode_uint64(42, &mut payload);
        let original = payload.clone().freeze();

        let fields: Vec<FieldMeta> = vec![
            meta("route", 1, Classification::Internal),
            meta("count", 2, Classification::Internal),
        ];
        let fields_static: &'static [FieldMeta] = Box::leak(fields.into_boxed_slice());
        let mut scratch = BytesMut::new();
        let out = scrub_payload(&original, fields_static, &mut scratch).expect("scrub");
        assert_eq!(out, &original[..]);
    }

    #[test]
    fn test_should_passthrough_unknown_field_numbers() {
        // Producer encoded a field number the consumer does not know.
        // We must pass it through untouched (forward compat).
        let mut payload = BytesMut::new();
        Tag::new(99, WireType::LengthDelimited).encode(&mut payload);
        types::encode_string("future", &mut payload);
        let original = payload.clone().freeze();

        const FIELDS: &[FieldMeta] = &[];
        let mut scratch = BytesMut::new();
        let out = scrub_payload(&original, FIELDS, &mut scratch).expect("scrub");
        assert_eq!(out, &original[..]);
    }

    #[test]
    fn test_should_redact_only_classified_in_mixed_payload() {
        let mut payload = BytesMut::new();
        Tag::new(1, WireType::LengthDelimited).encode(&mut payload);
        types::encode_string("public", &mut payload);
        Tag::new(2, WireType::LengthDelimited).encode(&mut payload);
        types::encode_string("topsecret", &mut payload);
        let payload = payload.freeze();

        let fields: Vec<FieldMeta> = vec![
            meta("route", 1, Classification::Internal),
            meta("password", 2, Classification::Secret),
        ];
        let fields_static: &'static [FieldMeta] = Box::leak(fields.into_boxed_slice());
        let mut scratch = BytesMut::new();
        let out = scrub_payload(&payload, fields_static, &mut scratch).expect("scrub");
        let s = String::from_utf8(out.to_vec()).expect("utf8");
        assert!(s.contains("public"));
        assert!(!s.contains("topsecret"));
        assert!(s.contains("<redacted-password>"));
    }
}
