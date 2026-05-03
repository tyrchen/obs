//! Generic payload decoders for `EventSchemaErased::render_json` and
//! `decode_to_otlp_kv`.
//!
//! These default impls walk a buffa wire-format payload using the
//! schema's [`FieldMeta`] table and project each field into either a
//! `serde_json::Map` (for `obs decode` / NDJSON sinks) or a
//! `Vec<(&'static str, OtlpValue)>` (for OTLP `LogRecord.body`).
//!
//! Per spec 14 § 8 these methods **never error** on unknown wire tags
//! or unrecognised field numbers; unknown fields are silently skipped
//! so a future-version producer does not crash older consumers. The
//! only failure mode is a truncated payload, which returns
//! [`DecodeError::Truncated`].
//!
//! Spec 14 § 8 / spec 93 P0-4.

use buffa::encoding::{Tag, WireType};
use bytes::Buf;
use obs_types::Classification;
use serde_json::{Map, Value};

use super::erased::{DecodeError, OtlpValue};
use crate::envelope::{FieldMeta, FieldRole};

/// Project a payload into `out` as a JSON object.
///
/// Each declared field becomes one `(name, value)` entry. The JSON
/// shape mirrors the wire types: `string` → `String`, `bytes` →
/// base64-encoded `String`, varints → `Number`, `bool` → `Bool`,
/// `f32`/`f64` → `Number`. Pii/Secret fields are projected as the
/// string `"<redacted>"` so the JSON output never carries the secret
/// even if the upstream caller forgot to scrub.
///
/// # Errors
///
/// Returns [`DecodeError::Truncated`] when the payload ends mid-field.
pub fn render_json_default(
    payload: &[u8],
    fields: &'static [FieldMeta],
    out: &mut Map<String, Value>,
) -> Result<(), DecodeError> {
    let mut cursor = payload;
    let mut offset: usize = 0;
    while cursor.has_remaining() {
        let before = cursor.remaining();
        let tag = match Tag::decode(&mut cursor) {
            Ok(t) => t,
            Err(_) => return Err(DecodeError::Truncated(offset)),
        };
        let consumed = before - cursor.remaining();
        offset += consumed;

        let meta = fields.iter().find(|m| m.number == tag.field_number());
        let name = meta.map(|m| m.name);
        let classification = meta
            .map(|m| m.classification)
            .unwrap_or(Classification::Unspecified);
        let is_classified = matches!(classification, Classification::Pii | Classification::Secret);

        let value = decode_field_value(&mut cursor, tag.wire_type(), &mut offset)?;
        if let Some(name) = name {
            let json = if is_classified {
                Value::String("<redacted>".to_string())
            } else {
                value_to_json(value)
            };
            out.insert(name.to_string(), json);
        }
        // Unknown field number → skip silently (forward-compat).
    }
    Ok(())
}

/// Project a payload into `out` as an OTLP `KeyValueList` body.
///
/// Same scrubbing as `render_json_default`. Unknown field numbers are
/// silently skipped (no error).
///
/// # Errors
///
/// Returns [`DecodeError::Truncated`] when the payload ends mid-field.
pub fn decode_to_otlp_kv_default(
    payload: &[u8],
    fields: &'static [FieldMeta],
    out: &mut Vec<(&'static str, OtlpValue)>,
) -> Result<(), DecodeError> {
    let mut cursor = payload;
    let mut offset: usize = 0;
    while cursor.has_remaining() {
        let before = cursor.remaining();
        let tag = match Tag::decode(&mut cursor) {
            Ok(t) => t,
            Err(_) => return Err(DecodeError::Truncated(offset)),
        };
        let consumed = before - cursor.remaining();
        offset += consumed;

        let meta = fields.iter().find(|m| m.number == tag.field_number());
        let value = decode_field_value(&mut cursor, tag.wire_type(), &mut offset)?;
        let Some(meta) = meta else { continue };

        let classification = meta.classification;
        let is_classified = matches!(classification, Classification::Pii | Classification::Secret);
        if is_classified {
            out.push((meta.name, OtlpValue::String("<redacted>".to_string())));
            continue;
        }

        // Promote MEASUREMENT fields to typed numerics where possible;
        // everything else mirrors the wire shape.
        let otlp = match (meta.role, value) {
            (FieldRole::Measurement, RawValue::Varint(v)) => OtlpValue::Int(v as i64),
            (FieldRole::Measurement, RawValue::Fixed64(v)) => OtlpValue::Double(f64::from_bits(v)),
            (FieldRole::Measurement, RawValue::Fixed32(v)) => {
                OtlpValue::Double(f64::from(f32::from_bits(v)))
            }
            (_, raw) => raw_to_otlp(raw),
        };
        out.push((meta.name, otlp));
    }
    Ok(())
}

#[derive(Debug)]
enum RawValue {
    Varint(u64),
    Fixed64(u64),
    Fixed32(u32),
    Bytes(Vec<u8>),
}

fn decode_field_value(
    cursor: &mut &[u8],
    wire: WireType,
    offset: &mut usize,
) -> Result<RawValue, DecodeError> {
    match wire {
        WireType::Varint => {
            let v = decode_varint(cursor).ok_or(DecodeError::Truncated(*offset))?;
            *offset += varint_len(v);
            Ok(RawValue::Varint(v))
        }
        WireType::Fixed64 => {
            if cursor.remaining() < 8 {
                return Err(DecodeError::Truncated(*offset));
            }
            let mut bytes = [0u8; 8];
            cursor.copy_to_slice(&mut bytes);
            *offset += 8;
            Ok(RawValue::Fixed64(u64::from_le_bytes(bytes)))
        }
        WireType::Fixed32 => {
            if cursor.remaining() < 4 {
                return Err(DecodeError::Truncated(*offset));
            }
            let mut bytes = [0u8; 4];
            cursor.copy_to_slice(&mut bytes);
            *offset += 4;
            Ok(RawValue::Fixed32(u32::from_le_bytes(bytes)))
        }
        WireType::LengthDelimited => {
            let len = decode_varint(cursor).ok_or(DecodeError::Truncated(*offset))? as usize;
            *offset += varint_len(len as u64);
            if cursor.remaining() < len {
                return Err(DecodeError::Truncated(*offset));
            }
            let mut bytes = vec![0u8; len];
            cursor.copy_to_slice(&mut bytes);
            *offset += len;
            Ok(RawValue::Bytes(bytes))
        }
        // Groups: spec 14 + obs-events do not use them; fall back to a
        // zero varint so the decoder makes progress without erroring.
        WireType::StartGroup | WireType::EndGroup => Ok(RawValue::Varint(0)),
        _ => Ok(RawValue::Varint(0)),
    }
}

fn value_to_json(v: RawValue) -> Value {
    match v {
        RawValue::Varint(n) => Value::from(n),
        RawValue::Fixed64(n) => Value::from(f64::from_bits(n)),
        RawValue::Fixed32(n) => Value::from(f64::from(f32::from_bits(n))),
        RawValue::Bytes(b) => match String::from_utf8(b.clone()) {
            Ok(s) => Value::String(s),
            // Non-UTF8: render as JSON array of bytes — JSON lacks a
            // native bytes type, and base64 would require an extra dep
            // path through the SDK; an array round-trips losslessly.
            Err(_) => Value::Array(b.into_iter().map(Value::from).collect()),
        },
    }
}

fn raw_to_otlp(v: RawValue) -> OtlpValue {
    match v {
        RawValue::Varint(n) => OtlpValue::Int(n as i64),
        RawValue::Fixed64(n) => OtlpValue::Double(f64::from_bits(n)),
        RawValue::Fixed32(n) => OtlpValue::Double(f64::from(f32::from_bits(n))),
        RawValue::Bytes(b) => match String::from_utf8(b.clone()) {
            Ok(s) => OtlpValue::String(s),
            Err(_) => OtlpValue::Bytes(b),
        },
    }
}

fn decode_varint(cursor: &mut &[u8]) -> Option<u64> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..10 {
        if !cursor.has_remaining() {
            return None;
        }
        let byte = cursor.get_u8();
        value |= u64::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
    }
    None
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use buffa::types;
    use bytes::BytesMut;
    use obs_types::Cardinality;

    use super::*;

    fn meta(name: &'static str, number: u32, role: FieldRole, c: Classification) -> FieldMeta {
        FieldMeta::new(name, number, role, Cardinality::Unspecified, c)
    }

    #[test]
    fn test_render_json_should_project_string_field() {
        let mut p = BytesMut::new();
        Tag::new(1, WireType::LengthDelimited).encode(&mut p);
        types::encode_string("alice", &mut p);
        let payload = p.freeze();

        let fields: &'static [FieldMeta] = Box::leak(
            vec![meta(
                "user",
                1,
                FieldRole::Attribute,
                Classification::Internal,
            )]
            .into_boxed_slice(),
        );
        let mut out = Map::new();
        render_json_default(&payload, fields, &mut out).expect("render");
        assert_eq!(out.get("user"), Some(&Value::String("alice".to_string())));
    }

    #[test]
    fn test_render_json_should_redact_pii_field() {
        let mut p = BytesMut::new();
        Tag::new(1, WireType::LengthDelimited).encode(&mut p);
        types::encode_string("alice@example.com", &mut p);
        let payload = p.freeze();

        let fields: &'static [FieldMeta] = Box::leak(
            vec![meta("email", 1, FieldRole::Attribute, Classification::Pii)].into_boxed_slice(),
        );
        let mut out = Map::new();
        render_json_default(&payload, fields, &mut out).expect("render");
        assert_eq!(
            out.get("email"),
            Some(&Value::String("<redacted>".to_string()))
        );
    }

    #[test]
    fn test_otlp_kv_should_promote_measurement_to_int() {
        let mut p = BytesMut::new();
        Tag::new(2, WireType::Varint).encode(&mut p);
        types::encode_uint64(1_500, &mut p);
        let payload = p.freeze();

        let fields: &'static [FieldMeta] = Box::leak(
            vec![meta(
                "latency_ms",
                2,
                FieldRole::Measurement,
                Classification::Internal,
            )]
            .into_boxed_slice(),
        );
        let mut out = Vec::new();
        decode_to_otlp_kv_default(&payload, fields, &mut out).expect("decode");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, "latency_ms");
        assert!(matches!(out[0].1, OtlpValue::Int(1_500)));
    }

    #[test]
    fn test_should_skip_unknown_field_numbers() {
        let mut p = BytesMut::new();
        Tag::new(99, WireType::LengthDelimited).encode(&mut p);
        types::encode_string("future", &mut p);
        let payload = p.freeze();

        const FIELDS: &[FieldMeta] = &[];
        let mut out = Map::new();
        render_json_default(&payload, FIELDS, &mut out).expect("render");
        assert!(out.is_empty());
    }
}
