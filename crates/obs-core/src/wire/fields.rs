//! Buffa wire-format helpers for `#[derive(Event)]` codegen.
//!
//! `EventSchema::encode_payload` writes the buffa-encoded payload bytes
//! that downstream sinks (Parquet, ClickHouse, OTLP) and the runtime
//! scrubber decode. Spec 12 § 1.2 requires the Rust-first authoring
//! path (`#[derive(Event)]`) and the proto-first path (`obs-build`) to
//! produce **byte-identical output** for the same field values.
//!
//! `obs-build` delegates to `buffa::Message::write_to` because its
//! generated structs implement `buffa::Message`. Hand-authored structs
//! under `#[derive(Event)]` do not — the derive macro therefore emits
//! per-field calls to [`BuffaEncodeField`], one tag-and-value pair per
//! field, matching the buffa wire format.
//!
//! Spec 12 § 1.2 / spec 14 § 5 / decision D6-1 (format_ver bump to 2).

use buffa::{
    encoding::{Tag, WireType},
    types,
};
use bytes::BytesMut;
use secrecy::{ExposeSecret, SecretBox, SecretString};

/// Encode a single struct field as a buffa wire-format `tag + value`
/// pair, using proto3's "skip default" semantics.
///
/// One impl per supported scalar type. The derive macro emits one
/// `self.field.buffa_encode_field(N, buf)` call per field; the trait
/// dispatch picks the right wire-format helper at compile time, so
/// the macro does not need to introspect the field's syntactic type.
pub trait BuffaEncodeField {
    /// Encode `self` under proto field number `number`. Implementations
    /// must skip emitting any bytes when `self` equals the proto3
    /// default for the type (empty string, zero integer, false, …) so
    /// the wire shape matches `buffa::Message::write_to`.
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut);
}

// ─── varint scalars ────────────────────────────────────────────────────

impl BuffaEncodeField for u32 {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self != 0 {
            Tag::new(number, WireType::Varint).encode(buf);
            types::encode_uint32(*self, buf);
        }
    }
}

impl BuffaEncodeField for u64 {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self != 0 {
            Tag::new(number, WireType::Varint).encode(buf);
            types::encode_uint64(*self, buf);
        }
    }
}

impl BuffaEncodeField for i32 {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self != 0 {
            Tag::new(number, WireType::Varint).encode(buf);
            types::encode_int32(*self, buf);
        }
    }
}

impl BuffaEncodeField for i64 {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self != 0 {
            Tag::new(number, WireType::Varint).encode(buf);
            types::encode_int64(*self, buf);
        }
    }
}

impl BuffaEncodeField for bool {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self {
            Tag::new(number, WireType::Varint).encode(buf);
            types::encode_bool(*self, buf);
        }
    }
}

// ─── fixed-width scalars ───────────────────────────────────────────────

impl BuffaEncodeField for f32 {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self != 0.0 {
            Tag::new(number, WireType::Fixed32).encode(buf);
            types::encode_float(*self, buf);
        }
    }
}

impl BuffaEncodeField for f64 {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if *self != 0.0 {
            Tag::new(number, WireType::Fixed64).encode(buf);
            types::encode_double(*self, buf);
        }
    }
}

// ─── length-delimited scalars ──────────────────────────────────────────

impl BuffaEncodeField for String {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if !self.is_empty() {
            Tag::new(number, WireType::LengthDelimited).encode(buf);
            types::encode_string(self, buf);
        }
    }
}

impl BuffaEncodeField for &str {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if !self.is_empty() {
            Tag::new(number, WireType::LengthDelimited).encode(buf);
            types::encode_string(self, buf);
        }
    }
}

impl BuffaEncodeField for Vec<u8> {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if !self.is_empty() {
            Tag::new(number, WireType::LengthDelimited).encode(buf);
            types::encode_bytes(self, buf);
        }
    }
}

impl BuffaEncodeField for &[u8] {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if !self.is_empty() {
            Tag::new(number, WireType::LengthDelimited).encode(buf);
            types::encode_bytes(self, buf);
        }
    }
}

// ─── secrecy wrappers ──────────────────────────────────────────────────
//
// `secrecy::SecretString` and `SecretBox<T>` implement Debug as the
// constant string `"[REDACTED ...]"`, so a careless `{:?}` or
// `tracing::error!(?evt)` cannot leak the value. The wire encoder calls
// `expose_secret()` only at the moment the bytes hit the payload buffer,
// after which the runtime scrubber (spec 14 § 5) re-encodes the payload
// with `<redacted-{name}>` markers before any sink sees it.
//
// Decision D6-2: SECRET-classified fields should be declared as
// `SecretString` / `SecretBox<T>` so their in-memory representation is
// also protected. PII-classified fields stay as their plain typed value
// since runtime redaction alone is sufficient for that threat model.

impl BuffaEncodeField for SecretString {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        let exposed = self.expose_secret();
        if !exposed.is_empty() {
            Tag::new(number, WireType::LengthDelimited).encode(buf);
            types::encode_string(exposed, buf);
        }
    }
}

impl BuffaEncodeField for SecretBox<Vec<u8>> {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        let exposed = self.expose_secret();
        if !exposed.is_empty() {
            Tag::new(number, WireType::LengthDelimited).encode(buf);
            types::encode_bytes(exposed, buf);
        }
    }
}

// ─── Option<T> ─────────────────────────────────────────────────────────
//
// proto3 explicit-presence is encoded by `optional` field qualifiers; for
// the SDK's purposes we treat `None` as "skip" (matching proto3 default
// elision) and `Some(v)` as the inner value's encoding.

impl<T: BuffaEncodeField> BuffaEncodeField for Option<T> {
    #[inline]
    fn buffa_encode_field(&self, number: u32, buf: &mut BytesMut) {
        if let Some(v) = self {
            v.buffa_encode_field(number, buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify the trait emits proto3-default elision for zero values.
    #[test]
    fn test_should_skip_default_string() {
        let mut buf = BytesMut::new();
        String::new().buffa_encode_field(1, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_should_emit_nonempty_string() {
        let mut buf = BytesMut::new();
        "hello".to_string().buffa_encode_field(1, &mut buf);
        // Tag (field=1, wire=2 LD) varint = 0x0A; then length prefix 5; then 'h','e','l','l','o'.
        assert_eq!(&buf[..], &[0x0A, 0x05, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_should_skip_default_uint64() {
        let mut buf = BytesMut::new();
        0u64.buffa_encode_field(2, &mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_should_emit_nonzero_uint64() {
        let mut buf = BytesMut::new();
        42u64.buffa_encode_field(2, &mut buf);
        // Tag (field=2, wire=0 Varint) = 0x10; then varint 42 = 0x2A.
        assert_eq!(&buf[..], &[0x10, 0x2A]);
    }

    #[test]
    fn test_should_emit_secret_string_via_expose() {
        let mut buf = BytesMut::new();
        let secret = SecretString::from("topsecret");
        secret.buffa_encode_field(3, &mut buf);
        // Same wire bytes as a plain String would produce — the
        // scrubber, not the encoder, is responsible for redaction.
        assert!(buf.starts_with(&[0x1A, 0x09]));
        assert!(buf.ends_with(b"topsecret"));
    }

    #[test]
    fn test_secret_string_debug_should_be_redacted() {
        let secret = SecretString::from("topsecret");
        let dbg = format!("{secret:?}");
        assert!(!dbg.contains("topsecret"));
        assert!(dbg.to_ascii_lowercase().contains("redacted"));
    }

    #[test]
    fn test_should_skip_none_option() {
        let mut buf = BytesMut::new();
        let v: Option<String> = None;
        v.buffa_encode_field(1, &mut buf);
        assert!(buf.is_empty());
    }
}
