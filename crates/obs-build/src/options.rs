//! Decoder for the `(obs.v1.event)` / `(obs.v1.field)` custom options
//! stored as `__buffa_unknown_fields` on the buffa-generated
//! `MessageOptions`/`FieldOptions`.
//!
//! The bytes follow the standard protobuf wire format:
//!
//! ```text
//!   tag = (field_number << 3) | wire_type
//!   field_number = 80001  (event extension)
//!   field_number = 80002  (field extension)
//!   wire_type    = 2      (LEN-delimited; submessage)
//! ```
//!
//! 80001 << 3 | 2 = 640010 = varint `0x8A 0x88 0x27`
//! 80002 << 3 | 2 = 640018 = varint `0x92 0x88 0x27`
//!
//! Inside the LEN payload we decode an `EventMeta` / `FieldMeta` whose
//! shape is fixed (spec 12 § 2). See `docs/research/spike-buffa-reflect.md`
//! for the validation memo.

use obs_types::{Cardinality, Classification, FieldKind, MetricKind, Severity, Tier};

/// Tag prefix for `(obs.v1.event)` (field 80001, wire type 2).
const EVENT_TAG_BYTES: [u8; 3] = [0x8A, 0x88, 0x27];
/// Tag prefix for `(obs.v1.field)` (field 80002, wire type 2).
const FIELD_TAG_BYTES: [u8; 3] = [0x92, 0x88, 0x27];

/// Decoded `(obs.v1.event)` payload.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct EventOptions {
    /// Tier declared by the schema; default `Tier::Log` if absent.
    pub tier: Option<Tier>,
    /// Default severity; default `Severity::Info` if absent.
    pub default_sev: Option<Severity>,
    /// Sibling full_name when this event participates in a
    /// Started/Completed pair (spec 93 P1-7).
    pub paired_with: Option<String>,
}

/// Decoded `(obs.v1.field)` payload.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct FieldOptions {
    /// Field role.
    pub kind: Option<FieldKind>,
    /// Cardinality cap.
    pub cardinality: Option<Cardinality>,
    /// PII / SECRET classification.
    pub classification: Option<Classification>,
    /// Metric spec when `kind = MEASUREMENT`.
    pub metric: Option<MetricSpec>,
}

/// Decoded `(obs.v1.MetricSpec)`.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct MetricSpec {
    /// Counter / Gauge / Histogram.
    pub kind: Option<MetricKind>,
    /// UCUM unit (`"ms"`, `"By"`, `"1"`, …).
    pub unit: Option<String>,
    /// Histogram buckets.
    pub bounds: Vec<f64>,
}

/// Errors returned by the option scanner.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CodegenError {
    /// The protoc invocation failed.
    #[error("protoc failed: {0}")]
    Protoc(String),

    /// The descriptor set could not be read.
    #[error("descriptor set IO: {0}")]
    DescriptorIo(#[source] std::io::Error),

    /// The descriptor set could not be decoded.
    #[error("descriptor decode failed: {0}")]
    DescriptorDecode(String),

    /// The buffa-build invocation failed.
    #[error("buffa-build failed: {0}")]
    Buffa(String),

    /// The custom option bytes could not be decoded.
    #[error("option decode failed for `{path}`: {detail}")]
    OptionDecode {
        /// Fully qualified message/field path.
        path: String,
        /// Human-readable detail.
        detail: String,
    },

    /// IO error while writing generated files.
    #[error("output IO: {0}")]
    OutputIo(#[source] std::io::Error),
}

/// Scan an `__buffa_unknown_fields` byte string for the
/// `(obs.v1.event)` extension and return the decoded payload.
///
/// Returns `Ok(None)` if the option is absent; `Err` only when the
/// bytes are malformed (truncated, invalid varint, etc.).
///
/// # Errors
///
/// Returns [`CodegenError::OptionDecode`] when the wire payload is
/// truncated or contains an invalid sub-message.
#[doc(hidden)]
pub fn read_event_options(bytes: &[u8], path: &str) -> Result<Option<EventOptions>, CodegenError> {
    let Some(payload) = find_tag_payload(bytes, &EVENT_TAG_BYTES) else {
        return Ok(None);
    };
    let mut out = EventOptions::default();
    walk_message(payload, |field, kind, value| {
        match (field, kind) {
            // tier (1, varint)
            (1, WireKind::Varint) => {
                if let Some(v) = value.varint() {
                    out.tier = decode_tier(v as i32);
                }
            }
            // default_sev (2, varint)
            (2, WireKind::Varint) => {
                if let Some(v) = value.varint() {
                    out.default_sev = decode_severity(v as i32);
                }
            }
            // paired_with (3, length-delimited string) — spec 93 P1-7.
            (3, WireKind::Length) => {
                if let Some(s) = value.length()
                    && let Ok(s) = std::str::from_utf8(s)
                {
                    out.paired_with = Some(s.to_string());
                }
            }
            _ => {}
        }
    })
    .map_err(|detail| CodegenError::OptionDecode {
        path: path.to_string(),
        detail: detail.to_string(),
    })?;
    Ok(Some(out))
}

/// Scan an `__buffa_unknown_fields` byte string for the
/// `(obs.v1.field)` extension. See [`read_event_options`].
///
/// # Errors
///
/// Returns [`CodegenError::OptionDecode`] when the wire payload is
/// truncated or contains an invalid sub-message.
#[doc(hidden)]
pub fn read_field_options(bytes: &[u8], path: &str) -> Result<Option<FieldOptions>, CodegenError> {
    let Some(payload) = find_tag_payload(bytes, &FIELD_TAG_BYTES) else {
        return Ok(None);
    };
    let mut out = FieldOptions::default();
    walk_message(payload, |field, kind, value| match (field, kind) {
        (1, WireKind::Varint) => {
            out.kind = value.varint().and_then(|v| decode_field_kind(v as i32))
        }
        (2, WireKind::Varint) => {
            out.cardinality = value.varint().and_then(|v| decode_cardinality(v as i32))
        }
        (3, WireKind::Varint) => {
            out.classification = value.varint().and_then(|v| decode_classification(v as i32))
        }
        (4, WireKind::Length) => {
            if let Some(submsg) = value.length() {
                let mut spec = MetricSpec::default();
                let _ = walk_message(submsg, |sf, sk, sv| match (sf, sk) {
                    (1, WireKind::Varint) => {
                        spec.kind = sv.varint().and_then(|v| decode_metric_kind(v as i32))
                    }
                    (2, WireKind::Length) => {
                        if let Some(s) = sv.length()
                            && let Ok(s) = std::str::from_utf8(s)
                        {
                            spec.unit = Some(s.to_string());
                        }
                    }
                    (3, WireKind::Length) => {
                        // Packed repeated double — 8 bytes each.
                        if let Some(s) = sv.length() {
                            for chunk in s.chunks_exact(8) {
                                if let Ok(arr) = <[u8; 8]>::try_from(chunk) {
                                    spec.bounds.push(f64::from_le_bytes(arr));
                                }
                            }
                        }
                    }
                    (3, WireKind::Fixed64) => {
                        if let Some(b) = sv.fixed64() {
                            spec.bounds.push(f64::from_le_bytes(b));
                        }
                    }
                    _ => {}
                });
                out.metric = Some(spec);
            }
        }
        _ => {}
    })
    .map_err(|detail| CodegenError::OptionDecode {
        path: path.to_string(),
        detail: detail.to_string(),
    })?;
    Ok(Some(out))
}

fn decode_tier(i: i32) -> Option<Tier> {
    Some(match i {
        1 => Tier::Log,
        2 => Tier::Metric,
        3 => Tier::Trace,
        4 => Tier::Audit,
        _ => Tier::Unspecified,
    })
}

fn decode_severity(i: i32) -> Option<Severity> {
    Some(match i {
        1 => Severity::Trace,
        2 => Severity::Debug,
        3 => Severity::Info,
        4 => Severity::Warn,
        5 => Severity::Error,
        6 => Severity::Fatal,
        _ => Severity::Unspecified,
    })
}

fn decode_field_kind(i: i32) -> Option<FieldKind> {
    Some(match i {
        1 => FieldKind::Label,
        2 => FieldKind::Attribute,
        3 => FieldKind::Measurement,
        4 => FieldKind::TraceId,
        5 => FieldKind::SpanId,
        6 => FieldKind::ParentSpanId,
        7 => FieldKind::TimestampNs,
        8 => FieldKind::DurationNs,
        9 => FieldKind::Forensic,
        _ => FieldKind::Unspecified,
    })
}

fn decode_cardinality(i: i32) -> Option<Cardinality> {
    Some(match i {
        1 => Cardinality::Low,
        2 => Cardinality::Medium,
        3 => Cardinality::High,
        4 => Cardinality::Unbounded,
        _ => Cardinality::Unspecified,
    })
}

fn decode_classification(i: i32) -> Option<Classification> {
    Some(match i {
        1 => Classification::Internal,
        2 => Classification::Pii,
        3 => Classification::Secret,
        _ => Classification::Unspecified,
    })
}

fn decode_metric_kind(i: i32) -> Option<MetricKind> {
    Some(match i {
        1 => MetricKind::Counter,
        2 => MetricKind::Gauge,
        3 => MetricKind::Histogram,
        _ => MetricKind::Unspecified,
    })
}

// ─── Minimal protobuf wire-format scanner ──────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum WireKind {
    Varint,
    Fixed64,
    Length,
    Fixed32,
}

enum WireValue<'a> {
    Varint(u64),
    Fixed64([u8; 8]),
    Length(&'a [u8]),
    #[allow(dead_code)] // emitted by walk_message; no current consumer
    Fixed32([u8; 4]),
}

impl<'a> WireValue<'a> {
    fn varint(&self) -> Option<u64> {
        match self {
            Self::Varint(v) => Some(*v),
            _ => None,
        }
    }
    fn fixed64(&self) -> Option<[u8; 8]> {
        match self {
            Self::Fixed64(v) => Some(*v),
            _ => None,
        }
    }
    fn length(&self) -> Option<&'a [u8]> {
        match self {
            Self::Length(s) => Some(*s),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct WireScanError(&'static str);

impl std::fmt::Display for WireScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Find the LEN payload of the first occurrence of a specific tag
/// prefix in the byte stream. Used to locate the `(obs.v1.event)` or
/// `(obs.v1.field)` extension payload.
fn find_tag_payload<'a>(bytes: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut i = 0;
    while i + tag.len() <= bytes.len() {
        if &bytes[i..i + tag.len()] == tag {
            // Tag matched; next is varint LEN.
            let mut j = i + tag.len();
            let (len, consumed) = read_varint(&bytes[j..]).ok()?;
            j += consumed;
            let start = j;
            let end = start.checked_add(len as usize)?;
            if end > bytes.len() {
                return None;
            }
            return Some(&bytes[start..end]);
        }
        i += 1;
    }
    None
}

fn walk_message<F>(payload: &[u8], mut visit: F) -> Result<(), WireScanError>
where
    F: FnMut(u32, WireKind, WireValue<'_>),
{
    let mut i = 0;
    while i < payload.len() {
        let (tag, consumed) =
            read_varint(&payload[i..]).map_err(|_| WireScanError("invalid tag varint"))?;
        i += consumed;
        let field = (tag >> 3) as u32;
        let wire = tag & 0b111;
        match wire {
            0 => {
                let (v, c) = read_varint(&payload[i..])
                    .map_err(|_| WireScanError("invalid value varint"))?;
                i += c;
                visit(field, WireKind::Varint, WireValue::Varint(v));
            }
            1 => {
                if i + 8 > payload.len() {
                    return Err(WireScanError("truncated fixed64"));
                }
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&payload[i..i + 8]);
                i += 8;
                visit(field, WireKind::Fixed64, WireValue::Fixed64(arr));
            }
            2 => {
                let (len, c) =
                    read_varint(&payload[i..]).map_err(|_| WireScanError("invalid LEN varint"))?;
                i += c;
                let end = i
                    .checked_add(len as usize)
                    .ok_or(WireScanError("LEN overflow"))?;
                if end > payload.len() {
                    return Err(WireScanError("truncated LEN payload"));
                }
                visit(field, WireKind::Length, WireValue::Length(&payload[i..end]));
                i = end;
            }
            5 => {
                if i + 4 > payload.len() {
                    return Err(WireScanError("truncated fixed32"));
                }
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&payload[i..i + 4]);
                i += 4;
                visit(field, WireKind::Fixed32, WireValue::Fixed32(arr));
            }
            _ => return Err(WireScanError("unknown wire type")),
        }
    }
    Ok(())
}

fn read_varint(bytes: &[u8]) -> Result<(u64, usize), &'static str> {
    let mut v: u64 = 0;
    let mut shift = 0u32;
    for (idx, b) in bytes.iter().enumerate().take(10) {
        v |= ((*b & 0x7f) as u64) << shift;
        if (*b & 0x80) == 0 {
            return Ok((v, idx + 1));
        }
        shift += 7;
    }
    Err("varint too long")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_decode_event_options_from_spike_payload() {
        // Bytes captured from docs/research/spike-buffa-reflect.md:
        // 8a 88 27 04 08 01 10 03 → (obs.v1.event) = { tier: 1, default_sev: 3 }
        let bytes = [0x8a, 0x88, 0x27, 0x04, 0x08, 0x01, 0x10, 0x03];
        let opts = read_event_options(&bytes, "test").unwrap().unwrap();
        assert_eq!(opts.tier, Some(Tier::Log));
        assert_eq!(opts.default_sev, Some(Severity::Info));
    }

    #[test]
    fn test_should_decode_field_options_from_spike_payload() {
        // 92 88 27 06 08 02 10 03 18 02 → (obs.v1.field) = {
        //     kind: ATTRIBUTE(2), cardinality: HIGH(3), classification: PII(2)
        // }
        let bytes = [0x92, 0x88, 0x27, 0x06, 0x08, 0x02, 0x10, 0x03, 0x18, 0x02];
        let opts = read_field_options(&bytes, "test").unwrap().unwrap();
        assert_eq!(opts.kind, Some(FieldKind::Attribute));
        assert_eq!(opts.cardinality, Some(Cardinality::High));
        assert_eq!(opts.classification, Some(Classification::Pii));
    }

    #[test]
    fn test_should_return_none_when_tag_absent() {
        let bytes = [0x00, 0x01, 0x02];
        assert!(read_event_options(&bytes, "test").unwrap().is_none());
    }
}
