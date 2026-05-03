//! W3C Trace Context propagation. Spec 20 § 2.6 / spec 40 § 1.

use http::HeaderMap;

/// Parsed W3C trace context.
#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    /// 32-character hex trace id.
    pub trace_id: String,
    /// 16-character hex span id.
    pub span_id: String,
    /// `01` (sampled) or `00`.
    pub flags: String,
    /// Optional `tracestate` header value (vendor-specific).
    pub tracestate: String,
}

impl TraceContext {
    /// True if `flags & 0x01 == 1`.
    #[must_use]
    pub fn sampled(&self) -> bool {
        self.flags.ends_with('1')
    }
}

/// W3C `traceparent` header propagator.
#[derive(Debug, Clone, Copy, Default)]
pub struct W3cPropagator;

impl W3cPropagator {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Parse `traceparent` from headers. Returns `None` if the header
    /// is missing or malformed.
    #[must_use]
    pub fn extract(&self, headers: &HeaderMap) -> Option<TraceContext> {
        let raw = headers.get("traceparent")?.to_str().ok()?;
        // Format: `00-<trace_id>-<span_id>-<flags>` (4 hyphen-
        // separated fields).
        let parts: Vec<&str> = raw.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }
        let trace_id = parts[1];
        let span_id = parts[2];
        let flags = parts[3];
        if trace_id.len() != 32 || span_id.len() != 16 || flags.len() != 2 {
            return None;
        }
        let tracestate = headers
            .get("tracestate")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        Some(TraceContext {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            flags: flags.to_string(),
            tracestate,
        })
    }

    /// Render `traceparent` and `tracestate` headers from `ctx`.
    pub fn inject(&self, headers: &mut HeaderMap, ctx: &TraceContext) {
        let value = format!("00-{}-{}-{}", ctx.trace_id, ctx.span_id, ctx.flags);
        if let Ok(v) = http::HeaderValue::from_str(&value) {
            headers.insert("traceparent", v);
        }
        if !ctx.tracestate.is_empty() {
            if let Ok(v) = http::HeaderValue::from_str(&ctx.tracestate) {
                headers.insert("tracestate", v);
            }
        }
    }
}

/// Map an HTTP status code to one of `2xx`, `3xx`, `4xx`, `5xx`,
/// `err`. Spec 40 § 2.
#[must_use]
pub fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "err",
    }
}

/// Generate a fresh 16-byte trace id rendered as 32 lowercase hex
/// characters.
#[must_use]
pub fn fresh_trace_id() -> String {
    let h = blake3_hash_64(&now_ns().to_le_bytes());
    let h2 = blake3_hash_64(&h.to_le_bytes());
    format!("{h:016x}{h2:016x}")
}

/// Generate a fresh 8-byte span id rendered as 16 lowercase hex
/// characters.
#[must_use]
pub fn fresh_span_id() -> String {
    let h = blake3_hash_64(&now_ns().to_le_bytes());
    format!("{h:016x}")
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn blake3_hash_64(bytes: &[u8]) -> u64 {
    // Light-weight hash without pulling in `blake3`. Use std's
    // `DefaultHasher` (siphash); good enough for synthetic ids.
    use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
    let mut h =
        BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default().build_hasher();
    h.write(bytes);
    h.finish()
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;

    use super::*;

    #[test]
    fn test_propagator_round_trip() {
        let mut headers = HeaderMap::new();
        let ctx_in = TraceContext {
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            span_id: "0123456789abcdef".to_string(),
            flags: "01".to_string(),
            tracestate: "vendor=value".to_string(),
        };
        let prop = W3cPropagator::new();
        prop.inject(&mut headers, &ctx_in);
        let ctx_out = prop.extract(&headers).expect("parse");
        assert_eq!(ctx_in.trace_id, ctx_out.trace_id);
        assert_eq!(ctx_in.span_id, ctx_out.span_id);
        assert_eq!(ctx_in.flags, ctx_out.flags);
        assert_eq!(ctx_in.tracestate, ctx_out.tracestate);
        assert!(ctx_out.sampled());
    }

    #[test]
    fn test_status_class_should_classify() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(503), "5xx");
        assert_eq!(status_class(0), "err");
    }

    #[test]
    fn test_fresh_ids_should_be_correct_length() {
        assert_eq!(fresh_trace_id().len(), 32);
        assert_eq!(fresh_span_id().len(), 16);
    }
}
