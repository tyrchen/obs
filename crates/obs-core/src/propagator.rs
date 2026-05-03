//! W3C Trace Context propagation. Spec 20 § 2.6 / spec 40 § 1.
//!
//! Lives in `obs-core` so any sink, middleware, or app can produce or
//! consume `traceparent` / `tracestate` headers without re-implementing
//! the parser. `obs-tower` and bridge work all funnel through this
//! module. Spec 93 P1-5.

use std::sync::OnceLock;

use blake3::Hasher;
use http::HeaderMap;

/// Parsed W3C trace context.
///
/// Fields are stored as the canonical lowercase-hex strings the W3C
/// `traceparent` header uses. `trace_id` is 32 hex chars, `span_id` is
/// 16 hex chars, `flags` is 2 hex chars (`00` = unsampled, `01` =
/// sampled).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObsTraceCtx {
    /// 32-character hex trace id.
    pub trace_id: String,
    /// 16-character hex span id.
    pub span_id: String,
    /// `01` (sampled) or `00`.
    pub flags: String,
    /// Optional `tracestate` header value (vendor-specific).
    pub tracestate: String,
}

impl ObsTraceCtx {
    /// True if `flags & 0x01 == 1`.
    #[must_use]
    pub fn sampled(&self) -> bool {
        self.flags.ends_with('1')
    }

    /// Build a fresh context with a new trace id and span id, with the
    /// supplied sampling decision. Useful for client-side root spans.
    #[must_use]
    pub fn fresh(sampled: bool) -> Self {
        Self {
            trace_id: fresh_trace_id(),
            span_id: fresh_span_id(),
            flags: if sampled {
                "01".to_string()
            } else {
                "00".to_string()
            },
            tracestate: String::new(),
        }
    }

    /// Build a child context that inherits the parent trace id and
    /// flags, but mints a fresh span id.
    #[must_use]
    pub fn child_of(&self) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: fresh_span_id(),
            flags: self.flags.clone(),
            tracestate: self.tracestate.clone(),
        }
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
    pub fn extract(&self, headers: &HeaderMap) -> Option<ObsTraceCtx> {
        extract_w3c(headers)
    }

    /// Render `traceparent` and `tracestate` headers from `ctx`.
    pub fn inject(&self, headers: &mut HeaderMap, ctx: &ObsTraceCtx) {
        inject_w3c(headers, ctx);
    }
}

/// Free-function form of `W3cPropagator::extract`. Spec 93 P1-5
/// requires this name in `obs_core::propagator`.
#[must_use]
pub fn extract_w3c(headers: &HeaderMap) -> Option<ObsTraceCtx> {
    let raw = headers.get("traceparent")?.to_str().ok()?;
    let mut parts = raw.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let span_id = parts.next()?;
    let flags = parts.next()?;
    if parts.next().is_some() || version != "00" {
        return None;
    }
    if trace_id.len() != 32 || span_id.len() != 16 || flags.len() != 2 {
        return None;
    }
    if !trace_id.bytes().all(is_hex) || !span_id.bytes().all(is_hex) || !flags.bytes().all(is_hex) {
        return None;
    }
    let tracestate = headers
        .get("tracestate")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    Some(ObsTraceCtx {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        flags: flags.to_string(),
        tracestate,
    })
}

/// Free-function form of `W3cPropagator::inject`.
pub fn inject_w3c(headers: &mut HeaderMap, ctx: &ObsTraceCtx) {
    let value = format!("00-{}-{}-{}", ctx.trace_id, ctx.span_id, ctx.flags);
    if let Ok(v) = http::HeaderValue::from_str(&value) {
        headers.insert("traceparent", v);
    }
    if !ctx.tracestate.is_empty()
        && let Ok(v) = http::HeaderValue::from_str(&ctx.tracestate)
    {
        headers.insert("tracestate", v);
    }
}

/// Generate a fresh 16-byte trace id rendered as 32 lowercase hex
/// characters. Uses BLAKE3 over a CSPRNG-friendly seed (spec 71
/// randomness rule).
#[must_use]
pub fn fresh_trace_id() -> String {
    let mut buf = [0u8; 32];
    fill_random(&mut buf);
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let hash = hasher.finalize();
    let bytes = hash.as_bytes();
    let mut out = String::with_capacity(32);
    for b in &bytes[..16] {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Generate a fresh 8-byte span id rendered as 16 lowercase hex
/// characters.
#[must_use]
pub fn fresh_span_id() -> String {
    let mut buf = [0u8; 16];
    fill_random(&mut buf);
    let mut hasher = Hasher::new();
    hasher.update(&buf);
    let hash = hasher.finalize();
    let bytes = hash.as_bytes();
    let mut out = String::with_capacity(16);
    for b in &bytes[..8] {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Map an HTTP status code to one of `1xx..5xx`, `err`. Spec 40 § 2.
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

fn is_hex(b: u8) -> bool {
    b.is_ascii_hexdigit()
}

fn fill_random(buf: &mut [u8]) {
    // CSPRNG via getrandom; spec 71 § randomness rule. Falls back to a
    // BLAKE3 hash of (process counter + monotonic ns) on platforms
    // without an OS RNG, which we treat as a hard error in practice
    // (every supported target has one).
    if getrandom::fill(buf).is_ok() {
        return;
    }
    let counter = next_counter();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut hasher = Hasher::new();
    hasher.update(&counter.to_le_bytes());
    hasher.update(&now.to_le_bytes());
    let h = hasher.finalize();
    let bytes = h.as_bytes();
    for (i, byte) in buf.iter_mut().enumerate() {
        if let Some(&b) = bytes.get(i % 32) {
            *byte = b;
        }
    }
}

fn next_counter() -> u64 {
    static COUNTER: OnceLock<std::sync::atomic::AtomicU64> = OnceLock::new();
    COUNTER
        .get_or_init(|| std::sync::atomic::AtomicU64::new(0))
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;

    use super::*;

    #[test]
    fn test_propagator_round_trip() {
        let mut headers = HeaderMap::new();
        let ctx_in = ObsTraceCtx {
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            span_id: "0123456789abcdef".to_string(),
            flags: "01".to_string(),
            tracestate: "vendor=value".to_string(),
        };
        inject_w3c(&mut headers, &ctx_in);
        let ctx_out = extract_w3c(&headers).expect("parse");
        assert_eq!(ctx_in.trace_id, ctx_out.trace_id);
        assert_eq!(ctx_in.span_id, ctx_out.span_id);
        assert_eq!(ctx_in.flags, ctx_out.flags);
        assert_eq!(ctx_in.tracestate, ctx_out.tracestate);
        assert!(ctx_out.sampled());
    }

    #[test]
    fn test_extract_rejects_malformed() {
        let mut headers = HeaderMap::new();
        headers.insert("traceparent", "garbage".parse().unwrap());
        assert!(extract_w3c(&headers).is_none());
        headers.insert("traceparent", "01-aa-bb-cc".parse().unwrap());
        assert!(extract_w3c(&headers).is_none());
    }

    #[test]
    fn test_fresh_ids_should_be_correct_length_and_hex() {
        let t = fresh_trace_id();
        let s = fresh_span_id();
        assert_eq!(t.len(), 32);
        assert_eq!(s.len(), 16);
        assert!(t.bytes().all(is_hex));
        assert!(s.bytes().all(is_hex));
    }

    #[test]
    fn test_fresh_ids_are_unique() {
        let a = fresh_trace_id();
        let b = fresh_trace_id();
        assert_ne!(a, b);
    }

    #[test]
    fn test_child_of_inherits_trace_id() {
        let parent = ObsTraceCtx::fresh(true);
        let child = parent.child_of();
        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.span_id, child.span_id);
        assert_eq!(parent.flags, child.flags);
    }

    #[test]
    fn test_status_class_should_classify() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(503), "5xx");
        assert_eq!(status_class(0), "err");
    }
}
