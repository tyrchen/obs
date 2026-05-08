//! Length-prefixed envelope framing for stream transports.
//!
//! The wire record is:
//!
//! ```text
//!   [ u32 BE length ][ buffa-encoded ObsEnvelope ]
//! ```
//!
//! No per-record CRC — stream transports (vsock, unix socket, TCP) are
//! reliable byte-streams; buffa decode catches truncation. Any
//! transport that needs integrity beyond TCP-level checksums can wrap
//! the framed stream in its own MAC layer.
//!
//! Boundary-review § 3.5 (moved upstream from `tok-initd::obs::ObsEnvelopeCodec`).
//!
//! # Usage
//!
//! Reuse a single [`buffa::SizeCache`] across every envelope in one
//! flush window — [`encode_into_with_cache`] calls `clear()` before
//! each encode so the backing storage is amortised without leaking
//! stale state between envelopes.
//!
//! ```no_run
//! use obs_core::wire::envelope_codec;
//! use obs_proto::obs::v1::ObsEnvelope;
//!
//! let env = ObsEnvelope::default();
//! let mut buf = Vec::with_capacity(4096);
//! let mut cache = buffa::SizeCache::new();
//! envelope_codec::encode_into_with_cache(&env, &mut buf, &mut cache);
//!
//! // On the other side:
//! if let Some((decoded, consumed)) =
//!     envelope_codec::decode_frame(&buf, 1 << 20).expect("framing ok")
//! {
//!     assert_eq!(consumed, buf.len());
//!     let _ = decoded;
//! }
//! ```

use std::io;

use buffa::{Message, SizeCache};
use obs_proto::obs::v1::ObsEnvelope;

/// Encode `env` into `out`, length-prefixed. Reuses `out`'s capacity
/// so caller can amortise allocations across envelopes within one
/// flush window.
///
/// Internally builds a fresh `SizeCache` per call. Prefer
/// [`encode_into_with_cache`] on batched paths so the cache amortises
/// across envelopes in the same flush window.
pub fn encode_into(env: &ObsEnvelope, out: &mut Vec<u8>) {
    let mut cache = SizeCache::new();
    encode_into_with_cache(env, out, &mut cache);
}

/// Encode `env` into `out` reusing a caller-owned [`SizeCache`].
///
/// The cache is cleared before each encode so subsequent calls see a
/// fresh computation but do not reallocate the backing storage. Drop
/// the cache when the flush window closes or simply keep it alive for
/// the life of the writer task — neither mutates the envelope.
pub fn encode_into_with_cache(env: &ObsEnvelope, out: &mut Vec<u8>, cache: &mut SizeCache) {
    cache.clear();
    let len = env.compute_size(cache);
    out.reserve(4 + len as usize);
    out.extend_from_slice(&len.to_be_bytes());
    env.write_to(cache, out);
}

/// Decode one length-prefixed envelope from `buf`.
///
/// Returns `Ok(None)` when the buffer is too short to contain a full
/// length + payload; otherwise returns the decoded envelope and the
/// number of bytes consumed. Caller is responsible for draining
/// consumed bytes.
///
/// `max_frame` bounds the declared length — anything larger is treated
/// as a framing error (a well-behaved emitter never produces envelopes
/// that big; crossing the limit usually means the stream is
/// desynchronised).
///
/// # Errors
///
/// Returns `io::ErrorKind::InvalidData` when the declared length
/// exceeds `max_frame` or when buffa decoding fails.
pub fn decode_frame(buf: &[u8], max_frame: usize) -> io::Result<Option<(ObsEnvelope, usize)>> {
    let Some(prefix) = buf.get(..4) else {
        return Ok(None);
    };
    let Ok(prefix) = <[u8; 4]>::try_from(prefix) else {
        // `buf.get(..4)` already guarantees the 4-byte slice; this
        // branch is unreachable but keeps the `try_from` happy without
        // an index or panic.
        return Ok(None);
    };
    let len = u32::from_be_bytes(prefix) as usize;
    if len > max_frame {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("obs envelope frame too large: {len} > {max_frame}"),
        ));
    }
    let Some(payload) = buf.get(4..4 + len) else {
        return Ok(None);
    };
    let env = ObsEnvelope::decode_from_slice(payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some((env, 4 + len)))
}

#[cfg(test)]
mod tests {
    use obs_proto::obs::v1::{Severity as PSeverity, Tier as PTier};

    use super::*;

    fn sample_env() -> ObsEnvelope {
        ObsEnvelope {
            full_name: "obs.test.EnvelopeCodec".to_string(),
            schema_hash: 0xdead_beef,
            tier: buffa::EnumValue::Known(PTier::TIER_LOG),
            sev: buffa::EnumValue::Known(PSeverity::SEVERITY_INFO),
            ts_ns: 42,
            service: "obs-core".to_string(),
            instance: "test".to_string(),
            version: "0.0.0".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_encode_decode_round_trips() {
        let env = sample_env();
        let mut buf = Vec::new();
        encode_into(&env, &mut buf);
        let (decoded, consumed) = decode_frame(&buf, 1 << 20).expect("ok").expect("some");
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.full_name, env.full_name);
        assert_eq!(decoded.schema_hash, env.schema_hash);
    }

    #[test]
    fn test_decode_frame_returns_none_on_short_buffer() {
        let got = decode_frame(&[0, 0, 0, 4], 1 << 20).expect("no err");
        assert!(got.is_none(), "incomplete buffer must return Ok(None)");
    }

    #[test]
    fn test_decode_frame_rejects_oversize() {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&u32::MAX.to_be_bytes());
        let err = decode_frame(&buf, 1024).expect_err("must reject");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_encode_into_with_cache_amortises_across_envelopes() {
        // Two sequential encodes into the same cache must produce the
        // same bytes as two independent `encode_into` calls — proves
        // `clear()` happens on the second pass.
        let env = sample_env();
        let mut cache = SizeCache::new();
        let mut batched = Vec::new();
        encode_into_with_cache(&env, &mut batched, &mut cache);
        encode_into_with_cache(&env, &mut batched, &mut cache);

        let mut independent = Vec::new();
        encode_into(&env, &mut independent);
        encode_into(&env, &mut independent);

        assert_eq!(batched, independent);
    }
}
