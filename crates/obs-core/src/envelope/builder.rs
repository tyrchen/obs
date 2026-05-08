//! Envelope construction helpers used by the emit hot path.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BytesMut;
use obs_proto::{
    __private::Message,
    obs::v1::{ObsEnvelope, SamplingReason, Severity},
};

use crate::{callsite::ObsCallsite, envelope::projection::EventSchema};

thread_local! {
    /// Per-thread reusable encode buffer. Cleared and reused every emit
    /// so steady-state has no per-event allocation (spec 11 § 5).
    static EMIT_BUF: std::cell::RefCell<BytesMut> = std::cell::RefCell::new(BytesMut::with_capacity(4096));
}

/// Newtype wrapper around the wire `ObsEnvelope` so tests and downstream
/// code don't depend directly on the buffa-generated type's
/// `Default + Clone` shape (which is private to `obs-proto`'s codegen
/// boundary).
#[derive(Debug, Clone, Default)]
pub struct Envelope(pub ObsEnvelope);

impl Envelope {
    /// Borrow the inner envelope.
    #[must_use]
    pub fn inner(&self) -> &ObsEnvelope {
        &self.0
    }

    /// Mutate the inner envelope.
    pub fn inner_mut(&mut self) -> &mut ObsEnvelope {
        &mut self.0
    }

    /// Take the inner envelope.
    #[must_use]
    pub fn into_inner(self) -> ObsEnvelope {
        self.0
    }
}

/// Build an envelope for the given event using its declared default
/// severity. The payload is encoded into a thread-local scratch buffer
/// and copied into the envelope's `payload` field; labels and lifted
/// fields are still empty here — `project()` runs next on the emit path.
///
/// Hot path. Spec 11 § 4.1 step 3.
#[must_use]
pub fn build_envelope<E: EventSchema>(callsite: &ObsCallsite, event: &E) -> ObsEnvelope {
    build_envelope_at::<E>(callsite, event, E::DEFAULT_SEV)
}

/// Like [`build_envelope`] but with a caller-specified severity (used
/// by `emit_at(sev)`).
#[must_use]
pub fn build_envelope_at<E: EventSchema>(
    callsite: &ObsCallsite,
    event: &E,
    sev: Severity,
) -> ObsEnvelope {
    let _ = callsite; // reserved: callsite metadata threading
    let payload = EMIT_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        event.encode_payload(&mut buf);
        buf.split().freeze().to_vec()
    });

    let ts_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    ObsEnvelope {
        full_name: E::FULL_NAME.to_string(),
        schema_hash: E::SCHEMA_HASH,
        tier: ::buffa::EnumValue::Known(E::TIER),
        sev: ::buffa::EnumValue::Known(sev),
        ts_ns,
        payload,
        sampling_reason: ::buffa::EnumValue::Known(SamplingReason::HeadRate),
        ..Default::default()
    }
}

/// Encode an `ObsEnvelope` into a `Vec<u8>`. Convenience for tests and
/// for sinks that ship raw envelope bytes (NDJSON, OTLP).
#[must_use]
#[allow(dead_code)] // re-emerges once Phase-3 NDJSON sink uses it
pub fn encode_envelope(env: &ObsEnvelope) -> Vec<u8> {
    let mut buf = Vec::with_capacity(env.encoded_len() as usize);
    env.encode(&mut buf);
    buf
}
