//! `NoopObserver` — the default. Drops every envelope. Pays only one
//! TLS check + one atomic load on the emit hot path.

use obs_proto::obs::v1::ObsEnvelope;

use super::Observer;

/// The default observer: drops every envelope.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

impl Observer for NoopObserver {
    fn emit_envelope(&self, _env: ObsEnvelope) {}
    fn enabled(&self, _callsite: &crate::ObsCallsite) -> bool {
        false
    }
}
