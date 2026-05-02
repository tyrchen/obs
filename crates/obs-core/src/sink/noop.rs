//! `NoopSink` — discards every envelope.
//!
//! Used as the default fallback in `SinkRouter` and in tests where
//! sink behaviour is not what's being asserted.

use crate::registry::ScrubbedEnvelope;

use super::Sink;

/// A sink that drops every envelope.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSink;

impl Sink for NoopSink {
    fn deliver(&self, _env: ScrubbedEnvelope<'_>) {}
}
