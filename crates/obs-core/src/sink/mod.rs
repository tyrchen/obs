//! Sinks consume `ScrubbedEnvelope` and ship it to a destination
//! (stdout, NDJSON file, OTLP, ClickHouse, etc.).
//!
//! Phase-1 ships only `NoopSink`, `InMemorySink` (test harness), and
//! `StdoutSink` with `FormatterStyle::Full`. The `Sink` trait shape
//! and lifecycle (`flush`, `shutdown`) match spec 11 § 4 / spec 14 § 5.

mod in_memory;
mod noop;
mod stdout;

use std::{future::Future, pin::Pin};

pub use self::{
    in_memory::{InMemoryHandle, InMemorySink},
    noop::NoopSink,
    stdout::{FormatterStyle, StdoutSink},
};
use crate::registry::ScrubbedEnvelope;

/// Pinned future returned by `Sink::flush` / `Sink::shutdown`. Spec 11 § 4
/// uses this exact shape so `Sink` is object-safe (CLAUDE.md async-trait
/// exception).
pub type SinkFut<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// A delivery destination. Called from per-tier worker tasks, never on
/// the emit thread.
pub trait Sink: Send + Sync + 'static {
    /// Hand the envelope to the sink. **Must not block**; long IO is
    /// queued internally. Spec 11 § 4 / spec 14 § 5.
    fn deliver(&self, env: ScrubbedEnvelope<'_>);

    /// Flush in-flight batches; awaits IO if needed.
    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async {})
    }

    /// Shut down (drain + close). Idempotent.
    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async {})
    }
}
