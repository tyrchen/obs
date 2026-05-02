//! Sinks consume `ScrubbedEnvelope` and ship it to a destination
//! (stdout, NDJSON file, OTLP, ClickHouse, etc.).
//!
//! Phase-3 surface (impl-plan tasks 3.7 / 3.12 + 3.1 worker pool):
//!
//! - `Sink` trait + `flush` / `shutdown` lifecycle.
//! - `NoopSink`, `InMemorySink` (test harness).
//! - `StdoutSink` with all four `FormatterStyle`s.
//! - `NdjsonFileSink` over `RollingFileWriter`.
//! - `MakeWriter` family: `StdoutWriter`, `StderrWriter`, `LevelSplitWriter`, `TeeWriter`,
//!   `RollingFileWriter`, `NonBlockingWriter`.

mod in_memory;
mod ndjson;
mod noop;
mod stdout;
pub(crate) mod writer;

use std::{future::Future, pin::Pin};

pub use self::{
    in_memory::{InMemoryHandle, InMemorySink},
    ndjson::NdjsonFileSink,
    noop::NoopSink,
    stdout::{FormatterStyle, StdoutSink},
    writer::{
        ErasedWriter, LevelSplitWriter, MakeWriter, NonBlockingHandle, NonBlockingWriter,
        RollingFileHandle, RollingFileWriter, RollingFileWriterBuilder, RollingPolicy,
        StderrWriter, StdoutWriter, TeeWriter, WorkerGuard,
    },
};
use crate::registry::ScrubbedEnvelope;

/// Pinned future returned by `Sink::flush` / `Sink::shutdown`. Spec 11 § 4.
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
