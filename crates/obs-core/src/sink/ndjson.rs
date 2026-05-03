//! `NdjsonFileSink` — writes one JSON object per line, on top of any
//! `MakeWriter` (typically a `RollingFileWriter`). Spec 20 § 3.6.

use std::{
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;

use super::{
    Sink,
    writer::{ErasedWriter, MakeWriter, RollingFileWriter},
};
use crate::registry::ScrubbedEnvelope;

/// File sink that writes envelopes as JSON lines to the underlying
/// `MakeWriter`. Buffers a `Mutex<ErasedWriter>` per batch to avoid
/// re-opening files for every event.
pub struct NdjsonFileSink {
    writer: Mutex<ErasedWriterMaker>,
    written: AtomicU64,
}

impl std::fmt::Debug for NdjsonFileSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NdjsonFileSink")
            .field("written", &self.written.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

struct ErasedWriterMaker {
    make: Box<dyn FnMut() -> ErasedWriter + Send>,
}

impl NdjsonFileSink {
    /// Build atop `RollingFileWriter`, the canonical file destination.
    #[must_use]
    pub fn new(rolling: RollingFileWriter) -> Self {
        Self::with_make_writer(rolling)
    }

    /// Build atop any `MakeWriter` (test harness, custom destinations).
    pub fn with_make_writer<M: MakeWriter>(mw: M) -> Self {
        let mw = Arc::new(mw);
        let make = Box::new(move || ErasedWriter::new(Arc::clone(&mw).make_writer()));
        Self {
            writer: Mutex::new(ErasedWriterMaker { make }),
            written: AtomicU64::new(0),
        }
    }

    /// Total events written successfully.
    #[must_use]
    pub fn written_total(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
}

impl Sink for NdjsonFileSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        let mut maker = self.writer.lock();
        let mut w = (maker.make)();
        // Reuse the JSON formatter from StdoutSink for consistency.
        // Spec 14 § 5 / spec 93 P0-8: render the *scrubbed* payload —
        // the worker has already redacted classified fields, and
        // `env.schema()`'s `render_json` walks the bytes here.
        let envelope = env.envelope();
        let value = render_json_value(envelope, env.payload(), env.schema());
        let _ = writeln!(&mut w, "{value}");
        let _ = w.flush();
        self.written.fetch_add(1, Ordering::Relaxed);
    }
}

fn render_json_value(
    env: &obs_proto::obs::v1::ObsEnvelope,
    payload: &[u8],
    schema: Option<&'static dyn crate::EventSchemaErased>,
) -> serde_json::Value {
    use serde_json::{Map, Value};
    let mut root = Map::new();
    root.insert("ts_ns".into(), Value::from(env.ts_ns));
    root.insert("full_name".into(), Value::from(env.full_name.clone()));
    if env.schema_hash != 0 {
        root.insert("schema_hash".into(), Value::from(env.schema_hash));
    }
    if env.callsite_id != 0 {
        root.insert("callsite_id".into(), Value::from(env.callsite_id));
    }
    if !env.service.is_empty() {
        root.insert("service".into(), Value::from(env.service.clone()));
    }
    if !env.instance.is_empty() {
        root.insert("instance".into(), Value::from(env.instance.clone()));
    }
    if !env.version.is_empty() {
        root.insert("version".into(), Value::from(env.version.clone()));
    }
    if !env.trace_id.is_empty() {
        root.insert("trace_id".into(), Value::from(env.trace_id.clone()));
    }
    if !env.span_id.is_empty() {
        root.insert("span_id".into(), Value::from(env.span_id.clone()));
    }
    if !env.parent_span_id.is_empty() {
        root.insert(
            "parent_span_id".into(),
            Value::from(env.parent_span_id.clone()),
        );
    }
    let mut labels = Map::new();
    for (k, v) in env.labels.iter() {
        labels.insert(k.clone(), Value::from(v.clone()));
    }
    if !labels.is_empty() {
        root.insert("labels".into(), Value::Object(labels));
    }
    // Project the typed payload (spec 14 § 4.2) so consumers see the
    // typed fields, not just the wire-bytes blob. Skipped when schema
    // is unknown or the projection errors (truncation).
    if !payload.is_empty()
        && let Some(s) = schema
    {
        let mut payload_map = Map::new();
        if s.render_json(payload, &mut payload_map).is_ok() && !payload_map.is_empty() {
            root.insert("payload".into(), Value::Object(payload_map));
        }
    }
    Value::Object(root)
}
