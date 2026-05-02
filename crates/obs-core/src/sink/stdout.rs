//! `StdoutSink` — formatter-driven stdout / writer sink. Spec 20 § 3.6.

use std::io::Write;

use obs_proto::obs::v1::ObsEnvelope;
use obs_types::{SamplingReason, Severity, Tier};
use parking_lot::Mutex;

use super::{
    Sink,
    writer::{ErasedWriter, MakeWriter, StdoutWriter},
};
use crate::registry::ScrubbedEnvelope;

/// Output style for [`StdoutSink`]. See spec 20 § 3.6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FormatterStyle {
    /// Single line; field names elided when obvious from event name.
    Compact,
    /// Single line; full envelope with explicit field names (default).
    #[default]
    Full,
    /// Multi-line; human-readable, dev-focused.
    Pretty,
    /// Newline-delimited JSON; production stdout.
    Json,
}

/// Stdout / writer-backed sink.
pub struct StdoutSink {
    style: FormatterStyle,
    writer: Mutex<ErasedWriterMaker>,
    severity_floor: Severity,
}

impl std::fmt::Debug for StdoutSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdoutSink")
            .field("style", &self.style)
            .field("severity_floor", &self.severity_floor)
            .finish_non_exhaustive()
    }
}

/// Erases the `MakeWriter` factory behind a closure so `StdoutSink`
/// can hold writers of arbitrary concrete types without leaking them
/// into the public API.
struct ErasedWriterMaker {
    make: Box<dyn FnMut(Severity) -> ErasedWriter + Send>,
}

impl StdoutSink {
    /// Construct a stdout sink with the given style; writes to
    /// `std::io::stdout()`.
    #[must_use]
    pub fn new(style: FormatterStyle) -> Self {
        Self::with_make_writer(style, StdoutWriter)
    }

    /// Construct with a caller-provided `MakeWriter`. Used to wire
    /// `LevelSplitWriter`, `RollingFileWriter`, `NonBlockingWriter`,
    /// or test harnesses.
    pub fn with_make_writer<M: MakeWriter>(style: FormatterStyle, mw: M) -> Self {
        let mw = std::sync::Arc::new(mw);
        let make = Box::new(move |sev: Severity| {
            let m = std::sync::Arc::clone(&mw);
            ErasedWriter::new(m.make_writer_for(sev))
        });
        Self {
            style,
            writer: Mutex::new(ErasedWriterMaker { make }),
            severity_floor: Severity::Trace,
        }
    }

    /// Set a severity floor; envelopes below it are dropped.
    #[must_use]
    pub fn severity_floor(mut self, sev: Severity) -> Self {
        self.severity_floor = sev;
        self
    }

    /// Test helper: build a sink that writes into `writer` using
    /// `FormatterStyle::Full`.
    pub fn with_writer<W: Write + Send + 'static>(style: FormatterStyle, writer: W) -> Self {
        struct OneShot<W>(parking_lot::Mutex<Option<W>>);
        impl<W: Write + Send + 'static> MakeWriter for OneShot<W> {
            type Writer = ErasedWriter;
            fn make_writer(&self) -> ErasedWriter {
                if let Some(w) = self.0.lock().take() {
                    ErasedWriter::new(w)
                } else {
                    // Subsequent writes fall back to discarding to keep
                    // the test contract simple.
                    ErasedWriter::new(std::io::sink())
                }
            }
        }
        // OneShot can't actually own a single writer across batches;
        // instead, share it through Arc<Mutex<...>>. Use a more
        // realistic shared-vec test writer.
        let shared = std::sync::Arc::new(parking_lot::Mutex::new(Some(writer)));
        struct Shared<W>(std::sync::Arc<parking_lot::Mutex<Option<W>>>);
        impl<W: Write + Send + 'static> MakeWriter for Shared<W> {
            type Writer = ErasedWriter;
            fn make_writer(&self) -> ErasedWriter {
                let mut g = self.0.lock();
                if let Some(w) = g.take() {
                    ErasedWriter::new(SharedWriter {
                        slot: Some(w),
                        back: std::sync::Arc::clone(&self.0),
                    })
                } else {
                    ErasedWriter::new(std::io::sink())
                }
            }
        }
        struct SharedWriter<W: Write> {
            slot: Option<W>,
            back: std::sync::Arc<parking_lot::Mutex<Option<W>>>,
        }
        impl<W: Write> Write for SharedWriter<W> {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                match self.slot.as_mut() {
                    Some(w) => w.write(b),
                    None => Ok(b.len()),
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                match self.slot.as_mut() {
                    Some(w) => w.flush(),
                    None => Ok(()),
                }
            }
        }
        impl<W: Write> Drop for SharedWriter<W> {
            fn drop(&mut self) {
                if let Some(w) = self.slot.take() {
                    *self.back.lock() = Some(w);
                }
            }
        }
        // Quiet the unused-OneShot warning: keep both for symmetry with
        // earlier API but use Shared for actual functionality.
        let _ = std::any::type_name::<OneShot<()>>();
        Self::with_make_writer(style, Shared(shared))
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new(FormatterStyle::Full)
    }
}

impl Sink for StdoutSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        let envelope = env.envelope();
        let sev = native_sev(envelope);
        if sev < self.severity_floor {
            return;
        }
        let mut maker = self.writer.lock();
        let mut w = (maker.make)(sev);
        match self.style {
            FormatterStyle::Compact => render_compact(&mut w, envelope),
            FormatterStyle::Full => render_full(&mut w, envelope, env.payload().len()),
            FormatterStyle::Pretty => render_pretty(&mut w, envelope, env.payload().len()),
            FormatterStyle::Json => render_json(&mut w, envelope),
        }
    }
}

fn native_sev(env: &ObsEnvelope) -> Severity {
    match env.sev {
        ::buffa::EnumValue::Known(s) => proto_sev_to_native(s),
        ::buffa::EnumValue::Unknown(_) => Severity::Unspecified,
    }
}

fn render_compact<W: Write>(w: &mut W, env: &ObsEnvelope) {
    let _ = writeln!(
        w,
        "{ts}.{ns:09} {sev} {full_name} {labels}",
        ts = env.ts_ns / 1_000_000_000,
        ns = env.ts_ns % 1_000_000_000,
        sev = sev_str(env),
        full_name = env.full_name,
        labels = compact_labels(env),
    );
    let _ = w.flush();
}

fn render_full<W: Write>(w: &mut W, env: &ObsEnvelope, payload_len: usize) {
    let _ = writeln!(
        w,
        "[{ts:>10}.{ns:09} {sev:<5}] {tier:<6} {full_name}",
        ts = env.ts_ns / 1_000_000_000,
        ns = env.ts_ns % 1_000_000_000,
        sev = sev_str(env),
        tier = tier_str(env),
        full_name = env.full_name,
    );
    let _ = writeln!(
        w,
        "  service={} instance={} version={} reason={}",
        dash_or(&env.service),
        dash_or(&env.instance),
        dash_or(&env.version),
        sampling_reason_str(env),
    );
    if !env.trace_id.is_empty() || !env.span_id.is_empty() {
        let _ = writeln!(
            w,
            "  trace_id={} span_id={} parent={}",
            dash_or(&env.trace_id),
            dash_or(&env.span_id),
            dash_or(&env.parent_span_id),
        );
    }
    if !env.labels.is_empty() {
        let mut keys: Vec<_> = env.labels.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(v) = env.labels.get(k) {
                let _ = writeln!(w, "  label.{k}={v}");
            }
        }
    }
    if payload_len > 0 {
        let _ = writeln!(w, "  payload_bytes={payload_len}");
    }
    let _ = w.flush();
}

fn render_pretty<W: Write>(w: &mut W, env: &ObsEnvelope, payload_len: usize) {
    let _ = writeln!(
        w,
        "─── {full_name} @ {ts}.{ns:09} {sev} {tier} ───",
        full_name = env.full_name,
        ts = env.ts_ns / 1_000_000_000,
        ns = env.ts_ns % 1_000_000_000,
        sev = sev_str(env),
        tier = tier_str(env),
    );
    let _ = writeln!(
        w,
        "    service: {} ({}) instance: {}",
        env.service, env.version, env.instance
    );
    if !env.trace_id.is_empty() {
        let _ = writeln!(
            w,
            "    trace:   {}/{} parent={}",
            env.trace_id, env.span_id, env.parent_span_id
        );
    }
    if !env.labels.is_empty() {
        let _ = writeln!(w, "    labels:");
        let mut keys: Vec<_> = env.labels.keys().collect();
        keys.sort();
        for k in keys {
            if let Some(v) = env.labels.get(k) {
                let _ = writeln!(w, "        {k} = {v}");
            }
        }
    }
    if payload_len > 0 {
        let _ = writeln!(w, "    payload: {payload_len} bytes");
    }
    let _ = w.flush();
}

fn render_json<W: Write>(w: &mut W, env: &ObsEnvelope) {
    use serde_json::{Map, Value};
    let mut root = Map::new();
    root.insert("ts_ns".into(), Value::from(env.ts_ns));
    root.insert("sev".into(), Value::from(sev_str(env)));
    root.insert("tier".into(), Value::from(tier_str(env)));
    root.insert("full_name".into(), Value::from(env.full_name.clone()));
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
    if env.schema_hash != 0 {
        root.insert("schema_hash".into(), Value::from(env.schema_hash));
    }
    if env.callsite_id != 0 {
        root.insert("callsite_id".into(), Value::from(env.callsite_id));
    }
    if !env.labels.is_empty() {
        let mut labels = Map::new();
        for (k, v) in env.labels.iter() {
            labels.insert(k.clone(), Value::from(v.clone()));
        }
        root.insert("labels".into(), Value::Object(labels));
    }
    let value = Value::Object(root);
    let _ = writeln!(w, "{value}");
    let _ = w.flush();
}

fn dash_or(s: &str) -> &str {
    if s.is_empty() { "-" } else { s }
}

fn compact_labels(env: &ObsEnvelope) -> String {
    if env.labels.is_empty() {
        return "{}".to_string();
    }
    let mut keys: Vec<_> = env.labels.keys().collect();
    keys.sort();
    let mut s = String::with_capacity(env.labels.len() * 16);
    s.push('{');
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        if let Some(v) = env.labels.get(*k) {
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
    }
    s.push('}');
    s
}

fn sev_str(env: &ObsEnvelope) -> &'static str {
    match env.sev {
        ::buffa::EnumValue::Known(s) => proto_sev_to_native(s).as_str(),
        ::buffa::EnumValue::Unknown(_) => Severity::Unspecified.as_str(),
    }
}

fn tier_str(env: &ObsEnvelope) -> &'static str {
    match env.tier {
        ::buffa::EnumValue::Known(t) => proto_tier_to_native(t).as_str(),
        ::buffa::EnumValue::Unknown(_) => Tier::Unspecified.as_str(),
    }
}

fn sampling_reason_str(env: &ObsEnvelope) -> &'static str {
    match env.sampling_reason {
        ::buffa::EnumValue::Known(r) => proto_reason_to_native(r).as_str(),
        ::buffa::EnumValue::Unknown(_) => SamplingReason::Unspecified.as_str(),
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_sev_to_native(s: obs_proto::obs::v1::Severity) -> Severity {
    use obs_proto::obs::v1::Severity as P;
    match s {
        P::SEVERITY_UNSPECIFIED => Severity::Unspecified,
        P::SEVERITY_TRACE => Severity::Trace,
        P::SEVERITY_DEBUG => Severity::Debug,
        P::SEVERITY_INFO => Severity::Info,
        P::SEVERITY_WARN => Severity::Warn,
        P::SEVERITY_ERROR => Severity::Error,
        P::SEVERITY_FATAL => Severity::Fatal,
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_tier_to_native(t: obs_proto::obs::v1::Tier) -> Tier {
    use obs_proto::obs::v1::Tier as P;
    match t {
        P::TIER_UNSPECIFIED => Tier::Unspecified,
        P::TIER_LOG => Tier::Log,
        P::TIER_METRIC => Tier::Metric,
        P::TIER_TRACE => Tier::Trace,
        P::TIER_AUDIT => Tier::Audit,
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_reason_to_native(r: obs_proto::obs::v1::SamplingReason) -> SamplingReason {
    use obs_proto::obs::v1::SamplingReason as P;
    match r {
        P::SAMPLING_REASON_UNSPECIFIED => SamplingReason::Unspecified,
        P::SAMPLING_REASON_HEAD_RATE => SamplingReason::HeadRate,
        P::SAMPLING_REASON_TAIL_ERROR => SamplingReason::TailError,
        P::SAMPLING_REASON_SLOW => SamplingReason::Slow,
        P::SAMPLING_REASON_FORENSIC => SamplingReason::Forensic,
        P::SAMPLING_REASON_AUDIT => SamplingReason::Audit,
        P::SAMPLING_REASON_RUNTIME => SamplingReason::Runtime,
        P::SAMPLING_REASON_OVERRIDE => SamplingReason::Override,
    }
}
