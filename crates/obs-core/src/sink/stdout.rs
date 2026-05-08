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
    /// Single line; tracing-fmt-shaped. Default — readable under `tail
    /// -f` and friendly to `grep`. Boundary-review § 4.6 + spec 20 § 3.6.
    #[default]
    Compact,
    /// Single line; full envelope with explicit field names.
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
        Self::new(FormatterStyle::default())
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
    // Match tracing-subscriber's compact format:
    //
    //   2026-05-07T15:31:00.123456Z  INFO scope{k=v ...}: target: message
    //
    // Mapping from the obs envelope:
    //   - timestamp     → RFC3339 UTC from `ts_ns`
    //   - LEVEL         → `sev_str(env)` upper-cased, right-padded to 5
    //   - scope{fields} → envelope `labels` when present (sorted)
    //   - target        → `env.full_name`
    //   - message       → trailing trace_id/span_id when present, empty otherwise (obs envelopes
    //     are schema-driven; the schema name IS the message).
    let iso = iso8601_utc(env.ts_ns);
    let lvl = sev_upper(env);

    // Scope is the labels block in tracing style: `name{k=v k=v}`.
    // There's no separate "span name" on the envelope, so use the
    // leaf of `full_name` — matches how `tracing::instrument` prints
    // the function name.
    let scope_leaf = env
        .full_name
        .rsplit_once('.')
        .map(|(_, leaf)| leaf)
        .unwrap_or(env.full_name.as_str());

    let fields = tracing_style_fields(env);
    let scope = if fields.is_empty() {
        String::new()
    } else {
        format!("{scope_leaf}{{{fields}}}: ")
    };

    // Target: the full schema name. tracing-subscriber prints the
    // crate::module path; the envelope's `full_name` is the analogue
    // for schema-driven emits.
    let target = &env.full_name;

    // Message tail: trace correlation when present. Keeps noise off
    // the common line while still surfacing the linkage for any emit
    // inside an active scope. When both are empty (the common case for
    // schema-only emits) the `: <tail>` suffix disappears entirely so
    // the line ends at the target name — no trailing `: ` dangler.
    if !env.trace_id.is_empty() || !env.span_id.is_empty() {
        let _ = writeln!(
            w,
            "{iso} {lvl:>5} {scope}{target}: trace_id={} span_id={}",
            dash_or(&env.trace_id),
            dash_or(&env.span_id),
        );
    } else {
        let _ = writeln!(w, "{iso} {lvl:>5} {scope}{target}");
    }
    let _ = w.flush();
}

/// Render `env.labels` as tracing-style `k=v k=v` — space-separated,
/// keys sorted, string values quoted iff they contain spaces or
/// `=`/`"` characters so trivial values stay unquoted.
fn tracing_style_fields(env: &ObsEnvelope) -> String {
    if env.labels.is_empty() {
        return String::new();
    }
    let mut keys: Vec<_> = env.labels.keys().collect();
    keys.sort();
    let mut s = String::with_capacity(env.labels.len() * 16);
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        if let Some(v) = env.labels.get(*k) {
            s.push_str(k);
            s.push('=');
            if needs_quoting(v) {
                s.push('"');
                // Escape embedded quotes + backslashes so the output
                // stays parseable.
                for ch in v.chars() {
                    if ch == '"' || ch == '\\' {
                        s.push('\\');
                    }
                    s.push(ch);
                }
                s.push('"');
            } else {
                s.push_str(v);
            }
        }
    }
    s
}

fn needs_quoting(v: &str) -> bool {
    v.is_empty()
        || v.chars()
            .any(|c| c.is_whitespace() || c == '=' || c == '"' || c == '{' || c == '}')
}

fn sev_upper(env: &ObsEnvelope) -> &'static str {
    match env.sev {
        ::buffa::EnumValue::Known(s) => match proto_sev_to_native(s) {
            Severity::Trace => "TRACE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            _ => "?",
        },
        ::buffa::EnumValue::Unknown(_) => "?",
    }
}

/// Render `ts_ns` (Unix epoch nanoseconds) as RFC3339 UTC with
/// microsecond resolution: `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Matches
/// tracing-subscriber's default timestamp format.
fn iso8601_utc(ts_ns: u64) -> String {
    // Unix day 0 is 1970-01-01 (Thursday). Use the civil-date algorithm
    // from Howard Hinnant — division-only, no lookup tables. Valid for
    // the entire range ts_ns can represent (through AD 2554).
    let secs = (ts_ns / 1_000_000_000) as i64;
    let nanos = (ts_ns % 1_000_000_000) as u32;
    let micros = nanos / 1_000;

    let days = secs.div_euclid(86_400);
    let sec_of_day = secs.rem_euclid(86_400);
    let hour = (sec_of_day / 3600) as u32;
    let minute = ((sec_of_day / 60) % 60) as u32;
    let second = (sec_of_day % 60) as u32;

    let (year, month, day) = civil_from_days(days);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z")
}

/// Convert days-since-1970 to `(year, month, day)`. Howard Hinnant's
/// [date algorithm](https://howardhinnant.github.io/date_algorithms.html)
/// — integer-only, no lookup tables.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
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

#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use obs_proto::obs::v1::Severity as PSev;

    use super::*;

    fn env(full_name: &str, sev: PSev, ts_ns: u64) -> ObsEnvelope {
        ObsEnvelope {
            full_name: full_name.to_string(),
            sev: ::buffa::EnumValue::Known(sev),
            ts_ns,
            ..Default::default()
        }
    }

    // 2026-05-07T15:31:00 UTC = 1778167860 seconds since epoch.
    // Add 123_456 µs = 123_456_000 ns to get the exact timestamp
    // from the tracing-fmt reference line.
    const REF_TS_NS: u64 = 1_778_167_860_000_000_000 + 123_456_000;

    #[test]
    fn test_iso8601_utc_matches_tracing_fmt_shape() {
        let s = iso8601_utc(REF_TS_NS);
        assert_eq!(s, "2026-05-07T15:31:00.123456Z");
    }

    #[test]
    fn test_render_compact_mirrors_tracing_fmt_compact() {
        // Matches the shape:
        //   2026-05-07T15:31:00.123456Z  INFO scope{k=v}: target
        // No trailing `: ` when there's no trace context / message.
        let mut e = env("my_crate.process_order", PSev::SEVERITY_INFO, REF_TS_NS);
        e.labels.insert("id".to_string(), "42".to_string());
        e.labels.insert("item".to_string(), "Rust Book".to_string());
        let mut buf: Vec<u8> = Vec::new();
        render_compact(&mut buf, &e);
        let line = String::from_utf8(buf).expect("utf-8");
        assert_eq!(
            line,
            "2026-05-07T15:31:00.123456Z  INFO process_order{id=42 item=\"Rust Book\"}: \
             my_crate.process_order\n"
        );
    }

    #[test]
    fn test_render_compact_appends_trace_context_when_present() {
        let mut e = env("x.y", PSev::SEVERITY_INFO, REF_TS_NS);
        e.trace_id = "0123456789abcdef0123456789abcdef".to_string();
        e.span_id = "0123456789abcdef".to_string();
        let mut buf: Vec<u8> = Vec::new();
        render_compact(&mut buf, &e);
        let line = String::from_utf8(buf).expect("utf-8");
        assert_eq!(
            line,
            "2026-05-07T15:31:00.123456Z  INFO x.y: trace_id=0123456789abcdef0123456789abcdef \
             span_id=0123456789abcdef\n"
        );
    }

    #[test]
    fn test_render_compact_drops_scope_block_when_no_labels() {
        // Empty labels → no `scope{...}` prefix, no trailing `: `.
        let e = env("x.y.Z", PSev::SEVERITY_INFO, REF_TS_NS);
        let mut buf: Vec<u8> = Vec::new();
        render_compact(&mut buf, &e);
        let line = String::from_utf8(buf).expect("utf-8");
        assert_eq!(line, "2026-05-07T15:31:00.123456Z  INFO x.y.Z\n");
    }

    #[test]
    fn test_render_compact_pads_severity_to_five() {
        let e = env("x.y", PSev::SEVERITY_WARN, 0);
        let mut buf: Vec<u8> = Vec::new();
        render_compact(&mut buf, &e);
        let line = String::from_utf8(buf).expect("utf-8");
        // `  WARN` — two-space lead (from the format right-pad), then
        // "WARN" (4 chars, padded to 5 = 1 trailing space).
        assert!(line.contains(" WARN "), "line: {line}");
    }

    #[test]
    fn test_tracing_style_fields_quotes_when_needed() {
        let mut e = env("x.y", PSev::SEVERITY_INFO, 0);
        e.labels.insert("a".to_string(), "simple".to_string());
        e.labels.insert("b".to_string(), "with space".to_string());
        e.labels
            .insert("c".to_string(), "with \"quote\"".to_string());
        let s = tracing_style_fields(&e);
        assert!(s.contains("a=simple"));
        assert!(s.contains("b=\"with space\""));
        assert!(s.contains(r#"c="with \"quote\"""#));
    }

    #[test]
    fn test_civil_from_days_round_trip_recent_dates() {
        // Unix day 0 = 1970-01-01.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        // 2026-05-07 — 20,580 days since epoch.
        assert_eq!(civil_from_days(20_580), (2026, 5, 7));
        // Leap day: 2024-02-29 — 19,782 days since epoch.
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
    }
}
