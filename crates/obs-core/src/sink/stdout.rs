//! `StdoutSink` — human-readable dev sink. Phase-1 implements only
//! `FormatterStyle::Full` (spec 20 § 3.6). Compact / Json / Noop styles
//! land in Phase 3 task 3.7.

use std::io::Write;

use obs_types::{SamplingReason, Severity, Tier};
use parking_lot::Mutex;

use crate::registry::ScrubbedEnvelope;

use super::Sink;

/// Output style for [`StdoutSink`]. See spec 20 § 3.6.
///
/// Phase-1 supports only [`FormatterStyle::Full`]; the other variants
/// are reserved so the API does not change in Phase 3 when they land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FormatterStyle {
    /// One-line, no payload — silent in production.
    Compact,
    /// Multi-line with full envelope and labels (Phase-1 default).
    #[default]
    Full,
    /// One JSON object per line.
    Json,
}

/// Pretty-prints envelopes to stdout. Synchronised with a `Mutex` so
/// concurrent emits don't interleave bytes.
pub struct StdoutSink {
    style: FormatterStyle,
    writer: Mutex<Box<dyn Write + Send>>,
}

impl std::fmt::Debug for StdoutSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StdoutSink")
            .field("style", &self.style)
            .finish_non_exhaustive()
    }
}

impl StdoutSink {
    /// Construct with the given style. Phase-1: only `Full` is
    /// implemented; passing another variant emits the same shape and
    /// will be replaced in Phase 3 task 3.7.
    #[must_use]
    pub fn new(style: FormatterStyle) -> Self {
        Self {
            style,
            writer: Mutex::new(Box::new(std::io::stdout())),
        }
    }

    /// Variant used by tests: write to a caller-provided sink (e.g.
    /// `Vec<u8>` wrapped in `Mutex`).
    pub fn with_writer<W: Write + Send + 'static>(style: FormatterStyle, writer: W) -> Self {
        Self {
            style,
            writer: Mutex::new(Box::new(writer)),
        }
    }
}

impl Default for StdoutSink {
    fn default() -> Self {
        Self::new(FormatterStyle::Full)
    }
}

impl Sink for StdoutSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        let e = env.envelope();
        let tier = match e.tier {
            ::buffa::EnumValue::Known(t) => proto_tier_to_str(t),
            ::buffa::EnumValue::Unknown(_) => "unspec",
        };
        let sev = match e.sev {
            ::buffa::EnumValue::Known(s) => proto_sev_to_str(s),
            ::buffa::EnumValue::Unknown(_) => "unspec",
        };
        let reason = match e.sampling_reason {
            ::buffa::EnumValue::Known(r) => proto_sampling_reason_to_str(r),
            ::buffa::EnumValue::Unknown(_) => "unspec",
        };

        let mut w = self.writer.lock();
        // Header line.
        let _ = writeln!(
            w,
            "[{ts_s:>10}.{ts_ns:09} {sev:<5}] {tier:<6} {full_name}",
            ts_s = e.ts_ns / 1_000_000_000,
            ts_ns = e.ts_ns % 1_000_000_000,
            sev = sev,
            tier = tier,
            full_name = e.full_name,
        );
        // Identity line.
        let _ = writeln!(
            w,
            "  service={} instance={} version={} reason={}",
            display_or_dash(&e.service),
            display_or_dash(&e.instance),
            display_or_dash(&e.version),
            reason,
        );
        // Trace correlation when set.
        if !e.trace_id.is_empty() || !e.span_id.is_empty() {
            let _ = writeln!(
                w,
                "  trace_id={} span_id={} parent={}",
                display_or_dash(&e.trace_id),
                display_or_dash(&e.span_id),
                display_or_dash(&e.parent_span_id),
            );
        }
        // Labels.
        if !e.labels.is_empty() {
            let mut keys: Vec<_> = e.labels.keys().collect();
            keys.sort();
            for k in keys {
                if let Some(v) = e.labels.get(k) {
                    let _ = writeln!(w, "  label.{k}={v}");
                }
            }
        }
        if matches!(self.style, FormatterStyle::Full) && !env.payload().is_empty() {
            let _ = writeln!(w, "  payload_bytes={}", env.payload().len());
        }
        let _ = w.flush();
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_tier_to_str(t: obs_proto::obs::v1::Tier) -> &'static str {
    use obs_proto::obs::v1::Tier as P;
    match t {
        P::TIER_UNSPECIFIED => Tier::Unspecified.as_str(),
        P::TIER_LOG => Tier::Log.as_str(),
        P::TIER_METRIC => Tier::Metric.as_str(),
        P::TIER_TRACE => Tier::Trace.as_str(),
        P::TIER_AUDIT => Tier::Audit.as_str(),
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_sev_to_str(s: obs_proto::obs::v1::Severity) -> &'static str {
    use obs_proto::obs::v1::Severity as P;
    match s {
        P::SEVERITY_UNSPECIFIED => Severity::Unspecified.as_str(),
        P::SEVERITY_TRACE => Severity::Trace.as_str(),
        P::SEVERITY_DEBUG => Severity::Debug.as_str(),
        P::SEVERITY_INFO => Severity::Info.as_str(),
        P::SEVERITY_WARN => Severity::Warn.as_str(),
        P::SEVERITY_ERROR => Severity::Error.as_str(),
        P::SEVERITY_FATAL => Severity::Fatal.as_str(),
    }
}

#[allow(non_snake_case, non_upper_case_globals)]
fn proto_sampling_reason_to_str(r: obs_proto::obs::v1::SamplingReason) -> &'static str {
    use obs_proto::obs::v1::SamplingReason as P;
    match r {
        P::SAMPLING_REASON_UNSPECIFIED => SamplingReason::Unspecified.as_str(),
        P::SAMPLING_REASON_HEAD_RATE => SamplingReason::HeadRate.as_str(),
        P::SAMPLING_REASON_TAIL_ERROR => SamplingReason::TailError.as_str(),
        P::SAMPLING_REASON_SLOW => SamplingReason::Slow.as_str(),
        P::SAMPLING_REASON_FORENSIC => SamplingReason::Forensic.as_str(),
        P::SAMPLING_REASON_AUDIT => SamplingReason::Audit.as_str(),
        P::SAMPLING_REASON_RUNTIME => SamplingReason::Runtime.as_str(),
        P::SAMPLING_REASON_OVERRIDE => SamplingReason::Override.as_str(),
    }
}

fn display_or_dash(s: &str) -> &str {
    if s.is_empty() { "-" } else { s }
}
