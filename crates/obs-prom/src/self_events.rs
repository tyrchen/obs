//! Runtime self-events emitted by [`PromRegistry`](crate::PromRegistry).
//!
//! Envelopes are built via [`obs_core::self_event`] so the shared
//! `tier` / `sev` / `sampling_reason` / `ts_ns` path applies.

use obs_core::{observer, self_event};
use obs_proto::obs::v1::{Severity, Tier};

/// Emitted when a series cap breach drops a new series.
pub(crate) fn emit_series_cap_exceeded(metric_name: &str, sample_labels: &str, cap: u32) {
    let mut env = self_event(
        "obs.runtime.v1.ObsPromSeriesCapExceeded",
        Tier::Metric,
        Severity::Warn,
    );
    env.labels
        .insert("metric_name".into(), truncate(metric_name, 256));
    env.labels
        .insert("sample_labels".into(), truncate(sample_labels, 512));
    env.labels.insert("cap".into(), cap.to_string());
    observer().emit_envelope(env);
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + 1);
    out.push_str(&s[..end]);
    out.push('…');
    out
}
