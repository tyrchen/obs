//! Runtime self-events emitted by [`PromRegistry`](crate::PromRegistry).

use buffa::EnumValue;
use obs_core::observer;
use obs_proto::obs::v1::{ObsEnvelope, Severity as PSeverity, Tier as PTier};

/// Emitted when a series cap breach drops a new series.
pub(crate) fn emit_series_cap_exceeded(metric_name: &str, sample_labels: &str, cap: u32) {
    let mut env = ObsEnvelope {
        full_name: "obs.runtime.v1.ObsPromSeriesCapExceeded".to_string(),
        tier: EnumValue::Known(PTier::TIER_METRIC),
        sev: EnumValue::Known(PSeverity::SEVERITY_WARN),
        ts_ns: now_ns(),
        ..Default::default()
    };
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

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}
