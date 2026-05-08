//! Runtime self-events emitted by [`BatchingSink`](crate::BatchingSink).
//!
//! All names live in the `obs.runtime.v1` namespace so any downstream
//! consumer renders them uniformly. Each event is a labels-only
//! [`ObsEnvelope`] — no obs-proto schema, no codegen. The framework
//! reaches the process-global observer via [`obs_core::observer`].

use buffa::EnumValue;
use obs_core::observer;
use obs_proto::obs::v1::{ObsEnvelope, Severity as PSeverity, Tier as PTier};

/// Emitted after a batch successfully ships.
pub(crate) fn emit_uploaded(
    backend: &'static str,
    backend_key: &str,
    partition: &str,
    events: u32,
    bytes: u64,
    duration_ms: u64,
    attempts: u32,
) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkUploaded",
        PTier::TIER_METRIC,
        PSeverity::SEVERITY_INFO,
    );
    insert_common(&mut env, backend, backend_key, partition);
    env.labels.insert("events".into(), events.to_string());
    env.labels.insert("bytes".into(), bytes.to_string());
    env.labels
        .insert("duration_ms".into(), duration_ms.to_string());
    env.labels.insert("attempts".into(), attempts.to_string());
    emit_self(env);
}

/// Emitted when a transient upload attempt fails and more attempts
/// remain.
pub(crate) fn emit_retry(
    backend: &'static str,
    backend_key: &str,
    partition: &str,
    attempt: u32,
    error: &str,
) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkRetry",
        PTier::TIER_LOG,
        PSeverity::SEVERITY_WARN,
    );
    insert_common(&mut env, backend, backend_key, partition);
    env.labels.insert("attempt".into(), attempt.to_string());
    env.labels.insert("error".into(), truncate(error, 512));
    emit_self(env);
}

/// Emitted when a fatal error takes a batch out of the retry loop.
pub(crate) fn emit_failed(
    backend: &'static str,
    backend_key: &str,
    partition: &str,
    attempts: u32,
    error: &str,
) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkFailed",
        PTier::TIER_LOG,
        PSeverity::SEVERITY_ERROR,
    );
    insert_common(&mut env, backend, backend_key, partition);
    env.labels.insert("attempts".into(), attempts.to_string());
    env.labels.insert("error".into(), truncate(error, 512));
    emit_self(env);
}

/// Emitted when retries exhaust and the batch is written to the spool.
pub(crate) fn emit_spooled(
    backend: &'static str,
    backend_key: &str,
    partition: &str,
    events: u32,
    attempts: u32,
) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkSpooled",
        PTier::TIER_METRIC,
        PSeverity::SEVERITY_WARN,
    );
    insert_common(&mut env, backend, backend_key, partition);
    env.labels.insert("events".into(), events.to_string());
    env.labels.insert("attempts".into(), attempts.to_string());
    emit_self(env);
}

/// Emitted after a spooled batch successfully re-ships on startup or
/// from the background retry task.
pub(crate) fn emit_recovered(backend: &'static str, partition: &str, envelopes: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkRecovered",
        PTier::TIER_LOG,
        PSeverity::SEVERITY_INFO,
    );
    insert_common(&mut env, backend, "", partition);
    env.labels.insert("envelopes".into(), envelopes.to_string());
    emit_self(env);
}

/// Emitted when a spool record cannot upload successfully within
/// `escalate_after` and moves to `{spool_root}/failed/{backend_id}/…`.
pub(crate) fn emit_escalated(
    backend: &'static str,
    partition: &str,
    path: &str,
    age_minutes: u32,
    last_error: &str,
) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkEscalatedToFailed",
        PTier::TIER_LOG,
        PSeverity::SEVERITY_ERROR,
    );
    insert_common(&mut env, backend, "", partition);
    env.labels.insert("path".into(), truncate(path, 512));
    env.labels
        .insert("age_minutes".into(), age_minutes.to_string());
    env.labels
        .insert("last_error".into(), truncate(last_error, 512));
    emit_self(env);
}

/// Emitted (at most once per flush window per partition) when the
/// per-partition ring buffer evicts oldest envelopes to make room.
pub(crate) fn emit_partition_overflow(
    backend: &'static str,
    partition: &str,
    evicted: u64,
    capacity: u32,
) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkPartitionOverflow",
        PTier::TIER_METRIC,
        PSeverity::SEVERITY_WARN,
    );
    insert_common(&mut env, backend, "", partition);
    env.labels.insert("evicted".into(), evicted.to_string());
    env.labels.insert("capacity".into(), capacity.to_string());
    emit_self(env);
}

/// Emitted when the ingress mpsc would block and the envelope is
/// dropped. Under the per-partition ring design (Option B in the spec)
/// this should be rare — new envelopes win at the ring, not the
/// channel — so a non-zero count usually means the worker is wedged.
pub(crate) fn emit_ingress_dropped(backend: &'static str, count: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkIngressDropped",
        PTier::TIER_METRIC,
        PSeverity::SEVERITY_WARN,
    );
    insert_common(&mut env, backend, "", "");
    env.labels.insert("dropped".into(), count.to_string());
    emit_self(env);
}

/// Emitted when a single envelope exceeds the `u32::MAX` frame cap.
/// The envelope is dropped before it reaches the encoder.
pub(crate) fn emit_envelope_too_large(backend: &'static str, full_name: &str, size: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsBatchSinkEnvelopeTooLarge",
        PTier::TIER_METRIC,
        PSeverity::SEVERITY_WARN,
    );
    insert_common(&mut env, backend, "", "");
    env.labels
        .insert("full_name".into(), truncate(full_name, 256));
    env.labels.insert("size".into(), size.to_string());
    emit_self(env);
}

fn base_envelope(full_name: &str, tier: PTier, sev: PSeverity) -> ObsEnvelope {
    ObsEnvelope {
        full_name: full_name.to_string(),
        tier: EnumValue::Known(tier),
        sev: EnumValue::Known(sev),
        ts_ns: now_ns(),
        ..Default::default()
    }
}

fn insert_common(env: &mut ObsEnvelope, backend: &'static str, backend_key: &str, partition: &str) {
    env.labels.insert("backend".into(), backend.to_string());
    if !backend_key.is_empty() {
        env.labels
            .insert("backend_key".into(), truncate(backend_key, 128));
    }
    if !partition.is_empty() {
        env.labels
            .insert("partition".into(), truncate(partition, 128));
    }
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

fn emit_self(env: ObsEnvelope) {
    observer().emit_envelope(env);
}
