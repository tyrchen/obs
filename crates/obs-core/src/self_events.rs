//! Helpers for emitting the spec 11 § 10 self-event catalogue.
//!
//! Self-events are envelopes the runtime emits to describe its own
//! behaviour: config reloads, schema-registry init, sink failures,
//! AUDIT spool activity, callsite-hash collisions. Spec 93 P1-2.
//!
//! Each helper constructs an [`ObsEnvelope`] with the proto-side
//! `full_name` / `tier` / `sev` and label set declared in
//! `crates/obs-proto/proto/obs/runtime/v1/self_events.proto`. The
//! payload is left empty — these self-events live as labels-only so
//! they round-trip through every sink without needing the schema
//! registry to be ready (which would be circular for the very first
//! `ObsRegistryInitialized` emit).
//!
//! The runtime helpers are package-internal — call them from the
//! observer / audit_spool / sink code that knows when the event
//! should fire, never from user code.

use buffa::EnumValue;
use obs_proto::obs::v1::{ObsEnvelope, Severity as PSeverity, Tier as PTier};

use crate::observer::observer;

/// Best-effort emit of `obs.runtime.v1.ObsRegistryInitialized` at
/// observer init time. Spec 11 § 10.
pub(crate) fn emit_registry_initialized(schema_count: u64, arrow_assembly_ns: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsRegistryInitialized",
        PSeverity::SEVERITY_DEBUG,
    );
    env.labels
        .insert("schema_count".to_string(), schema_count.to_string());
    env.labels.insert(
        "arrow_assembly_ns".to_string(),
        arrow_assembly_ns.to_string(),
    );
    emit_self(env);
}

/// Emitted when `EventsConfig::reload_config` succeeds.
pub(crate) fn emit_config_reloaded(config_hash: u64) {
    let mut env = base_envelope("obs.runtime.v1.ObsConfigReloaded", PSeverity::SEVERITY_INFO);
    env.labels
        .insert("config_hash".to_string(), format!("{config_hash:016x}"));
    emit_self(env);
}

/// Emitted when reload validation fails or the loader errors.
pub(crate) fn emit_config_reload_failed(reason: &str) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsConfigReloadFailed",
        PSeverity::SEVERITY_WARN,
    );
    env.labels
        .insert("reason".to_string(), truncate(reason, 512));
    emit_self(env);
}

/// Emitted when a sink looks up an envelope's schema and finds no
/// registration. Should not fire in well-formed deployments — useful
/// during dev / migration.
#[allow(dead_code)]
pub(crate) fn emit_schema_unknown(sink: &str, full_name: &str) {
    let mut env = base_envelope("obs.runtime.v1.ObsSchemaUnknown", PSeverity::SEVERITY_DEBUG);
    env.labels.insert("sink".to_string(), sink.to_string());
    env.labels
        .insert("full_name".to_string(), full_name.to_string());
    emit_self(env);
}

/// Emitted when the AUDIT path falls back to disk spool because the
/// in-memory channel was full longer than `audit.block_ms_max`.
pub(crate) fn emit_audit_spooled(full_name: &str) {
    let mut env = base_envelope("obs.runtime.v1.ObsAuditSpooled", PSeverity::SEVERITY_WARN);
    env.labels
        .insert("full_name".to_string(), full_name.to_string());
    emit_self(env);
}

/// Emitted when the spool itself cannot accept a record (disk full,
/// permission, etc). Severity FATAL because AUDIT must not silently
/// drop.
pub(crate) fn emit_audit_spool_failed(reason: &str) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsAuditSpoolFailed",
        PSeverity::SEVERITY_FATAL,
    );
    env.labels
        .insert("reason".to_string(), truncate(reason, 512));
    emit_self(env);
}

/// Emitted when the same envelope is dropped at the worker because
/// it would re-enter an active emit (cycle protection).
pub(crate) fn emit_sink_dropped(tier: &str, reason: &str) {
    let mut env = base_envelope("obs.runtime.v1.ObsSinkDropped", PSeverity::SEVERITY_WARN);
    env.labels.insert("tier".to_string(), tier.to_string());
    env.labels.insert("reason".to_string(), reason.to_string());
    emit_self(env);
}

/// Emitted at registry init when two distinct events share the same
/// `schema_hash` 8-byte prefix. Spec 14 § 8 row 2 / spec 93 P2-9.
pub fn emit_callsite_hash_collision_pub(hash: u64, first: &str, second: &str) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsCallsiteHashCollision",
        PSeverity::SEVERITY_WARN,
    );
    env.labels
        .insert("schema_hash".to_string(), format!("{hash:016x}"));
    env.labels.insert("first".to_string(), first.to_string());
    env.labels.insert("second".to_string(), second.to_string());
    emit_self(env);
}

/// Emitted when a Started event has no matching Completed within the
/// configured `pair_timeout`. Spec 93 P1-7. Public so the OTLP trace
/// sink can fire it from outside `obs-core`.
pub fn emit_span_pair_orphaned_pub(full_name: &str) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsSpanPairOrphaned",
        PSeverity::SEVERITY_DEBUG,
    );
    env.labels
        .insert("full_name".to_string(), full_name.to_string());
    emit_self(env);
}

/// Emitted when bridge field promotion sees a label-key whose distinct
/// value count crossed the declared cardinality cap. Public so the
/// `obs-tracing-bridge` field-promoter can fire it. Spec 30 § 2.4 /
/// spec 94 § 2.6 / P1-D.
pub fn emit_label_cardinality_high_pub(full_name: &str, label_key: &str, estimated_distinct: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsLabelCardinalityHigh",
        PSeverity::SEVERITY_WARN,
    );
    env.labels
        .insert("full_name".to_string(), full_name.to_string());
    env.labels
        .insert("label_key".to_string(), label_key.to_string());
    env.labels.insert(
        "estimated_distinct".to_string(),
        estimated_distinct.to_string(),
    );
    emit_self(env);
}

/// Emitted when an envelope exceeds `limits.max_payload_bytes` at
/// projection time.
pub(crate) fn emit_oversized_dropped(full_name: &str, size_bytes: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsOversizedDropped",
        PSeverity::SEVERITY_WARN,
    );
    env.labels
        .insert("full_name".to_string(), full_name.to_string());
    env.labels
        .insert("size_bytes".to_string(), size_bytes.to_string());
    emit_self(env);
}

/// Emitted when a single label value exceeds
/// `limits.max_label_value_bytes`. Spec 11 § 6.2 / spec 94 § 3.5.
pub(crate) fn emit_oversized_label_dropped(full_name: &str, label_name: &str, size_bytes: u64) {
    let mut env = base_envelope(
        "obs.runtime.v1.ObsOversizedDropped",
        PSeverity::SEVERITY_WARN,
    );
    env.labels
        .insert("full_name".to_string(), full_name.to_string());
    env.labels
        .insert("label_name".to_string(), label_name.to_string());
    env.labels
        .insert("size_bytes".to_string(), size_bytes.to_string());
    env.labels.insert("reason".to_string(), "label".to_string());
    emit_self(env);
}

fn base_envelope(full_name: &str, sev: PSeverity) -> ObsEnvelope {
    ObsEnvelope {
        full_name: full_name.to_string(),
        tier: EnumValue::Known(PTier::TIER_LOG),
        sev: EnumValue::Known(sev),
        ..Default::default()
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut out = s.chars().take(max).collect::<String>();
    out.push('…');
    out
}

fn emit_self(env: ObsEnvelope) {
    // `observer()` returns the active observer — global, thread-local
    // override, or the no-op fallback. Self-events skip the typed
    // emit-with-callsite path because the runtime's own `ObsCallsite`
    // statics are not registered against the user's
    // `EVENT_SCHEMAS` slice.
    observer().emit_envelope(env);
}
