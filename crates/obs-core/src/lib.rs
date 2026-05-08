#![forbid(unsafe_code)]
#![warn(rust_2024_compatibility, missing_docs, missing_debug_implementations)]
// Tests routinely use `.unwrap()` for clarity; production code uses `?`.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

//! Runtime engine for the obs SDK — the spine that user code emits into.
//!
//! Phase-3 surface (specs/91-impl-plan.md tasks 3.1–3.15):
//!
//! - [`callsite`] — `ObsCallsite`, atomic-`Interest` cache (spec 11 § 2).
//! - [`observer`][mod@observer] — three-tier resolution + per-tier worker pool + `StandardObserver`
//!   (spec 11 §§ 3, 4 + 6.4).
//! - [`registry`] — schema registry + `ScrubbedEnvelope` (spec 14).
//! - [`envelope`] — envelope builder + projection helpers (spec 11 § 5).
//! - [`scope`] — `obs::scope!` / `obs::context!` runtime support (spec 13 §§ 2, 3, 6).
//! - [`instrumented`] — `Instrumented<F>` future adapter and `Instrument` / `WithObserver` traits
//!   (spec 13 § 3).
//! - [`sampling`] — head sampler (spec 13 § 6).
//! - [`filter`] — EnvFilter-shaped `obs::Filter` (spec 13 § 7).
//! - [`audit_spool`] — AUDIT-tier disk spool (spec 11 § 6.4).
//! - [`panic_hook`] — `install_panic_hook` (spec 11 § 6.1).
//! - [`span_trace`] — `obs::SpanTrace` (spec 13 § 9).
//! - [`sink`] — sink trait + writers (spec 20).

pub mod audit_spool;
// NB: previously `aux` — renamed because `aux` is a reserved Windows
// filename (`aux.rs` silently omitted from published tarballs on
// win32; flagged by `cargo publish`). The `aux` module path is still
// re-exported below for backwards compatibility.
pub mod callsite;
pub mod codegen_helpers;
pub mod config;
pub mod config_watcher;
pub mod emit;
pub mod envelope;
pub mod filter;
pub mod forensic;
pub mod instrumented;
pub mod metric;
pub mod observer;
pub mod panic_hook;
pub mod propagator;
pub mod registry;
pub mod resource;
pub mod sampling;
pub mod scope;
mod self_event;
pub(crate) mod self_events;
pub use self_event::{now_ns, self_event};
/// Public re-exports for self-event helpers consumed by sinks/middleware
/// that emit them on behalf of the runtime (e.g. OTLP trace sink emits
/// `ObsSpanPairOrphaned` after pair_timeout). Spec 93 P1-2 + P1-7.
pub mod self_events_public {
    pub use crate::self_events::{
        emit_callsite_hash_collision_pub as emit_callsite_hash_collision,
        emit_label_cardinality_high_pub as emit_label_cardinality_high,
        emit_oversized_label_dropped_pub as emit_label_oversized,
        emit_span_pair_orphaned_pub as emit_span_pair_orphaned,
    };
}
pub mod sink;
pub mod span_trace;
#[cfg(feature = "test")]
pub mod test;
pub mod wire;

#[doc(hidden)]
#[cfg(feature = "test")]
pub mod __macro_deps {
    pub use serde_json;
}

/// Cap an external string at `max_bytes` (UTF-8-safe boundary), append
/// the `…<truncated:N>` suffix when the input was clipped, and emit
/// one `ObsLabelOversized` self-event for telemetry. Returns the
/// (possibly truncated) string. Spec 95 § 3.10 / P2-AH.
///
/// Boundary callers (HTTP middleware, bridge field visitor) should
/// run this on every untrusted string before it lands in
/// `env.labels` or the typed payload.
#[must_use]
pub fn cap_external_string(field: &'static str, raw: String, max_bytes: u16) -> String {
    let max = max_bytes as usize;
    if raw.len() <= max {
        return raw;
    }
    let original_size = raw.len() as u64;
    // Find the largest UTF-8-safe truncation point ≤ max.
    let mut end = max;
    while end > 0 && !raw.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + 24);
    out.push_str(&raw[..end]);
    out.push_str(&format!("…<truncated:{}>", original_size));
    self_events::emit_oversized_label_dropped_pub(field, original_size, out.len() as u64);
    out
}

pub use callsite::ObsCallsite;
/// Back-compat alias: the module used to be called `aux`. Kept as a
/// re-export so downstream `use obs_core::aux::…` paths keep working.
#[doc(hidden)]
pub use codegen_helpers as aux;
pub use codegen_helpers::{BuildableTo, EnumCount, FieldCapture, SpanCtx, SpanFrame};
pub use config::{EventsConfig, SamplingConfig};
pub use config_watcher::{ConfigWatcher, DEFAULT_DEBOUNCE};
pub use emit::Emit;
pub use envelope::{Envelope, EventSchema, FieldMeta, FieldRole};
pub use filter::Filter;
pub use instrumented::{Instrument, Instrumented, WithObserver};
pub use metric::{MetricEmitter, NoopMetricEmitter};
pub use obs_proto::{
    ENVELOPE_FORMAT_VER,
    obs::v1::{
        Cardinality, Classification, FieldKind, MetricKind, ObsBatch, ObsEnvelope, SamplingReason,
        Severity, Tier,
    },
};
pub use observer::{
    BuildError, InMemoryHandle, InMemoryObserver, NoopObserver, Observer, StandardObserver,
    StandardObserverBuilder, ThreadObserverGuard, WeakObserver, WorkerCounters, install_observer,
    install_observer_arc, observer, observer_weak, with_observer_task, with_observer_task_sync,
    with_observer_thread_local, with_test_observer,
};
pub use panic_hook::install_panic_hook;
pub use propagator::{
    ObsTraceCtx, W3cPropagator, extract_w3c, fresh_span_id, fresh_trace_id, inject_w3c,
    status_class,
};
pub use registry::{
    ArrowEventSchema, ArrowField, ArrowLeafType, ArrowSchemaModel, ArrowStructBuilder,
    CallsiteRecord, CallsiteSource, DecodeError, ENVELOPE_COLUMNS, EVENT_SCHEMAS,
    EventSchemaErased, ObsCallsiteRegistry, OtelAttributeView, OtlpValue, SchemaRegistry,
    ScrubError, ScrubbedEnvelope, callsite_id, scrub_payload,
};
pub use resource::ResourceAttrs;
pub use sampling::{SamplingDecision, decide as sample_decide};
pub use scope::{ScopeField, ScopeFrame, ScopeFrameBuilder, ScopeGuard, ScopeKind};
pub use sink::{
    FanOutSink, FormatterStyle, InMemorySink, LevelSplitWriter, MakeWriter, NdjsonFileSink,
    NonBlockingWriter, NoopSink, RollingFileWriter, RollingFileWriterBuilder, RollingPolicy, Sink,
    StderrWriter, StdoutSink, StdoutWriter, TeeWriter, WorkerGuard,
};
pub use span_trace::SpanTrace;

/// Re-exports for code generated by `obs-macros::derive(Event)` and
/// `obs-build`. End users should not depend on these directly.
#[doc(hidden)]
pub mod __private {
    pub use std::sync::OnceLock;

    pub use buffa;
    pub use bytes::BytesMut;
    pub use linkme;
    /// Enum re-exports so macros can reach these through the
    /// `__private` namespace without forcing user code to depend on
    /// `obs-proto` directly. The `Proto*` aliases survive as back-compat
    /// symbols; after Phase 3b (obs-types retirement) they point at
    /// the same types the short-name re-exports do.
    pub use obs_proto::obs::v1::{
        Cardinality, Classification, FieldKind, MetricKind, SamplingReason,
        SamplingReason as ProtoSamplingReason, Severity, Severity as ProtoSeverity, Tier,
        Tier as ProtoTier,
    };
    pub use obs_proto::{__private::*, UnknownVariant};
    pub use secrecy;
    pub use serde_json;

    pub use crate::{
        callsite::ObsCallsite,
        codegen_helpers::{BuildableTo, EnumCount, FieldCapture, SpanCtx, SpanFrame},
        forensic::{ForensicLimiter, ensure_limiter, try_acquire_forensic},
        registry::{EVENT_SCHEMAS, EventSchemaErased, Sealed},
        scope::{ScopeField, ScopeGuard, ScopeKind},
        wire::BuffaEncodeField,
    };

    /// L011 helper: const-time test that a string starts with a known
    /// prefix.
    #[must_use]
    #[allow(clippy::indexing_slicing)]
    pub const fn starts_with_obs(name: &str, prefix: &str) -> bool {
        let n = name.as_bytes();
        let p = prefix.as_bytes();
        if n.len() < p.len() {
            return false;
        }
        let mut i = 0;
        while i < p.len() {
            if n[i] != p[i] {
                return false;
            }
            i += 1;
        }
        true
    }
}
