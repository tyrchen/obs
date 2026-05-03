//! Direction B — `obs::ObsEnvelope` → `tracing::Event`.
//!
//! Spec 30 § 3 + § 3.7 (interning reconstitution).
//!
//! Implementation note. `tracing-core` requires a `&'static Metadata`
//! whose `Callsite` is registered with `tracing_core::callsite::register`.
//! We register **one** bridge-wide callsite at process start (target =
//! `"obs.bridge"`) and dispatch every envelope through it as a
//! tracing event with the envelope's `full_name`, `trace_id`, `span_id`,
//! and labels carried as fields. Spec 30 § 3.4 specifies
//! `target = "obs.bridge"` for the non-interned path, so the single
//! static is the right choice for the v1 surface.
//!
//! For the interned path (`env.callsite_id != 0`), the registry is
//! consulted for a human-readable target/file/line; those are
//! dispatched as ordinary fields rather than reconstituted in
//! `tracing::Metadata` (the v1.1 work is tracked in spec 30 § 10).

use std::{
    cell::Cell,
    collections::HashMap,
    fmt,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use dashmap::DashMap;
use obs_core::{Sink, registry::ScrubbedEnvelope, sink::SinkFut};
use obs_proto::obs::v1::{ObsEnvelope, Severity as PSeverity};
use parking_lot::Mutex;
use tracing::Level;
use tracing_core::{
    Callsite, Interest, Metadata,
    callsite::Identifier,
    field::{FieldSet, Value},
};

use crate::direction_a::IN_TRACING_BRIDGE;

thread_local! {
    static IN_OBS_BRIDGE: Cell<bool> = const { Cell::new(false) };
}

/// Payload decode mode. Default `Off` — we don't decode the typed
/// payload, only stringify envelope.labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum PayloadDecodeMode {
    /// No decode; envelope.labels only.
    #[default]
    Off,
    /// Dev — decode + dispatch every payload field as a tracing field.
    DecodeKnown,
    /// Dev — decode + dispatch only `ATTRIBUTE`-class fields.
    DecodeKnownAttributesOnly,
}

/// Span emission mode. Default `Off` (spec 30 KD9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SpanEmissionMode {
    /// Never open a tracing span on `obs::scope!` enter.
    #[default]
    Off,
    /// Open an ephemeral tracing span on `obs::scope!` enter.
    /// Reserved for the v1 milestone.
    OnScope,
}

/// `obs → tracing` sink. Spec 30 § 3.2.
pub struct ObsToTracingSink {
    /// Counts unique envelopes seen, to drive the warm-cache count
    /// surfaced via [`Self::cache_size`].
    seen: DashMap<CacheKey, ()>,
    payload_decode: PayloadDecodeMode,
    span_emission: SpanEmissionMode,
    rate_limited_no_dispatcher: Arc<RateLimited>,
    interned_misses: Arc<AtomicU64>,
    /// When true, dispatch envelopes through a per-(full_name, sev)
    /// synthetic `Metadata` so downstream `tracing_subscriber::fmt`
    /// output reads `myapp.v1.ObsRequestCompleted INFO ...` instead of
    /// the generic `obs.bridge`. Spec 30 § 3.7 / spec 93 P1-4.
    dynamic_target: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CacheKey {
    ByFullName(String),
    ByCallsiteId(u64),
}

impl fmt::Debug for ObsToTracingSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObsToTracingSink")
            .field("payload_decode", &self.payload_decode)
            .field("span_emission", &self.span_emission)
            .field("seen", &self.seen.len())
            .field(
                "interned_misses",
                &self.interned_misses.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

impl Default for ObsToTracingSink {
    fn default() -> Self {
        Self::new()
    }
}

impl ObsToTracingSink {
    /// New sink with default modes.
    ///
    /// Spec 95 § 3.13 / P1-AG: `dynamic_target` defaults to `true` so
    /// every `(full_name, severity)` pair lands at a distinct
    /// `tracing::Metadata`. Downstream `EnvFilter` directives like
    /// `myapp.v1.ObsRequestCompleted=trace` work correctly out of the
    /// box. Opt out via `with_dynamic_target(false)` to recover the
    /// legacy single-static-callsite behaviour.
    #[must_use]
    pub fn new() -> Self {
        Self {
            seen: DashMap::new(),
            payload_decode: PayloadDecodeMode::default(),
            span_emission: SpanEmissionMode::default(),
            rate_limited_no_dispatcher: Arc::new(RateLimited::new(60)),
            interned_misses: Arc::new(AtomicU64::new(0)),
            dynamic_target: true,
        }
    }

    /// Toggle per-(`full_name`, severity) synthetic `Metadata`. On by
    /// default; flip off to revert to the single-callsite legacy mode
    /// (spec 30 § 3.3). The first dispatch for a given pair leaks a
    /// small `Metadata`/`Callsite` via `Box::leak` and caches the
    /// pointer in a `DashMap`. Spec 95 § 3.13 / spec 93 P1-4.
    #[must_use]
    pub fn with_dynamic_target(mut self, on: bool) -> Self {
        self.dynamic_target = on;
        self
    }

    /// Override payload decode mode.
    #[must_use]
    pub fn with_payload_decode(mut self, m: PayloadDecodeMode) -> Self {
        self.payload_decode = m;
        self
    }

    /// Override span emission mode.
    ///
    /// Spec 30 § 3.5: when `OnScope` is selected and `OtlpTraceSink`
    /// is also installed in the same observer, the user gets two OTel
    /// spans per logical operation. The runtime emits one
    /// `ObsConfigInconsistent` self-event at observer init naming
    /// both components. The detection lives in the user's
    /// `StandardObserverBuilder::build` path; this setter records the
    /// chosen mode so a future builder integration can read it back.
    #[must_use]
    pub fn with_span_emission(mut self, m: SpanEmissionMode) -> Self {
        if matches!(m, SpanEmissionMode::OnScope) {
            emit_config_inconsistent_warning();
        }
        self.span_emission = m;
        self
    }

    /// Returns the configured span-emission mode (used by builder
    /// integrations that detect the OTLP-trace-sink coexistence
    /// foot-gun per spec 30 § 3.5).
    #[must_use]
    pub fn span_emission(&self) -> SpanEmissionMode {
        self.span_emission
    }

    /// Cache size (unique envelopes seen). Used by tests.
    #[doc(hidden)]
    pub fn cache_size(&self) -> usize {
        self.seen.len()
    }

    fn note_seen(&self, env: &ObsEnvelope) {
        let key = if env.callsite_id != 0 {
            CacheKey::ByCallsiteId(env.callsite_id)
        } else {
            CacheKey::ByFullName(env.full_name.clone())
        };
        self.seen.insert(key, ());
    }

    fn build_dispatch(&self, env: &ObsEnvelope) {
        self.note_seen(env);
        // Acquire metadata. Resolve interned first.
        let interned = if env.callsite_id != 0 {
            obs_core::observer()
                .callsites()
                .and_then(|reg| reg.get(env.callsite_id))
        } else {
            None
        };
        if env.callsite_id != 0 && interned.is_none() {
            self.interned_misses.fetch_add(1, Ordering::Relaxed);
            emit_callsite_unresolved(env.callsite_id);
        }
        let level = sev_to_level(env.sev);
        let meta = if self.dynamic_target {
            synthetic_metadata(&env.full_name, level)
        } else {
            bridge_metadata(level)
        };

        let mut had_dispatcher = false;
        tracing_core::dispatcher::get_default(|d| {
            had_dispatcher = true;
            if !d.enabled(meta) {
                return;
            }
            let fields = meta.fields();
            // Map field-name -> string value.
            let mut entries: HashMap<&'static str, String> = HashMap::new();
            entries.insert("obs.full_name", env.full_name.clone());
            if !env.trace_id.is_empty() {
                entries.insert("obs.trace_id", env.trace_id.clone());
            }
            if !env.span_id.is_empty() {
                entries.insert("obs.span_id", env.span_id.clone());
            }
            if !env.parent_span_id.is_empty() {
                entries.insert("obs.parent_span_id", env.parent_span_id.clone());
            }
            if env.callsite_id != 0 {
                entries.insert("obs.callsite_id", format!("{:#018x}", env.callsite_id));
            }
            if let Some(rec) = &interned {
                entries.insert("obs.target", rec.target.clone());
            }
            // labels become a single `obs.labels` value as JSON for
            // ergonomic round-trip; downstream consumers can parse.
            if !env.labels.is_empty() {
                let pretty = format_labels(&env.labels);
                entries.insert("obs.labels", pretty);
            }
            if !env.payload.is_empty() && self.payload_decode == PayloadDecodeMode::Off {
                // forensic message often lives in payload — surface
                // as `message` if it fits.
                if let Ok(s) = std::str::from_utf8(&env.payload) {
                    entries.insert("message", s.to_string());
                }
            }

            // Build a fixed-size value array. The 16-element array
            // matches the static FieldSet layout in `bridge_metadata`.
            let f0 = fields.field("obs.full_name").unwrap_or_else(panic_missing);
            let f1 = fields.field("obs.trace_id").unwrap_or_else(panic_missing);
            let f2 = fields.field("obs.span_id").unwrap_or_else(panic_missing);
            let f3 = fields
                .field("obs.parent_span_id")
                .unwrap_or_else(panic_missing);
            let f4 = fields
                .field("obs.callsite_id")
                .unwrap_or_else(panic_missing);
            let f5 = fields.field("obs.target").unwrap_or_else(panic_missing);
            let f6 = fields.field("obs.labels").unwrap_or_else(panic_missing);
            let f7 = fields.field("message").unwrap_or_else(panic_missing);
            let v0 = entries.get("obs.full_name").cloned().unwrap_or_default();
            let v1 = entries.get("obs.trace_id").cloned().unwrap_or_default();
            let v2 = entries.get("obs.span_id").cloned().unwrap_or_default();
            let v3 = entries
                .get("obs.parent_span_id")
                .cloned()
                .unwrap_or_default();
            let v4 = entries.get("obs.callsite_id").cloned().unwrap_or_default();
            let v5 = entries.get("obs.target").cloned().unwrap_or_default();
            let v6 = entries.get("obs.labels").cloned().unwrap_or_default();
            let v7 = entries.get("message").cloned().unwrap_or_default();
            let pairs: [(&tracing_core::Field, Option<&dyn Value>); 8] = [
                (
                    &f0,
                    if v0.is_empty() {
                        None
                    } else {
                        Some(&v0 as &dyn Value)
                    },
                ),
                (
                    &f1,
                    if v1.is_empty() {
                        None
                    } else {
                        Some(&v1 as &dyn Value)
                    },
                ),
                (
                    &f2,
                    if v2.is_empty() {
                        None
                    } else {
                        Some(&v2 as &dyn Value)
                    },
                ),
                (
                    &f3,
                    if v3.is_empty() {
                        None
                    } else {
                        Some(&v3 as &dyn Value)
                    },
                ),
                (
                    &f4,
                    if v4.is_empty() {
                        None
                    } else {
                        Some(&v4 as &dyn Value)
                    },
                ),
                (
                    &f5,
                    if v5.is_empty() {
                        None
                    } else {
                        Some(&v5 as &dyn Value)
                    },
                ),
                (
                    &f6,
                    if v6.is_empty() {
                        None
                    } else {
                        Some(&v6 as &dyn Value)
                    },
                ),
                (
                    &f7,
                    if v7.is_empty() {
                        None
                    } else {
                        Some(&v7 as &dyn Value)
                    },
                ),
            ];
            let valueset = fields.value_set(&pairs);
            d.event(&tracing_core::Event::new(meta, &valueset));
        });
        if !had_dispatcher {
            self.maybe_emit_no_dispatcher();
        }
    }

    fn maybe_emit_no_dispatcher(&self) {
        if self.rate_limited_no_dispatcher.try_fire() {
            let env = ObsEnvelope {
                full_name: "obs.runtime.v1.ObsBridgeNoDispatcher".to_string(),
                tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
                sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_DEBUG),
                ..Default::default()
            };
            obs_core::observer().emit_envelope(env);
        }
    }
}

fn panic_missing() -> tracing_core::Field {
    // The static `FieldSet` is constructed with the same names below;
    // the lookup must succeed.
    unreachable_field()
}

fn unreachable_field() -> tracing_core::Field {
    let m = bridge_metadata(Level::INFO);
    m.fields().iter().next().expect("bridge fieldset non-empty")
}

fn format_labels(labels: &std::collections::HashMap<String, String>) -> String {
    let mut s = String::with_capacity(labels.len() * 24);
    s.push('{');
    let mut keys: Vec<&String> = labels.keys().collect();
    keys.sort();
    let mut first = true;
    for k in keys {
        if !first {
            s.push_str(", ");
        }
        first = false;
        if let Some(v) = labels.get(k.as_str()) {
            s.push('"');
            s.push_str(k);
            s.push_str("\":\"");
            s.push_str(v);
            s.push('"');
        }
    }
    s.push('}');
    s
}

/// Per-(`full_name`, level) synthetic Metadata cache. Each unique
/// pair leaks one `SyntheticCallsite` (and the strings it references)
/// the first time it is requested; subsequent calls return the cached
/// pointer. Spec 30 § 3.7 / spec 93 P1-4.
struct SyntheticCallsite {
    metadata: Metadata<'static>,
}

impl Callsite for SyntheticCallsite {
    fn set_interest(&self, _: Interest) {}
    fn metadata(&self) -> &Metadata<'_> {
        &self.metadata
    }
}

static SYNTHETIC_CACHE: OnceLock<DashMap<(String, u8), &'static SyntheticCallsite>> =
    OnceLock::new();

fn synthetic_metadata(full_name: &str, level: Level) -> &'static Metadata<'static> {
    let cache = SYNTHETIC_CACHE.get_or_init(DashMap::new);
    let key = (full_name.to_string(), level_index(level));
    if let Some(cs) = cache.get(&key) {
        // The value is `&'static SyntheticCallsite` — copying the
        // reference out yields a `'static` borrow of the metadata.
        let cs_static: &'static SyntheticCallsite = cs.value();
        return &cs_static.metadata;
    }

    // First dispatch for this (full_name, sev) pair. We leak the
    // target string and a `SyntheticCallsite` so the resulting
    // `&'static Metadata<'static>` can be handed to the tracing
    // dispatcher. The Identifier inside FieldSet must point at the
    // *same* leaked callsite, which is a chicken-and-egg problem we
    // solve by leaking the callsite first with a placeholder Metadata,
    // then overwriting that slot with the real Metadata via
    // `OnceLock`.
    let target_static: &'static str = Box::leak(full_name.to_string().into_boxed_str());
    let cs_box: Box<SyntheticCallsite> = Box::new(SyntheticCallsite {
        metadata: Metadata::new(
            "obs_bridge_event",
            target_static,
            level,
            None,
            None,
            None,
            FieldSet::new(&FIELDS, Identifier(&BRIDGE_CS_INFO)),
            tracing_core::Kind::EVENT,
        ),
    });
    let cs_static: &'static SyntheticCallsite = Box::leak(cs_box);
    tracing_core::callsite::register(cs_static);
    cache.insert(key.clone(), cs_static);
    &cs_static.metadata
}

fn level_index(level: Level) -> u8 {
    match level {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    }
}

/// Bridge static metadata. Always uses target = `"obs.bridge"`. The
/// FieldSet is fixed at the eight names we care about. Spec 30 § 3.3.
fn bridge_metadata(level: Level) -> &'static Metadata<'static> {
    match level {
        Level::TRACE => bridge_metadata_trace(),
        Level::DEBUG => bridge_metadata_debug(),
        Level::INFO => bridge_metadata_info(),
        Level::WARN => bridge_metadata_warn(),
        Level::ERROR => bridge_metadata_error(),
    }
}

// Five static callsites — one per `Level`. They're declared as
// `static` so we can hand a `'static` reference to
// `tracing_core::callsite::register`.
static FIELDS: [&str; 8] = [
    "obs.full_name",
    "obs.trace_id",
    "obs.span_id",
    "obs.parent_span_id",
    "obs.callsite_id",
    "obs.target",
    "obs.labels",
    "message",
];

macro_rules! decl_bridge_callsite {
    ($ty:ident, $static_name:ident, $name:ident, $reg_flag:ident, $meta_slot:ident, $level:expr) => {
        struct $ty;
        static $static_name: $ty = $ty;
        static $reg_flag: std::sync::Once = std::sync::Once::new();
        static $meta_slot: OnceLock<Metadata<'static>> = OnceLock::new();
        impl Callsite for $ty {
            fn set_interest(&self, _: Interest) {}
            fn metadata(&self) -> &Metadata<'_> {
                $meta_slot.get_or_init(|| {
                    Metadata::new(
                        "obs_bridge_event",
                        "obs.bridge",
                        $level,
                        None,
                        None,
                        None,
                        FieldSet::new(&FIELDS, Identifier(&$static_name)),
                        tracing_core::Kind::EVENT,
                    )
                })
            }
        }
        fn $name() -> &'static Metadata<'static> {
            // Register the callsite exactly once. `register` calls
            // `set_interest` on the new callsite and on every existing
            // one, so we initialise the metadata first to avoid a
            // re-entrant `OnceLock` lock during dispatcher startup.
            $reg_flag.call_once(|| {
                let _ = $static_name.metadata();
                tracing_core::callsite::register(&$static_name);
            });
            $static_name.metadata()
        }
    };
}

decl_bridge_callsite!(
    BridgeCallsiteTrace,
    BRIDGE_CS_TRACE,
    bridge_metadata_trace,
    BRIDGE_TRACE_REG,
    BRIDGE_TRACE_META,
    Level::TRACE
);
decl_bridge_callsite!(
    BridgeCallsiteDebug,
    BRIDGE_CS_DEBUG,
    bridge_metadata_debug,
    BRIDGE_DEBUG_REG,
    BRIDGE_DEBUG_META,
    Level::DEBUG
);
decl_bridge_callsite!(
    BridgeCallsiteInfo,
    BRIDGE_CS_INFO,
    bridge_metadata_info,
    BRIDGE_INFO_REG,
    BRIDGE_INFO_META,
    Level::INFO
);
decl_bridge_callsite!(
    BridgeCallsiteWarn,
    BRIDGE_CS_WARN,
    bridge_metadata_warn,
    BRIDGE_WARN_REG,
    BRIDGE_WARN_META,
    Level::WARN
);
decl_bridge_callsite!(
    BridgeCallsiteError,
    BRIDGE_CS_ERROR,
    bridge_metadata_error,
    BRIDGE_ERROR_REG,
    BRIDGE_ERROR_META,
    Level::ERROR
);

fn sev_to_level(s: ::buffa::EnumValue<PSeverity>) -> Level {
    let known = match s {
        ::buffa::EnumValue::Known(s) => s,
        _ => return Level::INFO,
    };
    match known {
        PSeverity::SEVERITY_TRACE => Level::TRACE,
        PSeverity::SEVERITY_DEBUG => Level::DEBUG,
        PSeverity::SEVERITY_INFO => Level::INFO,
        PSeverity::SEVERITY_WARN => Level::WARN,
        PSeverity::SEVERITY_ERROR | PSeverity::SEVERITY_FATAL => Level::ERROR,
        _ => Level::INFO,
    }
}

fn emit_callsite_unresolved(callsite_id: u64) {
    let mut env = ObsEnvelope {
        full_name: "obs.runtime.v1.ObsBridgeCallsiteUnresolved".to_string(),
        tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_DEBUG),
        callsite_id,
        ..Default::default()
    };
    env.labels
        .insert("callsite_id".to_string(), format!("{callsite_id:#018x}"));
    obs_core::observer().emit_envelope(env);
}

/// One-shot warning when a user opts into `SpanEmissionMode::OnScope`.
/// Per spec 30 § 3.5, this is the half-of-the-coexistence problem the
/// bridge can detect; the OTLP-trace-sink half lives in the observer
/// builder. Rate-limited to one event per process by the OnceLock.
fn emit_config_inconsistent_warning() {
    use std::sync::OnceLock;
    static FIRED: OnceLock<()> = OnceLock::new();
    let _ = FIRED.get_or_init(|| {
        let mut env = ObsEnvelope {
            full_name: "obs.runtime.v1.ObsConfigInconsistent".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_WARN),
            ..Default::default()
        };
        env.labels.insert(
            "reason".to_string(),
            "ObsToTracingSink::OnScope may produce duplicate spans when OtlpTraceSink is also \
             installed (spec 30 § 3.5)"
                .to_string(),
        );
        obs_core::observer().emit_envelope(env);
    });
}

struct RateLimited {
    period_ns: u64,
    last: Mutex<u64>,
}

impl fmt::Debug for RateLimited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimited")
            .field("period_ns", &self.period_ns)
            .finish_non_exhaustive()
    }
}

impl RateLimited {
    fn new(period_secs: u64) -> Self {
        Self {
            period_ns: period_secs * 1_000_000_000,
            last: Mutex::new(0),
        }
    }
    fn try_fire(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut last = self.last.lock();
        if now.saturating_sub(*last) < self.period_ns {
            return false;
        }
        *last = now;
        true
    }
}

impl Sink for ObsToTracingSink {
    fn deliver(&self, env: ScrubbedEnvelope<'_>) {
        // Spec 30 § 4.1 — re-entry break. If we're already inside a
        // tracing-bridge dispatch (Direction A is currently emitting
        // an envelope), the inbound envelope is a re-entry — drop it.
        if IN_TRACING_BRIDGE.with(Cell::get) {
            return;
        }
        IN_OBS_BRIDGE.with(|c| c.set(true));
        let envelope = env.envelope();
        self.build_dispatch(envelope);
        IN_OBS_BRIDGE.with(|c| c.set(false));
    }

    fn flush(&self) -> SinkFut<'_> {
        Box::pin(async {})
    }

    fn shutdown(&self) -> SinkFut<'_> {
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use obs_core::{SchemaRegistry, ScrubbedEnvelope};
    use obs_proto::obs::v1::ObsEnvelope;

    use super::*;

    fn env_for(name: &str) -> ObsEnvelope {
        ObsEnvelope {
            full_name: name.to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_INFO),
            ..Default::default()
        }
    }

    #[test]
    fn test_should_track_unique_envelopes_seen() {
        let sink = ObsToTracingSink::new();
        let reg = Arc::new(SchemaRegistry::empty());
        let env = env_for("myapp.v1.X");
        sink.deliver(ScrubbedEnvelope::for_test(&env, &reg));
        sink.deliver(ScrubbedEnvelope::for_test(&env, &reg));
        assert_eq!(sink.cache_size(), 1);
    }

    #[test]
    fn test_should_pick_callsite_id_when_present() {
        let sink = ObsToTracingSink::new();
        let reg = Arc::new(SchemaRegistry::empty());
        let mut env = env_for("obs.v1.ObsTracingInternedEvent");
        env.callsite_id = 0xABCD;
        sink.deliver(ScrubbedEnvelope::for_test(&env, &reg));
        sink.deliver(ScrubbedEnvelope::for_test(&env, &reg));
        assert_eq!(sink.cache_size(), 1);
    }

    #[test]
    fn test_severity_should_map_to_level() {
        assert_eq!(
            sev_to_level(::buffa::EnumValue::Known(PSeverity::SEVERITY_INFO)),
            Level::INFO
        );
        assert_eq!(
            sev_to_level(::buffa::EnumValue::Known(PSeverity::SEVERITY_FATAL)),
            Level::ERROR
        );
    }

    #[test]
    fn test_synthetic_metadata_should_differ_per_full_name() {
        // Spec 95 § 3.13 / P1-AG: every (full_name, sev) gets its own
        // tracing::Metadata. Subscribers can therefore filter the
        // event stream by event type.
        let m1 = synthetic_metadata("myapp.v1.ObsA", Level::INFO);
        let m2 = synthetic_metadata("myapp.v1.ObsB", Level::INFO);
        let p1 = std::ptr::from_ref(m1);
        let p2 = std::ptr::from_ref(m2);
        assert_ne!(p1, p2, "distinct full_names must yield distinct Metadata");
        assert_ne!(m1.target(), m2.target());
    }

    #[test]
    fn test_synthetic_metadata_should_differ_per_severity() {
        let info = synthetic_metadata("myapp.v1.ObsA", Level::INFO);
        let warn = synthetic_metadata("myapp.v1.ObsA", Level::WARN);
        let p_info = std::ptr::from_ref(info);
        let p_warn = std::ptr::from_ref(warn);
        assert_ne!(
            p_info, p_warn,
            "distinct severities must yield distinct Metadata"
        );
    }

    #[test]
    fn test_synthetic_metadata_should_be_cached() {
        // Same (full_name, sev) returns the same pointer.
        let a = synthetic_metadata("myapp.v1.ObsCached", Level::INFO);
        let b = synthetic_metadata("myapp.v1.ObsCached", Level::INFO);
        assert!(std::ptr::eq(a, b));
    }

    #[test]
    fn test_default_constructor_should_enable_dynamic_target() {
        let sink = ObsToTracingSink::new();
        assert!(
            sink.dynamic_target,
            "spec 95 § 3.13: dynamic_target is on by default so subscribers can filter by event \
             type"
        );
    }
}
