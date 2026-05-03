//! Direction A — `tracing::Event` → `obs::ObsEnvelope` via a
//! `tracing-subscriber` Layer. Spec 30 § 2.

use std::{
    cell::{Cell, RefCell},
    fmt,
    sync::Arc,
    time::Instant,
};

use dashmap::DashMap;
use obs_core::{
    FieldCapture, ObsCallsiteRegistry, SpanCtx, SpanFrame, fresh_span_id, fresh_trace_id,
};
use obs_proto::obs::v1::ObsEnvelope;
use obs_types::Severity;
use tracing::{Subscriber, field::Visit};
use tracing_core::{Event, Field, Level, callsite::Identifier};
use tracing_subscriber::{EnvFilter, Layer, layer::Context, registry::LookupSpan};

thread_local! {
    /// Spec 30 § 4.1: thread-local re-entry guard. The bridge's
    /// outbound emit (Layer → `obs::observer().emit_envelope`) runs
    /// synchronously, so a thread-local cell suffices to break the
    /// loop without the cross-thread coupling an `AtomicBool` would
    /// introduce.
    pub(crate) static IN_TRACING_BRIDGE: Cell<bool> = const { Cell::new(false) };

    /// Per-thread `FieldVisitor` rented for `on_event`. Spec 71 § 7 +
    /// § 10 / spec 93 P2-2: avoid re-allocating `Vec<(name, value)>`
    /// on every event by reusing one buffer per thread.
    static FIELD_VISITOR_BUF: RefCell<FieldVisitor> = RefCell::new(FieldVisitor::default());
}

use crate::{
    field_promotions::{FieldPromotions, level_to_severity},
    interning::{intern_or_lookup, run_prewarm},
    prewarm::PREWARM_CALLSITES,
    redactor::{DefaultPiiPatternRedactor, RedactAction, Redactor},
    typed::TypedMatcher,
};

/// Span emission mode (spec 30 § 2.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SpanEventMode {
    /// Default — never emit `ObsSpanEntered`; emit one
    /// `ObsSpanCompleted` on close.
    #[default]
    Off,
    /// Emit `ObsSpanEntered` on `on_new_span` and `ObsSpanCompleted`
    /// on `on_close`.
    Both,
    /// Emit nothing for spans (used when an upstream tracer already
    /// produces span signals and obs is consumer-only).
    Suppressed,
}

/// Callsite interning mode. Default `Off` per spec 31 § 4 KD2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum InterningMode {
    /// No interning — envelopes carry full target/template strings.
    #[default]
    Off,
    /// Hybrid — interned `callsite_id` plus the rendered message
    /// (~50 % wire savings, still readable without registry).
    Hybrid,
    /// Compact — interned id only; payload carries args. Requires the
    /// registry on the consumer side.
    Compact,
}

/// One typed promoter — matcher + closure that turns a tracing event
/// into a typed ObsEnvelope.
type TypedFn = dyn Fn(&Event<'_>, &SpanCtx<'_>, &mut FieldCapture) -> ObsEnvelope + Send + Sync;
struct TypedPromoter {
    matcher: TypedMatcher,
    promote: Arc<TypedFn>,
}

impl fmt::Debug for TypedPromoter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedPromoter")
            .field("matcher", &self.matcher)
            .finish_non_exhaustive()
    }
}

/// `tracing` → `obs` layer.
pub struct TracingToObsLayer {
    promotions: Arc<FieldPromotions>,
    redactor: Arc<dyn Redactor>,
    span_event_mode: SpanEventMode,
    typed: Vec<TypedPromoter>,
    interning: InterningMode,
    prewarm_enabled: bool,
    /// Per-callsite typed-promoter cache. Looked up by
    /// `tracing_core::callsite::Identifier` so each callsite only pays
    /// the linear `pick_typed` scan once. Spec 93 P1-3.
    typed_cache: Arc<DashMap<Identifier, Option<usize>>>,
    /// Optional `EnvFilter` syntax filter applied per event. Spec 93
    /// P1-3 (`with_filter`).
    env_filter: Option<Arc<EnvFilter>>,
}

impl fmt::Debug for TracingToObsLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TracingToObsLayer")
            .field("span_event_mode", &self.span_event_mode)
            .field("interning", &self.interning)
            .field("typed_promoters", &self.typed.len())
            .field("prewarm", &self.prewarm_enabled)
            .finish_non_exhaustive()
    }
}

impl Default for TracingToObsLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl TracingToObsLayer {
    /// Construct with the default redactor + empty promotion list +
    /// `SpanEventMode::Off`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            promotions: Arc::new(FieldPromotions::new()),
            redactor: Arc::new(DefaultPiiPatternRedactor::new()),
            span_event_mode: SpanEventMode::Off,
            typed: Vec::new(),
            interning: InterningMode::Off,
            prewarm_enabled: true,
            typed_cache: Arc::new(DashMap::new()),
            env_filter: None,
        }
    }

    /// Add an `EnvFilter`-style filter that gates events before they
    /// reach the bridge. Accepts the same grammar
    /// `tracing-subscriber::EnvFilter` does (e.g.
    /// `"warn,my_noisy_crate=off"`). Spec 93 P1-3.
    #[must_use]
    pub fn with_filter<S: AsRef<str>>(mut self, directives: S) -> Self {
        self.env_filter = match EnvFilter::try_new(directives) {
            Ok(f) => Some(Arc::new(f)),
            Err(_) => None,
        };
        self
    }

    /// Override the promotion allowlist.
    #[must_use]
    pub fn with_field_promotions(mut self, p: FieldPromotions) -> Self {
        self.promotions = Arc::new(p);
        self
    }

    /// Override the redactor.
    #[must_use]
    pub fn with_redactor(mut self, r: Arc<dyn Redactor>) -> Self {
        self.redactor = r;
        self
    }

    /// Override the span emission mode.
    #[must_use]
    pub fn with_span_events(mut self, mode: SpanEventMode) -> Self {
        self.span_event_mode = mode;
        self
    }

    /// Set callsite interning mode. Default `Off` (spec 31 § 4 KD2).
    #[must_use]
    pub fn with_interning(mut self, mode: InterningMode) -> Self {
        self.interning = mode;
        self
    }

    /// Disable startup pre-warm of well-known third-party callsites.
    /// Spec 31 § 3.3.
    #[must_use]
    pub fn with_prewarm(mut self, on: bool) -> Self {
        self.prewarm_enabled = on;
        self
    }

    /// Register a typed promoter. Multiple registrations are tried in
    /// order; the first matcher that fires wins. Spec 30 § 2.5.
    #[must_use]
    pub fn register_typed<F>(mut self, matcher: TypedMatcher, promote: F) -> Self
    where
        F: Fn(&Event<'_>, &SpanCtx<'_>, &mut FieldCapture) -> ObsEnvelope + Send + Sync + 'static,
    {
        self.typed.push(TypedPromoter {
            matcher,
            promote: Arc::new(promote),
        });
        self
    }

    fn dispatch_prewarm(&self) {
        if !self.prewarm_enabled {
            return;
        }
        let Some(observer) = obs_core::observer().callsites() else {
            return;
        };
        let _ = run_prewarm(&observer, PREWARM_CALLSITES);
    }

    /// Per-callsite typed-promoter pick with a `DashMap` cache so the
    /// linear scan over `self.typed` runs only once per unique callsite.
    /// Spec 93 P1-3.
    fn pick_typed(&self, meta: &'static tracing_core::Metadata<'static>) -> Option<&TypedPromoter> {
        let id = meta.callsite();
        if let Some(slot) = self.typed_cache.get(&id) {
            return slot.and_then(|i| self.typed.get(i));
        }
        let chosen = self.typed.iter().position(|p| p.matcher.matches(meta));
        self.typed_cache.insert(id, chosen);
        chosen.and_then(|i| self.typed.get(i))
    }

    /// Build the envelope for one tracing event. Walks the active span
    /// scope to populate `span_path`, sources `trace_id`/`span_id` from
    /// the innermost bridged span (so sibling `obs::emit!` calls
    /// correlate). Spec 93 P0-3 + P1-3.
    fn record_to_envelope<S>(&self, event: &Event<'_>, ctx: &Context<'_, S>) -> ObsEnvelope
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let metadata = event.metadata();

        // Walk active spans innermost-first. Build:
        //   * span_path: dotted path of span names ("outer.inner")
        //   * (trace_id, span_id, parent_span_id) from the innermost bridged span's stored
        //     `BridgedSpanCtx`
        let (mut span_path, trace_id, span_id, parent_span_id) = collect_span_context(ctx, event);

        FIELD_VISITOR_BUF.with(|cell| {
            let mut visitor = cell.borrow_mut();
            visitor.reset();
            event.record(&mut *visitor);

            // Try typed promoters first.
            if let Some(promoter) = self.pick_typed(metadata) {
                let mut cap = FieldCapture::new();
                for pair in &visitor.pairs {
                    cap.record_str(pair.0, pair.1.as_str());
                }
                let ctx_obj = SpanCtx::empty();
                let mut env = (promoter.promote)(event, &ctx_obj, &mut cap);
                stamp_correlation(&mut env, &trace_id, &span_id, &parent_span_id);
                return env;
            }

            // Default forensic mapping (spec 30 § 2.3 typed proto).
            let mut env = ObsEnvelope {
                full_name: "obs.v1.ObsTracingForensicEvent".to_string(),
                tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
                sev: ::buffa::EnumValue::Known(level_to_proto_sev(*metadata.level())),
                ts_ns: now_ns(),
                sampling_reason: ::buffa::EnumValue::Known(
                    obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_HEAD_RATE,
                ),
                ..Default::default()
            };
            env.labels
                .insert("target".to_string(), metadata.target().to_string());
            if let Some(module) = metadata.module_path() {
                env.labels.insert("module".to_string(), module.to_string());
            }
            if !span_path.is_empty() {
                // Take ownership; we don't need it after this point.
                env.labels
                    .insert("span_path".to_string(), std::mem::take(&mut span_path));
            }
            let target = metadata.target().to_string();
            for (name, raw_value) in &visitor.pairs {
                let mut value = raw_value.clone();
                match self.redactor.redact(&target, name, &mut value) {
                    RedactAction::Drop => continue,
                    RedactAction::Keep | RedactAction::Replaced => {}
                }
                if *name == "message" {
                    env.payload = value.into_bytes();
                    continue;
                }
                if self.promotions.admit(name, &value).is_some() {
                    env.labels.insert((*name).to_string(), value);
                } else {
                    // Avoid per-attr `format!("attr.{name}")` on the hot
                    // path by preallocating once. Spec 93 P2-2.
                    let mut key = String::with_capacity(name.len() + 5);
                    key.push_str("attr.");
                    key.push_str(name);
                    env.labels.insert(key, value);
                }
            }

            // Interning: if non-Off, intern the callsite, switch
            // full_name + adjust the wire shape (spec 31 § 4 / spec 93
            // P2-11). Hybrid keeps target/module + rendered message in
            // the typed envelope; Compact drops them in favour of the
            // length-prefixed field map.
            if !matches!(self.interning, InterningMode::Off) {
                if let Some(observer) = obs_core::observer().callsites() {
                    let level_native = level_to_severity(*metadata.level());
                    let field_names: Vec<&str> = visitor.pairs.iter().map(|(n, _)| *n).collect();
                    let (id, _new) = intern_or_lookup(
                        &observer,
                        metadata.target(),
                        metadata.name(),
                        metadata.module_path().unwrap_or(""),
                        metadata.file().unwrap_or(""),
                        metadata.line(),
                        level_native,
                        &field_names,
                        "",
                    );
                    env.callsite_id = id;
                    match self.interning {
                        InterningMode::Hybrid => {
                            // Keep typed message + labels but flip
                            // full_name to the interned schema.
                            env.full_name = "obs.v1.ObsForensicInternedEvent".to_string();
                        }
                        InterningMode::Compact => {
                            env.full_name = "obs.v1.ObsTracingInternedEvent".to_string();
                            // Compact: drop static target/module +
                            // payload (rendered message); the
                            // length-prefixed args travel as labels.
                            env.labels.remove("target");
                            env.labels.remove("module");
                            env.payload.clear();
                        }
                        InterningMode::Off => {}
                    }
                }
            }

            stamp_correlation(&mut env, &trace_id, &span_id, &parent_span_id);
            env
        })
    }
}

/// Per-span context the bridge stashes in span extensions. Driven by
/// `on_new_span` so subsequent `on_event` calls can stamp the same
/// `(trace_id, span_id, parent_span_id)` on every envelope. Spec 93
/// P0-3.
#[derive(Debug, Clone)]
pub(crate) struct BridgedSpanCtx {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub opened_at: Instant,
}

fn collect_span_context<S>(
    ctx: &Context<'_, S>,
    event: &Event<'_>,
) -> (String, String, String, String)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let mut span_path = String::new();
    let mut trace_id = String::new();
    let mut span_id = String::new();
    let mut parent_span_id = String::new();
    if let Some(scope) = ctx.event_scope(event) {
        // event_scope walks innermost-first; iterate via from_root so
        // we build the dotted path in outer→inner order. Innermost
        // BridgedSpanCtx wins (last write).
        let mut names: Vec<&'static str> = Vec::new();
        for s in scope.from_root() {
            names.push(s.metadata().name());
            if let Some(bs) = s.extensions().get::<BridgedSpanCtx>() {
                trace_id = bs.trace_id.clone();
                span_id = bs.span_id.clone();
                parent_span_id = bs.parent_span_id.clone();
            }
        }
        if !names.is_empty() {
            span_path = names.join(".");
        }
    }
    (span_path, trace_id, span_id, parent_span_id)
}

fn stamp_correlation(env: &mut ObsEnvelope, trace_id: &str, span_id: &str, parent_span_id: &str) {
    if env.trace_id.is_empty() && !trace_id.is_empty() {
        env.trace_id = trace_id.to_string();
    }
    if env.span_id.is_empty() && !span_id.is_empty() {
        env.span_id = span_id.to_string();
    }
    if env.parent_span_id.is_empty() && !parent_span_id.is_empty() {
        env.parent_span_id = parent_span_id.to_string();
    }
}

#[derive(Default)]
struct FieldVisitor {
    pairs: Vec<(&'static str, String)>,
}

impl FieldVisitor {
    /// Reset for the next event (keeps capacity). Spec 93 P2-2.
    fn reset(&mut self) {
        self.pairs.clear();
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.pairs.push((field.name(), value.to_string()));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.pairs.push((field.name(), value.to_string()));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.pairs.push((field.name(), value.to_string()));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.pairs.push((field.name(), value.to_string()));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.pairs.push((field.name(), value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.pairs.push((field.name(), format!("{value:?}")));
    }
}

fn level_to_proto_sev(level: Level) -> obs_proto::obs::v1::Severity {
    use obs_proto::obs::v1::Severity as P;
    match level_to_severity(level) {
        Severity::Trace => P::SEVERITY_TRACE,
        Severity::Debug => P::SEVERITY_DEBUG,
        Severity::Info => P::SEVERITY_INFO,
        Severity::Warn => P::SEVERITY_WARN,
        Severity::Error => P::SEVERITY_ERROR,
        Severity::Fatal => P::SEVERITY_FATAL,
        _ => P::SEVERITY_UNSPECIFIED,
    }
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Tiny helper used by the integration tests — checks if pre-warm has
/// run by counting registered entries.
#[doc(hidden)]
pub fn _prewarm_count(registry: &ObsCallsiteRegistry) -> usize {
    registry.len()
}

impl<S> Layer<S> for TracingToObsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Static defence-in-depth: skip events whose target starts
        // with `obs.bridge` (spec 30 § 4.1).
        let target = event.metadata().target();
        if target == "obs.bridge" || target.starts_with("obs.bridge.") {
            return;
        }
        // EnvFilter gate (spec 93 P1-3). Re-checks per-event because
        // the layer is registered without an explicit filter wrapper —
        // EnvFilter runs against the metadata + active span path.
        if let Some(filter) = self.env_filter.as_ref() {
            // EnvFilter's `enabled` takes `Context` by value; cloning
            // is cheap (the inner `Subscriber` ref-count is shared).
            if !filter.enabled(event.metadata(), ctx.clone()) {
                return;
            }
        }
        // Thread-local re-entry guard. Set the flag for the duration
        // of our outbound emit; on re-entry from a sink-synthesised
        // tracing event the layer returns early.
        let was_in = IN_TRACING_BRIDGE.with(|c| c.replace(true));
        if was_in {
            return;
        }
        // Lazy pre-warm — first event triggers it (the global
        // observer must be installed by now).
        static PREWARM_DONE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        let _ = PREWARM_DONE.get_or_init(|| {
            self.dispatch_prewarm();
        });
        let env = self.record_to_envelope(event, &ctx);
        obs_core::observer().emit_envelope(env);
        IN_TRACING_BRIDGE.with(|c| c.set(false));
    }

    fn on_new_span(
        &self,
        attrs: &tracing_core::span::Attributes<'_>,
        id: &tracing_core::span::Id,
        ctx: Context<'_, S>,
    ) {
        let metadata = attrs.metadata();

        // Mint trace correlation for this span: inherit from parent if
        // present (carrying its trace_id forward, becoming this span's
        // parent_span_id), otherwise root a new trace. Spec 93 P0-3.
        let parent_ctx = ctx
            .lookup_current()
            .and_then(|s| s.extensions().get::<BridgedSpanCtx>().cloned());
        let (trace_id, parent_span_id) = match parent_ctx {
            Some(p) => (p.trace_id, p.span_id),
            None => (fresh_trace_id(), String::new()),
        };
        // Allow the user to override trace_id via a `trace_id` field.
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        let trace_id = visitor
            .pairs
            .iter()
            .find(|(n, _)| *n == "trace_id")
            .map(|(_, v)| v.clone())
            .unwrap_or(trace_id);
        let span_id = fresh_span_id();

        if let Some(span) = ctx.span(id) {
            let mut ext = span.extensions_mut();
            ext.insert(BridgedSpanCtx {
                trace_id: trace_id.clone(),
                span_id: span_id.clone(),
                parent_span_id: parent_span_id.clone(),
                opened_at: Instant::now(),
            });
        }
        if self.span_event_mode != SpanEventMode::Both {
            return;
        }
        let mut env = ObsEnvelope {
            full_name: "obs.v1.ObsSpanEntered".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_TRACE),
            ts_ns: now_ns(),
            trace_id,
            span_id,
            parent_span_id,
            ..Default::default()
        };
        env.labels
            .insert("name".to_string(), metadata.name().to_string());
        env.labels
            .insert("target".to_string(), metadata.target().to_string());
        let was_in = IN_TRACING_BRIDGE.with(|c| c.replace(true));
        if !was_in {
            obs_core::observer().emit_envelope(env);
            IN_TRACING_BRIDGE.with(|c| c.set(false));
        }
    }

    fn on_close(&self, id: tracing_core::span::Id, ctx: Context<'_, S>) {
        if self.span_event_mode == SpanEventMode::Suppressed {
            return;
        }
        let Some(span) = ctx.span(&id) else { return };
        let metadata = span.metadata();
        let bridged = span.extensions().get::<BridgedSpanCtx>().cloned();
        let (opened_at, trace_id, span_id, parent_span_id) = match bridged {
            Some(b) => (b.opened_at, b.trace_id, b.span_id, b.parent_span_id),
            None => (Instant::now(), String::new(), String::new(), String::new()),
        };
        let mut env = ObsEnvelope {
            full_name: "obs.v1.ObsSpanCompleted".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_DEBUG),
            ts_ns: now_ns(),
            trace_id,
            span_id,
            parent_span_id,
            ..Default::default()
        };
        env.labels
            .insert("name".to_string(), metadata.name().to_string());
        env.labels
            .insert("target".to_string(), metadata.target().to_string());
        let latency_ns = opened_at.elapsed().as_nanos() as u64;
        env.labels
            .insert("latency_ns".to_string(), latency_ns.to_string());
        let was_in = IN_TRACING_BRIDGE.with(|c| c.replace(true));
        if !was_in {
            obs_core::observer().emit_envelope(env);
            IN_TRACING_BRIDGE.with(|c| c.set(false));
        }
    }

    fn register_callsite(
        &self,
        meta: &'static tracing_core::Metadata<'static>,
    ) -> tracing_core::Interest {
        // Cache typed-promoter pick at register time so the per-event
        // path is a single DashMap lookup. Spec 93 P1-3.
        let id = meta.callsite();
        if !self.typed_cache.contains_key(&id) {
            let chosen = self.typed.iter().position(|p| p.matcher.matches(meta));
            self.typed_cache.insert(id, chosen);
        }
        tracing_core::Interest::always()
    }
}

#[allow(dead_code)]
fn _ensure_span_frame_compiles() -> SpanFrame<'static> {
    SpanFrame {
        name: "n",
        target: "t",
    }
}

#[cfg(test)]
mod tests {
    use obs_core::{
        Observer,
        observer::{InMemoryObserver, with_test_observer},
    };
    use tracing::Level;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    #[test]
    fn test_should_lift_event_to_envelope() {
        let observer = InMemoryObserver::new();
        let handle = observer.handle();
        let observer: Arc<dyn Observer> = Arc::new(observer);
        let subscriber = tracing_subscriber::registry().with(TracingToObsLayer::new());
        with_test_observer(observer, || {
            tracing::subscriber::with_default(subscriber, || {
                tracing::info!(target: "myapp", route = "list_users", "request done");
            });
        });
        let drained = handle.drain();
        assert!(!drained.is_empty(), "expected at least one envelope");
        let env = drained.into_iter().next().unwrap();
        assert_eq!(env.full_name, "obs.v1.ObsTracingForensicEvent");
        assert_eq!(env.labels.get("target"), Some(&"myapp".to_string()));
        let _ = Level::INFO;
    }
}
