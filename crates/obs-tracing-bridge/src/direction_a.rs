//! Direction A — `tracing::Event` → `obs::ObsEnvelope` via a
//! `tracing-subscriber` Layer. Spec 30 § 2.

use std::{cell::Cell, fmt, sync::Arc, time::Instant};

use obs_proto::obs::v1::ObsEnvelope;
use parking_lot::Mutex;
use tracing::{Subscriber, field::Visit};
use tracing_core::{Event, Field, Level};
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

thread_local! {
    /// Spec 30 § 4.1: thread-local re-entry guard. The bridge's
    /// outbound emit (Layer → `obs::observer().emit_envelope`) runs
    /// synchronously, so a thread-local cell suffices to break the
    /// loop without the cross-thread coupling an `AtomicBool` would
    /// introduce. Mirrors the `IN_TRACING_BRIDGE` cell named in spec
    /// 30 § 4.1.
    static IN_TRACING_BRIDGE: Cell<bool> = const { Cell::new(false) };
}

use crate::{
    field_promotions::{FieldPromotions, level_to_severity},
    redactor::{DefaultPiiPatternRedactor, RedactAction, Redactor},
};

/// Span emission mode (spec 30 § 2.3 + § 3.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SpanEventMode {
    /// Default — never emit `ObsSpanEntered`; emit one `ObsSpanCompleted`
    /// on close.
    #[default]
    Off,
    /// Emit `ObsSpanEntered` on `on_new_span` and `ObsSpanCompleted`
    /// on `on_close`.
    Both,
    /// Emit nothing for spans (used when an upstream tracer already
    /// produces span signals and obs is consumer-only).
    Suppressed,
}

/// `tracing` → `obs` layer.
pub struct TracingToObsLayer {
    promotions: Arc<FieldPromotions>,
    redactor: Arc<dyn Redactor>,
    span_event_mode: SpanEventMode,
}

impl fmt::Debug for TracingToObsLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TracingToObsLayer")
            .field("span_event_mode", &self.span_event_mode)
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
        }
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

    fn record_to_envelope(&self, event: &Event<'_>) -> ObsEnvelope {
        let metadata = event.metadata();
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
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
        // Promotions: walk recorded fields, consult allowlist + redactor.
        let target = metadata.target().to_string();
        for (name, raw_value) in visitor.into_pairs() {
            let mut value = raw_value;
            match self.redactor.redact(&target, name, &mut value) {
                RedactAction::Drop => continue,
                RedactAction::Keep | RedactAction::Replaced => {}
            }
            if name == "message" {
                env.payload = value.into_bytes();
                continue;
            }
            if self.promotions.admit(name, &value).is_some() {
                env.labels.insert(name.to_string(), value);
            } else {
                // Falls into the attrs slot; for the Phase-3 minimal
                // representation we put it under a `attr.<key>` label
                // so the InMemoryObserver tests can assert on it.
                env.labels.insert(format!("attr.{name}"), value);
            }
        }
        env
    }
}

#[derive(Default)]
struct FieldVisitor {
    pairs: Vec<(&'static str, String)>,
}

impl FieldVisitor {
    fn into_pairs(self) -> Vec<(&'static str, String)> {
        self.pairs
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

#[allow(non_snake_case, non_upper_case_globals)]
fn level_to_proto_sev(level: Level) -> obs_proto::obs::v1::Severity {
    use obs_proto::obs::v1::Severity as P;
    match level_to_severity(level) {
        obs_types::Severity::Trace => P::SEVERITY_TRACE,
        obs_types::Severity::Debug => P::SEVERITY_DEBUG,
        obs_types::Severity::Info => P::SEVERITY_INFO,
        obs_types::Severity::Warn => P::SEVERITY_WARN,
        obs_types::Severity::Error => P::SEVERITY_ERROR,
        obs_types::Severity::Fatal => P::SEVERITY_FATAL,
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

impl<S> Layer<S> for TracingToObsLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Loop guard: skip events whose target starts with `obs.bridge`
        // (defence-in-depth from spec 30 § 4.1).
        let target = event.metadata().target();
        if target == "obs.bridge" || target.starts_with("obs.bridge.") {
            return;
        }
        // Thread-local re-entry guard. If a sink synthesises a
        // tracing event while we are already inside the bridge, that
        // synthetic event sees `IN_TRACING_BRIDGE = true` and returns
        // immediately. Spec 30 § 4.1.
        let was_in = IN_TRACING_BRIDGE.with(|c| c.replace(true));
        if was_in {
            return;
        }
        let env = self.record_to_envelope(event);
        obs_core::observer().emit_envelope(env);
        IN_TRACING_BRIDGE.with(|c| c.set(false));
    }

    fn on_new_span(
        &self,
        attrs: &tracing_core::span::Attributes<'_>,
        id: &tracing_core::span::Id,
        ctx: Context<'_, S>,
    ) {
        if self.span_event_mode != SpanEventMode::Both {
            return;
        }
        let metadata = attrs.metadata();
        let mut env = ObsEnvelope {
            full_name: "obs.v1.ObsSpanEntered".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_TRACE),
            ts_ns: now_ns(),
            ..Default::default()
        };
        env.labels
            .insert("name".to_string(), metadata.name().to_string());
        env.labels
            .insert("target".to_string(), metadata.target().to_string());
        // Stash open instant onto the span's extension so on_close can
        // compute latency.
        if let Some(span) = ctx.span(id) {
            let mut ext = span.extensions_mut();
            ext.insert(SpanOpenedAt(Instant::now()));
        }
        obs_core::observer().emit_envelope(env);
    }

    fn on_close(&self, id: tracing_core::span::Id, ctx: Context<'_, S>) {
        if self.span_event_mode == SpanEventMode::Suppressed {
            return;
        }
        let Some(span) = ctx.span(&id) else { return };
        let metadata = span.metadata();
        let opened_at = span
            .extensions()
            .get::<SpanOpenedAt>()
            .map(|s| s.0)
            .unwrap_or_else(Instant::now);
        let mut env = ObsEnvelope {
            full_name: "obs.v1.ObsSpanCompleted".to_string(),
            tier: ::buffa::EnumValue::Known(obs_proto::obs::v1::Tier::TIER_LOG),
            sev: ::buffa::EnumValue::Known(obs_proto::obs::v1::Severity::SEVERITY_DEBUG),
            ts_ns: now_ns(),
            ..Default::default()
        };
        env.labels
            .insert("name".to_string(), metadata.name().to_string());
        env.labels
            .insert("target".to_string(), metadata.target().to_string());
        let latency_ns = opened_at.elapsed().as_nanos() as u64;
        env.labels
            .insert("latency_ns".to_string(), latency_ns.to_string());
        obs_core::observer().emit_envelope(env);
    }
}

struct SpanOpenedAt(Instant);

#[allow(dead_code)]
fn _ensure_mutex_compiles() -> Mutex<()> {
    Mutex::new(())
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
        // route is not promoted; lands under attr.route by default.
        assert!(env.labels.contains_key("attr.route") || env.labels.contains_key("route"));
        let _ = Level::INFO;
    }
}
