//! `StandardObserver` — the production-ready observer.
//!
//! Phase-1 surface (task 1.8): single-tier wired (no per-tier mpsc
//! workers yet); `ArcSwap<EventsConfig>` reload hook; bumps generation
//! on `reload_filter()`; `SinkRouter` chooses one sink per envelope
//! based on tier matching.
//!
//! Per-tier mpsc workers + AUDIT spool + flush/shutdown lifecycle land
//! in Phase 3 task 3.1 / 3.12.

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use arc_swap::ArcSwap;
use obs_proto::obs::v1::ObsEnvelope;
use obs_types::Tier;

use super::Observer;
use crate::{
    callsite::ObsCallsite,
    config::EventsConfig,
    registry::{SchemaRegistry, ScrubbedEnvelope},
    sink::{NoopSink, Sink, StdoutSink},
};

/// Tier-matching dispatcher. Phase-1 supports per-tier override and a
/// fallback sink. The `SeverityMatcher` row from spec 11 § 4 lands with
/// the worker pool in Phase 3.
#[derive(Default)]
struct SinkRouter {
    log: Option<Arc<dyn Sink>>,
    metric: Option<Arc<dyn Sink>>,
    trace: Option<Arc<dyn Sink>>,
    audit: Option<Arc<dyn Sink>>,
    fallback: Option<Arc<dyn Sink>>,
}

impl std::fmt::Debug for SinkRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkRouter")
            .field("log", &self.log.as_ref().map(|_| "..."))
            .field("metric", &self.metric.as_ref().map(|_| "..."))
            .field("trace", &self.trace.as_ref().map(|_| "..."))
            .field("audit", &self.audit.as_ref().map(|_| "..."))
            .field("fallback", &self.fallback.as_ref().map(|_| "..."))
            .finish()
    }
}

impl SinkRouter {
    fn route(&self, tier: Tier) -> Option<&Arc<dyn Sink>> {
        let primary = match tier {
            Tier::Log => self.log.as_ref(),
            Tier::Metric => self.metric.as_ref(),
            Tier::Trace => self.trace.as_ref(),
            Tier::Audit => self.audit.as_ref(),
            // Tier is #[non_exhaustive] — defensively fall through.
            _ => None,
        };
        primary.or(self.fallback.as_ref())
    }
}

/// Production-ready observer with reloadable config and per-tier sink
/// dispatch.
pub struct StandardObserver {
    router: SinkRouter,
    registry: Arc<SchemaRegistry>,
    config: ArcSwap<EventsConfig>,
    generation: AtomicU32,
    service: String,
    instance: String,
    version: String,
}

impl std::fmt::Debug for StandardObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardObserver")
            .field("schemas", &self.registry.len())
            .field("service", &self.service)
            .field("instance", &self.instance)
            .field("version", &self.version)
            .field("generation", &self.generation.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl StandardObserver {
    /// Builder entry. See [`StandardObserverBuilder`].
    #[must_use]
    pub fn builder() -> StandardObserverBuilder {
        StandardObserverBuilder::default()
    }

    /// Convenience for dev: a `StandardObserver` with a default
    /// `StdoutSink(FormatterStyle::Full)` as the fallback.
    ///
    /// # Errors
    ///
    /// Returns `BuildError` if config validation fails (it cannot
    /// fail with default config; this is a forward-compatibility
    /// hook).
    pub fn dev() -> Result<Self, BuildError> {
        Self::builder()
            .service("dev", env!("CARGO_PKG_VERSION"))
            .sink_fallback(Arc::new(StdoutSink::default()))
            .build()
    }

    /// Read-only access to the registry (used by sinks).
    #[must_use]
    pub fn registry(&self) -> Arc<SchemaRegistry> {
        Arc::clone(&self.registry)
    }

    /// Read-only access to the live config.
    #[must_use]
    pub fn config(&self) -> arc_swap::Guard<Arc<EventsConfig>> {
        self.config.load()
    }

    /// Atomically swap the config and bump the generation so all
    /// callsites re-probe. Spec 11 § 3.2.
    pub fn reload_config(&self, new_config: EventsConfig) {
        self.config.store(Arc::new(new_config));
        self.generation.fetch_add(1, Ordering::Release);
    }

    fn fill_identity(&self, env: &mut ObsEnvelope) {
        if env.service.is_empty() {
            env.service.clone_from(&self.service);
        }
        if env.instance.is_empty() {
            env.instance.clone_from(&self.instance);
        }
        if env.version.is_empty() {
            env.version.clone_from(&self.version);
        }
    }

    fn dispatch(&self, env: &ObsEnvelope, tier: Tier) {
        let Some(sink) = self.router.route(tier) else {
            // No sink wired and no fallback — silently drop. The
            // user added this observer expecting sinks; we'll surface
            // this with a runtime warning in Phase 3.
            return;
        };
        let scrubbed = ScrubbedEnvelope::pass_through(env, &self.registry);
        sink.deliver(scrubbed);
    }
}

impl Observer for StandardObserver {
    fn emit_envelope(&self, mut env: ObsEnvelope) {
        self.fill_identity(&mut env);
        let tier = match env.tier {
            buffa::EnumValue::Known(t) => proto_tier_to_native(t),
            buffa::EnumValue::Unknown(_) => Tier::Unspecified,
        };
        self.dispatch(&env, tier);
    }

    fn enabled(&self, callsite: &ObsCallsite) -> bool {
        // Phase-1: severity floor only. Filter DSL lands in spec 13 §
        // 7 / Phase 3 task 3.6.
        let cfg = self.config.load();
        callsite.default_sev() >= cfg.sampling.always_log_at_or_above
            || cfg.sampling.default_rate > 0.0
    }

    fn generation(&self) -> u32 {
        self.generation.load(Ordering::Acquire)
    }

    fn reload_filter(&self) {
        self.generation.fetch_add(1, Ordering::Release);
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

/// Builder for [`StandardObserver`].
#[derive(Default)]
pub struct StandardObserverBuilder {
    router: SinkRouter,
    registry: Option<Arc<SchemaRegistry>>,
    config: Option<EventsConfig>,
    service: Option<String>,
    instance: Option<String>,
    version: Option<String>,
}

impl std::fmt::Debug for StandardObserverBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandardObserverBuilder")
            .field("service", &self.service)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

impl StandardObserverBuilder {
    /// Set service identity.
    #[must_use]
    pub fn service(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.service = Some(name.into());
        self.version = Some(version.into());
        self
    }

    /// Set instance id (e.g. `hostname` or pod name).
    #[must_use]
    pub fn instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    /// Wire a sink for a specific tier. Calling this twice for the
    /// same tier replaces the prior sink.
    #[must_use]
    pub fn sink_for(mut self, tier: Tier, sink: Arc<dyn Sink>) -> Self {
        match tier {
            Tier::Log => self.router.log = Some(sink),
            Tier::Metric => self.router.metric = Some(sink),
            Tier::Trace => self.router.trace = Some(sink),
            Tier::Audit => self.router.audit = Some(sink),
            _ => {} // Unspecified or future variants
        }
        self
    }

    /// Wire a fallback sink used when no tier-specific sink matches.
    #[must_use]
    pub fn sink_fallback(mut self, sink: Arc<dyn Sink>) -> Self {
        self.router.fallback = Some(sink);
        self
    }

    /// Set an explicit config. If absent, the observer is built with
    /// `EventsConfig::default()`.
    #[must_use]
    pub fn config(mut self, cfg: EventsConfig) -> Self {
        self.config = Some(cfg);
        self
    }

    /// Use a specific schema registry. If absent, the observer walks
    /// `EVENT_SCHEMAS` at build time.
    #[must_use]
    pub fn registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Finalise. Validates the config; falls back to `Noop` for
    /// unsourced tier fields.
    ///
    /// # Errors
    ///
    /// Returns `BuildError::InvalidConfig` if `EventsConfig::validate`
    /// rejects the supplied config.
    pub fn build(self) -> Result<StandardObserver, BuildError> {
        let cfg = self.config.unwrap_or_default();
        cfg.validate().map_err(BuildError::InvalidConfig)?;

        let registry = self
            .registry
            .unwrap_or_else(|| Arc::new(SchemaRegistry::from_link_section()));

        let router = self.router;

        // Service defaults from env.
        let service = self
            .service
            .or_else(|| std::env::var("OTEL_SERVICE_NAME").ok())
            .unwrap_or_else(|| "obs".to_string());
        let version = self
            .version
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        let instance = self.instance.unwrap_or_default();

        Ok(StandardObserver {
            router,
            registry,
            config: ArcSwap::from_pointee(cfg),
            generation: AtomicU32::new(1),
            service,
            instance,
            version,
        })
    }
}

/// Errors returned by [`StandardObserverBuilder::build`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// `EventsConfig::validate` returned an error.
    #[error("invalid config: {0}")]
    InvalidConfig(#[from] crate::config::ConfigError),
}

// Defensive: silence the "unused" warnings on `NoopSink` import the
// module currently does not use directly. The fallback default
// could be `NoopSink` in a future revision.
#[allow(dead_code)]
fn _ensure_noop_compiles() {
    let _: Arc<dyn Sink> = Arc::new(NoopSink);
}
